import json
import textwrap
import prestodb

# from pyhive import presto
from threading import Lock
from .base import BaseDriver
from ..common.logging import log
from ..common.tools import *
from ..interface import *



PARTITIONING_COLUMN_NAME = 'verdictcol'

RANDOMIZED_KEY_ALIAS = 'verdict_rand_key'


class PrestoDriver(BaseDriver):

    def __init__(self, host='localhost', sample_catalog='hive', sample_schema='verdict', 
                query_concurrency=10):
        assert_type(host, str)
        port = 8080
        if ':' in host:
            tokens = host.split(':')
            assert len(tokens) == 2
            host = tokens[0]
            port = tokens[1]
        self._host = host
        self._port = port
        self._sample_catalog = sample_catalog
        self._sample_schema = sample_schema
        # self._default_catalog = default_catalog
        # self._default_schema = default_schema
        self._query_concurrency = query_concurrency
        self._gen_cursor_pool(query_concurrency)
        self.id = 'presto'


    # HELPERS
    def copy_table(self, dest, source):
        result, desc = self._sql_with_meta(
            f"""CREATE TABLE IF NOT EXISTS {dest} AS SELECT * FROM {source}""")
        inserted_rows_count = self.row_count(dest)
        log(f"{inserted_rows_count} rows have been copied to {dest}.", 'debug')
        return inserted_rows_count

    def drop_all_tables(self, catalog, schema):
        result, desc = self._sql_with_meta(f"SHOW TABLES IN {catalog}.{schema}")
        table_names = [r[0] for r in result]
        for t in table_names:
            full_table_name = f"{catalog}.{schema}.{t}"
            self.drop_table(full_table_name)
            log(f"Dropped {full_table_name}.", 'info')

    def drop_table(self, table_name, exists=False):
        """
        @param table_name  A full name of the table to drop
        @param exists  If true, include 'if exists'
        """
        if exists:
            self._sql_with_meta(f"DROP TABLE IF EXISTS {table_name}")
        else:
            self._sql_with_meta(f"DROP TABLE {table_name}")

    def create_table(self, table_name, data, col_def):
        # create the table
        col_def_expr = ", ".join([col[0] + " " + col[1] for col in col_def])
        sql = f"CREATE TABLE {table_name} ({col_def_expr})"
        self._sql_with_meta(sql)

        # insert the values
        assert len(data) > 0
        col_types = [col[1].lower() for col in col_def]
        values_expr = []
        for row in data:
            row_quoted = []
            for i, value in enumerate(row):
                if col_types[i].startswith("varchar"):
                    row_quoted.append(repr(value))
                elif col_types[i] == "date":
                    row_quoted.append(f"cast('{value}' as date)")
                else:
                    row_quoted.append(f"cast({value} as {col_types[i]})")
            values_expr.append("(" + ", ".join(row_quoted) + ")")
        values_expr = ", ".join(values_expr)
        sql = f"INSERT INTO {table_name} VALUES {values_expr}"
        result, dsec = self._sql_with_meta(sql)
        return result[0][0]


    # SAMPLING METHODS
    def get_cache(self, sample_id, sampling_predicate):
        """Retrieves the raw data
        @param sample_id  The unique ID for a sample table
        @param sampling_predicate  { part_id (int): ratio }
        @return
            1. cache_meta: {
                'sampling_ratio': double,
                'part_col_value': int,
                }
            2. cache_data_col: (data, col_def)
                where data := [tuple, ...]
                      col_def := [(name, type), ...]
        """
        sample_table = self._sample_table_name(sample_id)
        part_col = PARTITIONING_COLUMN_NAME

        cache_part_value = max(sampling_predicate.keys())
        cache_ratio = sampling_predicate[cache_part_value]

        cache_predicate = f"{part_col} = {cache_part_value}"
        sql = f"SELECT * FROM {sample_table} WHERE {cache_predicate}"
        result, desc = self._sql_with_meta(sql)
        # cache_df = pandas_df_from_result(result, desc)

        cache_meta = {
            "sampling_ratio": cache_ratio,
            "part_col_value": cache_part_value,
        }

        return cache_meta, result, desc


    def create_sample(self, table_name, key_col, sampling_predicate, output_sql_only):
        """
        @param table_name  The source table name
        @param key_col  A column name in table_name
        @param sampling_predicate  { part_id (int): ratio }
        @param output_sql_only  If true, simply outputs sql query and terminates
        @return
            {
                'sample_id': str,
                'table_name': str,
                'key_col': '_rowid',
                'part_col': string,
                'partitions': [
                    { 'col_value': int, 'sampling_ratio': float }
                ]
            }
        """
        sid = self._gen_sample_id(key_col)
        column_names = [c[0] for c in self.columns(table_name)]
        target_table = self._sample_table_name(sid)
        sql = self._gen_presto_sampling_query(table_name, target_table, key_col, 
                                              column_names, sampling_predicate)
        if output_sql_only:
            print(sql)
            return None

        try:
            result, desc = self._sql_with_meta(sql)
            new_row_count = result[0][0]
            if new_row_count == 0:
                raise ValueError(
                    f"No rows have been inserted into the sample."
                    f"The source table ({table_name}) seems to be empty.")

            part_col = PARTITIONING_COLUMN_NAME
            return {
                "sample_id": sid,
                "source_name": table_name,
                "table_name": target_table,
                "key_col": key_col,
                "part_col": part_col,
                "partitions": [
                    { "col_value": p[0], "sampling_ratio": p[1]} for p in sampling_predicate.items()
                ]
            }

        except Exception:
            log("Error occurred. We drop sample tables that may have been created.")
            self.drop_table(target_table, exists=True)
            raise

    def _sample_table_name(self, sample_id):
        return f"{self._sample_catalog}.{self._sample_schema}.{sample_id}"

    def _gen_sample_id(self, key_col):
        return 's' + uuid.uuid4().hex + key_col

    def _gen_presto_sampling_query(self, source_table, target_table, key_col, 
                                   source_columns, sampling_predicate):
        sampling_ratios = [(s[0], s[1]) for s in sampling_predicate.items()]
        partitioning_expr = self._gen_partitioning_expr(RANDOMIZED_KEY_ALIAS, sampling_ratios)
        rand_key = self._gen_rand_func(key_col)
        col_names = ", ".join(source_columns)

        sql = textwrap.dedent(f'''\
            CREATE TABLE {target_table}
            WITH (
                partitioned_by = ARRAY['{PARTITIONING_COLUMN_NAME}'],
                format='PARQUET'
            ) AS
            SELECT *
            FROM (
                SELECT {col_names}, {partitioning_expr} {PARTITIONING_COLUMN_NAME}
                FROM (
                    SELECT *, {rand_key} {RANDOMIZED_KEY_ALIAS}
                    FROM {source_table}
                ) t1
            ) t2
            WHERE {PARTITIONING_COLUMN_NAME} > 0''')
        return sql

    def _gen_partitioning_expr(self, rand_colname, sampling_ratios):
        """
        Generates a SQL expression that returns an integer value (for each row), which is used for
        locating the row's partition. For example, given sampling_ratios = [0.1, 0.4, 0.3, 0.2], the
        generated  expression (by this function) will return 1 for 10% of data, 2 for 40% of data, 3 for
        30% of data, 4 for 20% of data.

        Specifically, this function will generate a case-when expression, that looks as follows:

            CASE WHEN verdict_rand_key < 0.5 THEN 1
                 WHEN verdict_rand_key < 0.75 THEN 2
                 WHEN verdict_rand_key < 0.875 THEN 3
                 WHEN verdict_rand_key < 1.0 THEN 4
            ELSE 0 END

        The rows assigned to 0 are expected to be discarded.

        @param rand_colname
            The name of the column that will include double values between 0 and 1. This will be
            'verdict_rand_key'.
        @param sampling_ratios
            This is a list of (col_value, ratio), where the sum of the ratios will be less than one.
            Each (double) value indicates the sampling ratio of a partition. If the sum of the
            values is equal to 1, all original data will be copied to a sample. Otherwise, a part of
            the original table will be copied.
        @return
            The generated SQL expression
        """
        expr = 'CASE'
        cumul_ratio = 0.0;
        for i, r in sampling_ratios:
            cumul_ratio += r
            expr += f' WHEN {rand_colname} < {cumul_ratio}'
            expr += f' THEN {i}'
        expr += ' ELSE 0 END'
        return expr

    def _gen_rand_func(self, key_col='_rowid'):
        """
        Generates a hash expression that converts an attribute value to a double between 0 and 1

        If the key_col == '_rowid' (which is the default), this function simply returns a rand function.
        """
        if key_col == '_rowid':
            return 'rand()'
        precision = int(1e12)
        expr = (
            f'(from_base(substr(to_hex(xxhash64(to_utf8(cast({key_col} as varchar)))), 1, 12), 16)'
            f' % {precision}) / cast({precision} as double)' )
        return expr

    def drop_sample(self, sample_id):
        sample_table = self._sample_table_name(sample_id)
        sql = f"DROP TABLE IF EXISTS {sample_table}"
        self._sql_with_meta(sql)


    # TABLE META INFO
    def row_count(self, name):
        sql = f'SELECT count(*) FROM {name}'
        result, desc = self._sql_with_meta(sql)
        return result[0][0]

    def columns(self, name):
        """
        @param name  A fully quantified name for a data source
        @return  A list of (attr name, attr type)
        """
        sql = f'describe {name}'
        result, desc = self._sql_with_meta(sql)
        columns = [(r[0], r[1]) for r in result]
        return columns

    def _name_for_sample(self, sample_id):
        return f'{self._sample_catalog}.{self._sample_schema}.{sample_id}'

    def retrieve_data(self, name):
        """
        @return  (data, desc), where
            data = [(row values), (row values), ... ]
            desc = [(col_name, type), ... ]
        """
        return self._sql_with_meta(f'SELECT * from {name}')

    def execute(self, query):
        """
        @param query  A query in the verdict query format
        @return  A result in json string format
        """
        assert_type(query, dict)
        log(f'Presto received a query: {query}')

        query_obj = from_verdict_query(query)
        assert_type(query_obj, DerivedTable)

        query_to_run = self._sample_to_base(query_obj)
        sql = self._to_sql(query_to_run)
        return self.sql(sql)

    # def _execute(self, element):
    #     element = self._sample_to_base(element)
    #     sql = self._to_sql(element)
    #     log(f'Converted Presto query: {sql}')
    #     return self.sql(sql)
        # result, desc = self._sql_with_meta(sql)
        # df = pd.DataFrame(result, columns=[c[0] for c in desc])
        # return df

    def sql(self, sql):
        result, desc = self._sql_with_meta(sql)
        df = pd.DataFrame(result, columns=[c[0] for c in desc])
        return df        

    def _sample_to_base(self, element):
        def replace(o):
            if o is None:
                return o
            return self._sample_to_base(o)

        if isinstance(element, SampleTable):
            base_table = BaseTable(self._name_for_sample(element.name()))
            part_col = PARTITIONING_COLUMN_NAME
            part_col_values = element.part_col_values()
            if len(part_col_values) > 0:
                predicate = None
                for val in part_col_values:
                    if predicate is None:
                        predicate = base_table.attr(part_col).eq(val)
                    else:
                        predicate = predicate.lor(base_table.attr(part_col).eq(val))
                base_table = base_table.filter(predicate)
            return base_table

        elif isinstance(element, DerivedTable):
            old_source = element.source()
            new_source = replace(old_source)
            element.set_source(new_source)
            element.set_relop_args([replace(a) for a in element.relop_args()])
            return element      

        else:
            return element

    def _to_sql(self, element):
        """Composes a complete SQL expression"""
        def se(element):
            return self._sql_expr(element)

        sql = ''
        if element.is_project():
            sql += se(element)
        elif element.is_select():
            sql += 'SELECT * ' + se(element)
        elif element.is_join():
            sql += 'SELECT * FROM ' + se(element)
        elif element.is_groupby():
            raise ValueError('groupby() must be followed by agg().')
        elif element.is_agg():
            sql += se(element)
        elif element.is_orderby():
            sql += se(element)
        elif element.is_limit():
            sql += se(element)
        else:
            raise ValueError
        return sql

    def _sql_expr(self, element):
        """Converts the query to Presto SQL.
        """
        def se(element):
            return self._sql_expr(element)

        if isinstance(element, Constant):
            value = element.value()
            if element.type_hint == 'date':
                return f"date '{value}'"
            elif element.type_hint == 'timestamp':
                return f"timestamp '{value}'"
            elif isinstance(value, str):
                return f"'{value}'"
            elif isinstance(value, int) or isinstance(value, float):
                return str(value)
            else:
                raise ValueError(f'Unexpected value: {value} of type {type(value)}')

        elif isinstance(element, BaseAttr):
            return element.name()

        elif isinstance(element, AggFunc):
            if len(element.args()) == 0:
                return f'{element.op()}()'
            else:
                processed_args = [se(arg) for arg in element.args()]
                args = ", ".join(processed_args)
                return f'{element.op()}({args})'

        elif isinstance(element, AttrOp):
            processed_args = [se(arg) for arg in element._args]
            op_name = element.op()

            if op_name == 'eq':
                return '(' + processed_args[0] + ' = ' + processed_args[1] + ')'
            elif op_name == 'ne':
                return '(' + processed_args[0] + ' <> ' + processed_args[1] + ')'
            elif op_name == 'gt':
                return '(' + processed_args[0] + ' > ' + processed_args[1] + ')'
            elif op_name == 'geq':
                return '(' + processed_args[0] + ' >= ' + processed_args[1] + ')'
            elif op_name == 'lt':
                return '(' + processed_args[0] + ' < ' + processed_args[1] + ')'
            elif op_name == 'leq':
                return '(' + processed_args[0] + ' <= ' + processed_args[1] + ')'
            elif op_name == 'add':
                return '(' + processed_args[0] + ' + ' + processed_args[1] + ')'
            elif op_name == 'sub':
                return '(' + processed_args[0] + ' - ' + processed_args[1] + ')'
            elif op_name == 'mul':
                return '(' + processed_args[0] + ' * ' + processed_args[1] + ')'
            elif op_name == 'div':
                return '(' + processed_args[0] + ' / ' + processed_args[1] + ')'
            elif op_name == 'and':
                return '(' + processed_args[0] + ' AND ' + processed_args[1] + ')'
            elif op_name == 'or':
                return '(' + processed_args[0] + ' OR ' + processed_args[1] + ')'
            elif op_name == 'substr':
                attr = processed_args[0]
                start = processed_args[1]
                length = processed_args[2]
                return f'substr({attr}, {start}, {length})'
            elif op_name == 'to_str':
                attr = processed_args[0]
                return f'cast({attr} as varchar)'
            elif op_name == 'concat':
                left = processed_args[0]
                right = processed_args[1]
                return f'concat({left}, {right})'
            elif op_name == 'length':
                attr = processed_args[0]
                return f'length({attr})'
            elif op_name == 'replace':
                attr = processed_args[0]
                pattern = processed_args[1]
                replace = processed_args[2]
                return f'replace({attr}, {pattern}, {replace})'
            elif op_name == 'upper':
                attr = processed_args[0]
                return f'upper({attr})'
            elif op_name == 'lower':
                attr = processed_args[0]
                return f'lower({attr})'
            elif op_name == 'startswith':
                attr = processed_args[0]
                pattern = processed_args[1]
                pattern = "'%" + pattern[1:]
                return f"({attr} LIKE {pattern})"
            elif op_name == 'contains':
                attr = processed_args[0]
                pattern = processed_args[1]
                pattern = "'%" + pattern[1:-1] + "%'"
                return f"({attr} LIKE {pattern})"
            elif op_name == 'endswith':
                attr = processed_args[0]
                pattern = processed_args[1]
                pattern = pattern[:-1] + "%'"
                return f"({attr} LIKE {pattern})"
            elif op_name == 'floor':
                attr = processed_args[0]
                return f'floor({attr})'
            elif op_name == 'ceil':
                attr = processed_args[0]
                return f'ceil({attr})'
            elif op_name == 'round':
                attr = processed_args[0]
                return f'round({attr})'
            elif op_name == 'year':
                attr = processed_args[0]
                return f'year({attr})'
            elif op_name == 'month':
                attr = processed_args[0]
                return f'month({attr})'
            elif op_name == 'day':
                attr = processed_args[0]
                return f'day({attr})'
            elif op_name == 'casewhen':
                assert len(processed_args)%2 == 1
                sql = '(CASE WHEN'
                predicate_count = int((len(processed_args) - 1) / 2)
                for i in range(predicate_count):
                    predicate = processed_args[i*2]
                    value = processed_args[i*2+1]
                    sql += ' ' + predicate + ' THEN '
                    sql += value
                sql += ' ELSE ' + processed_args[-1] + ' END)'
                return sql
            else:
                raise ValueError(f'Unknown attriute operation: {op_name}')

        elif isinstance(element, BaseTable):
            return f'{element.name()} {element.uid}'

        elif isinstance(element, DerivedTable):
            sql = ''

            if element.is_project():
                sql += 'SELECT ' + ', '.join([f'{se(attr_alias[0])} AS {attr_alias[1]}'
                                                for attr_alias in element.relop_args()])
                if isinstance(element._source, BaseTable):
                    sql += ' FROM ' + se(element._source)
                elif element._source.is_project():
                    sql += ' FROM (' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_select():
                    sql += ' ' + se(element._source)
                elif element._source.is_join():
                    sql += ' FROM ' + se(element._source)
                elif element._source.is_groupby():
                    sql += ' ' + se(element._source)
                elif element._source.is_agg():
                    sql += ' FROM (' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_orderby():
                    raise ValueError('project() cannot appear after orderby().')
                elif element._source.is_limit():
                    raise ValueError('project() cannot appear after limit().')
                else:
                    raise ValueError(self._relop_name)

            elif element.is_select():
                if isinstance(element._source, BaseTable):
                    # pdb.set_trace()
                    sql += 'FROM ' + se(element._source)
                    sql += ' WHERE ' + se(element.relop_args()[0])
                elif element._source.is_project():
                    sql += 'FROM (' + self._to_sql(element._source) + ') ' + element._source.uid
                    sql += ' WHERE ' + se(element.relop_args()[0])
                elif element._source.is_select():
                    # pdb.set_trace()
                    sql += se(element._source)
                    sql += ' AND ' + se(element.relop_args()[0])
                elif element._source.is_join():
                    sql += 'FROM ' + se(element._source)
                    sql += ' WHERE ' + se(element.relop_args()[0])
                elif element._source.is_groupby():
                    raise ValueError('groupby() must be followed by agg().')
                elif element._source.is_agg():
                    sql += 'FROM (' + self._to_sql(element._source) + ') ' + element._source.uid
                    sql += ' WHERE ' + se(element.relop_args()[0])
                elif element._source.is_orderby():
                    raise ValueError('select() or filter() cannot appear after orderby().')
                elif element._source.is_limit():
                    raise ValueError('select() or filter() cannot appear after limit().')
                else:
                    raise ValueError(self._relop_name)

            elif element.is_join():
                join_type = element.join_type()
                join_expr = {
                    'inner': 'INNER JOIN',
                    'left': 'LEFT JOIN',
                    'right': 'RIGHT JOIN',
                    'cross': 'CROSS JOIN',
                    'outer': 'OUTER JOIN'
                }[join_type]

                left_join_tab_alias = element._source.uid
                if isinstance(element._source, BaseTable):
                    sql += se(element._source)
                elif element._source.is_project():
                    sql += '(' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_select():
                    sql += '(' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_join():
                    sql += se(element._source)
                elif element._source.is_groupby():
                    raise ValueError('join() cannot appear after groupby().')
                elif element._source.is_agg():
                    sql += '(' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_orderby():
                    raise ValueError('join() cannot appear after orderby().')
                elif element._source.is_limit():
                    raise ValueError('join() cannot appear after limit().')
                else:
                    raise ValueError(element._relop_name)
                left_join_key = left_join_tab_alias + '.' + element.left_join_col().name()

                right_join_table = element.right_join_table()
                right_col_name = element.right_join_col().name()
                if isinstance(right_join_table, BaseTable):
                    right_tab_alias = right_join_table.uid
                    sql += (' ' + join_expr + ' ' + se(right_join_table)
                            + ' ON ' + f'{left_join_key} = {right_tab_alias}.{right_col_name}')
                elif isinstance(right_join_table, DerivedTable):
                    right_tab_alias = right_join_table.uid
                    sql += (' ' + join_expr + ' (' + self._to_sql(right_join_table) + ') ' + right_join_table.uid
                            + ' ON ' + f'{left_join_key} = {right_tab_alias}.{right_col_name}')
                else:
                    raise ValueError(f'Unexpected right join table type: {type(right_join_table)}.')

            elif element.is_groupby():
                if isinstance(element._source, BaseTable):
                    sql += 'FROM ' + se(element._source)
                elif element._source.is_project():
                    sql += 'FROM (' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_select():
                    sql += se(element._source)
                elif element._source.is_join():
                    sql += 'FROM ' + se(element._source)
                elif element._source.is_groupby():
                    raise ValueError('groupby() must be followed by agg().')
                elif element._source.is_agg():
                    sql += 'FROM (' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_orderby():
                    raise ValueError('groupby() cannot appear after orderby().')
                elif element._source.is_limit():
                    raise ValueError('groupby() cannot appear after limit().')
                else:
                    raise ValueError(element._relop_name)
                sql += ' GROUP BY ' + ', '.join([a.sql_expr() for a in element._relop_args])

            elif element.is_agg():
                if isinstance(element._source, BaseTable):
                    sql += 'SELECT ' + ', '.join(
                        [f'{se(aa[0])} AS {aa[1]}' for aa in element.relop_args()])
                    sql += ' FROM ' + se(element._source)
                elif element._source.is_project():
                    sql += 'SELECT ' + ', '.join(
                        [f'{se(aa[0])} AS {aa[1]}' for aa in element.relop_args()])
                    sql += ' FROM (' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_select():
                    sql += 'SELECT ' + ', '.join(
                        [f'{se(aa[0])} AS {aa[1]}' for aa in element.relop_args()])
                    sql += ' ' + se(element._source)
                elif element._source.is_join():
                    sql += 'SELECT ' + ', '.join(
                        [f'{se(aa[0])} AS {aa[1]}' for aa in element.relop_args()])
                    sql += ' FROM ' + se(element._source)
                elif element._source.is_groupby():
                    # append grouping columns
                    sql += 'SELECT '
                    sql += ', '.join([se(a) for a in element._source.relop_args()])
                    sql += ', ' + ', '.join(
                        [f'{se(aa[0])} AS {aa[1]}' for aa in element.relop_args()])
                    sql += ' ' + se(element._source)
                elif element._source.is_agg():
                    sql += 'SELECT ' + ', '.join(
                        [f'{se(aa[0])} AS {aa[1]}' for aa in element.relop_args()])
                    sql += ' FROM (' + self._to_sql(element._source) + ') ' + element._source.uid
                elif element._source.is_orderby():
                    raise ValueError('agg() cannot appear after orderby().')
                elif element._source.is_limit():
                    raise ValueError('agg() cannot appear after limit().')
                else:
                    raise ValueError(self._relop_name)

            elif element.is_orderby():
                if isinstance(element._source, BaseTable):
                    sql += 'SELECT * FROM ' + se(element._source)
                elif element._source.is_project():
                    sql += se(element._source)
                elif element._source.is_select():
                    sql += 'SELECT * FROM ' + se(element._source)
                elif element._source.is_join():
                    sql += 'SELECT * FROM ' + se(element._source)
                elif element._source.is_groupby():
                    raise ValueError('groupby() must be followed by agg().')
                elif element._source.is_agg():
                    sql += se(element._source)
                elif element._source.is_orderby():
                    raise ValueError('orderby() cannot be followed by another orderby().')
                elif element._source.is_limit():
                    raise ValueError('orderby() cannot appear after limit().')
                else:
                    raise ValueError(element._relop_name)
                sql += ' ORDER BY '
                sql += ", ".join([by + ' ' + asc for by, asc in element._relop_args])

            elif element.is_limit():
                if isinstance(element._source, BaseTable):
                    sql += 'SELECT * FROM ' + se(element._source)
                elif element._source.is_project():
                    sql += se(element._source)
                elif element._source.is_select():
                    sql += 'SELECT * FROM ' + se(element._source)
                elif element._source.is_join():
                    sql += 'SELECT * FROM ' + se(element._source)
                elif element._source.is_groupby():
                    raise ValueError('groupby() must be followed by agg().')
                elif element._source.is_agg():
                    sql += se(element._source)
                elif element._source.is_orderby():
                    sql += se(element._source)
                elif element._source.is_limit():
                    raise ValueError('limit() is not expected to be followed by another limit()')
                else:
                    raise ValueError(element._source._relop_name)
                sql += ' LIMIT ' + str(se(element.relop_args()[0]))

            else:
                raise ValueError(element._relop_name)

            return sql

        else:
            raise ValueError(element)

    def _sql(self, sql):
        result, desc = self._sql_with_meta(sql)
        return result

    def _sql_with_meta(self, sql):
        """
        @return (data, col_def)
            data := [tuple, ...]
            col_def := [(name, type), ...]
        """
        sql_for_log = (sql[:1000] + "...") if len(sql) > 1002 else sql
        log(f"sending a query to the backend Presto: {sql_for_log}", 'DEBUG')
        cursor, cursor_lock = self._get_cursor()
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            desc = [(d[0], d[1]) for d in cursor.description]
            return result, desc
        finally:
            cursor_lock.release()

    def _get_cursor(self):
        """Returns a cursor from the cursor pool. The cursor_lock is provided as acquired; thus,
        the lock must be released after obtaining results from the cursor.
        """
        with self._query_cursor_itr_lock:
            next_cursor = self._query_cursor_pool[self._query_cursor_itr]
            cursor_lock = self._query_cursor_lock_pool[self._query_cursor_itr]
            cursor_lock.acquire()
            self._query_cursor_itr = (self._query_cursor_itr + 1) % self._query_concurrency
            return next_cursor, cursor_lock

    def _gen_cursor_pool(self, num):
        self._query_cursor_itr = 0
        self._query_cursor_itr_lock = Lock()
        self._query_cursor_pool = [self._create_cursor() for i in range(num)]
        self._query_cursor_lock_pool = [Lock() for i in range(num)]

    def _create_cursor(self):
        conn = prestodb.dbapi.connect(
            host=self._host,
            port=self._port,
            user='verdict',
            catalog='hive',
            schema='default',
        )
        return conn.cursor()

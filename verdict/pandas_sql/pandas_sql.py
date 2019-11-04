"""Execute relational operations using Pandas
"""

import json
import numpy as np
import pandas as pd
import pickle
from verdict.core.relobj import *
from verdict.interface import from_verdict_query



def init_logger(log_dir=None):
    format_str = "%(asctime)s %(name)s %(levelname)s - %(message)s "
    logging.basicConfig(level=logging.CRITICAL, format=format_str)
    pandas_sql_logger = logging.getLogger("pandas_sql")
    pandas_sql_logger.setLevel(logging.DEBUG)

    if log_dir is not None and len(pandas_sql_logger.handlers) == 0:
        formatter = logging.Formatter(format_str)
        path = pathlib.Path(log_dir)
        path.mkdir(parents=True, exist_ok=True)
        log_filename = os.path.join(log_dir, '{:%Y-%m-%d}.log'.format(datetime.now()))
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(formatter)
        pandas_sql_logger.addHandler(file_handler)

    return pandas_sql_logger


class PandasSQL(object):

    def __init__(self, log_dir=None):
        self.id = 'pandas'
        self._tables = {}
        self._logger = init_logger(log_dir)

    def _log(self, msg):
        self._logger.debug(msg)

    def drop_all_tables(self):
        del self._tables
        self._tables = {}

    def row_count(self, name):
        raise NotImplementedError

    def columns(self, name):
        """
        @param name  A fully quantified name for a data source
        @return  A list of (attr name, attr type)
        """
        df = self._tables[name]
        return [(c, None) for c in df.columns]

    def create_table(self, name, data, col_def):
        col_names = [c[0] for c in col_def]
        col_types = [c[1] for c in col_def]
        intermediate = pd.DataFrame(data, columns=col_names)

        new_df = pd.DataFrame(index=intermediate.index)
        for i, colname in enumerate(col_names):
            if col_types[i] == "date":
                new_df[colname] = pd.to_datetime(intermediate[colname])
            else:
                new_df[colname] = intermediate[colname]
        self._tables[name] = new_df
        return len(new_df.index)

    @staticmethod
    def frame_from_data(data, col_def):
        col_names = [c[0] for c in col_def]
        col_types = [c[1] for c in col_def]
        intermediate = pd.DataFrame(data, columns=col_names)

        new_df = pd.DataFrame(index=intermediate.index)
        for i, colname in enumerate(col_names):
            if col_types[i] == "date":
                new_df[colname] = pd.to_datetime(intermediate[colname])
            else:
                new_df[colname] = intermediate[colname]
        return new_df

    def load_table(self, table_name, file_path, if_not_exists=False):
        if table_name in self._tables:
            if if_not_exists:
                pass
            else:
                # if_not_exists == False means that the caller was expecting no specified table
                # exists. 
                raise ValueError(f"The specified table, {table_name}, already exists.")

        else:
            with open(file_path, 'rb') as f:
                df = pickle.load(f)
                assert isinstance(df, pd.core.frame.DataFrame)
                self.register_table(table_name, df)
                self._log(f"The table, {table_name}, has been loaded.")
                
        return len(self._tables[table_name].index)

    def register_table(self, table_name, frame):
        if table_name in self._tables:
            raise ValueError(f"The table name ({table_name}) already exists.")
        self._tables[table_name] = frame

    def drop_table(self, name, if_exists=False):
        if name not in self._tables:
            if if_exists == False:
                raise ValueError(f"The specified table, {name}, does not exist.")
            else:
                pass
        else:
            del self._tables[name]

    def get_df(self, name):
        return self._tables[name]

    def execute(self, query):
        """
        @param query  A query in the verdict query format
        @return  A result in json string format
        """
        assert_type(query, dict)
        self._log(f'PandasDB received a query: {query}')
        query_obj = from_verdict_query(query)
        assert_type(query_obj, DerivedTable)

        self._attach_column_names(query_obj)
        query_obj = self._pushdown_project(query_obj)
        self._log(f"PandasDB's internal optimized query: {query_obj}")
        return self._execute(query_obj)

    def _attach_column_names(self, query_obj):
        base_tables = find_base_tables(query_obj, include_samples=True)
        for table in base_tables:
            column_names = [c[0] for c in self.columns(table.name())]
            table.set_column_names(column_names)

    def _pushdown_project(self, query_obj):
        return self._pushdown_project_inner(query_obj, [])

    def _pushdown_project_inner(self, query_obj, pushdown_list):
        """If there are project, aggregate, or select before join, we push down those columns below
        the join. This operation improves the join speed significantly.
        """
        def get_baseattr(attr):
            if isinstance(attr, Constant):
                return []
            elif isinstance(attr, BaseAttr):
                return [attr]
            elif isinstance(attr, AttrOp):
                return flatten([get_baseattr(a) for a in attr.args()])
            elif isinstance(attr, AggFunc):
                return flatten([get_baseattr(a) for a in attr.args()])
            elif isinstance(attr, (List, tuple)):
                # the args of project and agg are in the form of (attr, alias)
                return get_baseattr(attr[0])
            raise ValueError(attr)

        def with_alias(attr_list):
            return [(attr, attr.name()) for attr in attr_list]

        if query_obj.is_basetable():
            if len(pushdown_list) > 0:
                return query_obj.project(with_alias(pushdown_list))
            else:
                return query_obj

        elif query_obj.is_sampletable():
            if len(pushdown_list) > 0:
                return query_obj.project(with_alias(pushdown_list))
            else:
                return query_obj            

        elif query_obj.is_project():
            source_pushdown_list = flatten([get_baseattr(a) for a in query_obj.relop_args()])
            source_pushdown_list = list(set(source_pushdown_list))
            new_source = self._pushdown_project_inner(query_obj.source(), source_pushdown_list)
            query_obj.set_source(new_source)
            if len(pushdown_list) > 0:
                return query_obj.project(with_alias(pushdown_list))
            else:
                return query_obj

        elif query_obj.is_select():
            source_pushdown_list = flatten([get_baseattr(a) for a in query_obj.relop_args()])
            source_pushdown_list.extend(pushdown_list)
            source_pushdown_list = list(set(source_pushdown_list))
            new_source = self._pushdown_project_inner(query_obj.source(), source_pushdown_list)
            query_obj.set_source(new_source)
            return query_obj

        elif query_obj.is_agg():
            source_pushdown_list = flatten([get_baseattr(a) for a in query_obj.relop_args()])
            source_pushdown_list = list(set(source_pushdown_list))
            new_source = self._pushdown_project_inner(query_obj.source(), source_pushdown_list)
            query_obj.set_source(new_source)
            if len(pushdown_list) > 0:
                return query_obj.project(with_alias(pushdown_list))
            else:
                return query_obj

        elif query_obj.is_join():
            left_table = query_obj.source()
            right_table = query_obj.right_join_table()
            left_pushdown_list = []
            right_pushdown_list = []
            for attr in pushdown_list:
                if left_table.has_col(attr.name()):
                    left_pushdown_list.append(attr)
                elif right_table.has_col(attr.name()):
                    right_pushdown_list.append(attr)
                else:
                    raise ValueError(f"No join table includes this column: {attr}")
            left_pushdown_list.append(query_obj.left_join_col())
            right_pushdown_list.append(query_obj.right_join_col())
            left_pushdown_list = list(set(left_pushdown_list))
            right_pushdown_list = list(set(right_pushdown_list))

            new_source = self._pushdown_project_inner(left_table, left_pushdown_list)
            new_right_table = self._pushdown_project_inner(right_table, right_pushdown_list)
            query_obj.set_source(new_source)
            query_obj.set_right_join_table(new_right_table)
            return query_obj

        elif query_obj.is_groupby():
            source_pushdown_list = flatten([get_baseattr(a) for a in query_obj.relop_args()])
            source_pushdown_list.extend(pushdown_list)
            source_pushdown_list = list(set(source_pushdown_list))
            new_source = self._pushdown_project_inner(query_obj.source(), source_pushdown_list)
            query_obj.set_source(new_source)
            return query_obj

        else:
            raise ValueError(query_obj)

    def _execute(self, element, context_df=None):
        """
        @param context_df  Used for processing the elements that require their parent dataframe.
        """
        if isinstance(element, Constant):
            return element.value()
            # return pd.DataFrame(element.value(), index=context_df.index, columns=[0])[0]

        elif isinstance(element, BaseAttr):
            col_name = element.name()
            assert context_df is not None
            if isinstance(context_df, pd.core.groupby.DataFrameGroupBy):
                if col_name not in context_df.obj.columns:
                    msg = f'Tried to access {col_name} from {context_df.columns}'
                    self._log(msg)
                    raise ValueError(msg)
            elif isinstance(context_df, pd.core.frame.DataFrame):
                if col_name not in context_df.columns:
                    msg = f'Tried to access {col_name} from {context_df.columns}'
                    self._log(msg)
                    raise ValueError(msg)
            return context_df[col_name]

        elif isinstance(element, AggFunc):
            if element.op() == 'count':
                if isinstance(context_df, pd.core.frame.DataFrame):
                    # When no groupby() procedes
                    return pd.Series(len(context_df.index))
                elif isinstance(context_df, pd.core.groupby.generic.DataFrameGroupBy):
                    # When groupby() procedes
                    return context_df.size()
                else:
                    raise ValueError(context_df)

            # for other aggregates: sum and avg
            arg = self._execute(element.args()[0], context_df)
            if isinstance(arg, pd.core.series.Series):
                # When no groupby() procedes
                if element.op() == 'sum':
                    return pd.Series(arg.sum())
                elif element.op() == 'avg':
                    return pd.Series(arg.mean())
                else:
                    raise NotImplementedError(element.op())
            elif isinstance(arg, pd.core.groupby.generic.SeriesGroupBy):
                # When groupby() procedes
                if element.op() == 'sum':
                    return arg.sum()
                elif element.op() == 'avg':
                    return arg.mean()
                else:
                    raise NotImplementedError(element.op())
            else:
                raise ValueError(f'Unsupported argument type within agg: {type(arg)}')

        elif isinstance(element, AttrOp):
            processed_args = [self._execute(arg, context_df) for arg in element._args]
            op_name = element.op()

            if op_name == 'eq':
                return processed_args[0] == processed_args[1]
            elif op_name == 'gt':
                return processed_args[0] > processed_args[1]
            elif op_name == 'geq':
                return processed_args[0] >= processed_args[1]
            elif op_name == 'lt':
                return processed_args[0] < processed_args[1]
            elif op_name == 'leq':
                return processed_args[0] <= processed_args[1]
            elif op_name == 'add':
                return processed_args[0] + processed_args[1]
            elif op_name == 'sub':
                return processed_args[0] - processed_args[1]
            elif op_name == 'mul':
                return processed_args[0] * processed_args[1]
            elif op_name == 'div':
                return processed_args[0] / processed_args[1]
            elif op_name == 'and':
                return processed_args[0] & processed_args[1]
            elif op_name == 'or':
                return processed_args[0] | processed_args[1]
            elif op_name == 'ne':
                return processed_args[0].ne(processed_args[1])
            elif op_name == 'substr':
                attr = processed_args[0]
                start = processed_args[1]
                length = processed_args[2]
                assert_type(attr, (pd.core.series.Series))
                assert_type(start, int)
                assert_type(length, int)
                assert start > 0
                assert length > 0
                return attr.str.slice(start-1, start+length-1)
            elif op_name == 'to_str':
                df_attr = processed_args[0]
                return df_attr.dt.strftime('%Y-%m-%d')
                # attr_value = df_attr[0]
                # if isinstance(attr_value, int) or isinstance(attr_value, float):
                #     return df_attr.to_string()
                # elif isinstance(attr_value, datetime):
                #     if attr_value.strftime("%H:%M:%S") == '00:00:00':
                #         # no time is set -> assumes this is a date object
                #         return df_attr.apply(lambda a: a.strftime("%Y-%m-%d"))
                #     else:
                #         # we assume it's timestamp
                #         # TODO: Ideally, this conversion must use the original type definition
                #         return df_attr.apply(lambda a: strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
                # else:
                #     raise ValueError(f'{attr_value} of type ({type(attr_value)})')
            elif op_name == 'concat':
                left = processed_args[0]
                right = processed_args[1]
                return left.str.cat(right)
            elif op_name == 'length':
                attr = processed_args[0]
                return attr.str.len()
            elif op_name == 'replace':
                attr = processed_args[0]
                pattern = processed_args[1]
                replace = processed_args[2]
                return attr.str.replace(pattern, replace)
            elif op_name == 'upper':
                attr = processed_args[0]
                return attr.str.upper()
            elif op_name == 'lower':
                attr = processed_args[0]
                return attr.str.lower()
            elif op_name == 'startswith':
                attr = processed_args[0]
                pattern = processed_args[1]
                return attr.str.startswith(pattern)
            elif op_name == 'contains':
                attr = processed_args[0]
                pattern = processed_args[1]
                return attr.str.contains(pattern)
            elif op_name == 'endswith':
                attr = processed_args[0]
                pattern = processed_args[1]
                return attr.str.endswith(pattern)
            elif op_name == 'floor':
                attr = processed_args[0]
                return np.floor(attr)
            elif op_name == 'ceil':
                attr = processed_args[0]
                return np.ceil(attr)
            elif op_name == 'round':
                attr = processed_args[0]
                return attr.round()
            elif op_name == 'year':
                attr = processed_args[0]
                return attr.dt.year
            elif op_name == 'month':
                attr = processed_args[0]
                return attr.dt.month
            elif op_name == 'day':
                attr = processed_args[0]
                return attr.dt.day
            elif op_name == 'casewhen':
                args = [a.pandas_attr(df) for a in element._args]
                assert len(args)%2 == 1
                # We apply the conditions in the reverse order
                # then, this satisfies the natural interpretation of the if-else structure
                attr = pd.DataFrame(0, index=df.index, columns=[0])[0]
                attr[:] = args[-1]
                predicate_count = int((len(args)-1) / 2)
                for i in range(predicate_count-1, -1, -1):
                    # i = count-1, count-2, ..., 0
                    predicate = args[i*2]
                    values = args[i*2+1]
                    attr.loc[predicate] = values
                return attr
            else:
                raise ValueError(f'Unknown attriute operation: {op_name}')

        elif isinstance(element, BaseTable):
            table_name = element.name()
            if table_name not in self._tables:
                raise ValueError(f"Tried to access non-existing table {table_name}")
            return self._tables[table_name]

        elif isinstance(element, SampleTable):
            table_name = element.name()
            if table_name not in self._tables:
                raise ValueError(f"Tried to access non-existing table {table_name}")
            return self._tables[table_name]

        elif isinstance(element, DerivedTable):
            if element.is_project():
                source = self._execute(element.source())
                df = pd.concat([self._execute(attr_alias[0], source).to_frame() for attr_alias
                                    in element.relop_args()], axis=1)
                # df.columns = [f'{self.uid}.{alias}' for alias in element.relop_args()[0].keys()]
                df.columns = [attr_alias[1] for attr_alias in element.relop_args()]
                return df
            elif element.is_select():
                source = self._execute(element.source())
                assert_equal(len(element.relop_args()), 1)
                predicate = self._execute(element.relop_args()[0], source)
                return source[predicate]
                
            elif element.is_join():
                join_type = element.join_type()
                if join_type == 'cross':
                    COMMON_JOIN_KEY = '_dummy_join_key'
                    source = self._execute(element)
                    source[COMMON_JOIN_KEY] = 0
                    right_join_table = self._execute(self.right_join_table())
                    right_join_table[COMMON_JOIN_KEY] = 0
                    return pd.merge(left=source, right=right_join_table, how='outer',
                                    left_on=COMMON_JOIN_KEY, right_on=COMMON_JOIN_KEY)
                else:
                    # left_join_key = self.left_join_col().full_name()
                    left_join_key = element.left_join_col().name()
                    source = self._execute(element.source())
                    right_join_table = self._execute(element.right_join_table())
                    right_join_key = element.right_join_col().name()
                    return pd.merge(left=source, right=right_join_table, how=join_type, 
                                    left_on=left_join_key, right_on=right_join_key)

            elif element.is_groupby():
                source = self._execute(element.source())
                attr_names = []
                for attr in element.relop_args():
                    assert isinstance(attr, BaseAttr)
                    attr_names.append(attr.name())
                return source.groupby(attr_names)

            elif element.is_agg():
                source = self._execute(element.source())
                # attr.pandas_attr() is expected to return a Series object.
                # We convert them to DataFrame, then concatenate.
                df = pd.concat([self._execute(attr_alias[0], source).to_frame() for attr_alias 
                                    in element.relop_args()], axis=1)
                df.columns = [attr_alias[1] for attr_alias in element.relop_args()]
                if isinstance(element._source, DerivedTable) and element._source.is_groupby():
                    # TODO: make sure that the column names for the groups are with the correct alias.
                    return df.reset_index()
                else:
                    return df

            elif self.is_orderby():
                source = self._execute(element.source())
                by = [f'{element.source().uid}.{a[0]}' for a in element.relop_args()]
                ascending = [True if a[1] == 'ASC' else False for a in element.relop_args()]
                source.sort_values(by=by, ascending=ascending, inplace=True)
                return source

            elif self.is_limit():
                source = self._execute(element)
                return source.head(self._execute(element.relop_args()[0]), source)

            else:
                raise NotImplementedError(self._relop_name)

        else:
            raise ValueError(element)


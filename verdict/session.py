import json
import pandas as pd
import pathlib
import pdb
import pprint
import threading
import time
from .common.tools import *
from .core.cache import CacheManager
from .core.querying2 import Querying
from .core.relobj import *
from .core.sampling import *
from .driver.base import BaseDriver
from .interface import *
from .metadata import Metadata


class AutoFormat(object):

    def __init__(self, value):
        if value is None:
            self._value = {}
        else:
            self._value = value

    def get(self):
        return self._value

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return pprint.pformat(self._value, indent=4)


class VerdictSession(object):
    """
    :param engine:
        A driver for the backend engine.

    :param cache_engine:
        This is expected to be an instance of :class:`~verdict.core.cache.CacheManager`

    :param redis_host:
        The redis address Verdict uses for managing metadata.
    """

    def __init__(self, engine, cache_engine, redis_host='localhost'):
        assert_type(engine, BaseDriver)
        assert_type(cache_engine, CacheManager)
        self._engine = engine
        self._cache_engine = cache_engine
        self._meta = Metadata(redis_host, self._engine.id)
        self._qid_lock = threading.Lock()

        # If not, create the directory for verdict
        path = pathlib.Path(verdict_dir)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            log(f"A directory is created for verdict: {verdict_dir}")


    def info(self, *args):
        """
        :param args:
            The displayed information differs based on the type of the argument passed. 
            Specifically,

            1. None: 
                Then, the registered (or index) tables are shown. Only these tables can be used for 
                querying.
            2. table_name:
                Its column names/types and the created samples are shown. Verdict uses these samples
                automatically to speed up query processing.
            3. sample_name:
                The sample's metadata is shown. Only for developers.
                
        """
        m = self._meta

        if len(args) == 0:
            tables = sorted(m.get_all_tables())
            output = { "Registered Tables": tables }
        elif len(args) == 1:
            source_name = args[0]
            # First try to get table information
            table_info = m.get_table_meta(source_name)
            if table_info is not None:
                output = {
                    "Column Names and Types": table_info['columns'],
                    "Row Count": table_info['row_count'],
                    "Created Samples": table_info['samples']
                }
            else:
                sample_meta = m.get_sample_meta(source_name)
                if sample_meta is not None:
                    output = sample_meta
        else:
            raise ValueError(f"Unexpected input to info(): {args}")

        return AutoFormat(output)


    # QUERYING METHODS
    def sql(self, sql, rel_err_bound=0.05):
        """Processes a SQL query, and makes a prediction that satisfies the specified relative error.

        :param sql: A SQL query.

        :param rel_err_bound: 
            A requested relative error. For example, 0.01 means 1% relative error. Verdict
            guarantees that the actual error is small than this specified error with 95% 
            probability.

        :return: An answer as pandas.DataFrame. This answer is expected to satisfy the specified
                 error level with high probability.
        """
        start = time.time()

        assert_type(sql, str)
        sql = sql.strip()
        sql = ' '.join(sql.split())
        log(f'verdict.sql received a query: {sql}', 'debug')

        if len(sql) >= 7 and sql[:7].lower().startswith('bypass '):    # bypass
            sql = sql[7:]
            result = self.execute_bypass(sql)
        else:
            request = sql2verdict_query(sql)
            request["type"] = "single_agg"

            result = self.json(request, rel_err_bound)
        log(f"Returning an answer in {time.time() - start} sec(s).")
        return result


    def sql_stream(self, sql):
        """Progressively processes a SQL query.

        :param sql: A SQL query.

        :return: An iterator from which a series of pandas.DataFrame can be retrieved. For example

            .. code-block:: python

                for result in v.json_stream(query):
                    print(result)
        """
        start = time.time()

        assert_type(sql, str)
        sql = sql.strip()
        sql = ' '.join(sql.split())
        log(f'verdict.stream received a query: {sql}', 'debug')

        request = sql2verdict_query(sql)
        request["type"] = "stream_agg"
        itr = self.json_stream(request)

        log(f"Returning an iterator (for answers) in {time.time() - start} sec(s).")
        return itr


    def json(self, query_request, rel_err_bound=0.05):
        """Processes a query in the json format. Using this method eliminates the overhead of
        converting SQL to a json representation (performed by the sql() method).

        :param query_request: The querying request must be in the following forms:

            .. code-block:: json

                {
                    "type": "single_agg",
                    "source": relops,
                    "agg": {
                        "alias": agg_func, ...
                    },
                    "groupby": [alias, ...],
                    "orderby": ["alias asc", ...],
                    "limit": int,
                }

                In the above json representation, relops must the *verdict query*.

        :param rel_err_bound: 
            The relative error verdict will ensure. The actual error will be smaller than this
            specified error with 95% probability.

        :return: A pandas.DataFrame that satisfies the specified relative error.
        """
        assert_type(query_request, dict)
        assert_type(query_request["source"], (dict, str))
        log(f'verdict.json received a query: {query_request}', 'debug')

        # To make every query independent (thus, thread-safe)
        query_executor = Querying(self._engine, self._cache_engine)

        request_obj = self.request2query_obj(query_request)
        return query_executor.json(request_obj, rel_err_bound)


    def json_stream(self, query_request):
        """Processes the query in the stream mode. Using this method eliminates the overhead of
        converting SQL to a json representation (inside the sql_stream() method).

        :param query_request: The querying request must be in the following forms:

            .. code-block:: json

                {
                    "type": "stream_agg",
                    "source": relops,
                    "agg": {
                        "alias": agg_func, ...
                    },
                    "groupby": [alias, ...],
                    "orderby": ["alias asc", ...],
                    "limit": int
                }

                In the above json representation, relops must the *verdict query*.

        :return: An iterator from which a series of pandas.DataFrame can be retrieved. For example

            .. code-block:: python

                for result in v.json_stream(query):
                    print(result)
        """
        assert_type(query_request, dict)
        assert_type(query_request["source"], (dict, str))

        # To make every query independent (thus, thread-safe)
        query_executor = Querying(self._engine, self._cache_engine)
        request_obj = self.request2query_obj(query_request)
        # raise Exception
        return query_executor.json_stream(request_obj)


    def request2query_obj(self, request):
        # Convert the verdict query to relobj
        request_obj = {}
        for name, obj_str in request.items():
            if name in ['type', 'options', 'limit']:
                request_obj[name] = obj_str
            elif name == 'orderby':
                ordering = []
                for attr_sort in obj_str:
                    ordering.append((from_verdict_query(attr_sort[0]), attr_sort[1]))
                request_obj[name] = ordering
            else:
                request_obj[name] = from_verdict_query(obj_str)

        # Retrieve information summaries
        m = self._meta
        base_tables = find_base_tables(request_obj['source'])
        sample_info = {}
        for base_table in base_tables:
            source_name = base_table.name()
            table_info = m.get_table_meta(source_name)
            if table_info is None:
                raise ValueError(f"Not a registered table: {source_name}")

            # # attach column names
            # print(table_info)
            # column_names = list(table_info['columns'].keys())
            # base_table.set_column_names(column_names)

            # retrieve summary info
            sample_ids = table_info['samples']
            sample_info[source_name] = []
            for sample_id in sample_ids:
                info = m.get_sample_meta(sample_id)
                info['sample_id'] = sample_id
                sample_info[source_name].append(info)
        log(f'Sample information has been retrieved from redis.', 'debug')
        request_obj['sample_info'] = sample_info

        return request_obj


    def execute_bypass(self, sql):
        log(f'Bypassing the query to the engine: {sql}')
        return self._engine.sql(sql)


    # SAMPLING METHODS
    def create_sample(self, table_name, key_col='_rowid', output_sql_only=False, 
                      cache_size=int(5e5)):
        """Creates a sample of the designated source table using the key column.

        In more detail, this method performs the following additional operations:

        1. Stores the metadata of the source table in Redis: (1) columns, and (2) row count
        2. Computes the actual sampling ratios
        3. Stores the cache (i.e., the smallest sample for each)

        :param key_col:
            Either '_rowid' or a column name
        :param output_sql_only: If true, simply prints out sql expression and terminate.
        """
        meta = self._meta
        engine = self._engine
        cache_engine = self._cache_engine
        sampling_algorithm = UniformRandom()

        table_info = meta.get_table_meta(table_name)
        if table_info is None:
            self.create_table_meta(table_name)
            table_info = meta.get_table_meta(table_name)
        source_row_count = table_info['row_count']

        # Sends a sampling query
        # sampling_predicate = { part_value (int): ratio }
        sampling_predicate = sampling_algorithm.gen_sampling_ratios(source_row_count, cache_size)
        sample_meta = engine.create_sample(table_name, key_col, sampling_predicate, output_sql_only)
        if output_sql_only:
            return None

        sample_id = sample_meta["sample_id"]
        log(f"A sample table has been created (sample_id: {sample_id}).")

        try:
            # Store sample_meta
            # sample_meta = self.create_accurate_sample_meta(sample_meta)
            sample_meta['source_name'] = table_name
            meta.store_sample_meta(table_name, sample_id, sample_meta)
            log(f"Sample metadata stored (sample_id: {sample_id}).")

            # Cache the sample table
            cache_meta, cache_data, col_def = engine.get_cache(sample_id, sampling_predicate)
            cache_engine.store_cache_meta(sample_id, cache_meta)
            cache_engine.store_cache_data(sample_id, cache_data, col_def)
            return sample_id
        except Exception:
            log(f"Error(s) occurred. We remove the tables and metadata that may have been created.")
            engine.drop_sample(sample_id)
            cache_engine.drop_cache_meta(sample_id)
            cache_engine.drop_cache_data(sample_id)


    def drop_sample(self, sample_id):
        meta = self._meta
        engine = self._engine
        cache_engine = self._cache_engine
        sample_meta = meta.get_sample_meta(sample_id)
        if sample_meta is None:
            raise ValueError(f'The information for sample_id = {sample_id} is not found.')

        source_table_name = sample_meta['source_name']
        cache_engine.drop_cache_data(sample_id)
        cache_engine.drop_cache_meta(sample_id)
        engine.drop_sample(sample_id)
        meta.drop_sample_meta(source_table_name, sample_id)


    def _create_accurate_sample_meta(self, partial_sample_meta):
        """Stores the following structure in the meta store.
            {
                "sample_id": str,
                "source_name": str,
                "table_name": str,
                "key_col": '_rowid',
                "part_col": str,
                "row_count": int,       # this is a new field
                "partitions": [         # this is a new field
                    { "col_value": int, "sampling_ratio": float }, ...
                ]
            }

        @param partial_sample_meta
            {
                "sample_id": str,
                "source_name": str,
                "table_name": str,
                "key_col": '_rowid',
                "part_col": str
            }
        """
        engine = self._engine
        meta = self._meta
        sample_id = partial_sample_meta["sample_id"]
        source_table = partial_sample_meta["source_name"]
        sample_table = partial_sample_meta["table_name"]
        key_col = partial_sample_meta["key_col"]
        part_col = partial_sample_meta["part_col"]

        def compute_part_sizes(table_name, part_col, key_col):
            part_size_expr = "count(*)" if key_col == "_rowid" else f"count(distinct {key_col})"
            sql = textwrap.dedent(f'''\
                SELECT {part_col}, {part_size_expr}
                FROM {table_name}
                GROUP BY {part_col}''')
            result, desc = engine._sql_with_meta(sql)
            return result

        part_sizes = compute_part_sizes(sample_table, part_col, key_col)
        total_row_count = sum([r[1] for r in part_sizes])
        partition_info = [{
                'col_value': r[0], 
                'sampling_ratio': r[1] / float(total_row_count)
            } for r in part_sizes]

        sample_meta = {}
        sample_meta.update(partial_sample_meta)
        sample_meta.update({
            "row_count": total_row_count,
            "partitions": partition_info,
            })
        return sample_meta
        # meta.store_sample_meta(source_table, sample_id, sample_meta)


    def create_table_meta(self, table_name):
        engine = self._engine
        meta = self._meta
        row_count = engine.row_count(table_name)
        columns = engine.columns(table_name)
        data = {
            "row_count": row_count,
            "columns": {
                c[0]: c[1] for c in columns
            },
            "samples": [],
        }
        meta.store_table_meta(table_name, data)

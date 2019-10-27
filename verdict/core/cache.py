"""
SET
id: verdict.cache_meta.sample_id (str)
value: {
    'sampling_ratio': double,
    'part_col_value': int,
    'source_name': str
}

SET
id: verdict.cache_data.sample_id (str)
value: pickled pandas.DataFrame
"""

import concurrent.futures
import copy
import os
import pandas as pd
import pdb
import pickle
import pathlib
import redis
import textwrap
import uuid
from ..interface import *
from ..common.tools import *
from ..config import *
from ..pandas_sql import PandasSQL, PandasSQLClient


class CacheManager(object):
    """A wrapper over an in-memory database. Responsible for automatically retrieving data into
    the in-memory db if those data are not present.

    :param server_mode:  
        If True, connects to Pandas SQL through the http connection. Otherwise, run Pandas SQL
        in the embedded mode.
    """

    def __init__(self, preload_cache=True, server_mode=False):
        """
        self._cache_info is the mapping from a table name to its counter. A positive counter
        means that the table must exist in the engine itself.
        """
        persist_dir = os.path.join(verdict_dir, 'cache')
        pathlib.Path(persist_dir).mkdir(parents=True, exist_ok=True)
        self._persist_dir = persist_dir
        self._cache_info = {}
        cache_host_port = cache_presto_host + ':' + cache_presto_port
        # self._cache_engine = PrestoEngine(host=cache_host_port, sample_catalog=cache_presto_catalog, 
        #                                   sample_schema=cache_presto_schema, query_concurrency=20)
        if server_mode:
            self._cache_engine = PandasSQLClient()
        else:
            self._cache_engine = PandasSQL()
        self._cache_catalog = cache_presto_catalog
        self._cache_schema = cache_presto_schema
        self.r = redis.Redis(host=cache_redis_host)

        if preload_cache:
            # initializes by loading cached data
            log(f"Starting: loading persisted cache into an in-memory engine.")
            self.load_all_cache()
            log(f"Done: the cache has been all loaded.")

    def load_all_cache(self):
        """Checks the metadata and loads all cached tables.

        @return  The id of the loaded data
        """
        sample_ids = [key.decode('utf-8').replace(CacheManager.cache_meta_id_prefix, '') 
                        for key in self.r.keys(self.cache_meta_id("*"))]

        for sample_id in sample_ids:
            self.increase_cache_counter(sample_id)

        # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        #     jobs = [executor.submit(self.increase_cache_counter, sid) for sid in sample_ids]
        #     for job in concurrent.futures.as_completed(jobs):
        #         try:
        #             sample_id = job.result()
        #         except Exception as e:
        #             log(f"{sample_id} generated an exception: {e}", "error")
        #         else:
        #             log(f"The cache has been loaded for {sample_id}", "debug")
        return sample_ids

    def execute(self, query):
        """
        @param query  A verdict query
        """
        assert_type(query, dict)
        query = copy.deepcopy(query)
        query = from_verdict_query(query)
        log(f'The cache engine received a query: {query}', 'debug')
        
        engine = self._cache_engine

        ratios = {}     # sampling ratios
        cache_sizes  = {}
        tables_to_cache = find_base_tables(query, include_samples=True)
        for cache_table in tables_to_cache:
            cache_name = cache_table.name()     # sample_id
            self.increase_cache_counter(cache_name)
            cache_meta = self.get_cache_meta(cache_name)
            ratios[cache_name] = float(cache_meta['sampling_ratio'])
            cache_sizes[cache_name] = self._cache_info[cache_name]['cache_size']

        GROUP_SIZE_ALIAS = '_group_size'

        def inject_group_size(rel_obj):
            if rel_obj.is_agg():
                rel_obj._relop_args.append((AggFunc.count(), GROUP_SIZE_ALIAS))
            elif isinstance(rel_obj, DerivedTable):
                rel_obj._source = inject_group_size(rel_obj._source)
            else:
                pass
            return rel_obj

        def remove_injected(df):
            cols_count = len(df.columns)
            assert df.columns[cols_count-1] == GROUP_SIZE_ALIAS
            group_sizes = list(df[df.columns[-1]])
            # min_group_size = df[df.columns[-1]].min()
            # max_group_size = df[df.columns[-1]].max()
            result = df[df.columns[0:cols_count-1]]
            return result, group_sizes

        query = inject_group_size(query)
        query_to_db = to_verdict_query(query)
        result = engine.execute(query_to_db)
        result, group_sizes = remove_injected(result)
        meta = {
            'ratio': min(ratios.values()),
            'min_group_size': min(group_sizes),
            'max_group_size': max(group_sizes),
            'total_cache_size': min(cache_sizes.values()),
        }

        # clear things up
        # for cache_table in tables_to_cache:
        #     cache_name = cache_table.name()
        #     self.reduce_cache_counter(cache_name)
        return result, meta

    def retrieve_min_group_size(self):
        return self.min_group_size

    def retrieve_max_group_size(self):
        return self.max_group_size

    def reduce_cache_counter(self, sample_id):
        if sample_id not in self._cache_info:
            return
        else:
            self._cache_info[sample_id]['counter'] -= 1 
            if self._cache_info[sample_id]['counter'] < 1:
                cache_dest = f"{self._cache_catalog}.{self._cache_schema}.{sample_id}"
                self._cache_engine.drop_table(cache_dest)
                self._cache_info[sample_id]
                log(f"Dropped {sample_id} from the in-memory db", 'debug')
        
    def increase_cache_counter(self, sample_id):
        if sample_id not in self._cache_info:
            # sample_meta = self.get_cache_meta(sample_id)
            cache_size = self.cache_to_db(sample_id)
            self._cache_info[sample_id] = {
                'counter': 1,
                'cache_size': cache_size,
            }
        else:
            if self._cache_info[sample_id]['counter'] <= 0:
                cache_size = self.cache_to_db(sample_id)
                self._cache_info[sample_id] = {
                    'counter': 1,
                    'cache_size': cache_size,
                }
            else:
                self._cache_info[sample_id]['counter'] += 1
        return sample_id


    def cache_to_db(self, sample_id):
        log(f"Starts to load the cache for (sample_id = {sample_id}).", "debug")
        rows_count = self.cache_to_pandasdb(sample_id)
        log(f"The cache of (sample_id = {sample_id}) has been loaded.", "debug")
        return rows_count


    def cache_to_prestodb(self, sample_id):
        data_col = self.get_cache_data(sample_id)
        if data_col is None:
            raise ValueError(f'Not found in cache: {sample_id}')
        data, col_def = data_col
        cache_dest = f"{self._cache_catalog}.{self._cache_schema}.{sample_id}"
        self._cache_engine.drop_table(cache_dest, exists=True)
        rows_count = self._cache_engine.create_table(cache_dest, data, col_def)
        return rows_count


    def cache_to_pandasdb(self, sample_id):
        cache_filename = self.get_cache_filename(sample_id)
        rows_count = self._cache_engine.load_table(sample_id, cache_filename)
        # data_col = self.get_cache_data(sample_id)
        # if data_col is None:
        #     raise ValueError(f'Not found in cache: {sample_id}')
        # data, col_def = data_col
        # rows_count = self._cache_engine.create_table(sample_id, data, col_def)
        return rows_count


    # CACHE META MANAGEMENT
    def get_cache_meta(self, sample_id):
        assert_type(sample_id, str)
        cache_id = self.cache_meta_id(sample_id)
        if cache_id is None:
            return None

        cache_meta = self.get(cache_id)
        assert cache_meta is not None
        return json.loads(cache_meta)

    def store_cache_meta(self, sample_id, sample_meta):
        """
        @param sample_id  sample_id
        @param sample_meta  dict
        """
        assert_type(sample_id, str)
        assert_type(sample_meta, dict)
        self.set(self.cache_meta_id(sample_id), json.dumps(sample_meta))

    def drop_cache_meta(self, sample_id):
        assert_type(sample_id, str)
        self.delete(self.cache_meta_id(sample_id))


    # CACHE DATA MANAGEMENT
    def get_cache_filename(self, sample_id):
        return os.path.join(self._persist_dir, self.cache_data_id(sample_id))
        
    def get_cache_data(self, sample_id):
        """
        @param sample_id  sample_id
        @return (data, col_def) where
            data := [tuple, ...]
            col_def := [(name, type), ...]
        """
        assert_type(sample_id, str)
        cache_filename = self.get_cache_filename(sample_id)
        with open(cache_filename, 'rb') as f:
            data_col = pickle.load(f)
            return data_col

    def store_cache_data(self, sample_id, data, col_def):
        """Stores the cache data to a file

        @param sample_id  sample_id
        @param data  [tuple, ...]
        @param col_def  [(name, type), ...]
        """
        assert_type(sample_id, str)
        assert_type(data, List)
        assert_type(col_def, List)
        cache_filename = self.get_cache_filename(sample_id)
        df = self._cache_engine.frame_from_data(data, col_def)
        with open(cache_filename, 'wb') as f:
            pickle.dump(df, f)
            log(f"The cache of (sample_id = {sample_id}) is saved to {cache_filename}.", "debug")

    def drop_cache_data(self, sample_id):
        cache_filename = self.get_cache_filename(sample_id)
        os.remove(cache_filename)


    cache_meta_id_prefix = "verdict.cache_meta."

    def cache_meta_id(self, sample_id):
        """
        @param sample_id  Mostly, sample_id
        """
        return CacheManager.cache_meta_id_prefix + sample_id

    def cache_data_id(self, sample_id):
        """
        @param sample_id  Mostly, sample_id
        """
        return f'verdict.cache_data.{sample_id}'

    def get(self, key):
        assert_type(key, str)
        value = self.r.get(key)
        return value if value is None else value.decode('utf-8')

    def set(self, key, value):
        assert_type(key, str)
        log(textwrap.dedent(f'''\
            The following metadata has been set.
                key = {key}
                value = {value}'''))
        self.r.set(key, value.encode('utf-8'))

    # def delete(self, key):
    #     self.r.delete(key)


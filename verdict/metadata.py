"""
Metadata entry (persistently stored in Redis)

SET
id: verdict.table_meta.presto.table_name (string)
value: {
    'row_count': int,
    'columns': { name: type },
    'samples': [ sample_id, ... ]
}

SET
id: verdict.sample_meta.sample_id (string)
value: {
    'source_name': str,     # source table name
    'table_name': str,      # sample table name
    'key_col': '_rowid',
    'row_count': int,       # this is the row count of this sample
    'part_col': string,
    'partitions': [
        {
            'col_value': int,
            'sampling_ratio': double,
        },
        ...
    ]
}
"""
import json
import pandas as pd
import pickle
import redis
import textwrap
from .common.logging import *
from .common.tools import *


class Metadata(object):
    """
    Stores and retrieves metadata using Redis.
    """

    def __init__(self, redis_host, engine_id):
        assert_type(redis_host, str)
        assert_type(engine_id, str)
        self.r = redis.Redis(host=redis_host)
        self._engine_id = engine_id

    def get(self, key):
        assert_type(key, str)
        value = self.r.get(key)
        return value if value is None else value.decode('utf-8')

    def set(self, key, value):
        assert_type(key, str)
        log(textwrap.dedent(f'''\
            The following metadata has been set.
                key = {key}
                value = {value}'''), 'debug')
        self.r.set(key, value.encode('utf-8'))

    def delete(self, key):
        self.r.delete(key)

    def get_all_tables(self):
        prefix = f"verdict.table_meta.{self._engine_id}"
        return [n.decode('utf-8').replace(prefix+'.', '') for n in self.r.keys(f'{prefix}*')]


    # TABLE META MANAGEMENT
    def get_table_meta(self, table_name):
        """
        @return  The following structure:
            {
                "columns": {
                    "name": str
                    "type": str
                },
                "row_count": int,
                "samples": [ str, ... ]
            }
        """
        assert_type(table_name, str)
        table_id = self.table_meta_id(table_name)
        if not self.r.exists(table_id):
            return None
        return json.loads(self.get(table_id))

    def store_table_meta(self, table_name, table_meta):
        """
        Stores the columns' information. The stored information can later be retrieved.

        @param columns_info  A map from column name to column type
        @return None
        """
        assert_type(table_name, str)
        assert_type(table_meta, dict)
        table_id = self.table_meta_id(table_name)
        self._store_table_meta(table_id, table_meta)

    def _store_table_meta(self, table_id, table_info):
        assert_type(table_info, dict)
        self.set(table_id, json.dumps(table_info))


    # SAMPLE META MANAGEMENT
    def get_sample_meta(self, sample_id):
        """Returns the following dictionary:
        {
            'source_name': str,
            'key_col': '_rowid',
            'row_count': int,       # this is the row count of this sample
            'part_col': string,
            'partitions': [
                {
                    'col_value': int,
                    'sampling_ratio': double,
                },
                ...
            ]
        }
        """
        sample_info = self.get(self.sample_meta_id(sample_id))
        if sample_info is None:
            return None
        return json.loads(sample_info)

    def store_sample_meta(self, table_name, sample_id, sample_meta):
        assert_type(table_name, str)
        assert_type(sample_id, str)
        assert_type(sample_meta, dict)
        table_id = self.table_meta_id(table_name)

        # original table meta
        table_meta = self.get_table_meta(table_name)
        assert table_meta is not None
        table_meta["samples"].append(sample_id)
        self.store_table_meta(table_name, table_meta)

        # sample table meta
        self.set(self.sample_meta_id(sample_id), json.dumps(sample_meta))

    def drop_sample_meta(self, source_table, sample_id):
        assert_type(source_table, str)
        assert_type(sample_id, str)
        table_meta = self.get_table_meta(source_table)
        assert table_meta is not None
        sample_ids = table_meta['samples']
        try:
            sample_ids.remove(sample_id)
        except ValueError:
            pass
        table_meta['samples'] = sample_ids
        self.store_table_meta(source_table, table_meta)

        self.delete(self.sample_meta_id(sample_id))
        log(f'Removed the metadata for the sample: {sample_id}', 'debug')


    def get_col_info(self, table_name):
        table_info = self.get_table_meta(table_name)
        if table_info is None:
            return None
        else:
            return table_info['columns']

    def exists(self, source_name):
        return r.exists(self.table_meta_id(source_name))


    # ID HELPERS
    def table_meta_id(self, source_name):
        if "verdict.table_meta" in source_name:
            raise ValueError(source_name)
        return f'verdict.table_meta.{self._engine_id}.{source_name}'

    def sample_meta_id(self, sample_id):
        return f'verdict.sample_meta.{sample_id}'



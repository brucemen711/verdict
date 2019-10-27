import keebo
from keebo.querying import *


accl = keebo.presto_accl()


def test_load_all_caches():
    tables = ['lineitem', 'orders', 'partsupp', 'part', 'supplier', 'customer', 'nation', 'region']
    for tab in tables:
        df = accl._pandas_dataframe_from_presto('tpch', 'tiny', tab)
        key = f'tpch.tiny.{tab}'
        accl.metadata().store_cache(key, df, sampling_ratio=1.0, part_col_value=0)

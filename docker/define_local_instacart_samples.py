"""Creates sample tables from local HDFS files
"""
import verdict
import prestodb
import sys
import textwrap
from verdict.common.tools import pandas_df_from_result
from verdict.core.sampling import UniformRandom
from pyhive import hive


verdict.set_loglevel("debug")
meta_setup = False
fast_meta = True
hostname = 'presto'
if len(sys.argv) > 1:
    hostname = sys.argv[1]
if len(sys.argv) > 2:
    if sys.argv[2].lower() == "true":
        meta_setup = True
if len(sys.argv) > 3:
    if sys.argv[3].lower() == "false":
        fast_meta = False
hive_cursor = hive.connect(hostname).cursor()

presto_conn = prestodb.dbapi.connect(
                host=hostname,
                port=8080,
                user='verdict',
                catalog='hive',
                schema='default',
                )
presto_cursor = presto_conn.cursor()
v = verdict.presto(presto_host=hostname, preload_cache=False)

part_col = 'verdictcol'
sample_schema = 'verdict'

# TODO: samples for customers table
source2samples = {
    'hive.instacart.orders': [
        's42c77785ebc246fba29fd05b1799b057_rowid',
        's1e51666201b04338bb10e00fb78a37eaorder_id',
    ],
    'hive.instacart.order_products': [
        'sd56a949e5d404a58b633e387a3401cc0_rowid',
        's8adf84d5a3374d85b847c581e7416145order_id'
    ],  
}

sample_columns = {}

sample_columns['orders'] = '''\
    order_id                int,
    user_id                 int,
    eval_set                string,
    order_number            int,
    order_dow               int,
    order_hour_of_day       int,
    days_since_prior        int'''

sample_columns['order_products'] = '''\
    order_id                int,
    product_id              int,
    add_to_car_order        int,
    reordered               int,
    product_name            string,
    aisle_id                int,
    department_id           int,
    aisle                   string,
    department              string'''



presto_cursor.execute(f'CREATE SCHEMA IF NOT EXISTS hive.{sample_schema}')
presto_cursor.fetchall()

for source, sample_list in source2samples.items():
    source_catalog, source_schema, source_table = source.split(".")
    if meta_setup:
        v.create_table_meta(source)

    for sample_name in sample_list:
        cols_def = sample_columns[source_table]
        hive_cursor.execute(textwrap.dedent(f'''\
            CREATE TABLE IF NOT EXISTS {sample_schema}.{sample_name} (
            {cols_def}
            ) 
            PARTITIONED BY (verdictcol integer)
            STORED AS PARQUET
            LOCATION '/data/instacart/{sample_name}' '''))

        try:
            hive_cursor.execute(f'MSCK REPAIR TABLE {sample_schema}.{sample_name}')
            hive_cursor.fetchall()
        except Exception:
            pass

        if meta_setup:
            sample_table_name = f"hive.{sample_schema}.{sample_name}"

            # Theoretical ratio
            presto_cursor.execute(f"SELECT max({part_col}) FROM {sample_table_name}")
            result = presto_cursor.fetchall()
            parts_count = result[0][0]
            key_col = sample_name[33:]

            sample_meta = {
                "sample_id": sample_name,
                "source_name": source,
                "table_name": f"hive.{sample_schema}.{sample_name}",
                "key_col": key_col,
                "part_col": part_col,
            }

            if fast_meta:
                u = UniformRandom()
                ratio_dict = u.gen_sampling_ratios_from_parts_count(parts_count)
                sample_meta["partitions"] = sorted([
                        {"col_value": p[0], "sampling_ratio": p[1]} for p in ratio_dict.items()
                    ], key=lambda r: r["col_value"])
            else:
                sample_meta = v.create_accurate_sample_meta(sample_meta)
                ratio_dict = {a["col_value"]: a["sampling_ratio"] for a in sample_meta["partitions"]}

            v._meta.store_sample_meta(source, sample_name, sample_meta)

            # Store cache
            cache_meta, cache_data, col_def = v._engine.get_cache(sample_name, ratio_dict)
            v._cache_engine.store_cache_meta(sample_name, cache_meta)
            v._cache_engine.store_cache_data(sample_name, cache_data, col_def)
            


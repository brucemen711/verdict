import verdict
import prestodb
import sys
import textwrap
from verdict.common.tools import pandas_df_from_result
from verdict.core.sampling import UniformRandom
from pyhive import hive


verdict.set_loglevel("debug")
meta_setup = False
hostname = 'presto'
if len(sys.argv) > 1:
    hostname = sys.argv[1]
if len(sys.argv) > 2:
    if sys.argv[2].lower() == "true":
        meta_setup = True
hive_cursor = hive.connect(hostname).cursor()

presto_conn = prestodb.dbapi.connect(
                host=hostname,
                port=8080,
                user='verdict',
                catalog='hive',
                schema='default',
                )
presto_cursor = presto_conn.cursor()
# presto_cursor = presto.connect(hostname).cursor()
v = verdict.presto(presto_host=hostname, preload_cache=True)

part_col = 'verdictcol'
sample_schema = 'verdict'

# TODO: samples for customers table
source2samples = {
    # # 1 GB
    'hive.tpch_sf1.lineitem': [
        'sdf0f876d729b4da0828d277c7a59e4a9_rowid',
        'sa9440b18e63040009591bedf0dbe31b0l_orderkey',
    ],
    'hive.tpch_sf1.orders': [
        's92af8dad6f324f1896306f7763ffd1e9_rowid',
        's99cbfe4a97fb440096c7c9cd232347c7o_orderkey'
    ],
    # 'hive.tpch_sf1.partsupp_premerged': [
    #     's78276ebf94b241bb9ab9e7bf51229010_rowid',
    #     's93ea62a576a64973b9822aff105e0b43ps_partkey'
    # ],
    # 100 GB
    'hive.tpch_sf100.lineitem': [
        's109e8ee2947c41bd9bfc8290d048d200_rowid',
        'sb73e0842207d42ba95a81b11a15c2012l_orderkey',
    ],
    'hive.tpch_sf100.orders': [
        's6de4797c5a6041499bd43f100916edb8_rowid',
        's39ca33df8cd047a0a9ec6b0efd7d2691o_orderkey',
    ]
}

sample_columns = {}

sample_columns['lineitem'] = '''\
    l_orderkey          bigint,
    l_partkey           bigint,
    l_suppkey           bigint,
    l_linenumber        int,
    l_quantity          double,
    l_extendedprice     double,
    l_discount          double,
    l_tax               double,
    l_returnflag        varchar,
    l_linestatus        varchar,
    l_shipdate          date,
    l_commitdate        date,
    l_receiptdate       date,
    l_shipinstruct      varchar,
    l_shipmode          varchar,
    l_comment           varchar,
    ps_availqty         int,
    ps_supplycost       double,
    ps_comment          varchar,
    p_name              varchar,
    p_mfgr              varchar,
    p_brand             varchar,
    p_type              varchar,
    p_size              int,
    p_container         varchar,
    p_retailprice       double,
    p_comment           varchar,
    s_name              varchar,
    s_address           varchar,
    s_nationkey         bigint,
    s_phone             varchar,
    s_acctbal           double,
    s_comment           varchar,
    sn_nationkey        bigint,
    sn_name             varchar,
    sn_regionkey        bigint,
    sn_comment          varchar,
    sr_name             varchar,
    sr_comment          varchar,
    verdictcol            integer'''

sample_columns['orders'] = '''\
    o_orderkey          bigint,
    o_custkey           bigint,
    o_orderstatus       varchar,
    o_totalprice        double,
    o_orderdate         date,
    o_orderpriority     varchar,
    o_clerk             varchar,
    o_shippriority      int,
    o_comment           varchar,
    c_name              varchar,
    c_address           varchar,
    c_nationkey         bigint,
    c_phone             varchar,
    c_acctbal           double,
    c_mktsegment        varchar,
    c_comment           varchar,
    cn_name             varchar,
    cn_regionkey        bigint,
    cn_comment          varchar,
    cr_name             varchar,
    cr_comment          varchar,
    verdictcol            integer'''

sample_columns['partsupp'] = '''\
    ps_partkey          bigint,
    ps_suppkey          bigint,
    ps_availqty         int,
    ps_supplycost       double,
    ps_comment          varchar,
    p_name              varchar,
    p_mfgr              varchar,
    p_brand             varchar,
    p_type              varchar,
    p_size              int,
    p_container         varchar,
    p_retailprice       double,
    p_comment           varchar,
    s_name              varchar,
    s_address           varchar,
    s_nationkey         bigint,
    s_phone             varchar,
    s_acctbal           double,
    s_comment           varchar,
    sn_nationkey        bigint,
    sn_name             varchar,
    sn_regionkey        bigint,
    sn_comment          varchar,
    sr_name             varchar,
    sr_comment          varchar,
    verdictcol          integer'''


presto_cursor.execute(f'CREATE SCHEMA IF NOT EXISTS hive.{sample_schema}')
presto_cursor.fetchall()

for source, sample_list in source2samples.items():
    source_catalog, source_schema, source_table = source.split(".")
    if meta_setup:
        v.create_table_meta(source)

    for sample_name in sample_list:
        cols_def = sample_columns[source_table]
        presto_cursor.execute(textwrap.dedent(f'''\
            CREATE TABLE IF NOT EXISTS hive.{sample_schema}.{sample_name} (
            {cols_def}
            ) WITH (
                format='PARQUET',
                external_location='s3a://verdictpublic/tpch/samples/{sample_name}/',
                partitioned_by = ARRAY['{part_col}']
            )'''))
        presto_cursor.fetchall()

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
            u = UniformRandom()
            ratio_dict = u.gen_sampling_ratios_from_parts_count(parts_count)
            
            sample_meta = {
                "sample_id": sample_name,
                "source_name": source,
                "table_name": f"hive.{sample_schema}.{sample_name}",
                "key_col": key_col,
                "part_col": part_col,
                "partitions": sorted([
                    {"col_value": p[0], "sampling_ratio": p[1]} for p in ratio_dict.items()
                ], key=lambda r: r["col_value"])
            }
            v._meta.store_sample_meta(source, sample_name, sample_meta)

            # Store cache
            cache_meta, cache_data, col_def = v._engine.get_cache(sample_name, ratio_dict)
            v._cache_engine.store_cache_meta(sample_name, cache_meta)
            v._cache_engine.store_cache_data(sample_name, cache_data, col_def)
            


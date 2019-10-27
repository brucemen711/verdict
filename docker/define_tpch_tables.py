import textwrap
import time
import sys
# from pyhive import presto
import prestodb

hostname = 'presto'
if len(sys.argv) > 1:
    hostname = sys.argv[1]
presto_conn = prestodb.dbapi.connect(
                host=hostname,
                port=8080,
                user='verdict',
                catalog='hive',
                schema='default',
                )
cursor = presto_conn.cursor()


our_schema = 'verdict'
tpch_sf1_schema = 'tpch_sf1'
tpch_sf100_schema = 'tpch_sf100'
# tables = ['lineitem', 'orders', 'partsupp', 'part', 'supplier', 'customer', 'nation', 'region',
#           'lineitem_premerged', 'orders_premerged', 'partsupp_premerged']
tables = ['lineitem', 'orders', 'partsupp']

def run_query(sql):
    print(sql, flush=True)
    cursor.execute(sql)
    print(cursor.fetchall(), flush=True)

# create schemas we will use
# run_query(f'CREATE SCHEMA IF NOT EXISTS hive.{our_schema}')
run_query(f'CREATE SCHEMA IF NOT EXISTS hive.{tpch_sf1_schema}')
run_query(f'CREATE SCHEMA IF NOT EXISTS hive.{tpch_sf100_schema}')


# define external tables
columns = {}
# columns['lineitem'] = '''\
#     l_orderkey          bigint,
#     l_partkey           bigint,
#     l_suppkey           bigint,
#     l_linenumber        int,
#     l_quantity          double,
#     l_extendedprice     double,
#     l_discount          double,
#     l_tax               double,
#     l_returnflag        varchar,
#     l_linestatus        varchar,
#     l_shipdate          date,
#     l_commitdate        date,
#     l_receiptdate       date,
#     l_shipinstruct      varchar,
#     l_shipmode          varchar,
#     l_comment           varchar'''

# columns['orders'] = '''\
#     o_orderkey          bigint,
#     o_custkey           bigint,
#     o_orderstatus       varchar,
#     o_totalprice        double,
#     o_orderdate         date,
#     o_orderpriority     varchar,
#     o_clerk             varchar,
#     o_shippriority      int,
#     o_comment           varchar'''

# columns['partsupp'] = '''\
#     ps_partkey          bigint,
#     ps_suppkey          bigint,
#     ps_availqty         int,
#     ps_supplycost       double,
#     ps_comment          varchar'''

# columns['part'] = '''\
#     p_partkey           bigint,
#     p_name              varchar,
#     p_mfgr              varchar,
#     p_brand             varchar,
#     p_type              varchar,
#     p_size              int,
#     p_container         varchar,
#     p_retailprice       double,
#     p_comment           varchar'''

# columns['supplier'] = '''\
#     s_suppkey           bigint,
#     s_name              varchar,
#     s_address           varchar,
#     s_nationkey         bigint,
#     s_phone             varchar,
#     s_acctbal           double,
#     s_comment           varchar'''

# columns['nation'] = '''\
#     n_nationkey         bigint,
#     n_name              varchar,
#     n_regionkey         bigint,
#     n_comment           varchar'''

# columns['region'] = '''\
#     r_regionkey         bigint,
#     r_name              varchar,
#     r_comment           varchar'''

# columns['customer'] = '''\
#     c_custkey           bigint,
#     c_name              varchar,
#     c_address           varchar,
#     c_nationkey         bigint,
#     c_phone             varchar,
#     c_acctbal           double,
#     c_mktsegment        varchar,
#     c_comment           varchar'''

columns['lineitem'] = '''\
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
    sr_comment          varchar'''

columns['orders'] = '''\
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
    cr_comment          varchar'''

columns['partsupp'] = '''\
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
    sr_comment          varchar'''


def tablename2bucketname(table_name):
    if table_name == 'lineitem':
        return 'lineitem_partsupp_part_supplier_nation_region'
    elif table_name == 'orders':
        return 'orders_customer_nation_region'
    elif table_name == 'partsupp':
        return 'partsupp_part_supplier_nation_region'
    else:
        return table_name

# SF1
for table_name in tables:
    cols_def = columns[table_name]
    cols_def = "\n".join(['    ' + l for l in textwrap.dedent(cols_def).split("\n")])
    bucket_name = tablename2bucketname(table_name)
    run_query(textwrap.dedent(f'''\
CREATE TABLE IF NOT EXISTS hive.{tpch_sf1_schema}.{table_name} (
{cols_def}
) WITH (
    format='PARQUET',
    external_location='s3a://verdictpublic/tpch/sf1/{bucket_name}/'
)'''))

# SF100
for table_name in tables:
    cols_def = columns[table_name]
    cols_def = "\n".join(['    ' + l for l in textwrap.dedent(cols_def).split("\n")])
    bucket_name = tablename2bucketname(table_name)
    run_query(textwrap.dedent(f'''\
CREATE TABLE IF NOT EXISTS hive.{tpch_sf100_schema}.{table_name} (
{cols_def}
) WITH (
    format='PARQUET',
    external_location='s3a://verdictpublic/tpch/sf100/{bucket_name}/'
)'''))


print("TPCH tables created.", flush=True)

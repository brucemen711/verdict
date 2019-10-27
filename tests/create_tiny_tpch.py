import textwrap
import time
import sys
import prestodb


# Make a connection
tpch_schema = "tpch_tiny"
hostname = 'presto'
if len(sys.argv) > 1:
    hostname = sys.argv[1]
presto_conn = prestodb.dbapi.connect(
                host=hostname,
                port=8080,
                user='keebo',
                catalog='hive',
                schema='default',
                )
cursor = presto_conn.cursor()


while True:
    try:
        cursor.execute('show catalogs')
        cursor.fetchall()
        break
    except Exception:
        print('''\
# Presto not ready yet. The setup script waits for another 10 seconds''', flush=True)
        time.sleep(10)


def run_query(sql):
    print(sql, flush=True)
    cursor.execute(sql)
    print(cursor.fetchall(), flush=True)

# create schemas we will use
run_query(f'CREATE SCHEMA IF NOT EXISTS hive.{tpch_schema}')


table_definitions = {}

table_definitions["lineitem"] = {
    "column_alias": """\
        l_orderkey     ,
        l_partkey      ,
        l_suppkey      ,
        l_linenumber   ,
        l_quantity     ,
        l_extendedprice,
        l_discount     ,
        l_tax          ,
        l_returnflag   ,
        l_linestatus   ,
        l_shipdate     ,
        l_commitdate   ,
        l_receiptdate  ,
        l_shipinstruct ,
        l_shipmode     ,
        l_comment      ,
        ps_availqty    ,
        ps_supplycost  ,
        ps_comment     ,
        p_name         ,
        p_mfgr         ,
        p_brand        ,
        p_type         ,
        p_size         ,
        p_container    ,
        p_retailprice  ,
        p_comment      ,
        s_name         ,
        s_address      ,
        s_nationkey    ,
        s_phone        ,
        s_acctbal      ,
        s_comment      ,
        sn_nationkey   ,
        sn_name        ,
        sn_regionkey   ,
        sn_comment     ,
        sr_name        ,
        sr_comment     """,
    "source_columns": """\
        l.orderkey       l_orderkey      ,
        l.partkey        l_partkey       ,
        l.suppkey        l_suppkey       ,
        l.linenumber     l_linenumber    ,
        l.quantity       l_quantity      ,
        l.extendedprice  l_extendedprice ,
        l.discount       l_discount      ,
        l.tax            l_tax           ,
        l.returnflag     l_returnflag    ,
        l.linestatus     l_linestatus    ,
        l.shipdate       l_shipdate      ,
        l.commitdate     l_commitdate    ,
        l.receiptdate    l_receiptdate   ,
        l.shipinstruct   l_shipinstruct  ,
        l.shipmode       l_shipmode      ,
        l.comment        l_comment       ,
        ps.availqty      ps_availqty     ,
        ps.supplycost    ps_supplycost   ,
        ps.comment       ps_comment      ,
        p.name           p_name          ,
        p.mfgr           p_mfgr          ,
        p.brand          p_brand         ,
        p.type           p_type          ,
        p.size           p_size          ,
        p.container      p_container     ,
        p.retailprice    p_retailprice   ,
        p.comment        p_comment       ,
        s.name           s_name          ,
        s.address        s_address       ,
        s.nationkey      s_nationkey     ,
        s.phone          s_phone         ,
        s.acctbal        s_acctbal       ,
        s.comment        s_comment       ,
        n.nationkey      sn_nationkey     ,
        n.name           sn_name          ,
        n.regionkey      sn_regionkey     ,
        n.comment        sn_comment       ,
        r.name           sr_name          ,
        r.comment        sr_comment       """,
    "source_tables": """\
        tpch.tiny.lineitem l
        INNER JOIN tpch.tiny.partsupp ps
                ON l.partkey = ps.partkey AND l.suppkey = ps.suppkey
        INNER JOIN tpch.tiny.part p
                ON ps.partkey = p.partkey
        INNER JOIN tpch.tiny.supplier s
                ON ps.suppkey = s.suppkey
        INNER JOIN tpch.tiny.nation n
                ON s.nationkey = n.nationkey
        INNER JOIN tpch.tiny.region r
                ON n.regionkey = r.regionkey"""
}

table_definitions["orders"] = {
    "column_alias": """\
        o_orderkey     ,
        o_custkey      ,
        o_orderstatus  ,
        o_totalprice   ,
        o_orderdate    ,
        o_orderpriority,
        o_clerk        ,
        o_shippriority ,
        o_comment      ,
        c_name         ,
        c_address      ,
        c_nationkey    ,
        c_phone        ,
        c_acctbal      ,
        c_mktsegment   ,
        c_comment      ,
        cn_name        ,
        cn_regionkey   ,
        cn_comment     ,
        cr_name        ,
        cr_comment     """,
    "source_columns": """\
        o.orderkey       o_orderkey     ,
        o.custkey        o_custkey      ,
        o.orderstatus    o_orderstatus  ,
        o.totalprice     o_totalprice   ,
        o.orderdate      o_orderdate    ,
        o.orderpriority  o_orderpriority,
        o.clerk          o_clerk        ,
        o.shippriority   o_shippriority ,
        o.comment        o_comment      ,
        c.name           c_name         ,
        c.address        c_address      ,
        c.nationkey      c_nationkey    ,
        c.phone          c_phone        ,
        c.acctbal        c_acctbal      ,
        c.mktsegment     c_mktsegment   ,
        c.comment        c_comment      ,
        n.name           cn_name        ,
        n.regionkey      cn_regionkey   ,
        n.comment        cn_comment     ,
        r.name           cr_name        ,
        r.comment        cr_comment     """,
    "source_tables": """\
        tpch.tiny.orders o
        INNER JOIN tpch.tiny.customer c
                ON o.custkey = c.custkey
        INNER JOIN tpch.tiny.nation n
                ON c.nationkey = n.nationkey
        INNER JOIN tpch.tiny.region r
                ON n.regionkey = r.regionkey"""
}



for table_name in sorted(table_definitions.keys()):
    column_alias = table_definitions[table_name]["column_alias"]
    source_columns = table_definitions[table_name]["source_columns"]
    source_tables = table_definitions[table_name]["source_tables"]

    sql = textwrap.dedent(f"""\
        CREATE TABLE IF NOT EXISTS hive.{tpch_schema}.{table_name} (
            {column_alias}
        ) AS
        SELECT {source_columns}
        FROM {source_tables}""")
    run_query(sql)

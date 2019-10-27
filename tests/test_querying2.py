"""
Prerequisite:
1. Run tests/create_tiny_tpch.py
1. Run tests/create_tiny_samples.py

In the test function names:
1. 'replaced' means the automatic table name replacement from original tables to sample tables
2. 'cache' or 'sample' means where the queries are supposed to run. If 'cache', it will be by pandas
    If 'sample', it will be by Presto
"""
import json
import time
import verdict
# from verdict.pandas_sql import pandas_server_start, pandas_server_stop

verdict_conn = []


def setup_module(module):
    verdict.set_loglevel('debug')
    v = verdict.presto(presto_host='localhost', preload_cache=False, pandas_sql_server_mode=False)
    assert len(verdict_conn) == 0
    verdict_conn.append(v)

def get_conn():
    return verdict_conn[0]

def run(sql):
    print(with_elapsed(get_conn().sql, sql))

def run_json(json_query):
    with_elapsed(get_conn().json, json_query)

def with_elapsed(func, *arg):
    start = time.time()
    result = func(*arg)
    elapsed = time.time() - start
    print(f'Elapsed time: {elapsed} secs')
    return result

def test_count_sql():
    run('''\
        select count(*)
        from hive.tpch_tiny.lineitem''')
    
def test_count_bypass():
    run('''\
        bypass select count(*)
        from hive.tpch_tiny.lineitem''')


def test_logical_op_sql():
    run('''\
        select l_shipdate, count(*)
        from hive.tpch_tiny.lineitem
        where l_returnflag = 'R' and 
              (l_shipdate >= date '1994-01-01' or l_shipdate <= date '1995-01-01')
        group by l_shipdate
        ''')


def test_cast_sql():
    run('''\
    SELECT shipdate, count(*) as c
    FROM (
        SELECT substr(cast(l_shipdate as varchar), 1, 7) as shipdate
        FROM hive.tpch_tiny.lineitem
    ) t
    GROUP BY shipdate
    ORDER BY shipdate''')


def test_dates_sql():
    run('''\
    SELECT shipyear, shipmonth, shipday, count(*) as c
    FROM (
        SELECT year(l_shipdate) shipyear, month(l_shipdate) shipmonth, day(l_shipdate) shipday
        FROM hive.tpch_tiny.lineitem
    ) t
    GROUP BY shipyear, shipmonth, shipday
    ORDER BY shipyear, shipmonth, shipday''')


def test_join_groupby_sql():
    run('''\
    SELECT o_orderstatus, count(*)
    FROM hive.tpch_tiny.lineitem l INNER JOIN
         hive.tpch_tiny.orders o ON l_orderkey = o_orderkey
    WHERE o_orderstatus != 'P'
    GROUP BY o_orderstatus
    ORDER BY o_orderstatus
    ''')


def test_agg_replaced_sample():
    query = {
        "type": "single_agg",
        "source": "table hive.tpch_tiny.lineitem",
        "agg": {
            "count": {
                "op": "count",
                "arg": []
            }
        },
        "options": {
            "bypass_cache": True
        }
    }
    result = run_json(query)
    print(result)

def test_agg_replaced_cache():
    query = {
        "type": "single_agg",
        "source": "table hive.tpch_tiny.lineitem",
        "agg": {
            "count": {
                "op": "count",
                "arg": []
            }
        }
    }
    result = run_json(query)
    print(result)


def test_join_replaced_cache():
    query = {
        "type": "single_agg",
        "source": {
            "op": "join",
            "source": "table hive.tpch_tiny.lineitem",
            "arg": {
                "join_to": "table hive.tpch_tiny.orders",
                "left_on": "attr l_orderkey",
                "right_on": "attr o_orderkey"
            }
        },
        "agg": {
            "count": {
                "op": "count",
                "arg": []
            }
        }
    }
    result = run_json(query)
    print(result)


def test_tpch_q1():
    run('''\
        select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(disc_price) as sum_disc_price,
        sum(charge) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from (
        select
            l_returnflag,
            l_quantity,
            l_extendedprice,
            l_discount,
            l_extendedprice,
            l_extendedprice * (1 - l_discount) disc_price,
            l_extendedprice * (1 - l_discount) * (1 + l_tax) charge,
            l_returnflag,
            l_linestatus
        from
            hive.tpch_tiny.lineitem
        where
            l_shipdate <= date '1998-12-01'
    ) t1
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;
        ''')


def test_tpch_q3():
    run("""\
        select
            l_orderkey,
            sum(revenue) as revenue,
            o_orderdate,
            o_shippriority
        from (
            select
                l_orderkey,
                l_extendedprice * (1 - l_discount) revenue,
                o_orderdate,
                o_shippriority
            from
                hive.tpch_tiny.lineitem l 
                    inner join hive.tpch_tiny.orders o
                    on l_orderkey = o_orderkey
            where
                c_mktsegment = 'BUILDING'
                and o_orderdate < date '1995-03-22'
                and l_shipdate > date '1995-03-22'
        ) t1
        group by
            l_orderkey,
            o_orderdate,
            o_shippriority
        order by
            revenue desc,
            o_orderdate
        LIMIT 10;
        """)


def test_tpch_q4():
    """
    select
        o_orderpriority,
        count(*) as order_count
    from
        orders as o
    where
        o_orderdate >= '1996-05-01'
        and o_orderdate < '1996-08-01'
        and exists (
            select
                *
            from
                lineitem
            where
                l_orderkey = o.o_orderkey
                and l_commitdate < l_receiptdate
        )
    group by
        o_orderpriority
    order by
        o_orderpriority;

    which is equivalent to

    select
        o_orderpriority,
        count(*) as order_count
    from
        orders o LEFT JOIN
        (
            select l_orderkey, count(*) exist_count
            from lineitem
            where l_commitdate < l_receiptdate
            group by l_orderkey
        ) t ON o.o_orderkey = t.l_orderkey
    where
        o_orderdate >= '1996-05-01'
        and o_orderdate < '1996-08-01'
        and exist_count > 0
    group by
        o_orderpriority
    order by
        o_orderpriority;
    """
    run("""\
        select
            o_orderpriority,
            count(*) as order_count
        from
            hive.tpch_tiny.orders o LEFT JOIN
            (
                select l_orderkey, count(*) exist_count
                from hive.tpch_tiny.lineitem
                where l_commitdate < l_receiptdate
                group by l_orderkey
            ) t ON o_orderkey = l_orderkey
        where
            o_orderdate >= date '1996-05-01'
            and o_orderdate < date '1996-08-01'
            and exist_count > 0
        group by
            o_orderpriority
        order by
            o_orderpriority
        """)

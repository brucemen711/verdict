import keebo
import pytest
from keebo.interface.sql2json import sql2keebo_query


keebo.set_loglevel('debug')


def run(sql):
    sql = sql.strip()
    sql = ' '.join(sql.split())
    keebo_json = sql2keebo_query(sql)
    print(keebo_json)


def test_select():
    with pytest.raises(ValueError):
        run("select a as ab from mytable")

def test_select2():
    with pytest.raises(ValueError):
        run("select a from mytable")

def test_agg1():
    run("select count(*) from mytable")

def test_agg2():
    run("select count(*), sum(l_extendedprice * l_discount) from mytable")

def test_orderby():
    run("select count(*) c from mytable order by c asc, c desc")

def test_groupby1():
    run("select mygroup, count(*) from mytable group by mygroup")

def test_groupby2():
    run("select mygroup1, mygroup2, count(*) from mytable group by mygroup1, mygroup2")

def test_join1():
    run('''\
        select count(*) 
        from hive.tpch.lineitem l 
             inner join hive.tpch.lineitem o
             on l.orderkey = o.orderkey''')

def test_join2():
    run('''\
        select count(*)
        from hive.tpch_sf1.lineitem_premerged l
             inner join hive.tpch_sf1.orders_premerged o 
             on l.l_orderkey = o.o_orderkey''')

def test_nested1():
    run('''\
        select count(*) 
        from (
            select l_extendedprice
            from hive.tpch.lineitem l 
                inner join hive.tpch.lineitem o
                on l.orderkey = o.orderkey) t''')

def test_nested2():
    run('''\
        SELECT shipdate, count(*) c
        FROM (SELECT cast(shipdate, varchar)
            FROM hive.tpch_sf1.lineitem_premerged
        ) t
        GROUP BY shipdate
        ORDER BY shipdate''')

def test_nested3():
    run('''\
        SELECT shipdate, count(*) c
        FROM (SELECT substr(cast(l_shipdate as varchar), 1, 7) as shipdate
            FROM hive.tpch_sf1.lineitem_premerged
        ) t
        GROUP BY shipdate
        ORDER BY shipdate''')

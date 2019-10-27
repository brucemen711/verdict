import keebo
from keebo import *


accel = keebo.presto_accelerator(host='localhost')


def test_project():
    t1 = accel.table('tpch.tiny.lineitem')
    print(t1.project({'name_alias': t1.attr('name')}).to_sql())

def test_filter():
    t1 = accel.table('tpch.tiny.lineitem')
    print(t1.filter(t1.attr('name') == 'cond_string')
            .to_sql())

def test_filter_project():
    t1 = accel.table('tpch.tiny.lineitem')
    print(t1.filter(t1.attr('myattr') == 'cond_string')
            .project({'name_alias': t1.attr('name')})
            .to_sql())

def test_project_filter():
    t1 = accel.table('tpch.tiny.lineitem')
    t2 = t1.project({'myattr_alias': t1.attr('myattr')})
    print(t2.filter(t2.attr('myattr') == 'cond_string')
            .to_sql())

def test_grouped_to_regular():
    t1 = accel.table('tpch.tiny.lineitem')

    def value_func(table):
        orders = accel.table('tpch.tiny.orders')
        t = table.join(orders, right_on=orders.attr('o_orderkey'))
        return t.agg({'total': AggFunc.sum(table.attr('l_quantity'))})

    t2 = GroupedTable(t1, 'l_orderkey').value(value_func)
    t3 = t2.to_table()
    print(t3.to_sql())
    
def test_agg_with_grouped():
    t1 = accel.table('tpch.tiny.lineitem')

    def value_func(table):
        orders = accel.table('tpch.tiny.orders')
        t = table.join(orders, right_on=orders.attr('o_orderkey'))
        return t.agg({'total': AggFunc.sum(table.attr('l_quantity'))})

    t2 = GroupedTable(t1, 'l_orderkey').value(value_func)
    t3 = t2.agg(lambda t: t.agg({'total_sum': AggFunc.sum(t.attr('price'))}))
    print(t3.to_sql())

    t4 = t2.agg(lambda t: t.groupby([t.attr('name'), t.attr('country')]) \
                           .agg({'total_sum': AggFunc.sum(t.attr('price'))}))
    print(t4.to_sql())

# def no_test_pandas_df_from_presto():
#     nation = accel.pandas_dataframe_from_presto('tpch', 'tiny', 'nation')
#     # print(nation)
#     accel._metadata.store_cache('tpch.tiny.nation', nation)

def no_test_load_cache():
    nation = accel._metadata.load_cache('tpch.tiny.nation')
    assert isinstance(nation, pd.DataFrame)

def test_pandas_project():
    nation = PandasTable(accel._metadata.load_cache('tpch.tiny.nation'))
    nationkey = nation.project({'nationkey': nation.attr('nationkey')})
    # print(nationkey.execute_pandas())

def test_pandas_project2():
    nation = PandasTable(accel._metadata.load_cache('tpch.tiny.nation'))
    # print(nation.execute_pandas())
    nationkey = nation.project({'sum': nation.attr('nationkey') + nation.attr('regionkey')})
    print(nationkey.execute_pandas())

def test_tpch_q1():
    """
    select
      l_returnflag,
      l_linestatus,
      sum(l_quantity) as sum_qty,
      sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      avg(l_quantity) as avg_qty,
      avg(l_extendedprice) as avg_price,
      avg(l_discount) as avg_disc,
      count(*) as count_order
    from
      lineitem
    where
      l_shipdate <= date '1998-12-01' - interval ':1' day
    group by
      l_returnflag,
      l_linestatus
    order by
      l_returnflag,
      l_linestatus
    LIMIT 1;
    """
    lineitem = PandasTable(accel._metadata.load_cache('tpch.tiny.lineitem'))
    result = lineitem.filter(lineitem.attr('shipdate') <= '1998-12-01') \
                     .groupby([lineitem.attr('returnflag'), lineitem.attr('linestatus')]) \
                     .agg({'sum_qty': AggFunc.sum(lineitem.attr('quantity'))})
    print(result.execute_pandas())

def test_tpch_q3():
    """
    select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    from
        customer,
        orders,
        lineitem
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date '1998-12-01'
        and l_shipdate > date '1996-12-01'
        and c_mktsegment = 'BUILDING'
    group by
        l_orderkey,
        o_orderdate,
        o_shippriority
    """
    l        = PandasTable(accel._metadata.load_cache('tpch.tiny.lineitem'))
    orders   = accel._metadata.load_cache('tpch.tiny.orders')
    customer = accel._metadata.load_cache('tpch.tiny.customer')
    oc = PandasTable(orders.merge(customer, on='custkey'))

    t1 = l.join(oc, left_on=l.attr('orderkey'), right_on=oc.attr('orderkey')) \
          .filter(l.attr('shipdate') > '1996-12-01') \
          .filter(oc.attr('orderdate') < '1998-12-01') \
          .filter(oc.attr('mktsegment') == 'BUILDING') \
          .project({
            'orderkey': l.attr('orderkey'),
            'revenue': l.attr('extendedprice') * (1 - l.attr('discount')),
            'orderdate': oc.attr('orderdate'),
            'shippriority': oc.attr('shippriority') })

    t2 = t1.groupby([t1.attr('orderkey'), t1.attr('orderdate'), t1.attr('shippriority')]) \
           .agg({'revenue': AggFunc.sum(t1.attr('revenue')) })
    print(t2.execute_pandas())


def test_tpch_q4():
    """
    select
        o_orderpriority,
        count(*) as order_count
    from
        orders
    where
        o_orderdate >= '1992-12-01'
        and o_orderdate < '1993-03-01'
        and exists (
            select
                *
            from
                lineitem
            where
                l_orderkey = o_orderkey
                and l_commitdate < l_receiptdate
        )
    group by
        o_orderpriority
    """
    o = PandasTable(accel._metadata.load_cache('tpch.tiny.orders'))
    l = PandasTable(accel._metadata.load_cache('tpch.tiny.lineitem'))
    t1 = l.filter(l.attr('commitdate') < l.attr('receiptdate')) \
          .agg({'total': AggFunc.count()})
    t2 = o.join(t1, join_type='cross') \
          .filter(t1.attr('total') >  0) \
          .filter(o.attr('orderdate') >= '1992-12-01') \
          .filter(o.attr('orderdate') < '1993-03-01') \
          .groupby([o.attr('orderpriority')]) \
          .agg({'order_count': AggFunc.count()})
    print(t2.execute_pandas())


def test_tpch_q5():
    """
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and o_orderdate >= date '1992-12-01'
        and o_orderdate < date '1998-12-01'
    group by
        n_name
    """
    l_df = accel._metadata.load_cache('tpch.tiny.lineitem')
    s_df = accel._metadata.load_cache('tpch.tiny.supplier')
    n_df = accel._metadata.load_cache('tpch.tiny.nation')
    r_df = accel._metadata.load_cache('tpch.tiny.region')
    o_df = accel._metadata.load_cache('tpch.tiny.orders')
    c_df = accel._metadata.load_cache('tpch.tiny.customer')

    lsng = PandasTable(l_df.merge(s_df, on='suppkey').merge(n_df, on='nationkey')
                                 .merge(r_df, on='regionkey'))
    os   = PandasTable(o_df.merge(c_df, on='custkey'))

    t1 = lsng.join(os, left_on=lsng.attr('orderkey'), right_on=os.attr('orderkey'), 
                   join_type='inner') \
             .filter(os.attr('orderdate') >= '1992-12-01') \
             .filter(os.attr('orderdate') < '1998-12-01') \
             .project({
                'name': lsng.attr('name'), 
                'revenue': lsng.attr('extendedprice') * (1 - lsng.attr('discount'))
             })
    t2 = t1.groupby([t1.attr('name')]) \
           .agg({'revenue': AggFunc.sum(t1.attr('revenue'))})
    print(t2.execute_pandas())



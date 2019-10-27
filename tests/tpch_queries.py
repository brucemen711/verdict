import keebo
import time
from keebo import *

keebo.set_loglevel('debug')
k = keebo.presto(keebo_host='local')


def tpch_q1():
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
        l_shipdate <= date '1998-12-01'
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;
    """
    start = time.time()
    def map_func(t):
        return t.filter(attr('l_shipdate') <= C.date('1998-12-01')) \
                .project({
                    'l_returnflag': attr('l_returnflag'),
                    'l_linestatus': attr('l_linestatus'),
                    'l_quantity': attr('l_quantity'),
                    'l_extendedprice': attr('l_extendedprice'),
                    'l_discount': attr('l_discount'),
                    'disc_price': attr('l_extendedprice') * (1 - attr('l_discount')),
                    'charge': attr('l_extendedprice') * (1 - attr('l_discount')) * (1 + attr('l_tax'))
                    })

    def agg_func(t):
        return t.groupby(attr('l_returnflag'), attr('l_linestatus')) \
                .agg({
                    'sum_qty': F.sum(attr('l_quantity')),
                    'sum_base_price': F.sum(attr('l_extendedprice')),
                    'sum_disc_price': F.sum(attr('disc_price')),
                    'sum_charge': F.sum(attr('charge')),
                    'avg_qty': F.avg(attr('l_quantity')),
                    'avg_price': F.avg(attr('l_extendedprice')),
                    'avg_disc': F.avg(attr('l_discount')),
                    'count_order': F.count()
                    })

    ans = presto.source('hive.tpch_sf1.lineitem_premerged') \
                .map_values(map_func) \
                .aggregate(agg_func, orderby=[('l_returnflag', 'asc'), ('l_linestatus', 'asc')])
    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')


def tpch_q3():
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
      c_mktsegment = 'BUILDING'
      and c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate < date '1995-03-22'
      and l_shipdate > date '1995-03-22'
    group by
      l_orderkey,
      o_orderdate,
      o_shippriority
    order by
      revenue desc,
      o_orderdate
    LIMIT 10;
    """
    start = time.time()
    def map_func(t):
        o = table('hive.tpch_sf1.orders_premerged')
        return t.join(o, right_on=attr('o_orderkey')) \
                .filter(attr('c_mktsegment') == 'BUILDING') \
                .filter(attr('o_orderdate') < C.date('1995-03-22')) \
                .filter(attr('l_shipdate') > C.date('1995-03-22')) \
                .project({
                    'l_orderkey': attr('l_orderkey'),
                    'o_orderdate': attr('o_orderdate'),
                    'o_shippriority': attr('o_shippriority'),
                    'disc_price': attr('l_extendedprice') * (1 - attr('l_discount')),
                    })

    def agg_func(t):
        return t.groupby(attr('l_orderkey'), attr('o_orderdate'), attr('o_shippriority')) \
                .agg({
                    'revenue': F.sum(attr('disc_price'))
                    })

    ans = presto.source('hive.tpch_sf1.lineitem_premerged', key_col='l_orderkey') \
              .map_values(map_func) \
              .aggregate(agg_func, 
                        orderby=[('revenue', 'desc'), ('o_orderdate', 'asc')],
                        limit=10)
    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')


def tpch_q4():
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
    start = time.time()
    def map_func(o):
        l = table('hive.tpch_sf1.lineitem_premerged')
        l = l.filter(attr('l_commitdate') < attr('l_receiptdate')) \
            .groupby(attr('l_orderkey')) \
            .agg({'exist_count': F.count()})

        return o.join(l, right_on=attr('l_orderkey'), join_type='left') \
            .project({'o_orderpriority': attr('o_orderpriority')})

    def agg_func(t):
        return t.groupby(attr('o_orderpriority')) \
                .count()

    ans = presto.source('hive.tpch_sf1.orders_premerged', key_col='o_orderkey') \
              .map_values(map_func) \
              .aggregate(agg_func, 
                        orderby=[('o_orderpriority', 'asc')])
    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')


def tpch_q5():
    """
    SELECT
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    FROM
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    WHERE
        c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND l_suppkey = s_suppkey
        AND c_nationkey = s_nationkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'ASIA'
        AND o_orderdate >= date '1994-01-01'
        AND o_orderdate < date '1994-01-01' + interval '1' year
    GROUP BY
        n_name
    ORDER BY
        revenue desc;
    """
    start = time.time()
    def map_func(t):
        o = table('hive.tpch_sf1.orders_premerged')
        o = o.filter(attr('r_name') == 'ASIA') \
            .filter(attr('o_orderdate') >= date('1994-01-01')) \
            .filter(attr('o_orderdate') < date('1995-01-01')) \
            .project({'nation': attr('n_name'), 'o_orderkey': attr('o_orderkey') })
        return t.join(o, right_on=attr('o_orderkey')) \
                .project({
                    'nation': attr('nation'),
                    'disc_price': attr('l_extendedprice') * (1 - attr('l_discount'))
                })

    def agg_func(t):
        return t.groupby(attr('nation')) \
                .agg( {'revenue': F.sum(attr('disc_price'))} )

    ans = presto.source('hive.tpch_sf1.lineitem_premerged', key_col='l_orderkey') \
              .map_values(map_func) \
              .aggregate(agg_func, 
                        orderby=[('revenue', 'desc')],
                        limit=10)
    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')


def tpch_q6():
    """
    SELECT
        sum(l_extendedprice * l_discount) as revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= date '1994-01-01'
        AND l_shipdate < date '1994-01-01' + interval '1' year
        AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
        AND l_quantity < 24;
    """
    start = time.time()
    def map_func(t):
        return t.project({
                    'disc_price': attr('l_extendedprice') * attr('l_discount')
                })

    def agg_func(t):
        return t.agg( {'revenue': F.sum(attr('disc_price'))} )

    ans = presto.source('hive.tpch_sf1.lineitem_premerged', key_col='l_orderkey') \
              .map_values(map_func) \
              .aggregate(agg_func)
    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')


def tpch_q7():
    """
    select
      supp_nation,
      cust_nation,
      l_year,
      sum(volume) as revenue
    from (
      select
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        year(l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
      from
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
      where
        s_suppkey = l_suppkey
        and o_orderkey = l_orderkey
        and c_custkey = o_custkey
        and s_nationkey = n1.n_nationkey
        and c_nationkey = n2.n_nationkey
        and (
          (n1.n_name = 'KENYA' and n2.n_name = 'PERU')
          or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')
        )
        and l_shipdate between '1995-01-01' and '1996-12-31'
    ) as shipping
    group by
      supp_nation,
      cust_nation,
      l_year
    order by
      supp_nation,
      cust_nation,
      l_year;
    """
    start = time.time()
    def map_func(l):
        return l.filter(attr('l_shipdate') >= date('1995-01-01')) \
            .filter(attr('l_shipdate') <= date('1996-12-31')) \
            .project({
                'l_shipdate': attr('l_shipdate'),
                'supp_nation': attr('n_name'),
                'l_extendedprice': attr('l_extendedprice'),
                'l_discount': attr('l_discount')
            }) \
            .join(
                table('hive.tpch_sf1.orders_premerged')
                .project({
                    'cust_nation': attr('n_name'),
                    'o_orderkey': attr('o_orderkey')}), 
                right_on=attr('o_orderkey')) \
            .project({
                'l_year': attr('l_shipdate').year(),
                'disc_price': attr('l_extendedprice') * (1 - attr('l_discount')),
                'supp_nation': attr('supp_nation'),
                'cust_nation': attr('cust_nation')
            })

    def agg_func(t):
        return t.groupby(attr('supp_nation'), attr('cust_nation'), attr('l_year')) \
                .agg( {'revenue': F.sum(attr('disc_price'))})

    ans = presto.source('hive.tpch_sf1.lineitem_premerged', key_col='l_orderkey') \
              .map_values(map_func) \
              .aggregate(agg_func, 
                        orderby=[('supp_nation', 'asc'), ('cust_nation', 'asc'), ('l_year', 'asc')])

    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')


def tpch_q8():
    """
    select
      o_year,
      sum(case
        when nation = 'PERU' then disc_price
        else 0
      end) / sum(disc_price) as mkt_share
    from
      (
        select
          year(o_orderdate) as o_year,
          l_extendedprice * (1 - l_discount) as disc_price,
          n2.n_name as nation
        from
          part,
          supplier,
          lineitem,
          orders,
          customer,
          nation n1,
          nation n2,
          region
        where
          p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and r_name = 'AMERICA'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between '1995-01-01' and '1996-12-31'
          and p_type = 'ECONOMY BURNISHED NICKEL'
      ) as all_nations
    group by
      o_year
    order by
      o_year;
    """
    start = time.time()
    def map_func(l):
        return l.filter(attr('l_shipdate') >= date('1995-01-01')) \
            .filter(attr('l_shipdate') <= date('1996-12-31')) \
            .filter(attr('p_type') == 'ECONOMY BURNISHED NICKEL') \
            .project({
                'supp_nation': attr('n_name'),
                'l_extendedprice': attr('l_extendedprice'),
                'l_discount': attr('l_discount')
            }) \
            .join(
                table('hive.tpch_sf1.orders_premerged')
                .filter(attr('r_name') == 'AMERICA') \
                .filter(attr('o_orderdate') >= date('1995-01-01')) \
                .filter(attr('o_orderdate') <= date('1996-12-31')) \
                .project({
                    'o_year': attr('o_orderdate').year(),
                    'o_orderkey': attr('o_orderkey')}), 
                right_on=attr('o_orderkey')) \
            .project({
                'o_year': attr('o_year'),
                'disc_price': attr('l_extendedprice') * (1 - attr('l_discount')),
                'supp_nation': attr('supp_nation')
            }) \
            .project({
                'o_year': attr('o_year'),
                'price_peru': casewhen(attr('supp_nation') == 'PERU', attr('disc_price'), 0),
                'disc_price': attr('disc_price')
            })

    def agg_func(t):
        return t.groupby(attr('o_year')) \
                .agg({
                    'peru_share': F.sum(attr('price_peru')),
                    'total_share': F.sum(attr('disc_price'))
                })

    ans = presto.source('hive.tpch_sf1.lineitem_premerged', key_col='l_orderkey') \
              .map_values(map_func) \
              .aggregate(agg_func, 
                        orderby=[('o_year', 'asc')])

    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')


def tpch_q9():
    """
    select
      nation,
      o_year,
      sum(amount) as sum_profit
    from (
        select
          n_name as nation,
          year(o_orderdate) as o_year,
          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
          part,
          supplier,
          lineitem,
          partsupp,
          orders,
          nation
        where
          s_suppkey = l_suppkey
          and ps_suppkey = l_suppkey
          and ps_partkey = l_partkey
          and p_partkey = l_partkey
          and o_orderkey = l_orderkey
          and s_nationkey = n_nationkey
          and p_name like '%plum%'
      ) as profit
    group by
      nation,
      o_year
    order by
      nation,
      o_year desc;
    """
    start = time.time()
    def map_func(l):
        return l.filter(attr('p_name').contains('plum')) \
            .project({
                'supp_nation': attr('n_name'),
                'amount': attr('l_extendedprice') * (1 - attr('l_discount'))
                    - attr('ps_supplycost') * attr('l_quantity')
            }) \
            .join(table('hive.tpch_sf1.orders_premerged'), right_on=attr('o_orderkey')) \
            .project({
                'o_year': attr('o_orderdate').year(),
                'supp_nation': attr('supp_nation'),
                'amount': attr('amount')
            })

    def agg_func(t):
        return t.groupby(attr('supp_nation'), attr('o_year')) \
                .agg({ 'sum_profit': F.sum(attr('amount')) })

    ans = presto.source('hive.tpch_sf1.lineitem_premerged', key_col='l_orderkey') \
              .map_values(map_func) \
              .aggregate(agg_func, orderby=[('supp_nation', 'asc'), ('o_year', 'desc')])

    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')


def tpch_q10():
    """
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= '1993-07-01'
        and o_orderdate < '1993-10-01'
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20;
    """
    start = time.time()
    def map_func(l):
        return l.join(
                table('hive.tpch_sf1.orders_premerged')
                    .project({
                        'c_custkey': attr('o_custkey'),
                        'c_name': attr('c_name'),
                        'o_orderkey': attr('o_orderkey')
                    }), 
                right_on=attr('o_orderkey')) \
            .project({
                'c_custkey': attr('c_custkey'),
                'c_name': attr('c_name'),
                'disc_price': attr('l_extendedprice') * (1 - attr('l_discount')),
            })

    def agg_func(t):
        return t.groupby(attr('c_custkey'), attr('c_name')) \
                .agg({ 'revenue': F.sum(attr('disc_price')) })

    ans = presto.source('hive.tpch_sf1.lineitem_premerged', key_col='l_orderkey') \
              .map_values(map_func) \
              .aggregate(agg_func, orderby=[('revenue', 'desc')])

    elapsed = time.time() - start
    print(ans)
    print(f'elapsed time: {elapsed} sec')

if __name__ == '__main__':
    tpch_q10()

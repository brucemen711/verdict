import keebo
from keebo import *
import pytest

keebo.set_loglevel('debug')
kp = keebo.presto(host='localhost')


def test_aggregate_on_cache():
    table = kp.source('tpch.tiny.lineitem', '_rowid')
    result1 = table.map_values(lambda l: l.project({'extendedprice': attr('extendedprice')})) \
                   .aggregate(lambda l: l.agg({'total_price': F.sum(attr('extendedprice'))}))
    print(result1)

    result2 = \
        table.map_values(lambda l: l.project({
                        'extendedprice': attr('extendedprice'),
                        'returnflag': attr('returnflag')
                    })) \
             .aggregate(lambda l: l.groupby(attr('returnflag'))
                                 .agg({'total_price': F.sum(attr('extendedprice'))}))
    print(result2)

def test_aggregate_on_presto():
    table = kp.source('tpch.tiny.lineitem')
    result1 = table.map_values(lambda l: l.project({'extendedprice': attr('extendedprice')})) \
                   .aggregate(lambda l: l.agg({'total_price': F.sum(attr('extendedprice'))}),
                        try_cache=False)
    print(result1)

    result2 = table.map_values(lambda l: l.project({
                            'extendedprice': attr('extendedprice'),
                            'returnflag': attr('returnflag')
                          })) \
                   .aggregate(lambda l: l.groupby([attr('returnflag')])
                                       .agg({'total_price': F.sum(attr('extendedprice'))}),
                            try_cache=False)
    print(result2)

def test_orderby_on_cache():
    result = kp.source('tpch.tiny.lineitem') \
                 .aggregate(lambda l: l.agg({'total': F.sum(attr('extendedprice'))}),
                           orderby=[('total', 'asc')])
    print(result)

def test_orderby_on_presto():
    result = kp.source('tpch.tiny.lineitem') \
                 .aggregate(lambda l: l.agg({'total': F.sum(attr('extendedprice'))}),
                           orderby=[('total', 'asc')],
                           try_cache=False)
    print(result)

def test_groupby_on_cache():
    result = kp.source('tpch.tiny.lineitem') \
                .aggregate(lambda l: l.groupby([attr('shipdate')]).count() )
    # print(result)

    result = kp.source('tpch.tiny.lineitem') \
                 .aggregate(lambda t: t.groupby(['shipdate']).count(),
                            orderby=[('shipdate', 'asc')] )
    print(result)

def test_groupby_substr():
    result = kp.source('tpch.tiny.lineitem') \
                 .map_values(lambda l: l.project({
                    'shipyear': attr('shipdate').to_str().substr(1, 4)
                    })) \
                 .aggregate(lambda l: l.groupby([attr('shipyear')]).count() )

    result = kp.source('tpch.tiny.lineitem') \
                 .map_values(lambda l: l.project({
                    'shipyear': attr('shipdate').to_str().substr(1, 4)
                    })) \
                 .aggregate(lambda l: l.groupby([attr('shipyear')]).count(),
                            try_cache=False)

    result = kp.source('tpch.tiny.lineitem') \
                 .map_values(lambda l: l.project({
                    'ship_year': attr('shipdate').to_str().substr(1, 4),
                    'ship_month': attr('shipdate').to_str().substr(6, 2),
                    })) \
                 .aggregate(lambda l: l.groupby([attr('ship_year'), attr('ship_month')]).count(),
                            orderby=[('ship_year', 'asc'), ('ship_month', 'asc')],
                            limit=10)
    print(result)

def test_select_star():
    with pytest.raises(ValueError):
        result = kp.source('tpch.tiny.customer') \
                     .aggregate(lambda l: l.project({'key': attr('custkey')}),
                                orderby=[('key', 'asc')],
                                limit=10)

    stream = kp.source('tpch.tiny.customer') \
                 .stream(lambda l: l.project({'key': attr('custkey')}),
                            orderby=[('key', 'asc')],
                            limit=10)
    for result in stream:
        print(result, flush=True)

def test_stream_count():
    stream = kp.source('tpch.tiny.lineitem') \
                 .stream(lambda l: l.groupby(attr('returnflag'), attr('linestatus')).count())

    for result in stream:
        print(result, flush=True)

def test_stream_count_join():
    def map_func(l):
        o = table('tpch.tiny.orders')
        o = o.project({'o_orderkey': attr('orderkey')})
        return l.join(o, right_on=attr('o_orderkey'), join_type='inner')

    stream = kp.source('tpch.tiny.lineitem', 'orderkey') \
                 .map_values(map_func) \
                 .stream(lambda l: l.groupby(attr('returnflag'), attr('linestatus')).count())

    for result in stream:
        print(result, flush=True)

def test_round():
    def map_func(t):
        return t.project({'price': (attr('extendedprice') / 100).round() * 100})

    res = kp.source('tpch.tiny.lineitem') \
            .map_values(map_func) \
            .aggregate(lambda t: t.groupby('price').count(),
                       orderby=[('price', 'asc')])
    print(res)

def test_string_functions():
    def map_func(t):
        return t.project({'price': 
            attr('extendedprice').to_str()
            .concat(attr('extendedprice').to_str())
            .lower().upper()
            .replace('0', '1')
            })

    res = kp.source('tpch.tiny.lineitem') \
            .map_values(map_func) \
            .aggregate(lambda t: t.groupby('price').count(),
                       orderby=[('price', 'asc')])


if __name__ == "__main__":
    test_string_functions()

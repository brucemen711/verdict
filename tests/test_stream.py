import keebo
from keebo import *


accl = keebo.presto_accl(host='localhost')


def test_simple_count():
    result = accl.source('tpch.sf1.lineitem') \
                 .stream(lambda t: t.agg({'count': AggFunc.count()}))

def test_groupby_count():
    stream = accl.source('tpch.sf1.lineitem') \
                 .stream(lambda t: t.groupby([t.attr('linestatus'), t.attr('returnflag')]) \
                                   .agg({'count': AggFunc.count()}))

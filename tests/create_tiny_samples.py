import textwrap
import time
import sys
import verdict


# Make a connection
tpch_schema = "tpch_tiny"
hostname = 'presto'
if len(sys.argv) > 1:
    hostname = sys.argv[1]

verdict.set_loglevel('debug')
v = verdict.presto(presto_host=hostname)


table2key_col = {
    'hive.tpch_tiny.lineitem': ['_rowid', 'l_orderkey'],
    'hive.tpch_tiny.orders': ['_rowid', 'o_orderkey'],
}


for table_name in sorted(table2key_col.keys()):
    for key_col in table2key_col[table_name]:
        v.create_sample(table_name, key_col, cache_size=10000)

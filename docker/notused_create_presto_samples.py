"""
Create samples of the tables that are created by 'define_presto_tables.py'
"""
import verdict
import sys


hostname = 'presto'
if len(sys.argv) > 1:
    hostname = sys.argv[1]
v = verdict.presto(presto_host=hostname, verdict_host='local')


# k.sql('''\
# BYPASS CREATE SCHEMA IF NOT EXISTS hive.web_samples
# WITH (location = 's3a://keebopilot/tpch/samples/')''')

tables_and_keys = [
    ('lineitem_premerged', '_rowid'), 
    ('lineitem_premerged', 'l_orderkey'), 
    ('orders_premerged', '_rowid'),
    ('orders_premerged', 'o_orderkey')]

for tab_and_key in tables_and_keys:
    tab, key = tab_and_key
    v.create_sample(f"hive.tpch_sf100.{tab}", key_col=key, output_sql_only=True)


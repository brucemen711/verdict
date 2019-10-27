"""
Prerequisite:
1. Run tests/create_tiny_tpch.py
1. Run tests/create_tiny_samples.py

"""
import json
import time
import verdict


verdict_conn = []


def setup_module(module):
    verdict.set_loglevel('debug')
    v = verdict.presto(presto_host='localhost', preload_cache=False, pandas_sql_server_mode=False)
    assert len(verdict_conn) == 0
    verdict_conn.append(v)

def get_conn():
    return verdict_conn[0]


def run_query(query):
    result = get_conn().sql_stream(query)
    return result

def test_count_sql():
    query = '''\
        select count(*)
        from hive.tpch_tiny.lineitem
    '''
    itr = run_query(query)
    for result in itr:
        print(result)

def test_count_sql2():
    query = '''\
         select count(*)
        from hive.tpch_tiny.lineitem l
             inner join hive.tpch_tiny.orders o 
             on l_orderkey = o_orderkey'''
    itr = run_query(query)
    for result in itr:
        print(result)

def test_predicate_sql():
    query = '''\
         select count(*)
        from hive.tpch_tiny.lineitem l
        where l_returnflag = 'R' '''
    itr = run_query(query)
    for result in itr:
        print(result)

def test_groupby_sql():
    query = '''\
         select count(*)
        from hive.tpch_tiny.lineitem l
        group by l_returnflag '''
    itr = run_query(query)
    for result in itr:
        print(result)

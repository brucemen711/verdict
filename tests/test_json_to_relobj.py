import json
from verdict.core.relobj import from_verdict_query


def test_agg_func():
    json_obj = json.loads('''
        {
            "op": "sum",
            "arg": ["attr l_extendedprice"]
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)

def test_attr_op():
    json_obj = json.loads('''
        {
            "op": "div",
            "arg": [
                {
                    "op": "add",
                    "arg": [
                        {
                            "op": "mul",
                            "arg": ["attr l_extendedprice", "attr l_discount"]
                        },
                        1
                    ]
                },
                "timestamp 2019-23-23 12:12:00.000"
            ]
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)

def test_project():
    json_obj = json.loads('''
        {
            "op": "project",
            "source": "table myschema.mytable",
            "arg": {
                "alias1": "attr mycol1",
                "alias2": "attr mycol2"
            }
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)

def test_filter():
    json_obj = json.loads('''
        {
            "op": "select",
            "source": "table myschema.mytable",
            "arg": {
                "op": "eq",
                "arg": [
                    "attr l_shipdate",
                    "date 1994-01-01"
                ]
            }
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)

def test_join():
    json_obj = json.loads('''
        {
            "op": "join",
            "source": "table hive.tpch.lineitem",
            "arg": {
                "join_to": "table hive.tpch.orders",
                "left_on": "attr l_orderkey",
                "right_on": "attr o_orderkey",
                "join_type": "inner"
            }
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)


def test_join2():
    verdict_query = {
        'op': 'join', 
        'source': 'table hive.tpch_sf1.lineitem_premerged l', 
        'arg': {
            'join_to': 'table hive.tpch_sf1.orders_premerged o', 
            'left_on': 'attr l.l_orderkey', 
            'right_on': 'attr o.o_orderkey', 
            'join_type': 'inner'
        }
    }
    rel_obj = from_verdict_query(verdict_query)
    print(rel_obj)

def test_groupby():
    json_obj = json.loads('''
        {
            "op": "groupby",
            "source": "table hive.tpch.lineitem",
            "arg": [
                "attr l_orderkey",
                "attr o_orderkey"
            ]
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)

def test_agg():
    json_obj = json.loads('''
        {
            "op": "agg",
            "source": "table hive.tpch.lineitem",
            "arg": {
                "alias1": {
                    "op": "sum",
                    "arg": [ "attr l_extendedprice" ]
                }
            }
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)

def test_orderby():
    json_obj = json.loads('''
        {
            "op": "orderby",
            "source": "table hive.tpch.lineitem",
            "arg": [
                ["attr l_extendedprice", "asc"]
            ]
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)

def test_limit():
    json_obj = json.loads('''
        {
            "op": "limit",
            "source": "table hive.tpch.lineitem",
            "arg": 10
        }''')
    rel_obj = from_verdict_query(json_obj)
    print(rel_obj)

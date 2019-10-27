import json
import keebo
from keebo.core.querying2 import *


def test_find_replacables_project():
    json_obj = json.loads('''
        {
            "op": "project",
            "source": "table hive.tpch.lineitem",
            "arg": {
                "orderkey": "attr l_orderkey"
            }
        }''')
    rel_obj = from_keebo_query(json_obj)

    sample_info = {
        "hive.tpch.lineitem": {
            "use_sample": True,
            "samples": {
                "_rowid": "sample_id1",
                "l_orderkey": "sample_id2"
            }
        }
    }

    candidates = find_replacables(rel_obj, sample_info)
    print(candidates)
    assert {
        'orderkey': {
            'hive.tpch.lineitem': {'key_col': 'l_orderkey', 'sample_id': 'sample_id2'}
        },
        '_rowid': {
            'hive.tpch.lineitem': {'key_col': '_rowid', 'sample_id': 'sample_id1'}
        }
    } == candidates


def test_find_replacables_join():
    json_obj = json.loads('''
        {
            "op": "join",
            "source": {
                "op": "project",
                "source": "table hive.tpch.orders",
                "arg": {
                    "o_orderkey": "attr o_orderkey"
                }
            },
            "arg": {
                "join_to": {
                    "op": "project",
                    "source": "table hive.tpch.lineitem",
                    "arg": {
                        "orderkey": "attr l_orderkey"
                    }
                },
                "left_on": "attr o_orderkey",
                "right_on": "attr orderkey",
                "join_type": "inner"
            }
        }''')
    rel_obj = from_keebo_query(json_obj)

    sample_info = {
        "hive.tpch.lineitem": {
            "use_sample": True,
            "samples": {
                "_rowid": "sample_id1",
                "l_orderkey": "sample_id2"
            }
        },
        "hive.tpch.orders": {
            "use_sample": True,
            "samples": {
                "_rowid": "sample_id3",
                "o_orderkey": "sample_id4"
            }
        }
    }

    candidates = find_replacables(rel_obj, sample_info)
    print(candidates)
    assert {
        'o_orderkey': {
            'hive.tpch.orders': {'key_col': 'o_orderkey', 'sample_id': 'sample_id4'}, 
            'hive.tpch.lineitem': {'key_col': 'l_orderkey', 'sample_id': 'sample_id2'}
        }, 
        'orderkey': {
            'hive.tpch.orders': {'key_col': 'o_orderkey', 'sample_id': 'sample_id4'}, 
            'hive.tpch.lineitem': {'key_col': 'l_orderkey', 'sample_id': 'sample_id2'}
        }
    } == candidates


def test_find_replacables_select():
    json_obj = json.loads('''
        {
            "op": "select",
            "source": "table hive.tpch.lineitem",
            "arg": {
                "op": "eq",
                "arg": [
                    "attr l_returnflag", "F"
                ]
            }
        }''')
    rel_obj = from_keebo_query(json_obj)

    sample_info = {
        "hive.tpch.lineitem": {
            "use_sample": True,
            "samples": {
                "_rowid": "sample_id1",
                "l_orderkey": "sample_id2"
            }
        }
    }

    candidates = find_replacables(rel_obj, sample_info)
    print(candidates)
    assert {
        '_rowid': {
            'hive.tpch.lineitem': {'key_col': '_rowid', 'sample_id': 'sample_id1'}
        }, 
        'l_orderkey': {
            'hive.tpch.lineitem': {'key_col': 'l_orderkey', 'sample_id': 'sample_id2'}
        }
    } == candidates


def test_find_replacables_groupby():
    json_obj = json.loads('''
        {
            "op": "groupby",
            "source": "table hive.tpch.lineitem",
            "arg": [ "attr l_orderkey", "attr l_returnflag" ]
        }''')
    rel_obj = from_keebo_query(json_obj)

    sample_info = {
        "hive.tpch.lineitem": {
            "use_sample": True,
            "samples": {
                "_rowid": "sample_id1",
                "l_orderkey": "sample_id2"
            }
        }
    }

    candidates = find_replacables(rel_obj, sample_info)
    print(candidates)
    assert {
        'l_orderkey': {
            'hive.tpch.lineitem': {'key_col': 'l_orderkey', 'sample_id': 'sample_id2'}
        }
    } == candidates


def test_find_replacables_orderby():
    json_obj = json.loads('''
        {
            "op": "orderby",
            "source": "table hive.tpch.lineitem",
            "arg": [ ["attr l_returnflag", "asc"] ]
        }''')
    rel_obj = from_keebo_query(json_obj)

    sample_info = {
        "hive.tpch.lineitem": {
            "use_sample": True,
            "samples": {
                "_rowid": "sample_id1",
                "l_orderkey": "sample_id2"
            }
        }
    }

    candidates = find_replacables(rel_obj, sample_info)
    print(candidates)
    assert {
        '_rowid': {
            'hive.tpch.lineitem': {'key_col': '_rowid', 'sample_id': 'sample_id1'}
        }, 
        'l_orderkey': {
            'hive.tpch.lineitem': {'key_col': 'l_orderkey', 'sample_id': 'sample_id2'}
        }
    } == candidates


def test_find_replacables_limit():
    json_obj = json.loads('''
        {
            "op": "limit",
            "source": "table hive.tpch.lineitem",
            "arg": 10
        }''')
    rel_obj = from_keebo_query(json_obj)

    sample_info = {
        "hive.tpch.lineitem": {
            "use_sample": True,
            "samples": {
                "_rowid": "sample_id1",
                "l_orderkey": "sample_id2"
            }
        }
    }

    candidates = find_replacables(rel_obj, sample_info)
    print(candidates)
    assert {} == candidates


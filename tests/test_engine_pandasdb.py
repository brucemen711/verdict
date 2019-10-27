import pandas as pd
import time
from keebo.engine.pandasdb import *


db = PandasDB()

# table 1
data = [[1, 2], [1, 3]]
columns = ['col1', 'col2']
db.create_table('table1', pd.DataFrame(data, columns=columns))

# table 2
data = [[4, 5], [4, 6]]
columns = ['col3', 'col4']
db.create_table('table2', pd.DataFrame(data, columns=columns))


def with_elapsed(func, *arg):
    start = time.time()
    result = func(*arg)
    elapsed = time.time() - start
    print(f'Elapsed time: {elapsed} secs')


def test_project():
    query = '''\
        {
            "op": "project",
            "source": "table table1",
            "arg": {
                "alias1": "attr col1",
                "alias2": "attr col2"
            }
        }'''
    start = time.time()
    result = with_elapsed(db.execute, query)
    elapsed = time.time() - start
    print(result)


def test_agg():
    query = '''\
        {
            "op": "agg",
            "source": "table table1",
            "arg": {
                "alias1": {
                    "op": "sum",
                    "arg": [ "attr col2" ]
                }
            }
        }'''
    result = with_elapsed(db.execute, query)
    print(result)

    query = '''\
        {
            "op": "agg",
            "source": {
                "op": "groupby",
                "source": "table table1",
                "arg": [ "attr col1" ]
            },
            "arg": {
                "alias1": {
                    "op": "sum",
                    "arg": [ "attr col2" ]
                }
            }
        }'''
    result = with_elapsed(db.execute, query)
    print(result)


def test_select():
    query = '''\
        {
            "op": "select",
            "source": "table table1",
            "arg": {
                "op": "eq",
                "arg": [ "attr col2", 2 ]
            }
        }'''
    result = with_elapsed(db.execute, query)
    print(result)

import os
from verdict.pandas_sql import *



def test_load_and_query():
    pd_sql = PandasSQL()

    # Load a table
    this_dir = os.path.dirname(os.path.abspath(__file__))
    df_path = os.path.join(this_dir, 'resources/test_df_shipcost')
    pd_sql.load_table('shipcost', df_path)

    # Run a simple query
    json_query = {
        "op": "agg",
        "arg": {
            "sum_cost": {
                "op": "sum",
                "arg": ["attr cost"]
            }
        },
        "source": {
            "op": "groupby",
            "arg": ["attr shipmethod"],
            "source": "table shipcost"
        }
    }
    result = pd_sql.execute(json_query)
    print(result)


def test_load_and_query_via_server():
    pandas_server_start(in_thread=True)

    pd_sql_client = PandasSQLClient()

    # Load a table
    this_dir = os.path.dirname(os.path.abspath(__file__))
    df_path = os.path.join(this_dir, 'resources/test_df_shipcost')
    pd_sql_client.load_table('shipcost', df_path)

    # Run a simple query
    json_query = {
        "op": "agg",
        "arg": {
            "sum_cost": {
                "op": "sum",
                "arg": ["attr cost"]
            }
        },
        "source": {
            "op": "groupby",
            "arg": ["attr shipmethod"],
            "source": "table shipcost"
        }
    }
    result = pd_sql_client.execute(json_query)
    print(result)

    pandas_server_stop()


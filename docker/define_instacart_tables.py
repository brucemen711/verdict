import textwrap
import time
import sys
import prestodb

hostname = 'presto'
if len(sys.argv) > 1:
    hostname = sys.argv[1]
presto_conn = prestodb.dbapi.connect(
                host=hostname,
                port=8080,
                user='verdict',
                catalog='hive',
                schema='default',
                )
cursor = presto_conn.cursor()


our_schema = 'verdict'
instacart_schema = 'instacart'
# tables = ['lineitem', 'orders', 'partsupp', 'part', 'supplier', 'customer', 'nation', 'region',
#           'lineitem_premerged', 'orders_premerged', 'partsupp_premerged']
tables = ['orders', 'order_products']

def run_query(sql):
    print(sql, flush=True)
    cursor.execute(sql)
    print(cursor.fetchall(), flush=True)

# create schemas we will use
run_query(f'CREATE SCHEMA IF NOT EXISTS hive.{instacart_schema}')


# define external tables
columns = {}

columns['orders'] = '''\
    order_id            INT,
    user_id             INT,
    eval_set            VARCHAR,
    order_number        INT,
    order_dow           INT,
    order_hour_of_day   INT,
    days_since_prior    INT'''

columns['order_products'] = '''\
    order_id            INT,
    product_id          INT,
    add_to_car_order    INT,
    reordered           INT,
    product_name        VARCHAR,
    aisle_id            INT,
    department_id       INT,
    aisle               VARCHAR,
    department          VARCHAR'''


def tablename2bucketname(table_name):
    if table_name == 'order_products':
        return 'order_products_merged'
    elif table_name == 'orders':
        return 'orders_parquet'
    else:
        return table_name


for table_name in tables:
    cols_def = columns[table_name]
    cols_def = "\n".join(['    ' + l for l in textwrap.dedent(cols_def).split("\n")])
    bucket_name = tablename2bucketname(table_name)
    run_query(textwrap.dedent(f'''\
CREATE TABLE IF NOT EXISTS hive.{instacart_schema}.{table_name} (
{cols_def}
) WITH (
    format='PARQUET',
    external_location='s3a://verdictpublic/instacart/{bucket_name}/'
)'''))



print("Instacart tables created.", flush=True)

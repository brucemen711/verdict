import sys
import time
import prestodb

hostname = 'presto'
if len(sys.argv) > 1:
    hostname = sys.argv[1]

def get_conn():
    presto_conn = prestodb.dbapi.connect(
                    host=hostname,
                    port=8080,
                    user='verdict',
                    catalog='hive',
                    schema='default',
                    )
    return presto_conn

# Wait until Presto is ready
while True:
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute('show catalogs')
        cursor.fetchall()
        break
    except Exception:
        print('''# Presto not ready yet. The setup script waits for another 10 seconds''', flush=True)
        time.sleep(10)
    finally:
        conn.close()

print('''\
#
# Presto is ready.
#''', flush=True)


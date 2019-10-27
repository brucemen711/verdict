import keebo

keebo.set_loglevel('debug')
k = keebo.presto(keebo_host='local')


def test_create_samples():
    k.sql("BYPASS CREATE SCHEMA IF NOT EXISTS hive.keebo")

    tables = ['lineitem', 'orders']
    for tab in tables:
        k.create_sample(f"tpch.tiny.{tab}")

    tables = ['lineitem', 'orders']
    for tab in tables:
        k.create_sample(f"tpch.tiny.{tab}", key_col='orderkey')


def _test_create_samples():
    k.sql("BYPASS CREATE SCHEMA IF NOT EXISTS hive.keebo")

    tables = ['nation']
    for tab in tables:
        k.create_sample(f"tpch.tiny.{tab}")


def _test_drop_samples():
    tables = ['lineitem', 'orders', 'partsupp', 'customer']
    for tab in tables:
        k.drop_sample('tpch', 'tiny', tab)


if __name__ == "__main__":
    test_create_samples()

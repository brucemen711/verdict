from .presto import PrestoDriver


db_drivers = {
    "presto": PrestoDriver,
}


def load_driver(driver_info):
    db_name = driver_info["name"]
    db_host = driver_info["host"]
    driver = db_drivers[db_name](host=db_host)
    return driver

from .common.logging import set_loglevel as set_log_level
from .core.cache import CacheManager
from .driver.loader import load_driver
from .session import VerdictSession


__version__ = '0.6.1'


def version():
    """Returns a version of this package.

    :return: a version string
    """
    return __version__

def set_loglevel(level):
    """Sets the log level of the entire package.

    :param level: One of four levels ("info", "debug", "warn", "error"). "info" by default. If you
           want to understand the internals, set this to "debug"

    :return: None
    """
    set_log_level(level)

def presto(presto_host, preload_cache=True, pandas_sql_server_mode=True):
    """Creates an instance of VerdictSession that connects to the Presto backend.

    :param presto_host: 
        The address of Presto server. Either "host" or "host:port". 
        If port is omitted, 8080 is used.

    :param preload_cache:   
        If True, a set of cache data are loaded from the persistent storage to memory. Otherwise, 
        those caches are lazily loaded.

    :param pandas_sql_server_mode:
        If True, connects to the Pandas SQL server over the network (as its client).

    :return:    
        An instance of the :class:`~verdict.session.VerdictSession` class.
    """
    driver_info = {
        "name": "presto",
        "host": presto_host
    }
    db_driver = load_driver(driver_info)
    cache = CacheManager(preload_cache=preload_cache, server_mode=pandas_sql_server_mode)
    return VerdictSession(db_driver, cache)

# presto = VerdictClient.presto

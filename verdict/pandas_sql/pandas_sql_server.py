import asyncio
import argparse
import concurrent.futures
import daemon
import json
import pandas as pd
import pickle
import psutil
import time
from threading import Thread
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
from .pandas_sql import PandasSQL, init_logger


PANDAS_SQL_DEFAULT_PORT = 7871

executor = concurrent.futures.ThreadPoolExecutor(1000)

pandas_sql_instance = [PandasSQL()]

pandas_sql_server_logger = init_logger()

running_server_instance = []


def pandas_server_log(msg, level="debug"):
    if level == "debug":
        pandas_sql_server_logger.debug(msg)
    elif level == "error":
        pandas_sql_server_logger.error(msg)


def get_pandas_sql():
    return pandas_sql_instance[0]



class PandasSQLHandler(RequestHandler):

    # def initialize(self):
    #     # Disable tornado's default logging
    #     hn = logging.NullHandler()      
    #     hn.setLevel(logging.DEBUG)
    #     logging.getLogger("tornado.access").addHandler(hn)
    #     logging.getLogger("tornado.access").propagate = False

    async def post(self):
        request = json.loads(self.request.body)
        pandas_server_log(f'PandasDB server received a request: {request}')
        try:
            result = await IOLoop.current().run_in_executor(executor, self.execute, request)
            response = pickle.dumps(result)
            self.write(response)
        except Exception as e:
            pandas_server_log(f"{e}", "error")
            response = pickle.dumps({
                "status": "error",
                "type": "result",
                "result": e
                })
            self.write(response)


    def execute(self, request):
        """
        @request  A request object in the dict type
        @return  A pandas dataframe that contains the result
        """
        request_type = request["type"]

        if request_type == "ping":
            return { 
                "status": "ok",
                "type": "result",
                "result": "pong",
            }

        elif request_type == "load-table":
            assert 'file-path' in request
            assert 'table-name' in request
            table_name = request['table-name']
            file_path = request['file-path']
            row_count = get_pandas_sql().load_table(table_name, file_path, if_not_exists=True)
            pandas_server_log(f"The requested table has been loaded: {table_name}.")
            return {
                "status": "ok",
                "type": "result",
                "result": row_count
            }

        elif request_type == "drop-table":
            assert 'table-name' in request
            table_name = request['table-name']
            get_pandas_sql().drop_table(table_name, if_exists=True)
            return {
                "status": "ok",
                "type": "status",
                "result": "ok"
            }

        elif request_type == "json-query":
            assert 'query' in request
            query = request['query']
            return {
                "status": "ok",
                "type": "result",
                "result": get_pandas_sql().execute(query)
            }

        else:
            raise ValueError(request_type)


def start_app(port, new_even_loop=False):
    if new_even_loop:
        asyncio.set_event_loop(asyncio.new_event_loop())
    app = Application([ (r".*", PandasSQLHandler), ])
    app.listen(port)
    server_instance = IOLoop.current()

    # Only one server instance is allowed per process
    assert len(running_server_instance) == 0
    running_server_instance.append(server_instance)
    server_instance.start()


def pandas_server_stop():
    assert len(running_server_instance) == 1
    event_loop = running_server_instance[0]
    event_loop.stop()
    event_loop.close()
    IOLoop.clear_current()


def pandas_server_start(port=PANDAS_SQL_DEFAULT_PORT, in_thread=False):
    pandas_server_log(f"Starts Pandas SQL server, listening on {port}.")
    if in_thread:
        new_even_loop = True
        Thread(target=start_app, args=(port, new_even_loop)).start()
    else:
        start_app(port)


def main():
    parser = argparse.ArgumentParser(description='PandasSQL server')
    parser.add_argument('--log-dir', type=str, nargs=1,
                        help='The directory to generate logs')
    parser.add_argument('-p', '--port', type=int, nargs=1,
                        help="The listening port of the server")
    args = parser.parse_args()

    if args.port is None:
        listening_port = PANDAS_SQL_DEFAULT_PORT
    else:
        listening_port = args.port[0]
    pandas_server_start(listening_port)

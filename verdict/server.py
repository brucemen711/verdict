import argparse
import concurrent.futures
import daemon
import json
import pandas as pd
import pickle
import psutil
import time
import types
import uuid
from tornado.ioloop import IOLoop
from queue import Queue
from tornado.web import Application, RequestHandler
from .common.logging import log
from .common.tools import *
from .core.cache import CacheManager
from .engine.presto import PrestoEngine
from .engine.loader import load_engine
from .verdict import Verdict, set_loglevel


KEEBO_DEFAULT_PORT = 7878

executor = concurrent.futures.ThreadPoolExecutor(1000)

cache = []

verdict_instances = {}

stream_result_queues = {}


class VerdictHandler(RequestHandler):

    # def initialize(self):
    #     # Disable tornado's default logging
    #     hn = logging.NullHandler()      
    #     hn.setLevel(logging.DEBUG)
    #     logging.getLogger("tornado.access").addHandler(hn)
    #     logging.getLogger("tornado.access").propagate = False

    async def post(self):
        request = json.loads(self.request.body)
        log(f'Verdict server received a request: {request}')
        try:
            result = await IOLoop.current().run_in_executor(executor, self.execute, request)
            response = pickle.dumps(result)
            self.write(response)
        except Exception as e:
            log(f"{e}", "error")
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
        client_id = request["client_id"]

        if request_type == "initialize-client":
            # look for db info
            assert "data_engine" in request
            assert "client_id" in request
            assert "query" not in request
            data_engine = load_engine(request["data_engine"])
            verdict_instances[client_id] = Verdict(data_engine, cache[0])
            return { 
                "status": "ok",
                "type": "status",
                "result": "ok",
            }

        elif request_type == "stream-result":
            assert "stream_id" in request
            stream_id = request["stream_id"]
            result_queue = stream_result_queues[stream_id]
            result = result_queue.get()

            if isinstance(result, Exception):
                return {
                    "status": "error",
                    "type": "result",
                    "result": result
                }
            else:
                return {
                    "status": "ok",
                    "type": "result",
                    "result": result
                    }

        elif request_type == "create-sample":
            assert "client_id" in request
            assert "table_name" in request
            assert "key_col" in request
            client_id = request['client_id']
            table_name = request['table_name']
            key_col = request['key_col']

            k = verdict_instances[client_id]
            sample_id = k.create_sample(table_name, key_col)
            return {
                "status": "ok",
                "type": "result",
                "result": sample_id,
            }

        elif request_type == "info":
            assert "client_id" in request
            assert "args" in request
            args = request['args']
            k = verdict_instances[client_id]
            result = k.info(*args)
            return {
                "status": "ok",
                "type": "result",
                "result": result,
            }

        else:
            k = verdict_instances[client_id]
            query_request = request["query"]

            if request_type == "sql-single" or request_type == "json-single":
                if request_type == "sql-single":
                    result = k.sql(query_request)
                else:
                    result = k.json(query_request)
                assert_type(result, pd.core.frame.DataFrame)
                return {
                    "status": "ok",
                    "type": "result",
                    "result": result
                }

            elif request_type == "sql-stream" or request_type == "json-stream":
                if request_type == "sql-stream":
                    result = k.sql_stream(query_request)
                else:
                    result = k.json_stream(query_request)
                assert_type(result, types.GeneratorType)

                # We start to collecting results using a queue
                result_queue = Queue()
                stream_id = 'stream' + uuid.uuid4().hex[:8]
                def store_results_async(results):
                    try:
                        for r in results:
                            result_queue.put(r)
                        result_queue.put("END")
                    except Exception as e:
                        result_queue.put(e)
                        import traceback
                        traceback.print_exc()
                executor.submit(store_results_async, result)
                stream_result_queues[stream_id] = result_queue

                # We instead return the information (for now) so that the client can request
                # actual result later.
                stream_info = {
                    "status": "ok",
                    "type": "stream_info",
                    "stream_id": stream_id
                }
                return stream_info

            else:
                raise ValueError(request_type)


def find_verdict_process():
    for process in psutil.process_iter():
        try:
            cmd = process.cmdline()
            if (len(cmd) >= 3) and ("verdict-server" in cmd[1]):
                return process
        except psutil.AccessDenied:
            continue
    return None


def main():
    parser = argparse.ArgumentParser(description='Verdict server process')
    parser.add_argument('command', type=str, nargs=1, choices=['start', 'stop'],
                        help='The main command: start or stop')
    parser.add_argument('--log-dir', type=str, nargs=1,
                        help='The directory to generate logs')
    parser.add_argument('-c', '--cache-server-host', type=str, nargs=1,
                        help="The address of the cache server")
    # parser.add_argument('--daemonize', type=str, nargs=1,
    #                     choices=['yes', 'no'],
    #                     help='If set to yes, run the server in the background.')
    args = parser.parse_args()

    command = args.command[0]
    if command == "stop":
        verdict_process = find_verdict_process()
        if verdict_process is None:
            print(f"Failed: didn't find any verdict servers.")
        else:
            verdict_pid = verdict_process.pid
            verdict_process.kill()
            print(f"Success: killed the verdict server (pid={verdict_pid}).")

    else:
        set_loglevel('debug')           # verdict's own logger
        # cache_host = 'localhost'        # default value
        # if args.cache_server_host is not None:
        #     cache_host = args.cache_server_host[0]
        #     assert isinstance(cache_host, str)
        assert len(cache) == 0
        cache.append(CacheManager())
        cache[0].load_all_from_meta()
        
        port = KEEBO_DEFAULT_PORT
        log(f'Starts listening on {port}', 'info')

        def start_app():
            app = Application([ (r".*", VerdictHandler), ])
            app.listen(port)
            IOLoop.current().start()
        
        start_app()


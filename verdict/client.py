import json
import pickle
import requests
import types
import uuid
from .common.tools import *
from .core.cache import CacheManager
from .driver.loader import load_driver
from .session import VerdictSession



class VerdictClient(object):

    def __init__(self, verdict_host, driver_info):
        """
        @param verdict_host  The listening server address. If None, don't talk to server; simply
                           import a Verdict instance directly. For remote servers, this may include
                           the port number, e.g., "localhost:7878". Otherwise, the default port
                           number 7878 is used.
        @param driver_info  The information about the backend database to make a connection.
        """
        assert_type(driver_info, dict)
        client_id = 'client' + uuid.uuid4().hex[:8]
        self.client_id = client_id

        # connect to remote server
        self._mode = 'server-client'
        self._url = f'http://{verdict_host}'
        try:
            response = self.request({
                    "type": "initialize-client",
                    "client_id": client_id,
                    "db_driver": driver_info
                })
            log(f'client_id: {client_id}', 'info')
            assert response["type"] == "status"
            assert response["result"] == "ok"

        except Exception:
            msg = (f"Failed to connect to the verdict server ({verdict_host}). "
                   f"For local connection, pass verdict_host='local'.")
            raise ValueError(msg)


    @staticmethod
    def presto(presto_host='localhost', verdict_host='localhost:7878'):
        """
        """
        if ':' not in presto_host:
            presto_host = presto_host + ':8080'
        driver_info = {
            "name": "presto",
            "host": presto_host
        }
        if verdict_host == 'local':
            db_driver = load_driver(driver_info)
            cache = CacheManager()
            return Verdict(db_driver, cache)
        else:
            return VerdictClient(verdict_host, driver_info)


    def info(self, *arg):
        if self._mode == 'local':
            result = self.k.info(*arg)
        else:
            response = self.request({
                "type": "info",
                "client_id": self.client_id,
                "args": arg
            })
            assert response["status"] == "ok"
            result = response["result"]
        return result


    def sql(self, sql):
        if self._mode == 'local':
            result = self.k.sql(sql)
        else:
            response = self.request({
                "type": "sql-single",
                "client_id": self.client_id,
                "query": sql
            })
            assert response["status"] == "ok"
            result = response["result"]

        assert_type(result, pd.core.frame.DataFrame)
        return result


    def sql_stream(self, sql):
        if self._mode == 'local':
            for r in self.k.sql_stream(sql):
                yield r
        else:
            response = self.request({
                "type": "sql-stream",
                "client_id": self.client_id,
                "query": sql 
            })

            assert response["type"] == "stream_info"
            stream_id = response["stream_id"]
            print(f"client stream_id: {stream_id}")
            while True:
                result = self._sql_stream_get_result(stream_id)
                if isinstance(result, str) and result == "END":
                    break
                else:
                    yield result

            
    def _sql_stream_get_result(self, stream_id):
        response = self.request({
                        "type": "stream-result",
                        "client_id": self.client_id,
                        "stream_id": stream_id
                    })
        if response["status"] == "ok":
            return response["result"]
        else:
            e = response["result"]
            raise e


    def json(self, query):
        if self._mode == 'local':
            result = self.k.json(query)
        else:
            response = self.request({
                "type": "json-single",
                "client_id": self.client_id,
                "query": query 
            })

        assert_type(result, pd.core.frame.DataFrame)
        return result


    def json_stream(self, query):
        raise NotImplementedError


    def create_sample(self, table_name, key_col='_rowid'):
        """
        @param table_name  The unique identical of a table. The format of this unique identifier may
                           be different for different data engines. For example, the PostgreSQL 
                           family uses "catalog.schema.table" while the MySQL family uses
                           "schema.table". Also, if a target database requires some quotes, the
                           quotes must be included in this table_name. Verdict simply treats the
                           passed argument as a string whether the table_name includes quotes or
                           not.
        @param key_col  The column name that will be used as a key. The key_col determines the types
                        of the sample table we will create. If the default value (i.e., '_rowid') is
                        used, a uniform random sample (or equivalently, a simple random sample) is
                        created. If key_col is explicitly specified, a universe sample is created.
                        Whether the key_col is specified or not, the samples are conceptually the
                        consistent in that a part of the original tables are selected based on
                        the attribute values stored in the key_col.
        """
        if self._mode == 'local':
            sample_id = self.k.create_sample(table_name, key_col)
        else:
            response = self.request({
                            "type": "create-sample",
                            "client_id": self.client_id,
                            "table_name": table_name,
                            "key_col": key_col,
                        })
            assert response['status'] == 'ok'
            sample_id = response['result']

        log(f"Created a sample (sample_id: {sample_id}).")
        log(f"To retrieve details, run k.info(sample_id).")


    def drop_sample(self, sample_id):
        if self._mode == 'local':
            sample_id = self.k.drop_sample(table_name, key_col)
        else:
            response = self.request({
                            "type": "create-sample",
                            "client_id": self.client_id,
                            "sample_id": sample_id,
                        })
            assert response['status'] == 'ok'

        log(f"Dropped a sample (sample_id: {sample_id}).")


    def request(self, request):
        assert_type(request, dict)
        r = requests.post(url=self._url, data=json.dumps(request))
        response_pickled = r.content
        response = pickle.loads(response_pickled)

        # error check
        if response["status"] == "error":
            e = response["result"]
            raise e

        return response


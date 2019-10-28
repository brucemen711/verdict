import json
import logging
import pickle
import requests
import types
import uuid
from .pandas_sql_server import PANDAS_SQL_DEFAULT_PORT



def init_logger():
    format_str = "%(asctime)s %(name)s %(levelname)s - %(message)s "
    logging.basicConfig(level=logging.CRITICAL, format=format_str)
    pandas_sql_logger = logging.getLogger("pandas_client")
    pandas_sql_logger.setLevel(logging.DEBUG)
    return pandas_sql_logger


class PandasSQLClient(object):

    def __init__(self, server_address=f"localhost:{PANDAS_SQL_DEFAULT_PORT}"):
        """
        :param server_address:
            The listening server address in the following form: "host:port"
        """
        client_id = 'client' + uuid.uuid4().hex[:8]
        self.client_id = client_id
        self._logger = init_logger()

        # connect to remote server
        self._url = f'http://{server_address}'
        try:
            response = self.request({
                "type": "ping"
            })
        except requests.exceptions.RequestException:
            msg = f"Failed to connect to the Pandas SQL server ({server_address})."
            raise ValueError(msg) from None

    def _log(self, msg):
        self._logger.debug(msg)

    def load_table(self, table_name, file_path, if_not_exists=True):
        """
        return:
            The number of rows in the loaded table.
        """
        response = self.request({
            "type": "load-table",
            "table-name": table_name,
            "file-path": file_path,
            "if-not-exists": if_not_exists,
            })
        return response["result"]

    def execute(self, json_query):
        response = self.request({
            "type": "json-query",
            "query": json_query
            })
        return response["result"]

    def request(self, request):
        assert isinstance(request, dict)
        r = requests.post(url=self._url, data=json.dumps(request))
        response_pickled = r.content
        response = pickle.loads(response_pickled)

        # error check
        if response["status"] == "error":
            trace = response["result"]
            e = response["error"]
            self._logger.error(trace)
            raise e

        return response


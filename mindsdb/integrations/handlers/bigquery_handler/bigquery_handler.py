import os
import json
from collections import OrderedDict

from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy_bigquery.base import BigQueryDialect
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log


class BigQueryHandler(DatabaseHandler):
    """
    This handler handles connection and exectuin of Google BigQuery statements
    """
    name = "bigquery"

    def __init__(self, name, connection_data, **kwargs):
        super().__init__(name)
        self.connection_data = connection_data
        self.client = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def _get_account_keys(self):
        if 'service_account_keys' in self.connection_data:
            if os.path.isfile(self.connection_data['service_account_keys']) is False:
                raise Exception("Service account keys' must be path to the file")
            with open(self.connection_data["service_account_keys"]) as source:
                info = json.load(source)
            return info
        elif 'service_account_json' in self.connection_data:
            info = self.connection_data['service_account_json']
            if not isinstance(info, dict):
                raise Exception("service_account_json has to be dict")
            info['private_key'] = info['private_key'].replace('\\n', '\n')
            return info
        else:
            raise Exception('Connection args have to content ether service_account_json or service_account_keys')

    def connect(self):
        """
        Handles the connection to a BigQuery
        """
        if self.is_connected is True:
            return self.client

        info = self._get_account_keys()
        storage_credentials = service_account.Credentials.from_service_account_info(info)
        client = bigquery.Client(
            project=self.connection_data["project_id"],
            credentials=storage_credentials
        )
        self.is_connected = True
        self.client = client
        return self.client

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the BigQuery
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)

        try:
            client = self.connect()
            client.query('SELECT 1;')

            # check dataset exists
            client.get_dataset(self.connection_data['dataset'])

            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to BigQuery {self.connection_data["project_id"]}, {e}!')
            response.error_message = e

        if not response.success and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in BigQuery
        :return: returns the records from the current recordset
        """
        client = self.connect()
        try:
            job_config = bigquery.QueryJobConfig(default_dataset=f"{self.connection_data['project_id']}.{self.connection_data['dataset']}")
            query = client.query(query, job_config=job_config)
            result = query.to_dataframe()
            if not result.empty:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    result
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            log.logger.error(f'Error running query: {query} on {self.connection_data["project_id"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        renderer = SqlalchemyRender(BigQueryDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in BigQuery
        """
        q = f"SELECT table_name, table_type, FROM \
             `{self.connection_data['project_id']}.{self.connection_data['dataset']}.INFORMATION_SCHEMA.TABLES`"
        return self.native_query(q)

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"SELECT column_name, data_type, FROM \
            `{self.connection_data['project_id']}.{self.connection_data['dataset']}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table_name}'"
        return self.native_query(q)


connection_args = OrderedDict(
    project_id={
        'type': ARG_TYPE.STR,
        'description': 'The BigQuery project id.'
    },
    dataset={
        'type': ARG_TYPE.STR,
        'description': 'The BigQuery dataset name.'
    },
    service_account_keys={
        'type': ARG_TYPE.PATH,
        'description': 'Full path or URL to the service account JSON file'
    },
    service_account_json={
        'type': ARG_TYPE.DICT,
        'description': 'Content of service account JSON file'
    },
)

connection_args_example = OrderedDict(
    project_id='tough-future-332513',
    service_account_keys='/home/bigq/tough-future-332513.json'
)

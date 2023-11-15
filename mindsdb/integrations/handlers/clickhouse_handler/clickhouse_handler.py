from collections import OrderedDict

import pandas as pd
from sqlalchemy import create_engine
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class ClickHouseHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the ClickHouse statements.
    """

    name = 'clickhouse'

    def __init__(self, name, connection_data, **kwargs):
        super().__init__(name)
        self.dialect = 'clickhouse'
        self.connection_data = connection_data
        self.renderer = SqlalchemyRender(ClickHouseDialect)
        self.is_connected = False
        self.protocol = connection_data.get('protocol', 'clickhouse')

        # region added for back-compatibility with connections creatad before 11.05.2023
        protocols_map = {
            'native': 'clickhouse+native',
            'http': 'clickhouse+http',
            'https': 'clickhouse+https',
        }
        if self.protocol in protocols_map:
            self.protocol = protocols_map[self.protocol]
        # endregion

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Handles the connection to a ClickHouse
        """
        if self.is_connected is True:
            return self.connection

        protocol = self.protocol
        host = self.connection_data['host']
        port = self.connection_data['port']
        user = self.connection_data['user']
        password = self.connection_data['password']
        database = self.connection_data['database']
        url = f'{protocol}://{user}:{password}@{host}:{port}/{database}'
        if self.protocol == 'clickhouse+https':
            url = url + "?protocol=https"

        engine = create_engine(url)
        connection = engine.raw_connection()
        self.is_connected = True
        self.connection = connection
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the ClickHouse database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            cur = connection.cursor()
            try:
                cur.execute('select 1;')
            finally:
                cur.close()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to ClickHouse {self.connection_data["database"]}, {e}!')
            response.error_message = e

        if response.success and need_to_close:
            self.disconnect()
        if not response.success and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in ClickHouse
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        cur = connection.cursor()
        try:
            cur.execute(query)
            if result := cur.fetchall():
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    pd.DataFrame(
                        result,
                        columns=[x[0] for x in cur.description]
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
            connection.commit()
        except Exception as e:
            log.logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
            connection.rollback()
        finally:
            cur.close()

        if need_to_close:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in ClickHouse db
        """
        q = f"SHOW TABLES FROM {self.connection_data['database']}"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"DESCRIBE {table_name}"
        return self.native_query(q)


connection_args = OrderedDict(
    protocol={
        'type': ARG_TYPE.STR,
        'description': 'The protocol to query clickhouse. Supported: clickhouse, clickhouse+native, clickhouse+http, clickhouse+https. Default: clickhouse',
        'required': True,
        'label': 'Protocol'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the ClickHouse server.',
        'required': True,
        'label': 'User'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the ClickHouse server.',
        'required': True,
        'label': 'Database name'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the ClickHouse server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the ClickHouse server. Must be an integer.',
        'required': True,
        'label': 'Port'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the ClickHouse server.',
        'required': True,
        'label': 'Password'
    },
    
)

connection_args_example = OrderedDict(
    protocol='clickhouse',
    host='127.0.0.1',
    port=9000,
    user='root',
    password='password',
    database='database'
)

import pyodbc

from collections import OrderedDict
import pandas as pd
from mindsdb_sql import parse_sql

from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class EmpressHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Empress Embedded statements.
    """

    name = 'empress'

    def __init__(self, name: str, **kwargs):
        """
        Initializes a new instance of the Empress Embedded handler.

        Args:
            name (str): The name of the database.
            connection_data (dict): parameters for connecting to the database
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'empress'
        self.connection_args = kwargs.get('connection_data')
        self.database = self.connection_args.get('database')
        self.server = self.connection_args.get('server')
        self.user = self.connection_args.get('user')
        self.password = self.connection_args.get('password')
        self.host = self.connection_args.get('host')
        self.port = self.connection_args.get('port', 6322)
        self.connection = None
        self.is_connected = False

    def __del__(self):
        """
        Destructor for the Empress Embedded class.
        """
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Establishes a connection to the Empress Embedded server.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return self.connection

        conn_str = f"DRIVER={{Empress ODBC Interface [Default]}};Server={self.server};Port={self.port};UID={self.user};PWD={self.password};Database={self.database};"
        self.connection = pyodbc.connect(conn_str)
        self.is_connected = True
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to Empress Embedded, {e}!')
            response.error_message = str(e)
        finally:
            if response.success and need_to_close:
                self.disconnect()
            if not response.success and self.is_connected is True:
                self.is_connected = False

        return response

    def disconnect(self):
        """
        Closes the connection to the Empress Embedded server.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def native_query(self, query: str) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): SQL query to execute.
        Returns:
            HandlerResponse
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                if result := cursor.fetchall():
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame.from_records(
                            result,
                            columns=[x[0] for x in cursor.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                    connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.connection_args["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )

        if need_to_close:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        renderer = SqlalchemyRender('sqlite')

        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Gets a list of table names in the database.

        Returns:
            list: A list of table names in the database.
        """
        connection = self.connect()
        cursor = connection.cursor()
        # Execute query to get all table names
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")

        table_names = [x[0] for x in cursor.fetchall()]

        # Create dataframe with table names
        df = pd.DataFrame(table_names, columns=['table_name', 'data_type'])

        return Response(RESPONSE_TYPE.TABLE, df)

    def get_columns(self, table_name: str) -> Response:
        """
        Gets a list of column names in the specified table.

        Args:
            table_name (str): The name of the table to get column names from.

        Returns:
            list: A list of column names in the specified table.
        """
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'"
        )
        results = cursor.fetchall()

        # construct a pandas dataframe from the query results
        df = pd.DataFrame(
            results,
            columns=['column_name', 'data_type']
        )

        return Response(RESPONSE_TYPE.TABLE, df)


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Empress Embedded server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect to Empress Embedded server'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Empress Embedded server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Empress Embedded server.'
    },
    server={
        'type': ARG_TYPE.STR,
        'description': 'The server name used to authenticate with the Empress Embedded server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specify database name to connect Empress Embedded server'
    },

)

connection_args_example = OrderedDict(
    host='localhost',
    port=6322,
    user='admin',
    password='password',
    server='test',
    database='test_db'
)

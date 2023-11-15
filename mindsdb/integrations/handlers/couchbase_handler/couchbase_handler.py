from datetime import timedelta
from collections import OrderedDict
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb_sql.parser.ast.base import ASTNode
import pandas as pd

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import UnAmbiguousTimeoutException
from couchbase.options import ClusterOptions
from couchbase.exceptions import KeyspaceNotFoundException, CouchbaseException


class CouchbaseHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Couchbase statements.
    """

    name = "couchbase"
    # TODO: Check the timeout value with the sdk default time
    DEFAULT_TIMEOUT_SECONDS = 60

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data")

        self.scope = self.connection_data.get("scope") or "_default"

        self.bucket_name = self.connection_data.get("bucket")
        self.cluster = None

        self.is_connected = False

    def connect(self):
        """
        Set up connections required by the handler.

        Returns:
            The connected cluster.
        """
        if self.is_connected:
            return self.cluster

        auth = PasswordAuthenticator(
            self.connection_data.get("user"),
            self.connection_data.get("password"),
            # NOTE: If using SSL/TLS, add the certificate path.
            # We strongly reccomend this for production use.
            # cert_path=cert_path
        )

        options = ClusterOptions(auth)

        if 'cloud.couchbase.com' in self.connection_data.get("host"):
            options.apply_profile('wan_development')

            endpoint = f"couchbases://{self.connection_data.get('host')}"
        else:
            endpoint = f"couchbase://{self.connection_data.get('host')}"

        cluster = Cluster(
            endpoint,
            options,
        )

        try:
            cluster.wait_until_ready(timedelta(seconds=self.DEFAULT_TIMEOUT_SECONDS))
            self.is_connected = cluster.connected
            self.cluster = cluster
        except UnAmbiguousTimeoutException:
            self.is_connected = False
            raise

        return self.cluster

    def disconnect(self):
        """Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is False:
            return
        self.is_connected = self.cluster.connected
        return

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Couchbase bucket
        :return: success status and error message if error occurs
        """
        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            cluster = self.connect()
            result.success = cluster.connected
        except UnAmbiguousTimeoutException as e:
            log.logger.error(
                f'Error connecting to Couchbase {self.connection_data["bucket"]}, {e}!'
            )
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False
        return result

    def native_query(self, query: str) -> Response:
        """Execute a raw query against Couchbase.

        Args:
            query (str): Raw Couchbase query.

        Returns:
            HandlerResponse containing query results.
        """
        self.connect()
        bucket = self.cluster.bucket(self.bucket_name)
        cb = bucket.scope(self.scope)

        data = {}
        try:
            for collection in cb.query(query):
                for collection_name, row in collection.items():
                    if isinstance(row, dict):
                        for k, v in row.items():
                            data.setdefault(k, []).append(v)
                    else:
                        for k, v in collection.items():
                            data.setdefault(k, []).append(v)

            response = Response(
                RESPONSE_TYPE.TABLE, pd.DataFrame(data) if data else RESPONSE_TYPE.OK
            )
        except CouchbaseException as e:
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e.error_context.first_error_message),
            )

        return response

    def query(self, query: ASTNode) -> Response:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        return self.native_query(query.to_string())

    def get_tables(self) -> Response:
        """
        Get a list with of collection in database
        """
        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        unique_collections = set()
        for scope in bucket.collections().get_all_scopes():
            for collection in scope.collections:
                unique_collections.add(collection.name)
        collections = list(unique_collections)
        df = pd.DataFrame(collections, columns=["TABLE_NAME"])
        return Response(RESPONSE_TYPE.TABLE, df)

    def get_columns(self, table_name) -> Response:
        """Returns a list of entity columns
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        """

        response = Response(False)

        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        cb = bucket.scope(self.scope)

        try:
            q = f"SELECT * FROM `{table_name}` limit 1"
            row_iter = cb.query(q)
            # print(row_iter.execute())
            data = []
            for row in row_iter:
                data.extend([k, type(v).__name__] for k, v in row[table_name].items())
            df = pd.DataFrame(data, columns=["Field", "Type"])
            response = Response(RESPONSE_TYPE.TABLE, df)
        except KeyspaceNotFoundException as e:
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: {e.error_context.first_error_message}",
            )

        return response


connection_args = OrderedDict(
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the Couchbase server.",
    },
    password={
        "type": ARG_TYPE.STR,
        "description": "The password to authenticate the user with the Couchbase server.",
    },
    bucket={
        "type": ARG_TYPE.STR,
        "description": "The database/bucket name to use when connecting with the Couchbase server.",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "--your-instance--.dp.cloud.couchbase.com or IP address of the Couchbase server.",
    },
    scope={
        "type": ARG_TYPE.STR,
        "description": 'The scope use in the query context in Couchbase server. If blank, scope will be "_default".',
    },
)
connection_args_example = OrderedDict(
    host="127.0.0.1", user="root", password="password", bucket="bucket"
)

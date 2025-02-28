from collections import OrderedDict
from typing import List, Union

import pandas as pd
import psycopg
from mindsdb_sql import ASTNode, Parameter, Identifier, Update, BinaryOperation
from pgvector.psycopg import register_vector

from mindsdb.integrations.handlers.postgres_handler.postgres_handler import (
    PostgresHandler,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    VectorStoreHandler,
)
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler


# todo Issue #7316 add support for different indexes and search algorithms e.g. cosine similarity or L2 norm
class PgVectorHandler(PostgresHandler, VectorStoreHandler):
    """This handler handles connection and execution of the PostgreSQL with pgvector extension statements."""

    name = "pgvector"

    def __init__(self, name: str, **kwargs):

        super().__init__(name=name, **kwargs)
        self.connect()

    @profiler.profile()
    def connect(self) -> psycopg.connection:
        """
        Handles the connection to a PostgreSQL database instance.
        """
        self.connection = super().connect()

        with self.connection.cursor() as cur:
            try:
                # load pg_vector extension
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                log.logger.info("pg_vector extension loaded")

            except psycopg.Error as e:
                log.logger.error(
                    f"Error loading pg_vector extension, ensure you have installed it before running, {e}!"
                )
                return HandlerResponse(resp_type=RESPONSE_TYPE.ERROR, error_message=str(e))

        # register vector type with psycopg2 connection
        register_vector(self.connection)

        return self.connection

    @staticmethod
    def _translate_conditions(conditions: List[FilterCondition]) -> Union[dict, None]:
        """
        Translate filter conditions to a dictionary
        """

        if conditions is None:
            return {}

        return {
            condition.column.split(".")[-1]: {
                "op": condition.op.value,
                "value": condition.value,
            }
            for condition in conditions
        }

    @staticmethod
    def _construct_where_clause(filter_conditions=None):
        """
        Construct where clauses from filter conditions
        """
        if filter_conditions is None:
            return ""

        where_clauses = []

        for key, value in filter_conditions.items():
            if key == "embeddings":
                continue
            if value['op'].lower() == 'in':
                values = list(repr(i) for i in value['value'])
                value['value'] = '({})'.format(', '.join(values))
            where_clauses.append(f'{key} {value["op"]} {value["value"]}')

        if len(where_clauses) > 1:
            return f"WHERE{' AND '.join(where_clauses)}"
        elif len(where_clauses) == 1:
            return f"WHERE {where_clauses[0]}"
        else:
            return ""

    @staticmethod
    def _construct_full_after_from_clause(
        offset_clause: str,
        limit_clause: str,
        where_clause: str,
    ) -> str:

        return f"{where_clause} {offset_clause} {limit_clause}"

    def _build_select_query(
        self,
        table_name: str,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        offset: int = None,
    ) -> str:
        """
        given inputs, build string query
        """
        limit_clause = f"LIMIT {limit}" if limit else ""
        offset_clause = f"OFFSET {offset}" if offset else ""

        # translate filter conditions to dictionary
        filter_conditions = self._translate_conditions(conditions)

        # check if search vector is in filter conditions
        embedding_search = filter_conditions.get("embeddings", None)

        # given filter conditions, construct where clause
        where_clause = self._construct_where_clause(filter_conditions)

        # construct full after from clause, where clause + offset clause + limit clause
        after_from_clause = self._construct_full_after_from_clause(
            where_clause, offset_clause, limit_clause
        )

        if filter_conditions:

            if embedding_search:
                # if search vector, return similar rows, apply other filters after if any
                search_vector = filter_conditions["embeddings"]["value"][0]
                filter_conditions.pop("embeddings")
                return f"SELECT * FROM {table_name} ORDER BY embeddings <=> '{search_vector}' {after_from_clause}"
            else:
                # if filter conditions, return filtered rows
                return f"SELECT * FROM {table_name} {after_from_clause}"
        else:
            # if no filter conditions, return all rows
            return f"SELECT * FROM {table_name} {after_from_clause}"

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        with self.connection.cursor() as cur:
            query = self._build_select_query(table_name, conditions, limit, offset)
            cur.execute(query)

            self.connection.commit()
            result = cur.fetchall()

        result = pd.DataFrame(
            result, columns=["id", "content", "embeddings", "metadata"]
        )
        # ensure embeddings are returned as string so they can be parsed by mindsdb
        result["embeddings"] = result["embeddings"].astype(str)

        return result

    def create_table(self, table_name: str, if_not_exists=True):
        """
        Run a create table query on the pgvector database.
        """
        with self.connection.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} (id text PRIMARY KEY, content text, embeddings vector, metadata jsonb)"
            )
            self.connection.commit()

    def insert(
        self, table_name: str, data: pd.DataFrame
    ):
        """
        Insert data into the pgvector table database.
        """
        data_dict = data.to_dict(orient="list")
        transposed_data = list(zip(*data_dict.values()))

        columns = ", ".join(data.keys())
        values = ", ".join(["%s"] * len(data.keys()))

        insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        with self.connection.cursor() as cur:
            cur.executemany(insert_statement, transposed_data)
            self.connection.commit()

    def update(
        self, table_name: str, data: pd.DataFrame, key_column: str = None
    ):
        """
        Udate data into the pgvector table database.
        """

        where = None
        update_columns = {}
        # col_map = {}

        for col in data.columns:
            value = Parameter('%s')
            # col_map[col] = value

            if col == key_column:
                where = BinaryOperation(
                    op='=',
                    args=[Identifier(key_column), value]
                )
            else:
                update_columns[col] = value

        query = Update(
            table=Identifier(table_name),
            update_columns=update_columns,
            where=where
        )

        with self.connection.cursor() as cur:
            transposed_data = []
            for _, record in data.iterrows():
                row = [
                    record[col]
                    for col in update_columns.keys()
                ]
                row.append(record[key_column])
                transposed_data.append(row)

            query_str = self.renderer.get_string(query)
            cur.executemany(query_str, transposed_data)
            self.connection.commit()


        #         for key, value in col_map.items():
        #             value.value = row[key]
        #
        #
        # with self.connection.cursor() as cur:
        #     for _, row in data.iterrows():
        #         for key, value in col_map.items():
        #             value.value = row[key]
        #
        #
        #         cur.execute(query_str)
        #     self.connection.commit()

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):

        filter_conditions = self._translate_conditions(conditions)
        search_vector = filter_conditions["embedding"]["value"]

        with self.connection.cursor() as cur:

            # convert search embedding to string

            # we need to use the <-> operator to search for similar vectors,
            # so we need to convert the string to a vector and also use a threshold (e.g. 0.5)

            query = (
                f"DELETE FROM {table_name} WHERE embeddings <=> '{search_vector}'"
            )
            cur.execute(query)
            self.connection.commit()

    # def get_tables(self) -> Response:
    #     """
    #     List all tables in PostgreSQL without the system tables information_schema and pg_catalog
    #     """
    #     return PostgresHandler.get_tables(self)

    def drop_table(self, table_name: str, if_exists=True):
        """
        Run a drop table query on the pgvector database.
        """
        with self.connection.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.connection.commit()

    def get_columns(self, table_name: str) -> HandlerResponse:
        """
        get columns in a given table
        """
        return VectorStoreHandler.get_columns(table_name)

    def query(self, query: ASTNode) -> HandlerResponse:
        return VectorStoreHandler.query(self, query)


connection_args = OrderedDict(
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the PostgreSQL server.",
        "required": True,
        "label": "User",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the PostgreSQL server.",
        "required": True,
        "label": "Password",
    },
    database={
        "type": ARG_TYPE.STR,
        "description": "The database name to use when connecting with the PostgreSQL server.",
        "required": True,
        "label": "Database",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the PostgreSQL server. NOTE: use '127.0.0.1' instead of 'localhost' to connect to local server.",
        "required": True,
        "label": "Host",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The TCP/IP port of the PostgreSQL server. Must be an integer.",
        "required": True,
        "label": "Port",
    },
    schema={
        "type": ARG_TYPE.STR,
        "description": "The schema in which objects are searched first.",
        "required": False,
        "label": "Schema",
    },
    sslmode={
        "type": ARG_TYPE.STR,
        "description": "sslmode that will be used for connection.",
        "required": False,
        "label": "sslmode",
    },
)

connection_args_example = OrderedDict(
    host="127.0.0.1", port=5432, user="root", password="password", database="database"
)

from collections import OrderedDict
from typing import List, Optional

import pandas as pd

from mindsdb.integrations.handlers.chromadb_handler.settings import ChromaHandlerConfig
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    TableField,
    VectorStoreHandler,
)
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.utilities import log


def get_chromadb():
    """
    Import and return the chromadb module, using pysqlite3 if available.
    this is a hack to make chromadb work with pysqlite3 instead of sqlite3 for cloud usage
    see https://docs.trychroma.com/troubleshooting#sqlite
    """

    try:
        import sys

        __import__("pysqlite3")
        sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")
    except ImportError:
        log.logger.error(
            "[Chromadb-handler] pysqlite3 is not installed, this is not a problem for local usage"
        )  # noqa: E501

    try:
        import chromadb

        return chromadb
    except ImportError:
        raise ImportError("Failed to import chromadb.")


class ChromaDBHandler(VectorStoreHandler):
    """This handler handles connection and execution of the ChromaDB statements."""

    name = "chromadb"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.handler_storage = HandlerStorage(kwargs.get("integration_id"))
        self._client = None
        self.persist_directory = None
        self.is_connected = False

        config = self.validate_connection_parameters(name, **kwargs)

        self._client_config = {
            "chroma_server_host": config.host,
            "chroma_server_http_port": config.port,
            "persist_directory": self.persist_directory,
        }

        self.connect()

    def validate_connection_parameters(self, name, **kwargs):
        """
        Validate the connection parameters.
        """

        _config = kwargs.get("connection_data")
        _config["vector_store"] = name

        config = ChromaHandlerConfig(**_config)

        if config.persist_directory and not self.handler_storage.is_temporal:
            # get full persistence directory from handler storage
            self.persist_directory = self.handler_storage.folder_get(
                config.persist_directory
            )

        return config

    def _get_client(self):
        client_config = self._client_config
        if client_config is None:
            raise Exception("Client config is not set!")

        chromadb = get_chromadb()

        # decide the client type to be used, either persistent or httpclient
        if client_config["persist_directory"] is not None:
            return chromadb.PersistentClient(path=client_config["persist_directory"])
        else:
            return chromadb.HttpClient(
                host=client_config["chroma_server_host"],
                port=client_config["chroma_server_http_port"],
            )

    def __del__(self):
        """Close the database connection."""

        if self.is_connected is True:
            if self.persist_directory:
                # sync folder to handler storage
                self.handler_storage.folder_sync(self.persist_directory)

            self.disconnect()

    def connect(self):
        """Connect to a ChromaDB database."""
        if self.is_connected is True:
            return self._client

        try:
            self._client = self._get_client()
            self.is_connected = True
            return self._client
        except Exception as e:
            self.is_connected = False
            raise Exception(f"Error connecting to ChromaDB client, {e}!")

    def disconnect(self):
        """Close the database connection."""

        if self.is_connected is False:
            return

        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the ChromaDB database."""
        response_code = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self._client.heartbeat()
            response_code.success = True
        except Exception as e:
            log.logger.error(f"Error connecting to ChromaDB , {e}!")
            response_code.error_message = str(e)
        finally:
            if response_code.success and need_to_close:
                self.disconnect()
            if not response_code.success and self.is_connected is True:
                self.is_connected = False

        return response_code

    def _get_chromadb_operator(self, operator: FilterOperator) -> str:
        mapping = {
            FilterOperator.EQUAL: "$eq",
            FilterOperator.NOT_EQUAL: "$ne",
            FilterOperator.LESS_THAN: "$lt",
            FilterOperator.LESS_THAN_OR_EQUAL: "$lte",
            FilterOperator.GREATER_THAN: "$gt",
            FilterOperator.GREATER_THAN_OR_EQUAL: "$gte",
        }

        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by ChromaDB!")

        return mapping[operator]

    def _translate_metadata_condition(
        self, conditions: List[FilterCondition]
    ) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects a dict that can be used by ChromaDB.
        E.g.,
        [
            FilterCondition(
                column="metadata.created_at",
                op=FilterOperator.LESS_THAN,
                value="2020-01-01",
            ),
            FilterCondition(
                column="metadata.created_at",
                op=FilterOperator.GREATER_THAN,
                value="2019-01-01",
            )
        ]
        -->
        {
            "$and": [
                {"created_at": {"$lt": "2020-01-01"}},
                {"created_at": {"$gt": "2019-01-01"}}
            ]
        }
        """
        # we ignore all non-metadata conditions
        if conditions is None:
            return None
        metadata_conditions = [
            condition
            for condition in conditions
            if condition.column.startswith(TableField.METADATA.value)
        ]
        if not metadata_conditions:
            return None

        # we translate each metadata condition into a dict
        chroma_db_conditions = []
        for condition in metadata_conditions:
            metadata_key = condition.column.split(".")[-1]
            chroma_db_conditions.append(
                {
                    metadata_key: {
                        self._get_chromadb_operator(condition.op): condition.value
                    }
                }
            )

        return (
            {"$and": chroma_db_conditions}
            if len(chroma_db_conditions) > 1
            else chroma_db_conditions[0]
        )

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> HandlerResponse:
        collection = self._client.get_collection(table_name)
        filters = self._translate_metadata_condition(conditions)
        # check if embedding vector filter is present
        vector_filter = (
            []
            if conditions is None
            else [
                condition
                for condition in conditions
                if condition.column == TableField.SEARCH_VECTOR.value
            ]
        )
        vector_filter = vector_filter[0] if vector_filter else None
        id_filters = None
        if conditions is not None:
            id_filters = [
                condition.value
                for condition in conditions
                if condition.column == TableField.ID.value
            ] or None

        if vector_filter is not None:
            # similarity search
            query_payload = {
                "where": filters,
                "query_embeddings": vector_filter.value
                if vector_filter is not None
                else None,
                "include": ["metadatas", "documents", "distances"],
            }
            if limit is not None:
                query_payload["n_results"] = limit

            result = collection.query(**query_payload)
            ids = result["ids"][0]
            documents = result["documents"][0]
            metadatas = result["metadatas"][0]
            distances = result["distances"][0]
        else:
            # general get query
            result = collection.get(
                ids=id_filters,
                where=filters,
                limit=limit,
                offset=offset,
            )
            ids = result["ids"]
            documents = result["documents"]
            metadatas = result["metadatas"]
            distances = None

        # project based on columns
        payload = {
            TableField.ID.value: ids,
            TableField.CONTENT.value: documents,
            TableField.METADATA.value: metadatas,
        }

        if columns is not None:
            payload = {
                column: payload[column]
                for column in columns
                if column != TableField.EMBEDDINGS.value
            }

        # always include distance
        if distances is not None:
            payload[TableField.DISTANCE.value] = distances
        result_df = pd.DataFrame(payload)
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=result_df)

    def insert(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """
        Insert data into the ChromaDB database.
        """

        collection = self._client.get_collection(table_name)

        # drop columns with all None values

        data.dropna(axis=1, inplace=True)

        data = data.to_dict(orient="list")

        collection.add(
            ids=data[TableField.ID.value],
            documents=data.get(TableField.CONTENT.value),
            embeddings=data[TableField.EMBEDDINGS.value],
            metadatas=data.get(TableField.METADATA.value),
        )

        return Response(resp_type=RESPONSE_TYPE.OK)

    def update(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ) -> HandlerResponse:
        """
        Update data in the ChromaDB database.
        TODO: not implemented yet
        """
        return super().update(table_name, data, columns)

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ) -> HandlerResponse:
        filters = self._translate_metadata_condition(conditions)
        # get id filters
        id_filters = [
            condition.value
            for condition in conditions
            if condition.column == TableField.ID.value
        ] or None

        if filters is None and id_filters is None:
            raise Exception("Delete query must have at least one condition!")
        collection = self._client.get_collection(table_name)
        collection.delete(ids=id_filters, where=filters)
        return Response(resp_type=RESPONSE_TYPE.OK)

    def create_table(self, table_name: str, if_not_exists=True) -> HandlerResponse:
        """
        Create a collection with the given name in the ChromaDB database.
        """
        self._client.create_collection(table_name, get_or_create=if_not_exists)
        return Response(resp_type=RESPONSE_TYPE.OK)

    def drop_table(self, table_name: str, if_exists=True) -> HandlerResponse:
        """
        Delete a collection from the ChromaDB database.
        """
        try:
            self._client.delete_collection(table_name)
        except ValueError:
            if if_exists:
                return Response(resp_type=RESPONSE_TYPE.OK)
            else:
                return Response(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_message=f"Table {table_name} does not exist!",
                )

        return Response(resp_type=RESPONSE_TYPE.OK)

    def get_tables(self) -> HandlerResponse:
        """
        Get the list of collections in the ChromaDB database.
        """
        collections = self._client.list_collections()
        collections_name = pd.DataFrame(
            columns=["table_name"],
            data=[collection.name for collection in collections],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=collections_name)

    def get_columns(self, table_name: str) -> HandlerResponse:
        # check if collection exists
        try:
            _ = self._client.get_collection(table_name)
        except ValueError:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        return super().get_columns(table_name)


connection_args = OrderedDict(
    host={
        "type": ARG_TYPE.STR,
        "description": "chromadb server host",
        "required": False,
    },
    port={
        "type": ARG_TYPE.STR,
        "description": "chromadb server port",
        "required": False,
    },
    persist_directory={
        "type": ARG_TYPE.STR,
        "description": "persistence directory for chroma",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    host="localhost",
    port="8000",
    persist_directory="chroma",
)

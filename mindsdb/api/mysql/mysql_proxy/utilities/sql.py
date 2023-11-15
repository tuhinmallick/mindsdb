import copy

import duckdb
from duckdb import InvalidInputException
import numpy as np

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.planner.utils import query_traversal
from mindsdb_sql.parser.ast import (
    Select, Identifier,
    Function, Constant
)

from mindsdb.utilities import log
from mindsdb.utilities.json_encoder import CustomJSONEncoder


def query_df_with_type_infer_fallback(query_str: str, dataframes: dict):
    ''' Duckdb need to infer column types if column.dtype == object. By default it take 1000 rows,
        but that may be not sufficient for some cases. This func try to run query multiple times
        increasing butch size for type infer

        Args:
            query_str (str): query to execute
            dataframes (dict): dataframes

        Returns:
            pandas.DataFrame
            pandas.columns
    '''

    for name, value in dataframes.items():
        locals()[name] = value

    con = duckdb.connect(database=':memory:')
    for sample_size in [1000, 10000, 1000000]:
        try:
            con.execute(f'set global pandas_analyze_sample={sample_size};')
            result_df = con.execute(query_str).fetchdf()
        except InvalidInputException:
            pass
        else:
            break
    else:
        raise InvalidInputException
    description = con.description
    con.close()

    return result_df, description


def query_df(df, query, session=None):
    """ Perform simple query ('select' from one table, without subqueries and joins) on DataFrame.

        Args:
            df (pandas.DataFrame): data
            query (mindsdb_sql.parser.ast.Select | str): select query

        Returns:
            pandas.DataFrame
    """

    if isinstance(query, str):
        query_ast = parse_sql(query, dialect='mysql')
    else:
        query_ast = copy.deepcopy(query)

    if not isinstance(query_ast, Select) or not isinstance(
        query_ast.from_table, Identifier
    ):
        raise Exception(
            "Only 'SELECT from TABLE' statements supported for internal query"
        )

    table_name = query_ast.from_table.parts[0]
    query_ast.from_table.parts = ['df']

    json_columns = set()

    def adapt_query(node, is_table, **kwargs):
        if is_table:
            return
        if isinstance(node, Identifier):
            if len(node.parts) > 1:
                node.parts = [node.parts[-1]]
                return node
        if isinstance(node, Function):
            fnc_name = node.op.lower()
            if fnc_name == 'database' and len(node.args) == 0:
                cur_db = session.database if session is not None else None
                return Constant(cur_db)
            if fnc_name == 'truncate':
                # replace mysql 'truncate' function to duckdb 'round'
                node.op = 'round'
                if len(node.args) == 1:
                    node.args.append(0)
            if fnc_name == 'json_extract':
                json_columns.add(node.args[0].parts[-1])

    query_traversal(query_ast, adapt_query)

    # convert json columns
    encoder = CustomJSONEncoder()

    def _convert(v):
        if isinstance(v, (dict, list)):
            try:
                return encoder.encode(v)
            except Exception:
                pass
        return v

    for column in json_columns:
        df[column] = df[column].apply(_convert)

    render = SqlalchemyRender('postgres')
    try:
        query_str = render.get_string(query_ast, with_failback=False)
    except Exception as e:
        log.logger.error(
            f"Exception during query casting to 'postgres' dialect. Query: {str(query)}. Error: {e}"
        )
        query_str = render.get_string(query_ast, with_failback=True)

    # workaround to prevent duckdb.TypeMismatchException
    if len(df) > 0:
        if table_name.lower() in ('models', 'predictors', 'models_versions'):
            if 'TRAINING_OPTIONS' in df.columns:
                df = df.astype({'TRAINING_OPTIONS': 'string'})
        if table_name.lower() == 'ml_engines':
            if 'CONNECTION_DATA' in df.columns:
                df = df.astype({'CONNECTION_DATA': 'string'})

    result_df, description = query_df_with_type_infer_fallback(query_str, {'df': df})
    result_df = result_df.replace({np.nan: None})

    new_column_names = {}
    real_column_names = [x[0] for x in description]
    for i, duck_column_name in enumerate(result_df.columns):
        new_column_names[duck_column_name] = real_column_names[i]
    result_df = result_df.rename(
        new_column_names,
        axis='columns'
    )
    return result_df

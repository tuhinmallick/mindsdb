from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast

import pandas as pd


class CoinBaseAggregatedTradesTable(APITable):

    DEFAULT_INTERVAL = 60
    DEFAULT_SYMBOL = 'BTC-USD'

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the CoinBase API and returns it as a pandas DataFrame.

        Returns dataframe representing the CoinBase API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        conditions = extract_comparison_conditions(query.where)

        params = {
            'interval': CoinBaseAggregatedTradesTable.DEFAULT_INTERVAL,
            'symbol': CoinBaseAggregatedTradesTable.DEFAULT_SYMBOL,
        }
        for op, arg1, arg2 in conditions:
            if arg1 == 'interval' and op == '=':
                params['interval'] = arg2

            elif arg1 == 'interval' or arg1 == 'symbol' and op != '=':
                raise NotImplementedError
            elif arg1 == 'symbol':
                params['symbol'] = arg2

        return self.handler.call_coinbase_api(
            method_name='get_candle', params=params
        )

    def get_columns(self):
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'symbol',
            'low',
            'high',
            'open',
            'close',
            'volume',
            'timestamp',
            'current_time'
        ]

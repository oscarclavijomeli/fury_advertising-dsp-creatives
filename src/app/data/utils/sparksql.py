"Utilities for connecting to databases"

import pandas as pd

from melitk import logging
from melitk.connectors.sources.sparksql import SparkSQL as SparSQLConn

logger = logging.getLogger(__name__)


class SparkSQL(SparSQLConn):
    """Represents a SparlSQL connection"""

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Execute a query and load it into a pandas.DataFrame

        :param str query: SQL query to be executed

        :return: Result of the query
        :rtype: pandas.DataFrame
        """
        logger.info("Executing...")
        return pd.DataFrame(self.execute_response(query))

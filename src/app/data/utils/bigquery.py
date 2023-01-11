"Utilities for connecting to databases"

import base64
import json

import pandas as pd
from melitk import logging
from melitk.connectors.sources.bigquery import BigQuery as BigQueryConn
from melitk.melipass import get_secret

logger = logging.getLogger(__name__)


class BigQuery:
    """Represents a BigQuery connection"""

    def __init__(self) -> None:
        """Create an instance of SparkSQL connector"""

        secret_b64 = get_secret("SECRET_SB_DSP_CREAT")
        if secret_b64 is not None:
            secret_decoded = base64.b64decode(secret_b64)
            gcp_creds = json.loads(secret_decoded)
            self.database: BigQueryConn = BigQueryConn(gcp_creds)

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Execute a query and load it into a pandas.DataFrame

        :param str query: SQL query to be executed

        :return: Result of the query
        :rtype: pandas.DataFrame
        """
        logger.info("Executing...")
        return self.database.execute_response(query)

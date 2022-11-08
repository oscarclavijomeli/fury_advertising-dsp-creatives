"""
Extracts data from Presto
"""

import pickle  # nosec
import base64
from datetime import datetime, timedelta

import pandas as pd
from retry import retry

from melitk import logging
from melitk.fda2 import runtime, inventory
from melitk.melipass import get_secret
from tiger_python_helper.services.tiger_service import TigerService

from app.data.utils.sparksql import SparkSQL
from app.data.utils.load_query import load_format

logger = logging.getLogger(__name__)


@retry(EOFError, tries=6, delay=60, backoff=2)
def run_query(query: str, spark: SparkSQL) -> pd.DataFrame:
    """
    Runs a query using a Spark instance and retry if an EOFError is found

    :param str query: Query to be executed
    :spark app.data.utils.sparksql.SparkSQL spark: Spark instance to be used to connect with the database

    :return: Result of the query execution
    :rtype: pandas.DataFrame
    """
    return spark.run_query(query)


class SparkExtractor:
    """Extracts data from Presto to an artifact"""

    def __init__(self, artifact_name: str) -> None:
        """
        Loads last version of the target artifact and creates a SparkSQL instance

        :param str artifact_name: Artifact name in Fury
        """
        self.artifact_name = artifact_name

        logger.info("Initializing SparkExtractor.")
        secret_b64 = get_secret("SPARK_MLDADVERTISING")
        secret_decoded = base64.b64decode(secret_b64)
        creds = pickle.loads(secret_decoded)  # nosec
        self._spark = SparkSQL(**creds)
        logger.info("SparkExtractor initialized.")

        logger.info("Preparing input artifact...")

        if not runtime.inputs.artifacts:
            tiger_service = TigerService()
            token = tiger_service.get_user_token(**creds)
            inventory.init(f"Bearer {token}")

            artifact = next(inventory.filter(name=artifact_name))
            version_decomposed = [int(part) for part in artifact.version.split(".")]
            version_decomposed[-1] += 1
            version_decomposed_str = [str(part) for part in version_decomposed]
            self.version_output = ".".join(version_decomposed_str)
            self.output = pickle.loads(artifact.load_to_bytes())  # nosec
        else:
            self.output = pickle.loads(runtime.inputs.artifacts[artifact_name].load_to_bytes())  # nosec
        logger.info("Input artifact prepared.")

    def update(self, query_path: str, default_params: dict, day_str: str) -> None:
        """
        Updates data adding 1 day from Presto

        :param str query_path: Path of query that will be run
        :param dict default_params: Dictionary with default parameters to modify the query
        :param str day_str: Date in '%Y-%m-%d' format used to filter the information that is going to be loaded
        """
        date_format = "%Y-%m-%d"
        day = datetime.strptime(day_str, date_format)
        next_day = day + timedelta(days=1)
        next_day_str = next_day.strftime(date_format)
        _time_stamps = [f"{day_str} {i:02d}" for i in range(24)] + [f"{next_day_str} 00"]

        params = default_params.copy()

        logger.info("Iterating time stamps loading...")
        for i in range(len(_time_stamps) - 1):
            params.update({"start_date": _time_stamps[i], "end_date": _time_stamps[i + 1]})
            query = load_format(query_path, params)
            logger.info("Loading info for %s time stamp...", _time_stamps[i])
            dataframe_temp = run_query(query=query, spark=self._spark)
            logger.info("%s time stamp loaded.", _time_stamps[i])
            self.output = pd.concat([self.output, dataframe_temp])
        logger.info("Iteration finished.")

    def preprocess(self, columns: list) -> None:
        """
        Removes nulls and transforms columns in integer type columns

        :param str columns: Columns to be modified
        """
        logger.info("Removing nulls in columns...")
        for column in columns:
            self.output = self.output[~pd.isnull(self.output[column])]
        logger.info("Nulls in columns removed.")

        logger.info("Transforming column data types...")
        self.output[columns] = self.output[columns].astype(int)
        logger.info("Column data types transformed.")

    def save(self) -> None:
        """Saves the data as an artifact"""

        logger.info("Saving info as an artifact...")
        # serialize
        data_bytes = pickle.dumps(self.output)  # nosec

        if not runtime.inputs.artifacts:

            # Create fda artifact
            artifact = inventory.create_artifact(self.artifact_name, version=self.version_output, type_="fda.Bytes")
            artifact.save_from_bytes(data=data_bytes)
        else:
            runtime.outputs[self.artifact_name].save_from_bytes(data=data_bytes)

        logger.info("Artifact saved")

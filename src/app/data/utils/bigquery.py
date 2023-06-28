"""Utilities for connecting to databases"""

import base64
import json
from typing import Any, Union

import pandas as pd
from google.cloud.bigquery import Client as GoogleBigQueryClient
from google.cloud.bigquery.job import QueryJob
from google.cloud.storage.client import Blob, Client
from google.oauth2.service_account import Credentials
from melitk import logging
from melitk.bigquery import BigQueryClientBuilder
from melitk.bigquery.client import BigQueryClient
from melitk.melipass import get_secret

logger = logging.getLogger(__name__)


class GoogleStorageClient(Client):
    """Add upload_blob_from_filename and download_blob_to_filename methods to google.cloud.storage.client.Client class"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # pylint: disable=useless-super-delegation
        """Initialize parents"""
        super().__init__(*args, **kwargs)

    def upload_blob_from_filename(self, blob_or_uri: Union[str, Blob], filename: str) -> None:
        """Upload blob to Blob object or uri from local filename
        :param Union[str, Blob] blob_or_uri: Blob object or uri where 'filename' will be uploaded
        :param str filename: file path to upload
        """

        if isinstance(blob_or_uri, str):
            bucket_name = blob_or_uri.split("/")[2]
            blob_name = "/".join(blob_or_uri.split("/")[3:])

            bucket = self.get_bucket(bucket_name)
            blob_or_uri = bucket.blob(blob_name)

        blob_or_uri.upload_from_filename(filename)

    def download_blob_to_filename(self, blob_or_uri: Union[str, Blob], filename: str) -> None:
        """Download blob from Blob object or uri to local filename
        :param Union[str, Blob] blob_or_uri: Blob object or uri to download
        :param str filename: file path where 'blob_or_uri' will be downloaded
        """
        with open(filename, "wb") as file:
            self.download_blob_to_file(blob_or_uri=blob_or_uri, file_obj=file)


class BigQuery:
    """Represents a BigQuery connection"""

    def __init__(self) -> None:
        """Create an instance of SparkSQL connector"""

        secret_b64 = get_secret("SECRET_SB_DSP_CREAT")
        if secret_b64 is not None:
            self.database: BigQueryClient = DataBase("SECRET_SB_DSP_CREAT")
        else:
            self.database = None

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Execute a query and load it into a pandas.DataFrame

        :param str query: SQL query to be executed

        :return: Result of the query
        :rtype: pandas.DataFrame
        """
        logger.info("Executing...")
        return self.database.execute_response_with_df(query)


class DataBase(BigQueryClient):
    """Add upload_from_filename method to the class BigQueryConn"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # pylint: disable=useless-super-delegation
        """Initialize parents"""
        self._database = BigQueryClientBuilder().with_encoded_secret(*args, **kwargs).build()
        self._credentials_name = args[0]
        self._bq_credentials = Credentials.from_service_account_info(self._credentials)
        self._client = GoogleBigQueryClient(project=self._bq_credentials._project_id, credentials=self._bq_credentials)

    @property
    def client(self) -> GoogleBigQueryClient:
        """ "Bigquery client property"""
        return self._client

    @property
    def _credentials(self) -> Any:
        """Get credentials from secret"""
        secret_b64 = get_secret(self._credentials_name)
        secret_decoded = base64.b64decode(secret_b64)
        credentials = json.loads(secret_decoded)
        return credentials

    @property
    def database(self) -> BigQueryClient:
        """Change bigquery.client.Client by BigQueryConn class"""
        return self._database

    @property
    def google_storage_client(self) -> GoogleStorageClient:
        """Change google.cloud.storage.client.Client by GoogleStorageClient class"""
        if not getattr(self, "_google_storage_client", None):
            self._google_storage_client = GoogleStorageClient(  # pylint: disable=W0212, W0201
                project=self._bq_credentials._project_id,
                credentials=self._bq_credentials,  # pylint: disable=W0212
            )

        return self._google_storage_client

    def execute_response_with_df(self, query: str) -> pd.DataFrame:
        """
        Execute a query and load it into a pandas.DataFrame
        :param str query: SQL query to be executed
        :return: Result of the query
        :rtype: pandas.DataFrame
        """
        logger.info("Executing...")
        return self.database.query_to_df(query).df

    def execute_query(self, query: str) -> QueryJob:
        """
        Execute a query
        :param str query: SQL query to be executed
        """
        logger.info("Executing...")
        return self.client.query(query)

    def load_data_from_source(self, dataframe: pd.DataFrame, table_id: str, mode: str) -> str:
        """Append dataframe records to BQ table.
        Args:
            dataframe (DataFrame): Data to add BQ table
            table_id (dict, optional): Etl configuration.
            mode (str): Write mode. [append, replace]
        Raises:
            execution_error: Query job execution exception.
        """

        logger.info("Writing data into: %s", table_id)
        try:
            response = self.database.df_to_gbq(dataframe, table_id, mode)
            return table_id
        except Exception as execution_error:
            logger.error("error[%s]", execution_error)
            logger.info("job_info_id: %s", response.job_id)  # pylint: disable=E0601
            raise execution_error

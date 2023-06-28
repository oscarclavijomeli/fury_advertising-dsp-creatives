"""
Creates great expectations service
"""

import copy
import datetime
import logging
import os
import pathlib
from typing import Union

import pandas as pd
import ruamel
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.exceptions import DataContextError
from great_expectations.validator.validator import Validator
from melitk.melipass import get_secret
from ruamel import yaml

import great_expectations as ge
from app.conf.settings import DATASET_ID, PROJECT_ID, SECRET_NAME

logging.basicConfig(format="%(asctime)s - %(levelname)s %(name)s : %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class DataQuality:
    """
    Sets service
    """

    def __init__(
        self,
        datasource_name: str,
        conexion_type: str,
        artifact: pd.DataFrame,
        environment: str,
    ) -> None:
        """
        Create great expectations context and default runtime datasource

        :param str datasource_name: ...
        ...
        """
        os.system("echo yes | great_expectations init")  # nosec
        self.environment = environment
        self.root_directory = f"{pathlib.Path(__file__).absolute().parents[1]}/great_expectations"
        self.datasource_name = datasource_name
        self.data_artifact = artifact

        context = ge.data_context.DataContext()

        if conexion_type == "Pandas":
            self.connector_name = "pandas_runtime_connector"
            self.runtime_parameters = {"batch_data": self.data_artifact}
            datasource_yaml = f"""
name: {self.datasource_name}
class_name: Datasource
execution_engine:
    class_name: PandasExecutionEngine
data_connectors:
    pandas_runtime_connector:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - run_id
"""
        elif conexion_type == "Bigquery":
            self.connector_name = "bigquery_runtime_connector"
            self.runtime_parameters = {"query": self.data_artifact}

            bigquery_access = "".join(
                [
                    f"{PROJECT_ID[self.environment]}/{DATASET_ID[self.environment]}",
                    f"?credentials_base64={get_secret(SECRET_NAME[self.environment])}",
                ]
            )

            datasource_yaml = f"""
name: {self.datasource_name}
class_name: Datasource
execution_engine:
    class_name: SqlAlchemyExecutionEngine
    connection_string: bigquery://{bigquery_access}
data_connectors:
    bigquery_runtime_connector:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - run_id
    default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
        include_schema_name: true
"""
        context.test_yaml_config(datasource_yaml)
        context.add_datasource(**yaml.load(datasource_yaml, Loader=ruamel.yaml.Loader))

        self.context: ge.data_context.DataContext = context

    def _create_expectation_suite_if_not_exist(self) -> None:
        """
        Create expectation suite if not exist
        """

        try:
            # create expectation suite
            self.context.create_expectation_suite(
                expectation_suite_name=f"{self.datasource_name}_expectation_suite",
                overwrite_existing=False,
            )
        except DataContextError as error:
            log.info(error)

    def get_expectation_suite(self, name_expectation_suite: Union[str, None] = None) -> ExpectationSuite:
        """
        Retrieve current expectation suite
        """
        if name_expectation_suite is None:
            name_expectation_suite = self.datasource_name
        return self.context.get_expectation_suite(f"{name_expectation_suite}_expectation_suite")

    def _create_checkpoint_if_not_exist(self) -> None:
        """
        Create checkpoint if not exist.
        """

        try:
            self.context.get_checkpoint(f"{self.datasource_name}_checkpoint")
            log.info("%s checkpoint is already created", self.datasource_name)

        except ge.exceptions.CheckpointNotFoundError:
            checkpoint_config = {
                "name": f"{self.datasource_name}_checkpoint",
                "config_version": 1.0,
                "class_name": "SimpleCheckpoint",
                "run_name_template": "%Y%m%d-%H%M%S",
            }
            self.context.test_yaml_config(yaml.dump(checkpoint_config))
            self.context.add_checkpoint(**checkpoint_config)

        except Exception as exception:
            log.error("Error getting checkpoint ")
            raise exception

    def _create_batch_data(self) -> RuntimeBatchRequest:
        """
        create runtime batch request from the input pandas dataframe
        """

        batch_request = RuntimeBatchRequest(
            datasource_name=self.datasource_name,
            data_connector_name=self.connector_name,
            data_asset_name=f"{self.datasource_name}_{datetime.datetime.today().strftime('%Y%m%d')}",
            batch_identifiers={
                "run_id": f"""
                {self.datasource_name}_runtime={datetime.datetime.today().strftime('%Y%m%d')}
                """,
            },
            runtime_parameters=self.runtime_parameters,
        )

        return batch_request

    def get_validator(self) -> Validator:
        """
        Retrieving a validator object for a fine grain adjustment on the expectation suite.
        """

        self._create_expectation_suite_if_not_exist()

        validator = self.context.get_validator(
            datasource_name=self.datasource_name,
            data_connector_name=self.connector_name,
            data_asset_name=f"{self.datasource_name}_{datetime.datetime.today().strftime('%Y%m%d')}",
            batch_identifiers={
                "run_id": f"""
                {self.datasource_name}_runtime={datetime.datetime.today().strftime('%Y%m%d')}
                """,
            },
            runtime_parameters=self.runtime_parameters,
            expectation_suite_name=f"{self.datasource_name}_expectation_suite",
        )

        return validator

    def _processing_results(self, data: dict) -> dict:
        results = {}
        expectation_result = {}
        expectation = {}
        expectations = []

        for _, value in data["run_results"].items():
            results["success"] = value["validation_result"]["success"]
            results["success_statistics"] = value["validation_result"]["statistics"]

            for item in value["validation_result"]["results"]:
                expectation["expectation_type"] = item["expectation_config"]["expectation_type"]
                expectation["success"] = item["success"]

                kwargs = item["expectation_config"]["kwargs"]
                kwargs.pop("batch_id")

                if len(kwargs) > 0:
                    expectation["args"] = kwargs

                for k in item["result"].keys():
                    if type(item["result"][k]) not in [int, float, bool, str] and item["result"][k] is not None:
                        if len(item["result"][k]) > 0:
                            expectation_result[k] = item["result"][k]
                    elif item["result"][k] is not None:
                        expectation_result[k] = item["result"][k]

                expectation["expectation_results"] = expectation_result
                expectations.append(copy.deepcopy(expectation))
                expectation.clear()

            results["expectations"] = expectations

        return results

    def validate_data(self) -> dict:
        """
        Validate dataset using the input dataset when initiated the class
        or user provided dataset when calling the method.
        """

        batch_request = self._create_batch_data()

        self._create_checkpoint_if_not_exist()

        # Run expectation_suite against data
        checkpoint_result = self.context.run_checkpoint(
            checkpoint_name=f"{self.datasource_name}_checkpoint",
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": f"{self.datasource_name}_expectation_suite",
                }
            ],
        )

        return self._processing_results(checkpoint_result)

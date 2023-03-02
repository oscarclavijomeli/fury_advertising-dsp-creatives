"""Estimates parameters of the Beta distribution and save them into an artifact"""

import json
from datetime import datetime
from typing import Dict, List, Tuple, Union

import pandas as pd
from melitk import logging, metrics
from melitk.fda2 import runtime

from app.conf.settings import (
    PARAMS,
    QUERY_PATH_INSERT_DATA,
    QUERY_PATH_PRINT_CHECK,
    QUERY_PATHS,
    TAGS,
)
from app.data.utils.bigquery import BigQuery
from app.data.utils.great_expectations_service import DataQuality
from app.data.utils.load_query import load_format
from app.data.utils.params_bigquery import ParamsBigquery

logger = logging.getLogger(__name__)

sites = ["MLA", "MLB", "MLC", "MCO", "MLM", "MLU", "MEC", "MPE"]


class BetaEstimator:
    """Class to estimate parameters of the Beta distribution and save them into an artifact"""

    def __init__(self, big_query: BigQuery = BigQuery()) -> None:
        """
        Loads grouped data
        """
        bigquery = big_query or BigQuery()

        logger.info("Checking if there is historical data for the site...")
        sql = load_format(path=QUERY_PATH_PRINT_CHECK, params=PARAMS)
        self.prints = bigquery.run_query(sql)["prints"][0]

        if self.prints == 0:
            logger.info("No data for site, artifact is going to be empty.")
        else:
            logger.info("Updating data...")
            sql = load_format(path=QUERY_PATHS["insert"], params=PARAMS)
            bigquery.run_query(sql)
            logger.info("Data updated.")

            logger.info("Grouping data...")
            sql = load_format(QUERY_PATHS["group"], params=PARAMS)
            self.input = bigquery.run_query(sql)
            logger.info("Data grouped.")

            self.sanity_check_results: Dict[str, str] = {}
            self.process = ""

    def calculate_beta_parameters(self, divider: float = PARAMS["divider"]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Calculates alpha and beta parameters

        :param float divider: Number to divide the performance to calculate the default parameters
        """

        # First, calculate the parameters for existing creative ids
        logger.info("Calculating alpha and beta parameters for existing creatives...")
        creatives = self.input.copy()
        creatives["alpha"] = creatives["n_clicks"] + 1
        creatives["beta"] = creatives["n_prints"] - creatives["n_clicks"] + 1
        logger.info("Alpha and beta parameters calculated for existing creatives.")

        # Second, calculate the default parameters for new creatives
        logger.info("Calculating alpha and beta parameters for a new creative...")
        grouped_by_lineitem = creatives.groupby(["campaign_id", "line_item_id"])
        line_items = (
            grouped_by_lineitem.agg({"creative_id": "count", "n_clicks": "sum", "n_prints": "sum", "hours": "max"})
            .reset_index()
            .rename({"creative_id": "n_creatives"}, axis=1)
        )
        line_items["alpha"] = line_items["n_clicks"] / (line_items["n_creatives"] * line_items["hours"] * divider) + 1
        line_items["beta"] = (line_items["n_prints"] - line_items["n_clicks"]) / (
            line_items["n_creatives"] * line_items["hours"] * divider
        ) + 1
        line_items["epsilon"] = PARAMS["epsilon"]

        logger.info("Alpha and beta parameters calculated for a new creative.")
        return creatives, line_items

    def run_sanity_checks(self, dataframe: pd.DataFrame, load_results: bool = True) -> None:
        """Applies great expectation to the output dataframe"""

        logger.info("Creates great expectations...")

        if "hour" in dataframe.columns:
            dq_checker = DataQuality(datasource_name="track1", conexion_type="Pandas", artifact=dataframe, environment=PARAMS["env"])
            self.process = "init_data"
        else:
            dq_checker = DataQuality(datasource_name="track2", conexion_type="Pandas", artifact=dataframe, environment=PARAMS["env"])
            self.process = "artifact_data"
        validator = dq_checker.get_validator()

        if "hour" in dataframe.columns:
            validator.expect_column_values_to_be_of_type("hour", "str")
            validator.expect_column_values_to_be_of_type("int_hour", "int")
            validator.expect_column_values_to_be_of_type("site", "str")
            validator.expect_column_values_to_be_of_type("n_prints", "int")
            validator.expect_column_values_to_be_of_type("n_clicks", "int")
            validator.expect_column_values_to_not_be_null("site")
            validator.expect_column_values_to_be_in_set("site", sites)
            validator.expect_column_values_to_not_be_null("n_prints")
            validator.expect_column_values_to_not_be_null("n_clicks")
            validator.expect_column_values_to_be_between("n_prints", min_value=0)
            validator.expect_column_values_to_be_between("n_clicks", min_value=0)
            validator.expect_column_values_to_be_between("int_hour", min_value=0, max_value=23)
            validator.expect_column_pair_values_A_to_be_greater_than_B("n_prints", "n_clicks", or_equal=True)
            validator.expect_compound_columns_to_be_unique(["ds", "hour", "campaign_id", "line_item_id", "creative_id"])
        else:
            validator.expect_column_values_to_be_of_type("alpha", "int")
            validator.expect_column_values_to_be_of_type("beta", "int")
            validator.expect_column_values_to_not_be_null("alpha")
            validator.expect_column_values_to_not_be_null("beta")
            validator.expect_column_values_to_be_between("alpha", min_value=1)
            validator.expect_column_values_to_be_between("beta", min_value=1)
            validator.expect_compound_columns_to_be_unique(["campaign_id", "line_item_id", "creative_id"])

        validator.expect_column_values_to_be_of_type("campaign_id", "int")
        validator.expect_column_values_to_be_of_type("line_item_id", "int")
        validator.expect_column_values_to_be_of_type("creative_id", "int")
        validator.expect_column_values_to_not_be_null("campaign_id")
        validator.expect_column_values_to_not_be_null("line_item_id")
        validator.expect_column_values_to_not_be_null("creative_id")
        validator.save_expectation_suite(discard_failed_expectations=False)

        results = dq_checker.validate_data()
        self.sanity_check_results = results

        for expectation in results["expectations"]:
            if expectation["success"] is False:
                metrics.record_count(
                    f"advertising.dsp_creatives.{self.process}_sanitycheck_etl_metrics.error_validations",
                    tags={"env": TAGS["env"]},
                )
                break

        logger.info("Inserting data validation results...")

        process_total = "dsp_creativos_" + self.process

        if load_results:
            params = ParamsBigquery(results=results, process=process_total, datetime_param=datetime.now()).create_params()
            query = load_format(path=QUERY_PATH_INSERT_DATA, params=params)
            BigQuery().run_query(query)

    @staticmethod
    def dataframe2json(
        creatives: pd.DataFrame, line_items: pd.DataFrame
    ) -> List[Dict[str, Union[int, List[Dict[str, Union[int, List[Dict[str, int]], Dict[str, float], float]]]]]]:
        """Transforms output type from pandas.DataFrame to dictionary"""

        creative_list = creatives.groupby(["campaign_id", "line_item_id"])[["creative_id", "alpha", "beta"]].apply(
            lambda x: x.set_index("creative_id").to_dict(orient="index")
        )
        creative_list = pd.DataFrame(
            creative_list.map(
                lambda x: [{"creative_id": int(key), "alpha": value["alpha"], "beta": value["beta"]} for key, value in x.items()]
            )
        )

        creative_list = creative_list.rename({0: "creatives"}, axis=1).join(line_items.set_index(["campaign_id", "line_item_id"]))

        creative_list = (
            creative_list.reset_index()
            .groupby("campaign_id")[["line_item_id", "creatives", "alpha", "beta", "epsilon"]]
            .apply(lambda x: x.set_index("line_item_id").to_dict(orient="index"))
        )
        creative_list = pd.DataFrame(
            creative_list.map(
                lambda x: [
                    {
                        "line_item": int(key),
                        "creatives": value["creatives"],
                        "default": {"alpha": value["alpha"], "beta": value["beta"]},
                        "epsilon": value["epsilon"],
                    }
                    for key, value in x.items()
                ]
            )
        )

        creative_list = creative_list.rename({0: "line_items"}, axis=1).to_dict(orient="index")
        result = [{"campaign_id": int(key), "line_items": value["line_items"]} for key, value in creative_list.items()]

        return result

    def save(self, creative_list: list, artifact_name: str) -> None:
        """
        Saves the alpha and beta parameters as an artifact

        :param str artifact_name: Output artifact name in Fury
        """

        logger.info("Saving info as an artifact...")
        # serialize
        data_bytes = json.dumps(creative_list).encode("utf-8")
        runtime.outputs[artifact_name].save_from_bytes(data=data_bytes)
        logger.info("Artifact saved.")

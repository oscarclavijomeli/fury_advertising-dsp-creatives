"""Estimates parameters of the Beta distribution and save them into an artifact"""

import json
from datetime import datetime
import pytz

import pandas as pd

from melitk import logging
from melitk.fda2 import runtime

from app.data.utils.bigquery import BigQuery
from app.data.utils.load_query import load_format
from app.conf.settings import DEFAULT_PARAMS, QUERY_PATH, TIME_TO_UPDATE

logger = logging.getLogger(__name__)
bigquery = BigQuery()


class BetaEstimator:
    """Class to estimate parameters of the Beta distribution and save them into an artifact"""

    def __init__(self) -> None:
        """
        Loads grouped data
        """

        logger.info("Updating data...")
        sql = load_format(path=QUERY_PATH, params=DEFAULT_PARAMS)
        bigquery.run_query(sql)
        logger.info("Data updated.")

        logger.info("Loading input.")

        sql = """
        SELECT TIMESTAMP_MILLIS(last_modified_time)
        FROM `meli-bi-data.SBOX_DSPCREATIVOS.__TABLES__`
        WHERE table_id = "BQ_PRINTS_CLICKS_PER_DAY"
        """
        timestamp = bigquery.run_query(sql).values[0, 0]
        if (datetime.now().astimezone(pytz.UTC) - timestamp.to_pydatetime()).seconds > TIME_TO_UPDATE:
            raise Exception(f"Data was not updated in the last {TIME_TO_UPDATE} seconds.")

        self.input = bigquery.run_query("SELECT * FROM meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS")
        logger.info("Input loaded.")

        self.output = pd.DataFrame()

    def calculate_beta_parameters(self, divider: float) -> None:
        """
        Calculates alpha and beta parameters

        :param float divider: Number to divide the performance to calculate the default parameters
        """

        # First, calculate the parameters for existing creative ids
        logger.info("Calculating alpha and beta parameters for existing creatives...")
        beta_parameters = self.input.copy()
        beta_parameters["alpha"] = beta_parameters["n_clicks"] + 1
        beta_parameters["beta"] = beta_parameters["n_prints"] - beta_parameters["n_clicks"] + 1
        logger.info("Alpha and beta parameters calculated for existing creatives.")

        # Second, calculate the default parameters for new creatives
        logger.info("Calculating alpha and beta parameters for a new creative...")
        grouped_by_lineitem = beta_parameters.groupby(["campaign_id", "line_item_id"])
        default_beta_parameters = (
            grouped_by_lineitem.agg({"creative_id": "count", "n_clicks": "sum", "n_prints": "sum", "days": "max"})
            .reset_index()
            .rename({"creative_id": "n_creatives"}, axis=1)
        )
        default_beta_parameters["alpha"] = (
            default_beta_parameters["n_clicks"] / (default_beta_parameters["n_creatives"] * default_beta_parameters["days"] * divider) + 1
        )
        default_beta_parameters["beta"] = (default_beta_parameters["n_prints"] - default_beta_parameters["n_clicks"]) / (
            default_beta_parameters["n_creatives"] * default_beta_parameters["days"] * divider
        ) + 1
        default_beta_parameters["creative_id"] = "default"

        return_columns = ["campaign_id", "line_item_id", "creative_id", "alpha", "beta"]
        self.output = (
            pd.concat([beta_parameters[return_columns], default_beta_parameters[return_columns]])
            .sort_values(by=["campaign_id", "line_item_id", "creative_id"])
            .reset_index(drop=True)
        )
        logger.info("Alpha and beta parameters calculated for a new creative.")

    def dataframe2dictionary(self) -> None:
        """Transforms output type from pandas.DataFrame to dictionary"""

        logger.info("Transforming output to dictionary...")
        creative_dictionary = self.output.groupby(["campaign_id", "line_item_id"])[["creative_id", "alpha", "beta"]].apply(
            lambda x: x.set_index("creative_id").to_dict(orient="index")
        )
        lineitem_dictionary = (
            creative_dictionary.reset_index()
            .set_index("line_item_id")
            .groupby(
                [
                    "campaign_id",
                ]
            )
            .agg(dict)
        )
        campaign_dictionary = {row[0]: row[1].values[0] for row in lineitem_dictionary.iterrows()}
        self.output = campaign_dictionary
        logger.info("Output transformed.")

    def save(self, artifact_name: str) -> None:
        """
        Saves the alpha and beta parameters as an artifact

        :param str artifact_name: Output artifact name in Fury
        """

        logger.info("Saving info as an artifact...")
        # serialize
        data_bytes = json.dumps(self.output).encode("utf-8")
        runtime.outputs[artifact_name].save_from_bytes(data=data_bytes)
        logger.info("Artifact saved.")

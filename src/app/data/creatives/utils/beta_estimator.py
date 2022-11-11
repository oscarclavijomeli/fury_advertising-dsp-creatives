"""Estimates parameters of the Beta distribution and save them into an artifact"""

import json
import pickle  # nosec
from datetime import datetime

import pandas as pd

from melitk import logging
from melitk.fda2 import runtime

logger = logging.getLogger(__name__)


class BetaEstimator:
    """Class to estimate parameters of the Beta distribution and save them into an artifact"""

    def __init__(self, artifact_name: str) -> None:
        """
        Loads input artifact

        :param str artifact_name: Input artifact name in Fury
        """

        logger.info("Loading input artifact.")
        self.input = pickle.loads(runtime.inputs.artifacts[artifact_name].load_to_bytes())  # nosec
        self.input = self.input[self.input["content_source"] == "DSP"]
        logger.info("Input artifact loaded.")

        self.output = pd.DataFrame()

    def calculate_beta_parameters(self, divider: float) -> None:
        """
        Calculates alpha and beta parameters

        :param float divider: Number to divide the performance to calculate the default parameters
        """

        logger.info("Formating dates...")
        dataframe = self.input.copy()
        dataframe["cdate"] = dataframe["cday"].map(lambda cday: datetime.strptime(cday, "%Y-%m-%d"))
        logger.info("Dates formated.")

        # First, calculate the parameters for existing creative ids
        logger.info("Calculating alpha and beta parameters for existing creatives...")
        grouped_by_creative = dataframe.groupby(["campaign_id", "line_item_id", "creative_id"])
        beta_parameters = (
            grouped_by_creative.agg({"n_clicks": "sum", "n_prints": "sum", "cdate": lambda cdate: (cdate.max() - cdate.min()).days + 1})
            .reset_index()
            .rename({"cdate": "days"}, axis=1)
        )
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
        logger.info("Artifact saved")

"""Ingests last day data"""

import time
from datetime import datetime


import pandas as pd

from melitk import logging, metrics
from retry import retry

from app.conf.settings import OUTPUT_ARTIFACT_NAME, PARAMS, QUERY_PATH_GREAT, TAGS
from app.data.creatives.utils.beta_estimator import BetaEstimator
from app.data.utils.bigquery import BigQuery
from app.data.utils.load_query import load_format

logger = logging.getLogger(__name__)
bigquery = BigQuery()


@retry(tries=3, backoff=60)
def run_etl() -> None:
    """Creates output artifact"""

    total_start_time = time.perf_counter()
    try:
        logger.info("Initialize beta Estimator.")
        beta_estimator = BetaEstimator()

        if beta_estimator.prints == 0:
            beta_estimator.save(creative_list=[{}], artifact_name=OUTPUT_ARTIFACT_NAME)

        else:

            logger.info("Applying sanity checks to initial data.")
            sql = load_format(path=QUERY_PATH_GREAT, params=PARAMS)
            initial_data = bigquery.run_query(sql)

            if len(initial_data) == 0:
                columns = [
                    "ds",
                    "hour",
                    "site",
                    "campaign_id",
                    "line_item_id",
                    "creative_id",
                    "sample_type",
                    "n_prints",
                    "n_clicks",
                    "int_hour",
                ]
                initial_data = pd.DataFrame(columns=columns)

            beta_estimator.run_sanity_checks(dataframe=initial_data)

            logger.info("Computing beta parameters.")
            creatives, line_items = beta_estimator.calculate_beta_parameters(divider=PARAMS["divider"])

            logger.info("Applying sanity checks to final data.")
            beta_estimator.run_sanity_checks(dataframe=creatives)

            logger.info("Formating data to json.")
            creative_list = beta_estimator.dataframe2json(creatives=creatives, line_items=line_items)

            logger.info("Saving new version of the artifact...")
            beta_estimator.save(creative_list=creative_list, artifact_name=OUTPUT_ARTIFACT_NAME)
            logger.info("Output artifact created.")

        metrics.record_count("advertising.dsp.etl.beta_estimation.success", tags=TAGS)

    except Exception as exception:  # pylint: disable=broad-except
        logger.info("Excecution time: %s", datetime.now().strftime("%Y-%m-%dT%H:%M"))
        logger.exception("Exception running task: %s", exception)
        metrics.record_count("advertising.dsp.etl.beta_estimation.error", tags=TAGS)

    finally:
        total_end_time = time.perf_counter()
        metrics.record_count(
            "advertising.dsp.etl.beta_estimation.total_execution_time", increment=(total_end_time - total_start_time), tags=TAGS
        )


if __name__ == "__main__":
    run_etl()

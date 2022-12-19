"""Ingests last day data"""

import time
from datetime import datetime

from retry import retry

from melitk import logging
from melitk import metrics

from app.data.creatives.utils.beta_estimator import BetaEstimator
from app.conf.settings import OUTPUT_ARTIFACT_NAME, DIVIDER, TAGS

logger = logging.getLogger(__name__)


@retry(tries=3, backoff=60)
def run_etl() -> None:
    """Creates output artifact"""

    total_start_time = time.perf_counter()
    try:
        logger.info("Downloading last version of the input artifact...")
        beta_estimator = BetaEstimator()

        logger.info("Loading data from the last day...")
        beta_estimator.calculate_beta_parameters(divider=DIVIDER)

        logger.info("Applying sanity checks...")
        beta_estimator.run_sanity_checks()

        logger.info("Transforming id columns in integers...")
        beta_estimator.dataframe2dictionary()

        logger.info("Saving new version of the artifact...")
        beta_estimator.save(artifact_name=OUTPUT_ARTIFACT_NAME)

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

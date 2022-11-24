"""Ingests last day data"""

from melitk import logging

from app.data.creatives.utils.beta_estimator import BetaEstimator
from app.conf.settings import OUTPUT_ARTIFACT_NAME, DIVIDER

logger = logging.getLogger(__name__)


def run_etl() -> None:
    """Creates output artifact"""

    logger.info("Downloading last version of the input artifact...")
    beta_estimator = BetaEstimator()

    logger.info("Loading data from the last day...")
    beta_estimator.calculate_beta_parameters(divider=DIVIDER)

    logger.info("Transforming id columns in integers...")
    beta_estimator.dataframe2dictionary()

    logger.info("Saving new version of the artifact...")
    beta_estimator.save(artifact_name=OUTPUT_ARTIFACT_NAME)

    logger.info("Output artifact created.")


if __name__ == "__main__":
    run_etl()

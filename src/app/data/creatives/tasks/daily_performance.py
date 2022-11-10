"""Ingests last day data"""

from melitk import logging

from app.data.creatives.utils.spark_extractor import SparkExtractor
from app.conf.settings import QUERY_PATH, ARTIFACT_NAME, DEFAULT_PARAMS, DATE_STR, COLUMNS

logger = logging.getLogger(__name__)


def run_etl() -> None:
    """Ingest last day data"""

    logger.info("Downloading last version of the artifact...")
    spark_extractor = SparkExtractor(artifact_name=ARTIFACT_NAME)

    logger.info("Loading data from the last day...")
    spark_extractor.update(query_path=QUERY_PATH, default_params=DEFAULT_PARAMS, day_str=DATE_STR)

    logger.info("Transforming id columns in integers...")
    spark_extractor.preprocess(columns=COLUMNS)

    logger.info("Saving new version of the artifact...")
    spark_extractor.save()

    logger.info("Updating finished.")


if __name__ == "__main__":
    run_etl()

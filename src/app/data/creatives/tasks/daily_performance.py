"""Ingests last day data"""

from melitk import logging

from app.data.creatives.utils.spark_extractor import SparkExtractor
from app.conf.settings import QUERY_PATH, ARTIFACT_NAME, DEFAULT_PARAMS, LAST_DATE_STR, COLUMNS_TO_INT, COLUMNS_TO_DROP_DUPLICATES

logger = logging.getLogger(__name__)


def run_etl() -> None:
    """Ingest last day data"""

    logger.info("Downloading last version of the artifact...")
    spark_extractor = SparkExtractor(artifact_name=ARTIFACT_NAME)

    logger.info("Loading data from the last day...")
    spark_extractor.update(query_path=QUERY_PATH, default_params=DEFAULT_PARAMS, last_day_str=LAST_DATE_STR)

    logger.info("Transforming id columns in integers and dropping duplicates...")
    spark_extractor.preprocess(columns_to_int=COLUMNS_TO_INT, columns_to_drop_duplicates=COLUMNS_TO_DROP_DUPLICATES)

    logger.info("Saving new version of the artifact...")
    spark_extractor.save()

    logger.info("Updating finished.")


if __name__ == "__main__":
    run_etl()

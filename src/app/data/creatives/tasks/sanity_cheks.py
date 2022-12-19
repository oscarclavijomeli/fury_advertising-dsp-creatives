"""Validates data per day"""
from datetime import datetime

from melitk import logging

from app.data.utils.bigquery import BigQuery
from app.data.utils.params_bigquery import ParamsBigquery
from app.data.utils.great_expectations_service import DataQuality
from app.data.utils.load_query import load_format
from app.conf.settings import DEFAULT_PARAMS, QUERY_PATH_GREAT, QUERY_PATH_INSERT_DATA

logger = logging.getLogger(__name__)


def run_sanity_checks() -> None:
    """
    Run sanity checks validations
    """
    sql = """
    (SELECT MAX(ds) FROM
    meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_DAY)
    """
    timestamp = BigQuery().run_query(sql).values[0, 0]
    start_date = timestamp.strftime("%Y-%m-%d")

    DEFAULT_PARAMS["ds"] = start_date

    sql = load_format(path=QUERY_PATH_GREAT, params=DEFAULT_PARAMS)

    logger.info("Running the wrapper in bigquery ...")
    dq_checker = DataQuality("track2", "Bigquery", sql, "test")

    validator = dq_checker.get_validator()

    logger.info("Generate great expectations ...")
    validator.expect_column_values_to_be_of_type("site", "STRING")
    validator.expect_column_values_to_be_of_type("campaign_id", "INTEGER")
    validator.expect_column_values_to_be_of_type("line_item_id", "INTEGER")
    validator.expect_column_values_to_be_of_type("creative_id", "INTEGER")
    validator.expect_column_values_to_be_of_type("n_prints", "INTEGER")
    validator.expect_column_values_to_be_of_type("n_clicks", "INTEGER")
    validator.expect_column_values_to_not_be_null("site")
    validator.expect_column_values_to_not_be_null("campaign_id")
    validator.expect_column_values_to_not_be_null("line_item_id")
    validator.expect_column_values_to_not_be_null("creative_id")
    validator.expect_column_values_to_not_be_null("n_prints")
    validator.expect_column_values_to_not_be_null("n_clicks")
    validator.expect_column_values_to_be_between("n_prints", min_value=0)
    validator.expect_column_values_to_be_between("n_clicks", min_value=0)
    validator.expect_column_pair_values_A_to_be_greater_than_B("n_prints", "n_clicks", or_equal=True)
    validator.expect_compound_columns_to_be_unique(["campaign_id", "line_item_id", "creative_id"])
    validator.save_expectation_suite()

    logger.info("Finish the validations.")
    results = dq_checker.validate_data()

    params = ParamsBigquery(results, "dsp_creativos_init_data", datetime.now()).create_params()

    query = load_format(path=QUERY_PATH_INSERT_DATA, params=params)

    BigQuery().run_query(query)


if __name__ == "__main__":
    run_sanity_checks()

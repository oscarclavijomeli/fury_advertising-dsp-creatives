"""
General app's level configuration items.
"""

from datetime import date, timedelta

MODEL_HYPER_PARAMETERS = {"foo": "bar"}
THE_ANSWER_TO_LIFE_THE_UNIVERSE_AND_EVERYTHING = 42

DEFAULT_PARAMS = {"start_date": "2022-09-01", "end_date": "2022-11-02", "site": "MLA", "click_window": 24 * 60 * 60}
ARTIFACT_NAME = "clicks_prints_per_day"
LAST_DATE_STR = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
QUERY_PATH = "src/app/data/creatives/queries/performance_per_hour.sql"
COLUMNS_TO_INT = ["campaign_id", "line_item_id", "creative_id"]
COLUMNS_TO_DROP_DUPLICATES = ["cday", "chour", "content_source", "campaign_id", "line_item_id", "creative_id"]
OUTPUT_ARTIFACT_NAME = "test_cr_parameters"
DIVIDER = 24

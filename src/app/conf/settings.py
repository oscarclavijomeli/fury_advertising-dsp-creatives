"""
General app's level configuration items.
"""

from datetime import date

MODEL_HYPER_PARAMETERS = {"foo": "bar"}
THE_ANSWER_TO_LIFE_THE_UNIVERSE_AND_EVERYTHING = 42

DEFAULT_PARAMS = {"end_date": date.today().strftime("%Y-%m-%d"), "site_id": "MLA", "click_window": 24 * 60 * 60}
QUERY_PATH = "src/app/data/creatives/queries/daily_insert.sql"
OUTPUT_ARTIFACT_NAME = "test_cr_parameters"
DIVIDER = 24
TIME_TO_UPDATE = 2 * 60 * 60

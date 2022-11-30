"""
General app's level configuration items.
"""

from datetime import date

MODEL_HYPER_PARAMETERS = {"foo": "bar"}
THE_ANSWER_TO_LIFE_THE_UNIVERSE_AND_EVERYTHING = 42

DEFAULT_PARAMS = {"end_date": date.today().strftime("%Y-%m-%d"), "site_id": "MLA", "click_window": 24 * 60 * 60}
QUERY_PATHS = {"insert": "src/app/data/creatives/queries/daily_insert.sql", "group": "src/app/data/creatives/queries/group.sql"}
OUTPUT_ARTIFACT_NAME = "test_cr_parameters"
DIVIDER = 24
TIME_TO_UPDATE = 2 * 60 * 60
TAGS = {"application": "advertising-dsp-creatives", "env": "prod"}
PROJECT_ID = {"test": "meli-bi-data", "prod": "meli-bi-data"}
DATASET_ID = {"test": "SBOX_DSPCREATIVOS", "prod": "SBOX_DSPCREATIVOS"}
SECRET_NAME = {"test": "SECRET_SB_DSP_CREAT", "prod": "SECRET_SB_DSP_CREAT"}

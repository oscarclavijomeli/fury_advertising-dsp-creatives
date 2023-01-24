"""
General app's level configuration items.
"""

from melitk.fda2 import runtime

MODEL_HYPER_PARAMETERS = {"foo": "bar"}
THE_ANSWER_TO_LIFE_THE_UNIVERSE_AND_EVERYTHING = 42

DEFAULT_PARAMS = {
    "start_date": "2022-12-07",
    "site_id": "MLA",
    "time_zone": "-04",
    "click_window": 24 * 60 * 60,
    "artifact_name": "ctr_beta_parameters",
    "env": "test",
    "divider": 1,
    "epsilon": 0.2,
    "application": "advertising-dsp-creatives",
}
PARAMS = runtime.inputs.parameters if dict(runtime.inputs.parameters) else DEFAULT_PARAMS
OUTPUT_ARTIFACT_NAME = f"{PARAMS['env']}_{PARAMS['artifact_name']}"
TAGS = {"application": PARAMS["application"], "env": PARAMS["env"], "site": PARAMS["site_id"]}

QUERY_PATHS = {"insert": "src/app/data/creatives/queries/daily_insert.sql", "group": "src/app/data/creatives/queries/group.sql"}
QUERY_PATH_GREAT = "src/app/data/creatives/queries/daily_great_expectations.sql"
QUERY_PATH_INSERT_DATA = "src/app/data/creatives/queries/data_validation_insert.sql"
PROJECT_ID = {"test": "meli-bi-data", "prod": "meli-bi-data"}
DATASET_ID = {"test": "SBOX_DSPCREATIVOS", "prod": "SBOX_DSPCREATIVOS"}
SECRET_NAME = {"test": "SECRET_SB_DSP_CREAT", "prod": "SECRET_SB_DSP_CREAT"}

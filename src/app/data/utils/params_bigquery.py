"""
Create data to insert into bigquery
"""
from datetime import datetime

from app.conf.settings import PARAMS


class ParamsBigquery:
    "Generate de date to insert into bigquery"

    def __init__(self, results: dict, process: str, datetime_param: datetime) -> None:
        """
        Gets information to create params
        """

        self.results = results
        self.process = process
        self.date = datetime_param.strftime("%Y-%m-%d")

    def create_params(self) -> dict:
        """Create params with the data"""

        values_insert = []
        for values in self.results["expectations"]:
            insert_data = (
                values["expectation_type"],
                values["success"],
                str(values["args"].values()).replace("dict_values", ""),
            )
            values_insert.append(insert_data)

        params = {
            "execution_date": self.date,
            "process": self.process,
            "success": self.results["success"],
            "success_statistics": tuple(self.results["success_statistics"].values()),
            "expectations": values_insert,
            "site": PARAMS["site_id"],
        }

        return params

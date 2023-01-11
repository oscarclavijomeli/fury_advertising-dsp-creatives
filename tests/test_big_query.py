"""
Unit and integration tests for BigQuery module.

"""
import pytest


class TestBigQueryModule:
    """Example tests for your ETL process. Replace with you own relevan tests."""

    def test_raises_if_run_fails(self, get_mocked_big_query_module):
        e = get_mocked_big_query_module("run_query")
        e.run_query.side_effect = Exception

        with pytest.raises(Exception):
            e.run_query()

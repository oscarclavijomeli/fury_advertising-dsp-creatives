"""Unittest to test BetaEstimator utils class"""
from typing import Any

import pytest
from pytest_mock import MockFixture

from src.app.data.creatives.utils.beta_estimator import BetaEstimator
from tests.generate_mock_data import N, generate_mock_data

mocked_data = generate_mock_data()


@pytest.fixture(name="bigquery_mock")
def fixture_bigquery_mock(mocker: MockFixture) -> Any:
    """Get mocked bigquery connection"""
    return mocker.patch("src.app.data.utils.bigquery.BigQuery")


class BetaEstimatorNoInitClass(BetaEstimator):
    """Synthetic class created to avoid the init method of the original class"""

    def __init__(self, bigquery_mock=None) -> None:
        """Override init method of the original class"""
        super().__init__(big_query=bigquery_mock)
        self.input = mocked_data


@pytest.fixture
def get_mocked_estimator(mocker, bigquery_mock):
    """Get mocked estimator"""

    def _create_mocked_estimator(*args):
        """Patch the object"""
        model = BetaEstimatorNoInitClass(bigquery_mock=bigquery_mock)
        for attr in args:
            mocker.patch.object(model, attr)
        return model

    return _create_mocked_estimator


@pytest.fixture
def mock_estimator(mocker, get_mocked_estimator):
    """Mocks de BetaEstimator that uses the train module"""

    def _mock_model(*args):
        """Patch the model"""
        model = get_mocked_estimator(*args)
        mocker.patch("app.data.creatives.utils.beta_estimator.BetaEstimator", return_value=model)
        return model

    return _mock_model


@pytest.fixture
def mocked_run_calculate_beta_parameters(mocker):
    """Patch the calculate parameters function"""
    return mocker.patch("app.data.creatives.utils.beta_estimator.BetaEstimator.calculate_beta_parameters")


class TestRunBetaEstimator:
    """Test class for beta estimator"""

    def test_calculate_beta_parameters_is_called(self, mock_estimator, bigquery_mock) -> None:
        """Test that the function is correctly called"""
        bigquery_mock.query.return_value.to_dataframe.return_value = mocked_data
        test_estimator = mock_estimator("calculate_beta_parameters", "dataframe2json")
        test_estimator.calculate_beta_parameters()
        assert test_estimator.calculate_beta_parameters.called

    def test_dataframe2json_is_called(self, mock_estimator, mocked_run_calculate_beta_parameters) -> None:
        """Test that the function is correctly called"""
        test_estimator = mock_estimator("calculate_beta_parameters", "dataframe2json")
        mocked_run_calculate_beta_parameters.return_value = mocked_data, mocked_data
        test_estimator.dataframe2json(creatives=mocked_data, line_items=mocked_data)
        assert test_estimator.dataframe2json.called

    def test_run_sanity_checks_works(self, mock_estimator, bigquery_mock) -> None:
        """Test that the function is correctly working on sanity checks"""
        bigquery_mock.run_query.return_value.to_dataframe.return_value = mocked_data
        mock_estimator = mock_estimator("calculate_beta_parameters", "dataframe2json")
        test_estimator = BetaEstimatorNoInitClass(bigquery_mock)
        creatives, _ = test_estimator.calculate_beta_parameters()
        mock_estimator.run_sanity_checks(creatives, load_results=False)
        assert mock_estimator.sanity_check_results is not None, "Returning None results"
        assert isinstance(mock_estimator.sanity_check_results, dict), "Returning something not a dictionary"
        assert list(mock_estimator.sanity_check_results.keys()) == ["success", "success_statistics", "expectations"], "Returning wrong keys"
        # Manual adds to test the complete data validation function
        creatives["ds"] = "today"
        creatives["hour"] = 1
        creatives["int_hour"] = 1
        mock_estimator.run_sanity_checks(creatives, load_results=False)
        assert mock_estimator.sanity_check_results is not None, "Returning None results"
        assert isinstance(mock_estimator.sanity_check_results, dict), "Returning something not a dictionary"
        assert list(mock_estimator.sanity_check_results.keys()) == ["success", "success_statistics", "expectations"], "Returning wrong keys"

    def test_calculate_beta_parameters_works(self, bigquery_mock) -> None:
        """Test that the function is correctly called and works as expected"""
        bigquery_mock.run_query.return_value.to_dataframe.return_value = mocked_data
        test_estimator = BetaEstimatorNoInitClass(bigquery_mock)
        creatives, line_items = test_estimator.calculate_beta_parameters()

        # Assertions fot the parameters calculation
        assert creatives.shape[0] == N, "Not returning the expected number of rows"
        assert creatives["creative_id"].nunique() == N, "Not returning the expected number of  creatives"
        assert (creatives[["alpha", "beta"]] > 0).all().all(), "A alpha/beta parameter is wrong calculated"

        creative_list = test_estimator.dataframe2json(creatives=creatives, line_items=line_items)

        # Assertions fot the json object creation
        assert test_estimator.sanity_check_results is not None, "None sanity check results"
        assert test_estimator.input is not None, "None estimator input"
        assert list(creative_list[0].keys()) == ["campaign_id", "line_items"], "Bad output for the main keys"
        assert list(creative_list[0]["line_items"][0].keys()) == [
            "line_item",
            "creatives",
            "default",
            "epsilon",
        ], "Bad output for the line item creatives"
        for creative in creative_list[0]["line_items"][0]["creatives"]:
            assert list(creative.keys()) == ["creative_id", "alpha", "beta"], f"Bad output for the creative {creative}"
            assert creative["alpha"] <= creative["beta"], f"Bad parameters calculation (realistic) for the creative {creative}"

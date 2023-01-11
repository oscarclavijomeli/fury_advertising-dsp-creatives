"""Test for the DataQuality class"""
import unittest
from unittest.mock import patch

from ruamel import yaml

from src.app.data.utils.great_expectations_service import DataQuality
from tests.generate_mock_data import generate_mock_data

mocked_data = generate_mock_data()


class TestDataQuality(unittest.TestCase):
    """Test class"""

    def test_bigquery(self) -> None:
        """Test bigquery approach"""
        with patch("great_expectations.data_context") as mock_ge:

            mock_dq = DataQuality("prueba", "Bigquery", mocked_data, "test")
            self.assertEqual(len(mock_ge.mock_calls), 3)
            mock_dq.get_validator()
            with open("tests/validation_result.yml") as file:
                data = yaml.load(file)
            mock_dq.validate_data()
            mock_dq._processing_results(data)
            mock_dq.get_expectation_suite("path_expectation_suite")
            mock_dq.get_expectation_suite()
            self.assertEqual(len(mock_ge.mock_calls), 12)
            self.assertEqual(len(mock_ge.method_calls), 1)

    def test_pandas(self) -> None:
        """Test pandas approach"""
        with patch("great_expectations.data_context") as mock_ge:

            mock_dq = DataQuality("prueba", "Pandas", mocked_data, "test")
            self.assertEqual(len(mock_ge.mock_calls), 3)
            mock_dq.get_validator()
            with open("tests/validation_result.yml") as file:
                data = yaml.load(file)
            mock_dq.validate_data()
            mock_dq._processing_results(data)
            mock_dq.get_expectation_suite()
            self.assertEqual(len(mock_ge.mock_calls), 11)
            self.assertEqual(len(mock_ge.method_calls), 1)


if __name__ == "__main__":
    unittest.main(exit=False)

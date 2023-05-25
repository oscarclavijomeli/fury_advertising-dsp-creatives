"""
Unit and integration tests for your ETL code.

"""
from app.data.training_dataset import (
    serialize_dataset,
    unserialize_dataset,
    InvalidDataError,
)


import pytest


def test_data_marshalling():
    test_data = b"so long, and thanks for all the fish"
    assert test_data == unserialize_dataset(serialize_dataset(test_data))


class TestMyETLRunTask:
    """Example tests for your ETL process. Replace with you own relevant tests."""

    def test_generated_data_is_validated(self, get_mocked_etl):
        e = get_mocked_etl("save_as_fda_artifact", "generate_data", "validate_data")
        e.run_task()
        e.validate_data.assert_called_with(e.generate_data.return_value)

    def test_artifact_is_saved_if_validation_succeeds(self, get_mocked_etl):
        e = get_mocked_etl("save_as_fda_artifact", "generate_data")
        e.run_task()
        e.save_as_fda_artifact.assert_called_with(e.generate_data.return_value)

    def test_raises_if_validation_fails(self, get_mocked_etl):
        e = get_mocked_etl("validate_data")
        e.validate_data.side_effect = InvalidDataError

        with pytest.raises(InvalidDataError):
            e.run_task()

    def test_artifact_not_saved_if_validation_fails(self, get_mocked_etl):
        e = get_mocked_etl("save_as_fda_artifact", "validate_data")
        e.validate_data.side_effect = InvalidDataError

        with pytest.raises(InvalidDataError):
            e.run_task()

        e.save_as_fda_artifact.assert_not_called()

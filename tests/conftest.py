"""Config test"""
import pytest

from app.data.training_dataset import MyETL
from app.model.dummy import DummyModel
from src.app.data.utils.bigquery import BigQuery


@pytest.fixture
def get_mocked_etl(mocker):
    def _create_mocked_etl(*args):
        mocked = MyETL()
        for attr in args:
            mocker.patch.object(mocked, attr)
        return mocked

    return _create_mocked_etl


@pytest.fixture
def get_mocked_model(mocker):
    def _create_mocked_model(*args):
        model = DummyModel()
        for attr in args:
            mocker.patch.object(model, attr)
        return model

    return _create_mocked_model


@pytest.fixture
def get_mocked_big_query_module(mocker):
    def _create_mocked_big_query_module(*args):
        mocked = BigQuery()
        for attr in args:
            mocker.patch.object(mocked, attr)
        return mocked

    return _create_mocked_big_query_module

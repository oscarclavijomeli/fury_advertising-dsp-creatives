import pytest

from app.data.training_dataset import MyETL
from app.model.dummy import DummyModel


@pytest.fixture
def get_mocked_etl(mocker):
    def _create_mocked_etl(*args):
        e = MyETL()
        for attr in args:
            mocker.patch.object(e, attr)
        return e
    return _create_mocked_etl

@pytest.fixture
def get_mocked_model(mocker):
    def _create_mocked_model(*args):
        model = DummyModel()
        for attr in args:
            mocker.patch.object(model, attr)
        return model
    return _create_mocked_model

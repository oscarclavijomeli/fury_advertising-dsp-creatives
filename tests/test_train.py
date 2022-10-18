"""
Unit and integration tests for your Training code.
"""
import pytest

from app.model.train import do_train, main, InvalidRuntime


SERIALIZED_INPUT = b"so long, and thanks for all the fish"
DESERIALIZED_INPUT = b"so long, and thanks for all the fish"

SERIALIZED_MODEL = b"so long, and thanks for all the fish"


@pytest.fixture
def mock_model(mocker, get_mocked_model):
    """Mocks de DummyModel that uses the train module"""
    def _mock_model(*args):
        model = get_mocked_model(*args)
        mocker.patch("app.model.train.DummyModel", return_value=model)
        return model
    return _mock_model


@pytest.fixture
def mocked_unserialize_dataset(mocker):
    return mocker.patch("app.model.train.unserialize_dataset")


class TestTrain:

    def test_do_train_calls_model_train_with_deserialized_input(self, mock_model, mocked_unserialize_dataset):
        mocked_unserialize_dataset.return_value = DESERIALIZED_INPUT
        model = mock_model("train", "validate_training", "serialize")

        do_train(SERIALIZED_INPUT)

        model.train.assert_called_with(DESERIALIZED_INPUT)

    def test_do_train_validates_the_training(self, mock_model, mocked_unserialize_dataset):
        model = mock_model("train", "validate_training", "serialize")

        do_train(SERIALIZED_INPUT)

        assert model.validate_training.called

    def test_do_train_returns_serialized_model(self, mock_model, mocked_unserialize_dataset):
        model = mock_model("train", "validate_training", "serialize")
        model.serialize.return_value = SERIALIZED_MODEL

        serialized_model = do_train(SERIALIZED_INPUT)
        assert serialized_model == SERIALIZED_MODEL

    def test_main_raises_exception_when_the_runtime_is_undefined(self, mocker):
        mocker.patch("app.model.train.runtime", None)
        with pytest.raises(InvalidRuntime):
            main()

from app.model.dummy import DummyModel
from app.mpi.bridge import load, predict


def test_load_uses_mymodel_load(mocker, tmpdir):
    fake_model_file = tmpdir.join("test")
    fake_model_file.write("test model data")
    mocker.patch.object(DummyModel, "load")

    fake_model = load(fake_model_file.strpath)
    DummyModel.load.assert_called_once_with(b"test model data")
    assert fake_model == DummyModel.load.return_value


def test_predict_uses_model_predict_on_data(mocker):
    fake_model = mocker.MagicMock()
    prediction = predict(fake_model, "fake data")
    fake_model.predict.assert_called_once_with("fake data")
    assert prediction == fake_model.predict.return_value

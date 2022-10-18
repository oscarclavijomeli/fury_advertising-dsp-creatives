from app.model.dummy import DummyModel


def load(path):
    with open(path, "rb") as model_file:
        model_instance = DummyModel.load(model_file.read())
    return model_instance


def predict(model, data):
    return model.predict(data)

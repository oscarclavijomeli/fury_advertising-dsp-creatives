import pickle

from melitk import logging

from app.conf.settings import THE_ANSWER_TO_LIFE_THE_UNIVERSE_AND_EVERYTHING


logger = logging.getLogger(__name__)


class InvalidModelError(Exception):
    pass


class DummyModel:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def train(self, dataset):
        """Train and test the model instance, from the given dataset."""
        # Implement whatever technique suits you better (dataset partition, cross-validation, etc.)
        return self

    def serialize(self):
        """Serialize current model instance into a stream of bytes."""
        # pickle.dumps is used as an example. Choose the serialization technique that works for you.
        return pickle.dumps(self)

    @staticmethod
    def load(bytes_data):
        """Load a serialized model instance from the given file."""

        logger.info("Loading model...")
        # pickle.load is used as an example. Choose the serialization technique that works for you.
        return pickle.loads(bytes_data)

    def predict(self, input):
        """Obtain the model's inference from the given input."""
        # Here we would use sklearn's `transform` operation or other technology's equivalent.
        return THE_ANSWER_TO_LIFE_THE_UNIVERSE_AND_EVERYTHING

    def validate_training(self):
        """Validate that the training was successful."""
        # Consider the MACHINE LEARNING QUALITY FRAMEWORK:
        # https://sites.google.com/mercadolibre.com.co/mlqframework/home
        if self.predict("foo") != THE_ANSWER_TO_LIFE_THE_UNIVERSE_AND_EVERYTHING:
            raise InvalidModelError("The model is not performing as expected.")

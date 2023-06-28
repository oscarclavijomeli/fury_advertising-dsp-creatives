import pickle

from melitk import logging
from melitk.fda2 import runtime

logger = logging.getLogger(__name__)


def serialize_dataset(dataset: object) -> bytes:
    """Serialize the given dataset (object) into a stream of bytes."""
    # The dataset can be a pickle, json, parquet, pandas... whatever you need.
    # Then, choose the serialization technique that better works for you
    return pickle.dumps(dataset)


def unserialize_dataset(stream: bytes) -> object:
    """Decode the given stream of bytes into a dataset (object)."""
    # The return value can be a pickle, json, parquet, pandas or whatever you need.
    return pickle.loads(stream)  # Unserialize based on your `serialize` function.


class InvalidDataError(Exception):
    pass


class MyETL:
    """Encapsulate ETL process to generate the training dataset."""

    def run_task(self):
        """Run the full ETL process and persist it as an FDA ETL output file."""

        dataset = self.generate_data()

        try:
            self.validate_data(dataset)
        except InvalidDataError as e:
            logger.error("The generated dataset is not valid: {}".format(e))
            raise
        else:
            self.save_as_fda_artifact(dataset)

    def generate_data(self):
        """Data extraction and transformation."""
        # Typically, here you'll connect to data-sources to generate a training dataset.
        return []

    def validate_data(self, data):
        """Validate the dataset's integrity."""
        # Consider the MACHINE LEARNING QUALITY FRAMEWORK:
        # https://sites.google.com/mercadolibre.com.co/mlqframework/home
        if len(data) != 0:
            raise InvalidDataError("Expected an empty dataset but got something else.")

    def save_as_fda_artifact(self, data):
        # This is how we manage the outputs
        dataset_artifact = runtime.outputs["example_training_dataset"]
        dataset_artifact.save_from_bytes(data=serialize_dataset(data))

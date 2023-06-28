from melitk import logging
from melitk.fda2 import runtime

from app.conf.settings import MODEL_HYPER_PARAMETERS
from app.data.training_dataset import unserialize_dataset
from app.model.dummy import DummyModel

logger = logging.getLogger(__name__)


class InvalidRuntime(Exception):
    pass


def do_train(artifact_data):
    """Train the project's model using the dataset specified as a runtime argument."""

    model = DummyModel(**MODEL_HYPER_PARAMETERS)
    dataset = unserialize_dataset(artifact_data)
    model.train(dataset)
    model.validate_training()
    logger.info("Trained DummyModel with params: {}".format(str(MODEL_HYPER_PARAMETERS)))
    return model.serialize()


def main():
    if runtime is None:
        logger.warning("Invalid runtime. Not running in an FDA 2 Task. Training won't run.")
        raise InvalidRuntime()
    else:
        raw_training_data = runtime.inputs.artifacts["example_training_dataset"].load_to_bytes()
        model_data = do_train(raw_training_data)
        runtime.outputs["example_model"].save_from_bytes(data=model_data)


if __name__ == "__main__":
    main()

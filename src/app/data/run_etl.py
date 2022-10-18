import sys

from melitk import logging
from melitk.fda2 import runtime


from app.data.training_dataset import MyETL


logger = logging.getLogger(__name__)


if __name__ == '__main__':

    if runtime is None:
        logger.warning("Invalid runtime. Not running in an FDA 2 Task. Dataset not generated.")
        sys.exit(1)

    etl = MyETL()
    etl.run_task()

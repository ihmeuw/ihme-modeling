import sys
import logging
from hybridizer.database import acause_from_id
from hybridizer.hybridizer import Hybridizer
import hybridizer.log_utilities as logs


def main():

    user, model_version_id, global_model_version_id, developed_model_version_id, conn_def = \
        sys.argv[1:6]

    model_version_id = int(model_version_id)
    global_model_version_id = int(global_model_version_id)
    developed_model_version_id = int(developed_model_version_id)
    acause = acause_from_id(model_version_id, conn_def)

    logs.setup_logging(model_version_id, acause, 'hybridizer')
    logger = logging.getLogger(__name__)

    h = Hybridizer(model_version_id, global_model_version_id,
                   developed_model_version_id, conn_def)
    logger.info("Running hybrid model.")
    h.run_hybridizer()


if __name__ == '__main__':
    main()

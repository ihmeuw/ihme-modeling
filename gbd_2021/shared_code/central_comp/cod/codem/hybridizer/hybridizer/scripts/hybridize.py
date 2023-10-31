import logging
import sys

from codem.reference import paths

import hybridizer.log_utilities as logs
from hybridizer import emails
from hybridizer.database import update_model_status
from hybridizer.hybridizer import Hybridizer


def main():

    user, model_version_id, global_model_version_id, datarich_model_version_id, conn_def = \
        sys.argv[1:6]

    model_version_id = int(model_version_id)
    global_model_version_id = int(global_model_version_id)
    datarich_model_version_id = int(datarich_model_version_id)
    base_dir = paths.get_base_dir(model_version_id=model_version_id, conn_def=conn_def)

    logs.setup_logging(base_dir)
    logger = logging.getLogger(__name__)

    h = Hybridizer(model_version_id, global_model_version_id,
                   datarich_model_version_id, conn_def, base_dir)
    try:
        h.run_hybridizer()
        update_model_status(model_version_id, 1, conn_def)
        emails.send_success_email(model_version_id,
                                  global_model_version_id,
                                  datarich_model_version_id,
                                  user, h.gbd_round_id, conn_def)
    except:
        update_model_status(model_version_id, 7, conn_def)
        log_dir = logs.get_log_dir(base_dir)
        emails.send_failed_email(model_version_id,
                                 global_model_version_id,
                                 datarich_model_version_id,
                                 user, log_dir, h.gbd_round_id, conn_def)



if __name__ == '__main__':
    main()

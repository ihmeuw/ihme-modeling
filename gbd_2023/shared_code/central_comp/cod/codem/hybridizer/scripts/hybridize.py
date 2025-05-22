import logging
import traceback

import hybridizer.log_utilities as logs
from hybridizer import emails
from hybridizer.database import update_model_status
from hybridizer.hybridizer import Hybridizer
from hybridizer.reference import paths
from hybridizer.utilities import get_args, translate_to_perd

logger = logging.getLogger(__name__)


def main() -> None:
    """Run a CODEm hybrid model."""
    args = get_args()
    user = args.user
    model_version_id = args.model_version_id
    global_model_version_id = args.global_model_version_id
    datarich_model_version_id = args.datarich_model_version_id
    release_id = args.release_id
    conn_def = args.conn_def

    base_dir = paths.get_base_dir(model_version_id=model_version_id, conn_def=conn_def)
    logs.setup_logging(base_dir)

    h = Hybridizer(
        model_version_id,
        global_model_version_id,
        datarich_model_version_id,
        release_id,
        conn_def,
        base_dir,
    )
    try:
        h.run_hybridizer()
        update_model_status(model_version_id, 1, conn_def)
        logger.info("Attempting translation to point estimates and reduced draws")
        translate_to_perd(model_version_id=model_version_id, release_id=release_id)
        emails.send_success_email(
            model_version_id,
            global_model_version_id,
            datarich_model_version_id,
            user,
            release_id,
            conn_def,
        )
    except Exception:
        logging.error(traceback.format_exc())
        update_model_status(model_version_id, 7, conn_def)
        log_dir = logs.get_log_dir(base_dir)
        emails.send_failed_email(
            model_version_id,
            global_model_version_id,
            datarich_model_version_id,
            user,
            log_dir,
            release_id,
            conn_def,
        )


if __name__ == "__main__":
    main()

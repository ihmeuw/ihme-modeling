import logging
import json

from codem.joblaunch.args import get_step_args
from codem.metadata.step_metadata import STEP_IDS
from codem.metadata.model_task import ModelTask
from codem.reference.log_config import setup_logging
from codem.joblaunch.step_profiling import inspect_all_inputs

logger = logging.getLogger(__name__)


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['InputData'])

    logger.info("Initiating the knockout generation.")
    t = ModelTask(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        old_covariates_mvid=args.old_covariates_mvid,
        debug_mode=args.debug_mode,
        cores=args.cores,
        step_id=STEP_IDS['InputData'],
        make_inputs=False,
        make_ko=False
    )
    logger.info("Finished with input creation.")
    t.alerts.alert("Done creating inputs.")

    logger.info("Updating job metadata parameters.")
    with open(t.model_paths.JOB_METADATA, 'r') as json_file:
        logger.info("Reading inputs json.")
        inputs_info = json.load(json_file)
        logger.info(f"{inputs_info}")

    inputs_info.update(inspect_all_inputs(t.model_metadata))
    with open(t.model_paths.JOB_METADATA, 'w') as outfile:
        logger.info("Writing inputs json.")
        logger.info(f"{inputs_info}")
        json.dump(inputs_info, outfile)

import logging

from codem.joblaunch.args import get_step_args
from codem.metadata.step_metadata import STEP_IDS
from codem.metadata.model_task import ModelTask
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['GenerateKnockouts'])

    logger.info("Initiating the knockout generation.")
    t = ModelTask(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        old_covariates_mvid=args.old_covariates_mvid,
        debug_mode=args.debug_mode,
        cores=args.cores,
        step_id=STEP_IDS['GenerateKnockouts'],
        make_inputs=False,
        make_ko=True
    )
    t.alerts.alert("Done creating knockouts.")
    logger.info("Finished with knockout creation.")

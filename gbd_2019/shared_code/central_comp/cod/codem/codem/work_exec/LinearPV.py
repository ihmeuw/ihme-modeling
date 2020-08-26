import logging

from codem.work_exec.PV import PV
from codem.joblaunch.args import get_step_args
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['LinearPV'])

    logger.info("Initiating Linear Model PV.")
    t = PV(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores,
        step_id=STEP_IDS['LinearPV']
    )
    t.alerts.alert("Calculating submodel predictive validity for linear models.")
    t.calculate_submodel_pv(input_object_name='linear_models_linear',
                            output_object_prefix='linear_models')
    t.save_outputs()
    t.alerts.alert("Done calculating submodel predictive validity for linear models.")

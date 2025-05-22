import logging

from codem.joblaunch.args import get_step_args
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging
from codem.work_exec.PV import PV

logger = logging.getLogger(__name__)


def main():
    args = get_step_args()
    setup_logging(
        model_version_id=args.model_version_id,
        step_id=STEP_IDS["SpacetimePV"],
        conn_def=args.conn_def,
    )
    logger.info("Initiating Spacetime PV.")
    t = PV(
        model_version_id=args.model_version_id,
        conn_def=args.conn_def,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores,
        step_id=STEP_IDS["SpacetimePV"],
    )
    t.alerts.alert("Calculating submodel predictive validity for spacetime models.")
    t.calculate_submodel_pv(
        input_object_name="st_models_gp", output_object_prefix="st_models"
    )
    t.save_outputs()
    t.alerts.alert("Done calculating submodel predictive validity for spacetime models.")

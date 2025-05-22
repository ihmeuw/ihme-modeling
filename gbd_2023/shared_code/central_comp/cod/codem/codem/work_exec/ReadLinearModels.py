import logging

from codem.joblaunch.args import get_step_args
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging
from codem.work_exec.ReadModels import ReadModels

logger = logging.getLogger(__name__)


def main():
    args = get_step_args()
    setup_logging(
        model_version_id=args.model_version_id,
        step_id=STEP_IDS["ReadLinearModels"],
        conn_def=args.conn_def,
    )

    logger.info("Initiating reading linear models.")
    t = ReadModels(
        model_version_id=args.model_version_id,
        conn_def=args.conn_def,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores,
        step_id=STEP_IDS["ReadLinearModels"],
    )
    t.alerts.alert("Reading linear models")
    t.read_models(
        input_json_path=t.model_paths.JSON_FILES["linear"],
        output_object_name="linear_models_linear",
    )
    t.save_outputs()
    t.alerts.alert("Done reading linear models")

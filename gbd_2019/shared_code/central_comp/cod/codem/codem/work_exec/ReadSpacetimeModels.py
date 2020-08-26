import logging

from codem.joblaunch.args import get_step_args
from codem.work_exec.ReadModels import ReadModels
from codem.reference.log_config import setup_logging
from codem.metadata.step_metadata import STEP_IDS

logger = logging.getLogger(__name__)


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['ReadSpacetimeModels'])

    logger.info("Initiating reading spacetime models.")
    t = ReadModels(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores,
        step_id=STEP_IDS['ReadSpacetimeModels']
    )
    t.alerts.alert("Reading spacetime models")
    t.read_models(input_json_path=t.model_paths.JSON_FILES['spacetime'],
                  output_object_name='st_models_linear')
    t.pickled_outputs['st_models_linear'].delete_mixed_model_parameters()
    t.save_outputs()
    t.alerts.alert("Done reading spacetime models")

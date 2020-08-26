import logging

from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)


class ApplySpacetimeSmoothing(ModelTask):
    def __init__(self, **kwargs):
        """
        Apply space-time smoothing to only the linear models
        that are going through space-time-smoothing.

        Eventually this can be parallelized over holdout but
        for now it's in the same spot.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['ApplySpacetimeSmoothing'])

    def apply_spacetime_smoothing(self):
        logger.info("Applying space-time smoothing.")
        self.pickled_outputs['st_models_spacetime'] = self.pickled_inputs['st_models_linear']
        del self.pickled_inputs['st_models_linear']
        self.pickled_outputs['st_models_spacetime'].apply_smoothing(
            self.model_metadata.data_frame,
            self.model_metadata.ko_data,
            self.model_metadata.model_parameters['omega_age_smooth'],
            self.model_metadata.model_parameters['lambda_time_smooth'],
            self.model_metadata.model_parameters['lambda_time_smooth_nodata'],
            self.model_metadata.model_parameters['zeta_space_smooth'],
            self.model_metadata.model_parameters['zeta_space_smooth_nodata']
        )
        logger.info("Resetting residuals after space-time smoothing.")
        self.pickled_outputs['st_models_spacetime'].reset_residuals(
            self.model_metadata.data_frame,
            self.model_metadata.ko_data,
            self.pickled_inputs['response_list']
        )


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['ApplySpacetimeSmoothing'])

    logger.info("Initiating spacetime smoothing.")
    t = ApplySpacetimeSmoothing(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.alerts.alert("Initiating spacetime smoothing")
    t.apply_spacetime_smoothing()
    t.save_outputs()
    t.alerts.alert("Done with spacetime smoothing")

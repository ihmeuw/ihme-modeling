import logging
import numexpr as ne

from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)


class ApplyGPSmoothing(ModelTask):
    def __init__(self, **kwargs):
        """
        Apply Gaussian process smoothing to the space-time predictions
        across all knockouts. Eventually this can be parallelized
        over knockouts. For now it's in the same spot.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['ApplyGPSmoothing'])

    def apply_gp_smoothing(self):
        logger.info("Applying GP Smoothing")
        self.pickled_outputs['st_models_gp'] = self.pickled_inputs['st_models_spacetime']
        del self.pickled_inputs['st_models_spacetime']
        self.pickled_outputs['st_models_gp'].gpr_all(
            data_frame=self.model_metadata.data_frame,
            knockouts=self.model_metadata.ko_data,
            response_list=self.pickled_inputs['response_list'],
            scale=self.model_metadata.model_parameters['gpr_year_corr'],
            decomp_step_id=self.model_metadata.model_parameters['decomp_step_id'],
            cores=self.cores
        )


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['ApplyGPSmoothing'])

    logger.info("Initiating GPR smoothing.")
    ne.set_num_threads(1)
    t = ApplyGPSmoothing(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.alerts.alert("Applying GP Smoothing")
    t.apply_gp_smoothing()
    t.save_outputs()
    t.alerts.alert("Done with GP Smoothing")

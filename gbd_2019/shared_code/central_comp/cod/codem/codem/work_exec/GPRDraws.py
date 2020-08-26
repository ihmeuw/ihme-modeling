import logging
import numexpr as ne

from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)


class GPRDraws(ModelTask):
    def __init__(self, **kwargs):
        """
        Creates draws from the space-time-GPR model lists
        that are input as 'st_models_id'.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['GPRDraws'])

    def make_draws(self):
        logger.info("Creating GPR draws.")
        self.pickled_outputs['st_models_draws'] = self.pickled_inputs['st_models_id']
        self.pickled_inputs['st_models_id'] = None

        self.pickled_outputs['st_models_draws'].all_gpr_draws2(
            df=self.model_metadata.data_frame,
            knockouts=self.model_metadata.ko_data,
            response_list=self.pickled_inputs['response_list'],
            scale=self.model_metadata.model_parameters['gpr_year_corr'],
            linear_floor=self.model_metadata.model_parameters['linear_floor_rate']
        )

        self.pickled_outputs['st_models_draws'].del_space_time()


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['GPRDraws'])
    ne.set_num_threads(1)
    logger.info("Initiating GPR draws.")
    t = GPRDraws(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.alerts.alert("Creating GPR draws.")
    t.make_draws()
    t.save_outputs()
    t.alerts.alert("Done creating GPR draws.")

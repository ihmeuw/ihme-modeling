import logging
import pandas as pd
import numexpr as ne

from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)


class LinearDraws(ModelTask):
    def __init__(self, **kwargs):
        """
        Creates draws from the linear model objects.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['LinearDraws'])

    def make_draws(self):
        logger.info("Creating linear draws.")
        self.pickled_outputs['linear_models_draws'] = self.pickled_inputs['linear_models_id']
        self.pickled_inputs['linear_models_id'] = None

        self.pickled_outputs['linear_models_draws'].all_linear_draws(
            df=pd.concat([self.model_metadata.data_frame,
                          self.model_metadata.covariates], axis=1),
            linear_floor=self.model_metadata.model_parameters['linear_floor_rate'],
            response_list=self.pickled_inputs['response_list']
        )


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['LinearDraws'])

    ne.set_num_threads(1)
    logger.info("Initiating linear draws.")
    t = LinearDraws(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.alerts.alert("Creating linear model draws")
    t.make_draws()
    t.save_outputs()
    t.alerts.alert("Done creating linear model draws")

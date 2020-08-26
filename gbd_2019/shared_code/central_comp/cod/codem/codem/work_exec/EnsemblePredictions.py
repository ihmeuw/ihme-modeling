import logging
import os
import pandas as pd
import numpy as np
import numexpr as ne

from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.db_write import submodels
from codem.reference.log_config import setup_logging
from codem.ensemble import PV

logger = logging.getLogger(__name__)


class EnsemblePredictions(ModelTask):
    def __init__(self, **kwargs):
        """
        Calculates coverage of the model using the draws from both linear models
        and space-time models. These predictive validity metrics will eventually
        be saved to the database.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['EnsemblePredictions'])

        self.data_frame = self.model_metadata.data_frame.copy()
        self.data_frame = self.data_frame[self.model_metadata.model_parameters['keep_vars']]

        self.pickled_outputs['model_pv'] = self.pickled_inputs['model_pv']
        self.all_st_predictions = self.pickled_inputs['st_models_draws'].all_models[-1]
        self.all_linear_predictions = self.pickled_inputs['linear_models_draws'].all_models[-1]

        self.ln_rate_nsv = self.all_st_predictions.nsv.ln_rate_nsv.values

        draw_id = self.pickled_inputs['st_models_draws'].draw_id + self.pickled_inputs['linear_models_draws'].draw_id
        self.pickled_outputs['draw_id'] = draw_id

        self.draws = PV.concat2([self.all_st_predictions.draw_preds,
                                 self.all_linear_predictions.draw_preds])

    def calculate_oos_coverage(self):
        logger.info("Calculating ensemble OOS coverage.")
        self.alerts.alert("Calculating ensemble OOS coverage")
        oos_cov = PV.coverage_out(
            st_models=self.pickled_inputs['st_models_draws'],
            lin_models=self.pickled_inputs['linear_models_draws'],
            knockouts=self.model_metadata.ko_data,
            df=self.model_metadata.data_frame
        )
        self.pickled_outputs['model_pv']['pv_coverage_out'] = oos_cov

    def calculate_is_coverage(self):
        logger.info("Calculating in sample coverage.")
        self.alerts.alert("Calculating ensemble in sample coverage")
        is_cov = PV.single_coverage_out(
            draw_preds=self.draws,
            df=self.model_metadata.data_frame,
            ln_rate_nsv=self.ln_rate_nsv,
            ko_vector=self.model_metadata.all_ko
        )
        self.pickled_outputs['model_pv']['pv_coverage_in'] = is_cov

    def write_pv(self):
        logger.info("Writing model predictive validity to cod.model_version.")
        self.alerts.alert("Writing predictive validity to the cod.model_version table")
        tags = ["pv_rmse_in", "pv_rmse_out", "pv_coverage_in", "pv_coverage_out",
                "pv_trend_in", "pv_trend_out", "pv_psi"]
        values = [
            self.pickled_outputs['model_pv']['pv_rmse_in'],
            self.pickled_outputs['model_pv']['pv_rmse_out'],
            self.pickled_outputs['model_pv']['pv_coverage_in'],
            self.pickled_outputs['model_pv']['pv_coverage_out'],
            self.pickled_outputs['model_pv']['pv_trend_in'],
            self.pickled_outputs['model_pv']['pv_trend_out'],
            self.pickled_outputs['model_pv']['best_psi'],
        ]
        for i in range(len(tags)):
            submodels.write_model_pv(
                tag=tags[i],
                value=float(values[i]),
                model_version_id=self.model_version_id,
                db_connection=self.db_connection
            )

    def add_draws_to_df(self, truncate=False, percentile=99):
        logger.info("Adding draws to data frame.")
        self.draws = np.exp(self.draws) * self.data_frame["pop"].values[:, np.newaxis]
        if truncate:
            self.draws = submodels.truncate_draws(self.draws, percent=percentile)
        draw_df = pd.DataFrame(self.draws)
        draw_df.columns = \
            ["draw_%d" % i for i in range(0, draw_df.shape[1])]
        self.data_frame = pd.concat([self.data_frame, draw_df], axis=1)
        self.data_frame.drop_duplicates(["year", "location_id", "age"], inplace=True)
        self.data_frame.reset_index(drop=True, inplace=True)

    def write_draws(self):
        """
        Write the most granular results to an HDF file.
        Make the keys a location_id year for ease of access in cod correct use.
        Includes all of the draws from codem as well as some other variables such as age, sex,
        population, envelope.
        """
        logger.info("Writing draws.")
        self.alerts.alert(f"Writing the draws "
                          f"to {self.model_paths.DRAW_FILE.format(sex=self.model_metadata.model_parameters['sex'])}")
        if not os.path.exists("draws"):
            os.makedirs("draws")
        self.data_frame['sex'] = self.model_metadata.model_parameters['sex_id']
        df2 = self.data_frame[self.model_metadata.model_parameters['draw_cols']]
        df2 = df2.rename(columns={"age": "age_group_id",
                                  "sex": "sex_id",
                                  "year": "year_id",
                                  "region": "region_id",
                                  "super_region": "super_region_id"})
        df2["measure_id"] = self.model_metadata.model_parameters['deaths_measure_id']
        df2["cause_id"] = self.model_metadata.model_parameters['cause_id']

        df2.location_id = df2.location_id.astype(int)
        df2.age_group_id = df2.age_group_id.astype(int)
        df2.year_id = df2.year_id.astype(int)

        df2.to_hdf(
            path_or_buf=self.model_paths.DRAW_FILE.format(
                sex=self.model_metadata.model_parameters['sex']
            ),
            key='data',
            mode='w',
            format='table',
            data_columns=['location_id', 'year_id', 'age_group_id']
        )
        pd.DataFrame(
            {"keys": ['location_id', 'year_id', 'age_group_id']}
        ).to_hdf(
            path_or_buf=self.model_paths.DRAW_FILE.format(
                sex=self.model_metadata.model_parameters['sex']
            ),
            key="keys",
            mode='a',
            format='table')


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['EnsemblePredictions'])

    ne.set_num_threads(1)
    logger.info("Initiating calculate coverage.")
    t = EnsemblePredictions(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.alerts.alert("Ensemble-ing all the predictions!")
    t.calculate_oos_coverage()
    t.calculate_is_coverage()
    t.write_pv()
    t.add_draws_to_df()
    t.write_draws()
    t.save_outputs()
    t.alerts.alert("Done with ensemble-ing the predictions.")


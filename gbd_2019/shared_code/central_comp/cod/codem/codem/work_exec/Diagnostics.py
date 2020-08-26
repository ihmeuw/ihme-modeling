import matplotlib
matplotlib.use('Agg')

import logging

from codem.metadata.model_task import ModelTask
from codem.joblaunch.args import get_step_args
from codem.metadata.step_metadata import STEP_IDS
from codem.stgpr.space_time_smoothing import import_json
import codem.db_write.table_diagnostics as t_diags
import codem.db_write.plot_diagnostics as p_diags
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)


class Diagnostics(ModelTask):
    def __init__(self, **kwargs):
        """
        Create diagnostic plots and tables for modelers to vet.
        Attach all information + predictive validity metrics to the modeler email
        and send to the relevant modelers.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['Diagnostics'])

        self.linear_models_json = import_json(self.model_paths.JSON_FILES['linear'])
        self.st_models_json = import_json(self.model_paths.JSON_FILES['spacetime'])

        self.submodel_summary_df = None
        self.covariate_summary_df = None
        self.model_type_df = None
        self.submodel_betas_df = None
        self.merged_submodels_df = None
        self.submodel_weighted_betas_df = None
        self.covariate_table = None
        self.beta_draws = None

    def get_table_diagnostics(self):
        logger.info("Getting diagnostics that will be saved as csv.")
        self.submodel_summary_df = t_diags.get_submodel_summary_df(
            model_version_id=self.model_version_id,
            submodel_covariates=self.pickled_inputs['submodel_covariates'],
            submodel_rmse=self.pickled_inputs['submodel_rmse'],
            submodel_trend=self.pickled_inputs['submodel_trend'],
            db_connection=self.db_connection
        )
        self.covariate_summary_df = t_diags.get_covariate_summary_df(
            submodel_summary_df=self.submodel_summary_df,
            draw_id=self.pickled_inputs['draw_id']
        )
        self.model_type_df = t_diags.get_model_type_df(
            covariate_summary_df=self.covariate_summary_df
        )
        self.submodel_betas_df = t_diags.get_submodel_betas_df(
            linear_models_json=self.linear_models_json,
            st_models_json=self.st_models_json,
            all_data_holdout=self.model_metadata.model_parameters['all_data_holdout']
        )
        self.merged_submodels_df = t_diags.get_all_submodel_info_df(
            data_frame=self.model_metadata.data_frame,
            covariate_df=self.model_metadata.covariates,
            covariate_summary_df=self.covariate_summary_df,
            submodel_betas_df=self.submodel_betas_df,
            lm_dict=self.linear_models_json,
            all_data_holdout=self.model_metadata.model_parameters['all_data_holdout']
        )
        self.submodel_weighted_betas_df = t_diags.create_weighted_beta_column(
            merged_submodels_df=self.merged_submodels_df
        )
        self.covariate_table = t_diags.create_covariate_table(
            covariate_summary_df=self.covariate_summary_df,
            priors=self.model_metadata.priors,
            submodel_weighted_betas_df=self.submodel_weighted_betas_df
        )
        self.beta_draws = t_diags.get_beta_draws(
            data_frame=self.model_metadata.data_frame,
            covariate_df=self.model_metadata.covariates,
            covariate_summary_df=self.covariate_summary_df,
            submodel_betas_df=self.submodel_betas_df,
            scale=100,
            lm_dict=self.linear_models_json,
            st_dict=self.st_models_json,
            all_data_holdout=self.model_metadata.model_parameters['all_data_holdout']
        )

    def save_tables(self):
        logger.info("Saving diagnostics to csv.")
        self.merged_submodels_df.to_csv(self.model_paths.diagnostics_dir("covariate_long_table.csv"))
        self.submodel_summary_df.to_csv(self.model_paths.diagnostics_dir("submodel_table.csv"))
        self.covariate_summary_df.to_csv(self.model_paths.diagnostics_dir("email_covariate_summary.csv"))
        self.covariate_table.to_csv(self.model_paths.diagnostics_dir("covariate_table.csv"))
        self.beta_draws.to_csv(self.model_paths.diagnostics_dir("beta_draws.csv"))
        self.model_type_df.to_csv(self.model_paths.diagnostics_dir("model_type_df.csv"))

    def get_plot_diagnostics(self):
        logger.info("Getting plot diagnostics that will be attached "
                    "to the email and included in the diagnostics folder.")
        p_diags.create_covar_ndraws_plot(
            covariate_summary_df=self.covariate_table,
            cause=self.model_metadata.model_parameters['cause_name'],
            date=self.model_metadata.model_parameters['date_inserted'],
            model_version_id=self.model_version_id,
            sex_id=self.model_metadata.model_parameters['sex_id'],
            filepath=self.model_paths.diagnostics_dir("covariate_draws.png")
        )
        p_diags.create_covar_submodel_plot(
            covariate_summary_df=self.covariate_table,
            cause=self.model_metadata.model_parameters['cause_name'],
            date=self.model_metadata.model_parameters['date_inserted'],
            model_version_id=self.model_version_id,
            sex_id=self.model_metadata.model_parameters['sex_id'],
            filepath=self.model_paths.diagnostics_dir("covariate_submodels.png")
        )
        p_diags.create_draw_plots(
            beta_draws=self.beta_draws,
            cause_name=self.model_metadata.model_parameters['cause_name'],
            date=self.model_metadata.model_parameters['date_inserted'],
            model_version_id=self.model_version_id,
            sex_id=self.model_metadata.model_parameters['sex_id'],
            filepath=self.model_paths.diagnostics_dir("covariate_distributions_{}.png")
        )
        self.alerts.alert(f"Your diagnostic tables and plots are saved at {self.model_paths.DIAGNOSTICS_DIR}")


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['Diagnostics'])

    logger.info("Initiating model diagnostics.")
    t = Diagnostics(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.alerts.alert("Making diagnostics for modeler")
    t.get_table_diagnostics()
    t.save_tables()
    t.get_plot_diagnostics()
    t.alerts.alert("Done making diagnostics for modeler")

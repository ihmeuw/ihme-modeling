import logging

import codem.data.queryStrings as QS
import codem.stgpr.space_time_smoothing as space
from codem.db_write import submodels
from codem.ensemble import PV
from codem.ensemble.psi_calculation import best_psi, psi_weights
from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference import db_connect
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)


class OptimalPSI(ModelTask):
    def __init__(self, cutoff=100, **kwargs):
        """
        Calculate the optimal PSI values that we should use
        to build the ensemble (optimal submodel weighting scheme.

        :param cutoff: what's the weight cutoff below which we want a submodel
                       to get 0 draws
        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS["OptimalPSI"])
        self.cutoff = cutoff
        self.pickled_outputs["model_pv"] = {}
        self.pickled_outputs["st_models_id"] = self.pickled_inputs["st_models_pv"]
        self.pickled_outputs["linear_models_id"] = self.pickled_inputs["linear_models_pv"]
        del self.pickled_inputs["st_models_pv"]
        del self.pickled_inputs["linear_models_pv"]

        self.submodel_covariates = None
        self.submodel_rmse = None
        self.submodel_trend = None
        self.submodel_mean_error = None
        self.submodel_coverage = None

    def database_wipe(self):
        """
        Delete from the submodel databases if this task has already been run for this
        model version. If nothing is in the submodel tables yet, then 0 rows will be
        affected.
        """
        # check if rows exist in cod.submodel
        df = db_connect.execute_select(
            QS.count_rows_query.format(table="submodel"),
            conn_def=self.conn_def,
            parameters={"model_version_id": self.model_version_id},
        )
        if df["count"].iloc[0] > 0:
            db_connect.call_stored_procedure(
                name="cod.delete_from_submodel_tables_by_request",
                args=[int(self.model_version_id)],
                conn_def=self.conn_def,
            )

    def get_best_psi(self):
        logger.info("Getting best psi.")
        self.alerts.alert("Getting the best value of psi by building lots of ensembles.")
        outputs = best_psi(
            data_frame=self.model_metadata.data_frame,
            knockouts=self.model_metadata.ko_data,
            window=self.model_metadata.model_parameters["rmse_window"],
            space_models=self.pickled_outputs["st_models_id"],
            linear_models=self.pickled_outputs["linear_models_id"],
            psi_values=self.model_metadata.model_parameters["psi_values"],
            cutoff=self.cutoff,
            spacetime_only=self.model_metadata.model_parameters["spacetime_only"],
        )
        self.alerts.alert(f"The best psi was {round(outputs[0], 3)}")
        self.pickled_outputs["model_pv"]["best_psi"] = outputs[0]
        self.pickled_outputs["st_models_id"].draws = outputs[1]
        self.pickled_outputs["linear_models_id"].draws = outputs[2]
        self.pickled_outputs["model_pv"]["pv_rmse_out"] = outputs[3]
        self.pickled_outputs["model_pv"]["pv_trend_out"] = outputs[4]
        self.pickled_outputs["st_models_id"].ranks = outputs[5]
        self.pickled_outputs["linear_models_id"].ranks = outputs[6]
        del outputs

    def calculate_weights(self):
        logger.info("Calculating the weights based on best psi.")
        weights = psi_weights(
            space_err=self.pickled_outputs["st_models_id"].RMSE
            + self.pickled_outputs["st_models_id"].trend,
            lin_err=self.pickled_outputs["linear_models_id"].RMSE
            + self.pickled_outputs["linear_models_id"].trend,
            psi=self.pickled_outputs["model_pv"]["best_psi"],
            cutoff=self.cutoff,
            spacetime_only=self.model_metadata.model_parameters["spacetime_only"],
        )
        self.pickled_outputs["st_models_id"].psi_weights = weights[0]
        self.pickled_outputs["linear_models_id"].psi_weights = weights[1]
        del weights

    def make_temp_ensemble_preds(self):
        logger.info(
            "Making temp predictions for the ensemble based on submodel means * weights."
        )
        preds = PV.final_preds(
            st_preds=self.pickled_outputs["st_models_id"].all_models[-1].pred_mat,
            lin_preds=self.pickled_outputs["linear_models_id"].all_models[-1].pred_mat,
            st_psi_weights=self.pickled_outputs["st_models_id"].psi_weights,
            lin_psi_weights=self.pickled_outputs["linear_models_id"].psi_weights,
        )
        self.pickled_outputs["ensemble_preds"] = preds
        del preds

    def calculate_in_sample_rmse(self):
        logger.info("Calculating in-sample RMSE for ensemble")
        self.alerts.alert("Calculating in sample RMSE for ensemble.")
        rmse_in = PV.rmse_in(
            pred=self.pickled_outputs["ensemble_preds"],
            observed=self.model_metadata.data_frame.ln_rate.values,
            ko=self.model_metadata.all_ko,
        )
        self.pickled_outputs["model_pv"]["pv_rmse_in"] = rmse_in

    def calculate_in_sample_trend(self):
        logger.info("Calculating in-sample trend for ensemble.")
        self.alerts.alert("Calculating in sample trend for ensemble.")
        trend_in = PV.trend_in(
            df=self.model_metadata.data_frame,
            ko=self.model_metadata.all_ko,
            pred_vec=self.pickled_outputs["ensemble_preds"],
            window=self.model_metadata.model_parameters["rmse_window"],
        )
        self.pickled_outputs["model_pv"]["pv_trend_in"] = trend_in

    def determine_submodel_ids(self):
        logger.info("Determining submodel IDs.")
        self.pickled_outputs["linear_models_id"].get_submodel_ids(
            linmod_type_list=self.pickled_inputs["linmod_type_list"],
            model_version_id=self.model_metadata.model_version_id,
            type_id=1,
        )
        self.pickled_outputs["st_models_id"].get_submodel_ids(
            linmod_type_list=self.pickled_inputs["linmod_type_list"],
            model_version_id=self.model_metadata.model_version_id,
            type_id=2,
        )

    def delete_predictions(self):
        logger.info("Deleting submodel predictions.")
        self.pickled_outputs["st_models_id"].del_predictions()
        self.pickled_outputs["linear_models_id"].del_predictions()

    def return_submodel_labels(self):
        logger.info("Labeling submodels.")
        submodel_covariates = space.import_cv_vars(
            rate_path=self.model_paths.COVARIATE_FILES["ln_rate"],
            cf_path=self.model_paths.COVARIATE_FILES["lt_cf"],
            include_counts=self.model_metadata.model_parameters["include_counts"],
        )
        space_ids = self.pickled_outputs["st_models_id"].submodel_ids
        mixed_ids = self.pickled_outputs["linear_models_id"].submodel_ids
        submodel_covariates["space"] = {
            space_ids[x]: submodel_covariates["space"][x] for x in range(len(space_ids))
        }
        submodel_covariates["mixed"] = {
            mixed_ids[x]: submodel_covariates["mixed"][x] for x in range(len(mixed_ids))
        }
        self.submodel_covariates = submodel_covariates
        self.pickled_outputs["submodel_covariates"] = self.submodel_covariates

    def submodel_pv(self):
        logger.info("Organizing submodel predictive validity info.")
        self.submodel_rmse = {
            submodel_id: rmse_value
            for submodel_id, rmse_value in zip(
                self.pickled_outputs["linear_models_id"].submodel_ids,
                self.pickled_outputs["linear_models_id"].RMSE,
            )
        }
        self.submodel_rmse.update(
            {
                submodel_id: rmse_value
                for submodel_id, rmse_value in zip(
                    self.pickled_outputs["st_models_id"].submodel_ids,
                    self.pickled_outputs["st_models_id"].RMSE,
                )
            }
        )
        self.submodel_trend = {
            submodel_id: trend_value
            for submodel_id, trend_value in zip(
                self.pickled_outputs["linear_models_id"].submodel_ids,
                self.pickled_outputs["linear_models_id"].trend,
            )
        }
        self.submodel_trend.update(
            {
                submodel_id: trend_value
                for submodel_id, trend_value in zip(
                    self.pickled_outputs["st_models_id"].submodel_ids,
                    self.pickled_outputs["st_models_id"].trend,
                )
            }
        )
        self.submodel_mean_error = {
            submodel_id: mean_error_value
            for submodel_id, mean_error_value in zip(
                self.pickled_outputs["linear_models_id"].submodel_ids,
                self.pickled_outputs["linear_models_id"].mean_error,
            )
        }
        self.submodel_mean_error.update(
            {
                submodel_id: mean_error_value
                for submodel_id, mean_error_value in zip(
                    self.pickled_outputs["st_models_id"].submodel_ids,
                    self.pickled_outputs["st_models_id"].mean_error,
                )
            }
        )
        self.submodel_coverage = {
            submodel_id: coverage_value
            for submodel_id, coverage_value in zip(
                self.pickled_outputs["linear_models_id"].submodel_ids,
                self.pickled_outputs["linear_models_id"].coverage,
            )
        }
        self.submodel_coverage.update(
            {
                submodel_id: coverage_value
                for submodel_id, coverage_value in zip(
                    self.pickled_outputs["st_models_id"].submodel_ids,
                    self.pickled_outputs["st_models_id"].coverage,
                )
            }
        )
        self.pickled_outputs["submodel_trend"] = self.submodel_trend
        self.pickled_outputs["submodel_rmse"] = self.submodel_rmse
        self.pickled_outputs["submodel_mean_error"] = self.submodel_mean_error
        self.pickled_outputs["submodel_coverage"] = self.submodel_coverage

    def write_submodel_covariates(self):
        """
        For every submodel that is used in codem write all the covariates for
        that submodel into the database for the ability to skip covariate
        selection in future runs of codem.
        """
        logger.info("Writing submodel covariates.")
        self.alerts.alert("Uploading submodel covariates to the database")
        link = {
            self.model_metadata.priors.name[i]: self.model_metadata.priors.covariate_model_id[
                i
            ]
            for i in range(len(self.model_metadata.priors))
        }
        for model in list(self.submodel_covariates.keys()):
            for submodel in list(self.submodel_covariates[model].keys()):
                dic = self.submodel_covariates[model][submodel]
                submodel_covariate_ids = [link[c] for c in dic]
                submodels.write_submodel_covariate(
                    submodel, submodel_covariate_ids, self.conn_def
                )


def main():
    args = get_step_args()
    setup_logging(
        model_version_id=args.model_version_id,
        step_id=STEP_IDS["OptimalPSI"],
        conn_def=args.conn_def,
    )

    logger.info("Initiating Optimal Psi")
    t = OptimalPSI(
        model_version_id=args.model_version_id,
        conn_def=args.conn_def,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores,
        cutoff=100,
    )
    t.alerts.alert("Beginning the optimal psi process")
    t.database_wipe()
    t.get_best_psi()
    t.calculate_weights()
    t.make_temp_ensemble_preds()
    t.calculate_in_sample_rmse()
    t.calculate_in_sample_trend()
    t.determine_submodel_ids()
    t.delete_predictions()
    t.return_submodel_labels()
    t.submodel_pv()
    t.write_submodel_covariates()
    t.save_outputs()
    logger.info(f"Finished finding optimal psi: {t.pickled_outputs['model_pv']['best_psi']}")
    t.alerts.alert("Finished with optimal psi")

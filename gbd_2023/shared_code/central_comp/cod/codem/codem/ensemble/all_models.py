import concurrent.futures
import logging
import sys
from typing import Dict, List

import numpy as np
import pandas as pd

from db_queries import get_age_spans

import codem.ensemble.draws as dr
import codem.ensemble.PV as pv
import codem.stgpr.gpr_smooth as gpr
import codem.stgpr.space_time_smoothing as space
from codem.db_write import submodels

logger = logging.getLogger(__name__)


class All_Models:
    def __init__(
        self,
        df: pd.DataFrame,
        knockouts: List[pd.DataFrame],
        linear_floor_rate: float,
        json_dict: Dict[str, str],
        conn_def: str,
        debug_mode: bool = False,
        make_preds: bool = True,
    ) -> None:
        """
        All model information is stored here for either spacetime or linear models.
        This includes all combinations of covariates and all knockout combinations.

        :param df: data frame
            data frame with all the demographic and mortality data for a
            particular cause.
        :param knockouts: list of data frames
            list of data frames where each data frame has whether each data point
            is part of the modeling process holdout 1 or holdout 2
        :param linear_floor_rate: float
            what the lowest value any model should produce is
        :param json_dict: dict
            dictionary with first stage model results
        :param conn_def: str
            database connection definition
        :param debug_mode: bool
            whether to propagate errors in debug mode
        :param make_preds: bool
            whether to make predictions in the build step
        """
        self.all_models = space.make_all_models(
            df, knockouts, linear_floor_rate, json_dict, make_preds
        )
        self.debug_mode = debug_mode
        self.RMSE = None
        self.RMSE_all = None
        self.trend = None
        self.trend_all = None
        self.mean_error = None
        self.mean_error_all = None
        self.coverage = None
        self.coverage_all = None
        self.draws = None
        self.ranks = None
        self.psi_weights = None
        self.submodel_ids = None
        self.draw_id = None
        self.conn_def = conn_def

    def apply_smoothing(
        self,
        df,
        knockouts,
        omega,
        time_weight_method,
        lambda_data,
        lambda_no_data,
        zeta,
        zeta_no_data,
    ):
        age_spans = get_age_spans()
        age_spans = age_spans.loc[age_spans.age_group_id.isin(list(df.age_group_id.unique()))]
        tempAge = list(age_spans.sort_values("age_group_years_start").age_group_id)
        df["ageC"] = np.array([tempAge.index(x) for x in list(df.age_group_id)])
        for i in range(len(self.all_models)):
            logger.info(f"Applying smoothing for knockout {i}")
            self.all_models[i].spacetime_predictions(
                df,
                knockouts[i],
                omega,
                time_weight_method,
                lambda_data,
                lambda_no_data,
                zeta,
                zeta_no_data,
            )

    def reset_residuals(self, df, knockouts, response_list):
        """
        (self, data frame, list of data frames, list) -> None

        Reset the residual data frame of all the models for post spacetime results.
        """
        for i in range(len(self.all_models)):
            logger.info(f"Resetting residuals for knockout {i}")
            self.all_models[i].reset_residuals(response_list, knockouts[i], df)

    def gpr_all(
        self,
        data_frame: pd.DataFrame,
        knockouts: pd.DataFrame,
        response_list: List[int],
        scale: int,
        amplitude_scalar: int,
        parallel: bool = True,
        cores: int = 20,
        model_version_id: int = None,
    ):
        """
        (self, data frame, list of data frames, list of str, float) -> None

        Runs all of the gpr smoothing in parallel using multiprocessing.
        """
        logger.info("Applying GPR.")
        inputs = [
            (
                data_frame[
                    ["location_id", "age_group_id", "source_type", "is_representative"]
                ],
                knockouts[i],
            )
            for i in range(len(knockouts))
        ]
        try:
            with concurrent.futures.ProcessPoolExecutor(cores) as executor:
                variance_list = list(executor.map(gpr.variance_map, inputs))
        except concurrent.futures.process.BrokenProcessPool:
            logger.error(
                "Process pool died abruptly. Returning exit code 137 for jobmon resource retry"
            )
            sys.exit(137)
        variables = [
            "location_id",
            "age_group_id",
            "year_id",
            "ln_rate",
            "lt_cf",
            "ln_rate_sd",
            "lt_cf_sd",
        ]
        inputs = [
            (
                data_frame[variables],
                knockouts[i],
                self.all_models[i].simple_st_ln_rate,
                self.all_models[i].simple_st_lt_cf,
                self.all_models[i].res_mat,
                variance_list[i],
                amplitude_scalar,
            )
            for i in range(len(knockouts))
        ]
        nsv, amplitudes = list(map(list, zip(*list(map(gpr.nsv_map, inputs)))))
        del inputs
        for i in range(len(amplitudes)):
            logger.info(f"Applying GPR smoothing to amplitude {i} out of {len(amplitudes)}")
            amplitudes[i] = gpr.median_amplitude(amplitudes[i])
        smooth_pred = [
            gpr.new_gpr(
                df=pd.concat([data_frame[variables], nsv[i]], axis=1),
                ko=knockouts[i],
                amplitude=amplitudes[i],
                preds=self.all_models[i].st_smooth_mat,
                variance=variance_list[i],
                response_list=response_list,
                scale=scale,
                parallel=parallel,
                cores=cores,
                model_version_id=model_version_id,
            )
            for i in range(len(knockouts))
        ]
        for i in range(len(self.all_models)):
            self.all_models[i].pred_mat = smooth_pred[i]
            self.all_models[i].nsv = nsv[i]
            self.all_models[i].amplitude = amplitudes[i]
            self.all_models[i].variance_type = variance_list[i]

    def uniform_predictions(self, df, response_list):
        """
        (self, data frame, list of strings) -> None

        Make all the predictions for an object of class All_Model so that they
        are predicting in log rate space.
        """
        for i in range(len(self.all_models)):
            logger.info(f"Calculating uniform predictions for knockout {i}")
            self.all_models[i].pred_mat = space.uniform_predictions(
                df, self.all_models[i].pred_mat, response_list
            )

    def rmse_out(self, df, knockouts):
        """Calculate the root mean squared error of all models."""
        logger.info("Calculating out-of-sample RMSE.")
        self.RMSE, self.RMSE_all = pv.rmse_out(self.all_models, df, knockouts)

    def trend_out(self, df, knockouts, window):
        """Calculate the trend error of all models."""
        logger.info("Calculating out-of-sample trend.")
        self.trend, self.trend_all = pv.trend_out(self.all_models, df, knockouts, window)

    def mean_error_out(self, df, knockouts):
        """Calculate the OOS mean error of all models"""
        logger.info("Calculating out-of-sample mean error.")
        self.mean_error, self.mean_error_all = pv.mean_error_submodel_out(
            self.all_models, df, knockouts
        )

    def coverage_out(self, df, knockouts, output_object_prefix):
        """Calculate the coverage error of all models."""
        logger.info("Calculating out-of-sample coverage.")
        self.coverage, self.coverage_all = pv.coverage_submodel_out(
            self.all_models, knockouts, df, output_object_prefix
        )

    def delete_mixed_model_parameters(self):
        """
        (self) -> None

        Delete all the Mixed model parameters from a all model class object.
        This is basically a space saving utility.
        """
        for i in range(len(self.all_models)):
            for j in range(len(self.all_models[i].models)):
                self.all_models[i].models[j].vcov = None
                self.all_models[i].models[j].fix_eff = None
                self.all_models[i].models[j].ran_eff = None
                self.all_models[i].models[j].random_intercepts = None
                self.all_models[i].models[j].variables = None
                self.all_models[i].models[j].beta_draw = None

    def get_lin_mod_parameters(self):
        """
        (self) -> list of lists

        Extracts all the things necessary for making predictions using draws
        from the variance covariance matrix. Should be able to make draws based
        off of the number of times we use a model for a particular ensemble.
        """
        mv = [
            [
                {
                    "fix_eff": self.all_models[i].models[j].fix_eff,
                    "ran_eff": self.all_models[i].models[j].ran_eff,
                    "vcov": self.all_models[i].models[j].vcov,
                    "variables": self.all_models[i].models[j].variables,
                }
                for j in range(len(self.all_models[i].models))
            ]
            for i in range(len(self.all_models))
        ]
        return mv

    def del_predictions(self):
        """
        (self) -> None

        Delete all the predictions from the knockout models.
        """
        for i in range(len(self.all_models)):
            self.all_models[i].pred_mat = None

    def del_space_time(self):
        """
        (self) -> None

        Delete all the predictions from the knockout models.
        """
        for i in range(len(self.all_models)):
            self.all_models[i].st_smooth_mat = None

    def all_linear_draws(self, df, linear_floor, response_list):
        logger.info("Creating all linear draws")
        lin_mod_parameters = self.get_lin_mod_parameters()
        temp = pd.get_dummies(df.age_group_id)
        temp.columns = [
            "age" + str(int(s)) if s >= 1 else "age" + str(s) for s in temp.columns
        ]
        df = pd.concat([df, temp], axis=1)
        df["level_1"] = df.level_1.astype(str)
        del temp
        inputs = [
            (lin_mod_parameters[i], df, linear_floor, response_list, self.draws)
            for i in range(len(lin_mod_parameters))
        ]
        all_linear = np.array(list(map(dr.linear_draws_map, inputs)))
        for i in range(len(all_linear)):
            logger.info(f"Working on knockout {i} for draws")
            if len(all_linear[0]) > 0:
                all_linear[i] = space.uniform_predictions(
                    df, all_linear[i], np.repeat(response_list, self.draws)
                )
            self.all_models[i].draw_preds = all_linear[i]

    def all_gpr_draws2(
        self, df, knockouts, response_list, scale, linear_floor, model_version_id
    ):
        logger.info("Creating GPR draws")
        if sum(self.draws) == 0:
            logger.info("There are no draws for st-models ... skipping.")
            for i in range(len(knockouts)):
                self.all_models[i].st_smooth_mat = None
                self.all_models[i].draw_preds = np.array([])
            return None
        variables = [
            "location_id",
            "age_group_id",
            "year_id",
            "ln_rate",
            "lt_cf",
            "ln_rate_sd",
            "lt_cf_sd",
        ]
        new_draws = [
            dr.new_ko_gpr_draws(
                df=pd.concat([df[variables], self.all_models[i].nsv], axis=1),
                ko=knockouts[i],
                amplitude=self.all_models[i].amplitude,
                preds=self.all_models[i].st_smooth_mat,
                variance=self.all_models[i].variance_type,
                response_list=response_list,
                scale=scale,
                draws=self.draws,
                random_seed=model_version_id,
            )
            for i in range(len(knockouts))
        ]
        for i in range(len(new_draws)):
            new_draws[i] = space.uniform_predictions(
                df, new_draws[i], np.repeat(response_list, self.draws)
            )
            new_draws[i] = space.bound_gpr_draws(df, new_draws[i], linear_floor)
            self.all_models[i].st_smooth_mat = None
            self.all_models[i].draw_preds = new_draws[i]

    def id_to_draws(self):
        temp = [
            [self.submodel_ids[i] for j in range(self.draws[i])]
            for i in range(len(self.draws))
        ]
        self.draw_id = [item for sub_list in temp for item in sub_list]

    def get_submodel_ids(
        self, linmod_type_list: List[str], model_version_id: int, type_id: int
    ) -> None:
        logger.info("Getting submodel IDs + draws based on weights.")
        dependent = np.array([4 * (x == "count") for x in linmod_type_list])
        dependent += np.array([2 * (x == "ln_rate") for x in linmod_type_list])
        dependent += np.array([1 * (x == "lt_cf") for x in linmod_type_list])
        self.submodel_ids = [0 for i in range(len(self.draws))]
        for i in range(len(self.psi_weights)):
            self.submodel_ids[i] = submodels.write_submodel(
                model_version_id,
                type_id,
                dependent[i],
                self.psi_weights[i],
                self.ranks[i],
                self.conn_def,
            )
        self.id_to_draws()

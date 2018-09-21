import warning as warn
from input import Inputs
import os, sys, subprocess
import space_time_smoothing as space
from all_models import All_Models
import psi_calculation as gs
import matplotlib.pyplot as plt
import query as Q
import PV
import pandas as pd
import numpy as np
import numexpr as ne
import warnings
import dill
import db_connect
warnings.filterwarnings("ignore", 'Mean of empty slice.')
pd.set_option('chained', None)
ne.set_num_threads(30)


class Ensemble:
    """
    Using a model_version_id runs a codem model
    """
    def __init__(self, model_version_id, db_connection, debug_mode=False,
                                    log_results=True, old_covariates_mvid=None):
        """
        Queries data base for model data

        :param model_version_id: int
            Integer representing a valid model version id
        :param log_results: bool
            Boolean indicating whether to write status updates to DB
        """
        self.db_connection = db_connection
        self.warnings = warn.WarningLog(model_version_id, db_connection)
        if log_results:
            self.warnings.time_stamp("Querying for model parameters")
        self.mod_inputs = Inputs(model_version_id, db_connection,
                                    update=log_results, debug_mode=debug_mode)
        self.log_results = log_results
        self.base_dir = "FILEPATH" 
        self.model_dir = "FILEPATH"
        rSource = "FILEPATH"
        self.rSource = rSource
        if not os.path.exists(self.rSource):
            self.rSource = "FILEPATH"
        self.linear_models = None
        self.st_models = None
        self.json_dict = None
        self.model_version_id = model_version_id
        self.data_attributes = ["data_frame", "country_df", "region_df", "super_region_df", "age_df", "global_df",
                                "age_location_df", "age_country_df", "age_region_df", "age_super_region_df"]
        self.debug_mode = debug_mode
        if debug_mode:
            self.seed = 0
        else:
            self.seed = model_version_id
        self.old_covariates_mvid = old_covariates_mvid

    def log_data_errors(self):
        """
        Logs missingness and and bad cf to the log file
        """
        self.warnings.log_missingness(self.mod_inputs.covariates,
                                      self.mod_inputs.cv_names)
        self.warnings.log_bad_cf(self.mod_inputs.data_frame, "cf")

    def move_run_location(self):
        """
        Move the location of where the code is running to be model specific
        """
        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
            subprocess.call("chmod 777 -R {}".format('/'.join(self.model_dir.split("/")[:-1])), shell=True)
        os.chdir(self.model_dir)

    def adjust_input_data(self, save=True):
        """
        Adjust the input data such that observations with missing covariates,
        or the envelope/population are equal to zero. Also change cf values of
        zero to NaN

        :param save: bool
            If true saves the data to disk
        """
        # remove observations where covariate values are missing
        self.mod_inputs.covariates.dropna(inplace=True)
        self.mod_inputs.data_frame.drop(
            np.setdiff1d(self.mod_inputs.data_frame.index.values,
                         self.mod_inputs.covariates.index.values), inplace=True)

        # remove observations where population or envelope is zero
        self.mod_inputs.data_frame = \
            self.mod_inputs.data_frame[(self.mod_inputs.data_frame.envelope > 0)
                                      & (self.mod_inputs.data_frame["pop"] > 0)]
        self.mod_inputs.covariates.drop(
            np.setdiff1d(self.mod_inputs.covariates.index.values,
                         self.mod_inputs.data_frame.index.values), inplace=True)

        # change cf values of zero and one in the main data frame to np.NaN
        self.mod_inputs.data_frame["cf"] = \
            self.mod_inputs.data_frame["cf"].map(lambda x:
                np.NaN if x <= 0.00000001 or x >= 1 else x)

        self.mod_inputs.data_frame["cf"][(self.mod_inputs.data_frame.lt_cf_sd.isnull()) |
                                    (self.mod_inputs.data_frame.ln_rate_sd.isnull())] = np.NaN

        self.mod_inputs.data_frame["ln_rate"][(self.mod_inputs.data_frame["cf"].isnull())] = np.NaN
        self.mod_inputs.data_frame["lt_cf"][(self.mod_inputs.data_frame["cf"].isnull())] = np.NaN

        self.mod_inputs.covariates.reset_index(drop=True, inplace=True)
        self.mod_inputs.data_frame.reset_index(drop=True, inplace=True)

        # make input data frame and save the data to disk
        if save:
            pd.concat([self.mod_inputs.data_frame,
                       self.mod_inputs.covariates],
                      axis=1).to_csv("FILEPATH", index=False)
            self.mod_inputs.priors.to_csv("FILEPATH", index=False)

    def covariate_selection(self):
        """
        Run covariate selection process.
        """
        if self.log_results:
            self.warnings.time_stamp("Running covariate selection")

        if self.old_covariates_mvid is None:
            # run the covariate selection process
            subprocess.call("R_PATH " +
                self.rSource + "FILEPATH " + str(self.model_version_id) +
                " " + self.db_connection + "SITE_PATH" + " " +
                self.model_dir, shell=True)
        else:
            cov_file_path = "FILEPATH"
            print cov_file_path
            subprocess.call("cp " + cov_file_path + " " + self.model_dir,
                            shell=True)

        if not os.path.isfile("FILEPATH"):
            sys.stderr.write("No covariates seem to have been selected.")

        self.mod_inputs.submodel_covariates = space.import_cv_vars("FILEPATH")

    def run_knockouts(self):
        """
        Run the CODEM knockout process
        """
        if self.log_results:
            self.warnings.time_stamp("Running KO process")
        self.mod_inputs.create_knockouts()

        pd.concat(self.mod_inputs.knockouts, axis=1).to_csv("FILEPATH",
                                                            index=False)

    def run_linear_model_builds(self):
        """
        Run the linear model portions of all codem models.
        """
        if self.log_results:
            self.warnings.time_stamp("Running Linear Model Builds")

        # run linear models and linear portion of space time models
        subprocess.call("R_PATH " +
                        self.rSource + "FILEPATH " + str(self.model_version_id)
                        + " " + self.model_dir, shell=True)

        if not os.path.isfile("FILEPATH"):
            sys.stderr.write("No covariates seem to have been selected.")

        # import json file from R build and save the model list portion
        self.json_dict = space.import_json("FILEPATH")
        self.mod_inputs.response_list = [x.rsplit("_", 1)[0] for x in
                                         sorted(self.json_dict.keys())]

    def read_spacetime_models(self):
        """
        Read the space time models created in R
        """
        if self.log_results:
            self.warnings.time_stamp("Read space time models & predictions")
        self.st_models = \
            All_Models(pd.concat([self.mod_inputs.data_frame,
                                  self.mod_inputs.covariates], axis=1),
                       self.mod_inputs.knockouts,
                       self.mod_inputs.linear_floor_rate, self.json_dict,
                       self.db_connection)
        if self.debug_mode:
            self.st_models.pred_mat_lin = self.st_models.all_models[-1].pred_mat.copy()
        else:
            self.st_models.delete_mixed_model_parameters()
            self.json_dict = None

    def apply_spacetime_smoothing(self):
        """
        Apply spacetime smoothing to linear models.
        """
        if self.log_results:
            self.warnings.time_stamp("Apply spacetime smoothing")
        self.st_models.apply_smoothing(self.mod_inputs.data_frame,
                                       self.mod_inputs.knockouts,
                                       self.mod_inputs.omega_age_smooth,
                                       self.mod_inputs.lambda_time_smooth,
                                       self.mod_inputs.lambda_time_smooth_nodata,
                                       self.mod_inputs.zeta_space_smooth,
                                       self.mod_inputs.zeta_space_smooth_nodata)

        if self.log_results:
            self.warnings.time_stamp("Reset residuals")
        self.st_models.reset_residuals(self.mod_inputs.data_frame,
                                       self.mod_inputs.knockouts,
                                       self.mod_inputs.response_list)
        if self.debug_mode:
            self.st_models.pred_mat_st = self.st_models.all_models[-1].st_smooth_mat.copy()
        else:
            pass
        ne.set_num_threads(1)

    def apply_gp_smoothing(self):
        """
        Apply gaussian process smoothintg to space time results.
        """
        if self.log_results:
            self.warnings.time_stamp("GPR smoothing")
        self.st_models.gpr_all(self.mod_inputs.data_frame,
                               self.mod_inputs.knockouts,
                               self.mod_inputs.response_list,
                               self.mod_inputs.gpr_year_corr)
        if self.debug_mode:
            self.st_models.pred_mat_gpr = self.st_models.all_models[-1].pred_mat.copy()
        else:
            pass

    def read_linear_models(self):
        """
        Read linear models created in R
        """
        json_dict = space.import_json("FILEPATH")
        self.linear_models = All_Models(pd.concat([self.mod_inputs.data_frame,
                                                   self.mod_inputs.covariates],
                                                   axis=1),
                                        self.mod_inputs.knockouts,
                                        self.mod_inputs.linear_floor_rate,
                                        json_dict, self.db_connection)
        if self.debug_mode:
            self.linear_models.pred_mat_lin = self.linear_models.all_models[-1].pred_mat.copy()

    def submodel_pv(self):
        """
        Calculate the RMSE and trend for all submodels
        """
        if self.log_results:
            self.warnings.time_stamp("Calculating RMSE/Trend")

        self.st_models.uniform_predictions(self.mod_inputs.data_frame,
                                           self.mod_inputs.response_list)

        self.linear_models.uniform_predictions(self.mod_inputs.data_frame,
                                               self.mod_inputs.response_list)

        self.linear_models.rmse_out(self.mod_inputs.data_frame,
                                    self.mod_inputs.knockouts)

        self.st_models.rmse_out(self.mod_inputs.data_frame,
                                self.mod_inputs.knockouts)

        self.linear_models.trend_out(self.mod_inputs.data_frame,
                                     self.mod_inputs.knockouts,
                                     self.mod_inputs.rmse_window)

        self.st_models.trend_out(self.mod_inputs.data_frame,
                                 self.mod_inputs.knockouts,
                                 self.mod_inputs.rmse_window)

    def optimal_psi(self):
        """
        Find the optimal psi valuse for the ensemble and use it to make final
        predictions.
        """
        if self.log_results:
            self.warnings.time_stamp("Optimal Psi")

        self.mod_inputs.best_psi, self.st_models.draws, \
            self.linear_models.draws, self.mod_inputs.pv_rmse_out, \
            self.mod_inputs.pv_trend_out, self.st_models.ranks, \
            self.linear_models.ranks = \
            gs.best_psi(self.mod_inputs.data_frame, self.mod_inputs.knockouts,
                        self.mod_inputs.rmse_window, self.st_models, self.linear_models,
                        self.mod_inputs.psi_values, 100)

        self.st_models.psi_weights, self.linear_models.psi_weights = \
            gs.psi_weights(self.st_models.RMSE + self.st_models.trend,
                           self.linear_models.RMSE + self.linear_models.trend,
                           self.mod_inputs.best_psi, 100)

        self.mod_inputs.ensemble_preds = \
            PV.final_preds(self.st_models.all_models[-1].pred_mat,
                           self.linear_models.all_models[-1].pred_mat,
                           self.st_models.psi_weights,
                           self.linear_models.psi_weights)

    def ensemble_pv(self):
        """
        Calculate trend and RMSE from the ensemble
        """
        if self.log_results:
            self.warnings.time_stamp("Calculate Predictive Validity in sample")

        self.mod_inputs.pv_rmse_in = \
            PV.rmse_in(self.mod_inputs.ensemble_preds,
                       self.mod_inputs.data_frame.ln_rate.values,
                       self.mod_inputs.knockouts[-1].ix[:, 0].values)

        self.mod_inputs.pv_trend_in = \
            PV.trend_in(self.mod_inputs.data_frame,
                        self.mod_inputs.knockouts[-1].ix[:, 0].values,
                        self.mod_inputs.ensemble_preds,
                        self.mod_inputs.rmse_window)

        self.linear_models.del_predictions()
        self.st_models.del_predictions()

        self.linear_models.get_submodel_ids(self.mod_inputs.response_list,
                                            self.mod_inputs.model_version_id, 1)
        self.st_models.get_submodel_ids(self.mod_inputs.response_list,
                                        self.mod_inputs.model_version_id, 2)

        self.mod_inputs.label_submodels(self.st_models.submodel_ids,
                                        self.linear_models.submodel_ids)

    def linear_draws(self):
        """
        Create all of a CODEm models draws from the linear models.
        """
        if self.log_results:
            self.warnings.time_stamp("Linear Draws")

        self.linear_models.all_linear_draws(pd.concat([self.mod_inputs.data_frame,
                                                       self.mod_inputs.covariates], axis=1),
                                            self.mod_inputs.linear_floor_rate,
                                            self.mod_inputs.response_list)

    def gpr_draws(self):
        """
        Create all of the CODEm models draws from the gpr models.
        """
        if self.log_results:
            self.warnings.time_stamp("GPR Draws")

        self.st_models.all_gpr_draws2(self.mod_inputs.data_frame, self.mod_inputs.knockouts,
                                      self.mod_inputs.response_list, self.mod_inputs.gpr_year_corr,
                                      self.mod_inputs.linear_floor_rate)

        self.st_models.del_space_time()

    def calculate_coverage(self):
        """
        Find the draw coverage of the final CODEm model
        """
        if self.log_results:
            self.warnings.time_stamp("Coverage Out")

        self.mod_inputs.pv_coverage_out = PV.coverage_out(self.st_models, self.linear_models,
                                                          self.mod_inputs.knockouts,
                                                          self.mod_inputs.data_frame)

        self.mod_inputs.pv_coverage_in = \
            PV.single_coverage_out(PV.concat2([self.st_models.all_models[-1].draw_preds,
                                               self.linear_models.all_models[-1].draw_preds]),
                                   self.mod_inputs.data_frame,
                                   self.st_models.all_models[-1].nsv.ln_rate_nsv.values,
                                   self.mod_inputs.knockouts[-1].ix[:, 0].values)

    def write_results(self):
        """
        Write codem results to appropriate folders and Databases.
        """
        if self.log_results:
            self.warnings.time_stamp("Getting logistics")

        self.mod_inputs.submodel_rmse = {x: y for x, y in zip(self.linear_models.submodel_ids, self.linear_models.RMSE)}
        self.mod_inputs.submodel_rmse.update({x: y for x, y in zip(self.st_models.submodel_ids, self.st_models.RMSE)})
        self.mod_inputs.submodel_trend = {x: y for x, y in zip(self.linear_models.submodel_ids,
                                                               self.linear_models.trend)}
        self.mod_inputs.submodel_trend.update({x: y for x, y in zip(self.st_models.submodel_ids,
                                                                    self.st_models.trend)})

        self.mod_inputs.draw_id = self.st_models.draw_id + self.linear_models.draw_id

        self.mod_inputs.draws = PV.concat2([self.st_models.all_models[-1].draw_preds,
                                            self.linear_models.all_models[-1].draw_preds])
        self.st_models.del_draws()
        self.linear_models.del_draws()

        if self.log_results:
            self.warnings.time_stamp("Post processing")
        self.mod_inputs.post_processing()

    def cleanup_code_base(self):
        """
        Delete the temporary location of codem for this model run.
        """
        if self.log_results:
            self.warnings.time_stamp("Finished")

        # log our warnings
        f = open("FILEPATH", "w")
        f.write(self.warnings.text)
        f.close()

        subprocess.call("rm -rf FILEPATH", shell=True)

    def aggregate_results(self):
        """
        Pull together results at every level into a single data frame.

        :return: data frame
            dataframe with all location levels and aggregated age levels
        """
        merge_data = ["location_id", "year", "age", "envelope", "pop"]
        merge_data += [i for i in self.mod_inputs.data_frame.columns if i.startswith("draw_")]
        return pd.concat([getattr(self.mod_inputs, i)[merge_data] for i in self.data_attributes])

    def delete_data(self):
        """
        Remove the data attributes of a ensemble model.
        """
        for data in self.data_attributes:
            setattr(self.mod_inputs, data, None)
        for data in ["covariates", "priors"]:
            setattr(self.mod_inputs, data, None)

    def reload_data(self):
        """
        Reload a codem model after it has been completed.
        """
        sexn = "male" if self.mod_inputs.sex_id == 1 else "female"
        st_model_file = FILEPATH
        json_dict = space.import_json(st_model_file)
        draw_data = pd.read_hdf(FILEPATH, "data")
        self.mod_inputs.data_frame, self.mod_inputs.covariates, self.mod_inputs.priors = \
            Q.getCodemInputData(self.model_version_id, self.db_connection)
        self.adjust_input_data(save=False)
        st_models_temp = \
            All_Models(pd.concat([self.mod_inputs.data_frame,
                                  self.mod_inputs.covariates], axis=1),
                       self.mod_inputs.knockouts,
                       self.mod_inputs.linear_floor_rate, json_dict,
                       self.db_connection, make_preds=False)
        self.st_models.all_models[-1].models = st_models_temp.all_models[-1].models
        self.mod_inputs.bare_necessities()
        draw_data = draw_data.rename(columns={"age_group_id": "age", "year_id": "year", "sex_id": "sex"})
        self.mod_inputs.data_frame = pd.merge(self.mod_inputs.data_frame, draw_data, how='inner')
        self.mod_inputs.data_frame.drop_duplicates(["year", "location_id", "age"], inplace=True)

    def model_type_draw(self):
        """
        Dictionary linking each draw to a particular model type
        """
        model_type_dict = {"draw_{}".format(i): "st_model" if m in self.st_models.submodel_ids
                           else "linear_model" for i, m in enumerate(self.mod_inputs.draw_id)}
        return model_type_dict

    def model_id_draw(self):
        """

        """
        return {"draw_{}".format(i): m for i, m in enumerate(self.mod_inputs.draw_id)}

    def submodel_id_to_position(self, submodel_id):
        """
        Given a submodel id returns the position in the relevant model type.

        :param submodel_id: int
            Integer representing a valid submodel type
        :return: string
            Position of the submodel
        """
        try:
            pos = self.linear_models.submodel_ids.index(submodel_id)
            model_type = "linear_models"
        except ValueError:
            pos = self.st_models.submodel_ids.index(submodel_id)
            model_type = "st_models"
        return model_type, pos

    def submodel_components(self, submodel_id):
        """
        Return the components of a submodel id.

        :param submodel_id: int
            Integer representing a valid submodel type
        :return: dict
            dictionary of submodel components
        """
        model_type, pos = self.submodel_id_to_position(submodel_id)
        return getattr(self, model_type).all_models[-1].models[pos]

    def subset_data(self, submodels):
        """
        Given submodel(s) returns the draws data frame limited to only those
        submodels draws.

        :param submodels: int or list of int
            submodels from which to look for draws for
        :return: data_frame
            data_frame limited to those draws from specified submodels
        """
        if not isinstance(submodels, list):
            submodels = [submodels]
        id_cols = [c for c in self.mod_inputs.data_frame.columns if not c.startswith("draw_")]
        inv_map = {}
        for k, v in self.model_id_draw().iteritems():
            inv_map[v] = inv_map.get(v, [])
            inv_map[v].append(k)
        draws = [inv_map[m] for m in submodels if m in inv_map.keys()]
        draws = [d for sub in draws for d in sub]
        assert len(draws) > 0, "no draws for the submodel(s)"
        return self.mod_inputs.data_frame[id_cols+draws]

    @staticmethod
    def transform_draws(df, model_space):
        """
        Transforms death draws into the desired model space.

        :param df: data_frame
            data frame with some "draw_" columns in death space
        :param model_space: str
            either "rate", "cf", or "death"
        :return:
            data frame with transformed draws and response variable (y)
        """
        assert model_space in ["death", "cf", "rate"], 'models_space must be either "death", "cf", or "rate"'
        assert len(np.setdiff1d(["cf", "envelope", "pop"], df.columns)) == 0, 'df must contain "cf", "envelope" & "pop"'
        draws = [d for d in df.columns if d.startswith("draw_")]
        if model_space == "death":
            df["y"] = df["cf"] * df["envelope"]
        elif model_space == "cf":
            df[draws] = df[draws].values / df["envelope"].values[..., np.newaxis]
            df["y"] = df["cf"]
        else:
            df[draws] = df[draws].values / df["pop"].values[..., np.newaxis] * 100000
            df["y"] = ((df["cf"] * df["envelope"]) / df["pop"]) * 100000
        df["y_hat"] = df[draws].mean(axis=1)
        return df

    def plot_location(self, location_id, age_group_id, model_space="death", submodels=None):
        """
        Plots a particular location form a codem model run. Can plot in three
        different model spaces, rate("rate"), cause fraction("cf"), and
        death("death") space. Graphs can also be limited to a certain number
        of submodel(s).

        :param location_id: int
            location id of the desired plot
        :param age_group_id: int
            age group id of the desired plot
        :param model_space: str
            either "rate", "cf", or "death" as specified in the description
        :param submodels: int or list of int
            submodels from which to look for draws for or Null for all draws
        :return: plot
            time series plot
        """
        if submodels is None:
            sub_df = self.mod_inputs.data_frame.copy()
        else:
            sub_df = self.subset_data(submodels)
        call = 'location_id == {l} & age == {a}'
        sub_df = sub_df.query(call.format(l=location_id, a=age_group_id))
        sub_df = self.transform_draws(sub_df, model_space)
        sub_df.sort("year", inplace=True)
        draws = [d for d in sub_df.columns if d.startswith("draw_")]
        plt.plot(sub_df.year, sub_df.y_hat)
        plt.plot(sub_df.year, sub_df.y, "ro")
        if len(draws) >= 20:
            sub_df["lower_bound"] = sub_df[draws].quantile(.025, axis=1)
            sub_df["upper_bound"] = sub_df[draws].quantile(.975, axis=1)
            plt.fill_between(sub_df.year, sub_df.lower_bound, sub_df.upper_bound, alpha=.2)
        plt.xlabel("Year")
        plt.ylabel(model_space)
        plt.title("location_id: {l}, age_group_id: {a}".format(l=location_id, a=age_group_id))

    def submodel_rank(self):
        """
        Pandas data frame

        Returns a dataframe of the submodels for the model version id,
        ranked by weight.
        """
        call = '''SELECT
            submodel_version_id,
            submodel_type_id,
            submodel_dep_id,
            weight,
            rank
        FROM
            cod.submodel_version
        WHERE
            model_version_id = {}
        '''
        call = call.format(self.model_version_id)
        df = db_connect.query(call, self.db_connection)
        return df.sort("rank")


def load_codem_model(model_version_id, db_connection):
    """
    Given a model version id loads the codem instance into memory for exploration

    :param model_version_id: int
        valid model version id pointing to a correct
    :return: Ensemble
        a codem Ensemble object
    """
    acause = Q.acause_from_id(model_version_id, db_connection)
    f = "FILEPATH"
    codem_model = dill.load(open(f, "rb"))
    codem_model.reload_data()
    return codem_model

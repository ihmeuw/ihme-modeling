
import matplotlib
matplotlib.use('Agg')
import os
import sys
import subprocess
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import numexpr as ne
import warnings
import logging

import codem.reference.warning as warn
import codem.ensemble.gangnam_style as gs
import codem.data.query as Q
import codem.ensemble.PV as PV
from codem.data.query import acause_from_id
from codem.joblaunch.run_utils import get_model_dir
from codem.joblaunch.linmodTask import LinMod
import codem.reference.db_connect as db_connect
import codem.stgpr.space_time_smoothing as space
from codem.ensemble.all_models import All_Models

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", 'Mean of empty slice.')
pd.set_option('chained', None)
ne.set_num_threads(30)


class Ensemble:
    """
    Using a model_version_id runs a codem model
    """
    def __init__(self, model_version_id, db_connection, gbd_round_id=5, debug_mode=False,
                 log_results=True, old_covariates_mvid=None):
        """
        Queries data base for model data

        :param model_version_id: int
            Integer representing a valid model version id
        :param log_results: bool
            Boolean indicating whether to write status updates to DB
        """
        self.model_version_id = model_version_id
        self.db_connection = db_connection
        self.acause = acause_from_id(self.model_version_id, self.db_connection)
        self.model_dir = get_model_dir(self.model_version_id, self.db_connection)
        self.gbd_round_id = gbd_round_id
        self.warnings = warn.WarningLog(model_version_id, db_connection)
        if log_results:
            self.warnings.time_stamp('Querying for model parameters')
        self.log_results = log_results
        self.linear_models = None
        self.st_models = None
        self.json_dict = None
        self.data_attributes = ['data_frame', 'country_df', 'region_df', 'super_region_df', 'age_df', 'global_df',
                                'age_location_df', 'age_country_df', 'age_region_df', 'age_super_region_df']
        self.debug_mode = debug_mode
        if debug_mode:
            self.seed = 0
        else:
            self.seed = self.model_version_id
        self.old_covariates_mvid = old_covariates_mvid

    def log_data_errors(self):
        """
        Logs missingness and and bad cf to the log file
        """
        logger.info("Logging data errors.")
        self.warnings.log_missingness(self.mod_inputs.covariates,
                                      self.mod_inputs.cv_names)
        self.warnings.log_bad_cf(self.mod_inputs.data_frame, "cf")

    def move_run_location(self):
        """
        Move the location of where the code is running to be model specific
        """
        logger.info("Moving run location.")
        os.chdir(self.model_dir)

    def submit_R(self, scriptname):
        LinMod(scriptname, self.model_version_id, self.db_connection, self.model_dir)

    def covariate_selection(self):
        """
        Run covariate selection process.
        """
        logger.info('Covariate selection')
        if self.log_results:
            self.warnings.time_stamp('Running covariate selection')
        if not os.path.isfile('cv_selected.txt'):
            if self.old_covariates_mvid is None:
                logger.info('Need to run covariate selection because there are no covariates yet.')
                self.submit_R("cvSelection")
            else:
                logger.info(('Using {} for old covariates.').format(self.old_covariates_mvid))
                cov_file_path = get_model_dir(self.old_covariates_mvid, self.db_connection) + '/cv_selected.txt'
                print(cov_file_path)
                subprocess.call('cp ' + cov_file_path + ' ' + self.model_dir, shell=True)
        else:
            logger.info('Covariate selection has already happened for this model version ID.')
        if not os.path.isfile('cv_selected.txt'):
            logger.info('No covariates seem to have been selected.')
        self.mod_inputs.submodel_covariates = space.import_cv_vars('cv_selected.txt')

    def run_knockouts(self):
        """
        Run the CODEM knockout process
        """
        logger.info("Running knockouts.")
        if self.log_results:
            self.warnings.time_stamp("Running KO process")
        self.mod_inputs.create_knockouts()

        # write the knockouts
        pd.concat(self.mod_inputs.knockouts, axis=1).to_csv("ko_data.csv",
                                                            index=False)

    def run_linear_model_builds(self):
        """
        Run the linear model portions of all codem models.
        """
        logger.info("Running linear model buids.")
        if self.log_results:
            self.warnings.time_stamp("Running Linear Model Builds")

        # run linear models and linear portion of space time models
        self.submit_R("lm_model_prototype")

        # check to make sure covariate selection picked something
        if not os.path.isfile("space_time_json.txt"):
            sys.stderr.write("No covariates seem to have been selected.")

        # import json file from R build and save the model list portion
        self.json_dict = space.import_json("space_time_json.txt")
        self.mod_inputs.response_list = [x.rsplit("_", 1)[0] for x in
                                         sorted(list(self.json_dict.keys()))]

    def read_spacetime_models(self):
        """
        Read the space time models created in R
        """
        logger.info("Reading spacetime models.")
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
        logger.info("Applying spacetime smoothing.")
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
        logger.info("Applying GP smoothing.")
        if self.log_results:
            self.warnings.time_stamp("GPR smoothing")
        self.st_models.gpr_all(self.mod_inputs.data_frame,
                               self.mod_inputs.knockouts,
                               self.mod_inputs.response_list,
                               self.mod_inputs.gpr_year_corr,
                               self.mod_inputs.decomp_step_id)
        if self.debug_mode:
            self.st_models.pred_mat_gpr = self.st_models.all_models[-1].pred_mat.copy()
        else:
            pass

    def read_linear_models(self):
        """
        Read linear models created in R
        """
        logger.info("Reading linear models.")
        json_dict = space.import_json("linear_model_json.txt")
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
        logger.info("Submodel predictive validity.")
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
        logger.info("Finding optimal psi value.")
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
        logger.info("Calculating final ensemble predictive validity.")
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
        logger.info("Creating linear model draws.")
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
        logger.info("Creating GPR draws.")
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
        logger.info("Calculating coverage.")
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
        logger.info('Writing results to the database and draw files.')
        if self.log_results:
            self.warnings.time_stamp("Getting all the logistical stuff")

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

        self.mod_inputs.post_processing()

    def cleanup_code_base(self):
        """
        Delete the temporary location of codem for this model run.
        """
        if self.log_results:
            self.warnings.time_stamp("Finished!!!")

        # log our warnings
        f = open("warnings.txt", "w")
        f.write(self.warnings.text)
        f.close()

        subprocess.call("rm -rf FILEPATH".format(self.model_version_id), shell=True)

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

    def reload_data(self, gbd_round_id):
        """
        Reload a codem model after it has been completed.
        Do not re-save model outliers.
        """
        sexn = "male" if self.mod_inputs.sex_id == 1 else "female"
        st_model_file = os.path.join(self.model_dir, "space_time_json.txt")
        json_dict = space.import_json(st_model_file)
        draw_data = pd.read_hdf(self.model_dir + "/draws/deaths_{}.h5".format(sexn), "data")
        self.mod_inputs.data_frame, self.mod_inputs.covariates, self.mod_inputs.priors = \
            Q.getCodemInputData(self.model_version_id, self.db_connection, gbd_round_id, outlier_save = False)
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
        :return: strint
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
        draws = [inv_map[m] for m in submodels if m in list(inv_map.keys())]
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


def load_codem_model(model_version_id, db_connection, gbd_round_id):
    """
    Given a model version id loads the codem instance into memory for exploration.
    Changed this to read_pickle rather than dill.load because it was not compatible with switching
    between pandas versions, and read_pickle is so much faster.

    :param model_version_id: int
        valid model version id pointing to a correct
    :return: Ensemble
        a codem Ensemble object
    """
    acause = Q.acause_from_id(model_version_id, db_connection)
    f = "FILEPATH".format(acause=acause, model=model_version_id)
    codem_model = pd.read_pickle(f)
    return codem_model

import space_time_smoothing as space
import numpy as np
import pandas as pd
import multiprocessing as mp
import gpr_smooth as gpr
import PV as pv
import draws as dr
import query as Q


class All_Models:
    def __init__(self, df, knockouts, linear_floor_rate, json_dict,
                            db_connection, debug_mode=False, make_preds=True):
        self.all_models = space.make_all_models(df, knockouts,
                                    linear_floor_rate, json_dict, make_preds)
        self.RMSE = None
        self.trend = None
        self.draws = None
        self.ranks = None
        self.gpr_parameters = None
        self.psi_weights = None
        self.submodel_ids = None
        self.draw_id = None
        self.db_connection = db_connection
        self.pred_mat_lin = None
        self.pred_mat_st = None
        self.pred_mat_gpr = None


    def apply_smoothing(self, df, knockouts, omega, lambda_data,
                        lambda_no_data, zeta, zeta_no_data):
        tempAge = sorted(list(df.age.unique()))
        df["ageC"] = np.array([tempAge.index(x) for x in list(df.age)])
        for i in range(len(self.all_models)):
            self.all_models[i].spacetime_predictions(df, knockouts[i], omega,
                                                     lambda_data, lambda_no_data,
                                                     zeta, zeta_no_data)

    def reset_residuals(self, df, knockouts, response_list):
        '''
        (self, data frame, list of data frames, list) -> None

        Reset the residual data frame of all the models for post spacetime results.
        '''
        for i in range(len(self.all_models)):
            self.all_models[i].reset_residuals(response_list, knockouts[i], df)

    def gpr_all(self, data_frame, knockouts, response_list, scale, parallel=True):
        '''
        (self, data frame, list of data frames, list of str, float) -> None

        Runs all of the gpr smoothing in parallel using  multiprocessing
        '''
        inputs = [(data_frame[["location_id", "age", "source_type", "national"]],
                   knockouts[i]) for i in range(len(knockouts))]
        p = mp.Pool(30)
        variance_list = p.map(gpr.variance_map, inputs)
        p.close()
        p.join()
        variables = ["location_id", "age", "year", "ln_rate",
                     "lt_cf", "ln_rate_sd", "lt_cf_sd"]
        inputs = [(data_frame[variables], knockouts[i],
                   self.all_models[i].simple_st_ln_rate,
                   self.all_models[i].simple_st_lt_cf,
                   self.all_models[i].res_mat, variance_list[i])
                  for i in range(len(knockouts))]
        nsv, amplitudes = map(list, zip(*map(gpr.nsv_map, inputs)))
        del inputs
        for i in range(len(amplitudes)):
            amplitudes[i] = gpr.median_amplitude(amplitudes[i])
        smooth_pred = \
            [gpr.new_gpr(pd.concat([data_frame[variables], nsv[i]], axis=1),
                         knockouts[i], amplitudes[i],
                         self.all_models[i].st_smooth_mat, variance_list[i],
                         response_list, scale, parallel)
             for i in range(len(knockouts))]
        for i in range(len(self.all_models)):
            self.all_models[i].pred_mat = smooth_pred[i]
            self.all_models[i].nsv = nsv[i]
            self.all_models[i].amplitude = amplitudes[i]
            self.all_models[i].variance_type = variance_list[i]

    def uniform_predictions(self, df, response_list):
        '''
        (self, data frame, list of strings) -> None

        Make all the predictions for an object of class All_Model so that they
        are predicting in log rate space.
        '''
        for i in range(len(self.all_models)):
            self.all_models[i].pred_mat = \
                space.uniform_predictions(df, self.all_models[i].pred_mat,
                                          response_list)

    def rmse_out(self, df, knockouts):
        '''

        Calculate the root mean squared error of all models.
        '''
        self.RMSE = pv.rmse_out(self.all_models, df, knockouts)

    def trend_out(self, df, knockouts, window):
        '''

        Calculate the trend error of all models.
        '''
        self.trend = pv.trend_out(self.all_models, df, knockouts, window)

    def delete_mixed_model_parameters(self):
        '''
        (self) -> None

        Delete all the Mixed model parameters from a all model class object.
        This is basically a space saving utility.
        '''
        for i in range(len(self.all_models)):
            for j in range(len(self.all_models[i].models)):
                self.all_models[i].models[j].vcov = None
                self.all_models[i].models[j].fix_eff = None
                self.all_models[i].models[j].ran_eff = None
                self.all_models[i].models[j].random_intercepts = None
                self.all_models[i].models[j].variables = None
                self.all_models[i].models[j].beta_draw = None

    def get_lin_mod_parameters(self):
        '''
        (self) -> list of lists

        Extracts all the things necessary for making predictions using draws
        from the variance covariance matrix. Should be able to make draws based
        off of the number of times we use a model for a particular ensemble.
        '''
        mv = [[{"fix_eff": self.all_models[i].models[j].fix_eff,
                "ran_eff": self.all_models[i].models[j].ran_eff,
                "vcov": self.all_models[i].models[j].vcov,
                "variables": self.all_models[i].models[j].variables}
               for j in range(len(self.all_models[i].models))]
              for i in range(len(self.all_models))]
        return mv

    def get_gpr_parameters(self):
        '''
        (self) -> list of lists

        Extracts all the things necessary for making predictions using draws
        from the mean and covariance functions. Should be able to make draws
        based off of the number of times we use a model for a particular
        ensemble.
        '''
        mv = [[self.all_models[i].models[j].gpr_dict
               for j in range(len(self.all_models[i].models))]
              for i in range(len(self.all_models) - 1)]
        return mv

    def del_predictions(self):
        '''
        (self) -> None

        Delete all the predictions from the knockout models.
        '''
        for i in range(len(self.all_models)):
            self.all_models[i].pred_mat = None

    def del_space_time(self):
        '''
        (self) -> None

        Delete all the predictions from the knockout models.
        '''
        for i in range(len(self.all_models)):
            self.all_models[i].st_smooth_mat = None

    def del_draws(self):
        '''
        (self) -> None

        Delete all the predictions from the knockout models.
        '''
        for i in range(len(self.all_models)):
            self.all_models[i].draw_preds = None

    def all_linear_draws(self, df, linear_floor, response_list):
        lin_mod_parameters = self.get_lin_mod_parameters()
        temp = pd.core.reshape.get_dummies(df.age)
        temp.columns = ["age" + str(int(s)) if s >= 1 else "age" + str(s)
                        for s in temp.columns]
        df = pd.concat([df, temp], axis=1)
        df["super_region"] = df.super_region.astype(str)
        del temp
        inputs = [(lin_mod_parameters[i], df, linear_floor, response_list,
                   self.draws) for i in range(len(lin_mod_parameters))]
        all_linear = np.array(map(dr.linear_draws_map, inputs))
        for i in range(len(all_linear)):
            if len(all_linear[0]) > 0:
                all_linear[i] = \
                    space.uniform_predictions(df, all_linear[i],
                                              np.repeat(response_list, self.draws))
            self.all_models[i].draw_preds = all_linear[i]

    def all_gpr_draws(self, df, knockouts, response_list):
        country_age_df = [df.loc[knockouts[i].ix[:, 2],
                                 ["location_id", "age"]].drop_duplicates()
                          for i in range(len(knockouts))]
        inputs = [(self.gpr_parameters[i], df[["location_id", "age", "year"]],
                   country_age_df[i], self.draws) for i in range(len(self.gpr_parameters))]
        all_gpr = np.array(map(dr.gpr_draws_map, inputs))
        for i in range(len(all_gpr)):
            all_gpr[i] = \
                space.uniform_predictions(df, all_gpr[i],
                                          np.repeat(response_list, self.draws))
            self.all_models[i].draw_preds = all_gpr[i]

    def all_gpr_draws2(self, df, knockouts, response_list, scale, linear_floor):
        if sum(self.draws) == 0:
            for i in range(len(knockouts)):
                self.all_models[i].st_smooth_mat = None
                self.all_models[i].draw_preds = np.array([])
            return None
        variables = ["location_id", "age", "year", "ln_rate",
                     "lt_cf", "ln_rate_sd", "lt_cf_sd"]
        new_draws = \
            [dr.new_ko_gpr_draws(pd.concat([df[variables],
                                            self.all_models[i].nsv], axis=1),
                                 knockouts[i], self.all_models[i].amplitude,
                                 self.all_models[i].st_smooth_mat,
                                 self.all_models[i].variance_type,
                                 response_list, scale, self.draws)
             for i in range(len(knockouts))]
        for i in range(len(new_draws)):
            new_draws[i] = \
                space.uniform_predictions(df, new_draws[i],
                                          np.repeat(response_list, self.draws))
            new_draws[i] = space.bound_gpr_draws(df, new_draws[i], linear_floor)
            self.all_models[i].st_smooth_mat = None
            self.all_models[i].draw_preds = new_draws[i]

    def id_to_draws(self):
        temp = [[self.submodel_ids[i] for j in range(self.draws[i])]
                for i in range(len(self.draws))]
        self.draw_id = [item for sub_list in temp for item in sub_list]

    def get_submodel_ids(self, response_list, model_version_id, type_id):
        dependent = np.array([2 * (x == "ln_rate") for x in response_list])
        dependent += np.array([1 * (x == "lt_cf") for x in response_list])
        self.submodel_ids = [0 for i in range(len(self.draws))]
        for i in range(len(self.psi_weights)):
            self.submodel_ids[i] = Q.write_submodel(model_version_id, type_id,
                                                    dependent[i],
                                                    self.psi_weights[i],
                                                    self.ranks[i],
                                                    self.db_connection)
        self.id_to_draws()

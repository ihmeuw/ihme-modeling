import pandas as pd
import numpy as np
import logging

import codem.stgpr.spacetime as ST

logger = logging.getLogger(__name__)


class Model_List:
    def __init__(self, list_of_models, df, ko, linear_floor):
        self.models = list_of_models
        self.pred_mat = None
        self.res_mat = None
        self.st_smooth_mat = None
        self.simple_st_ln_rate = df.ln_rate.values * -1.
        self.simple_st_lt_cf = df.lt_cf.values * -1.
        self.nsv = None
        self.draw_preds = None
        self.amplitude = None
        self.variance_type = None

    def make_prediction_matrix(self, df, ko, linear_floor):
        """
        (self, data frame, data frame, float) -> array

        Given a data frame, a set of knockouts used for the model
        list and a linear floor to put a lower range on predictions
        returns a matrix of predictions for each model.
        """
        logger.info("Making prediction matrix.")
        f = lambda x: x.make_prediction(df, ko, linear_floor)
        self.pred_mat = np.array(list(map(f, self.models))).T

    def make_residual_matrix(self, df, ko):
        """
        (self, data frame, data frame) -> array

        Given a data frame from which observations are taken and
        a set of knockouts returns a matrix of residuals but only
        for data that was used in the training set.
        """
        ko2 = ko.copy()
        ko2 = ko2.reset_index(drop=True)
        keep_rows = ko2[ko2.ix[:,0]].index.values
        logger.info("Creating residual matrix.")
        res_mat = np.array([self.models[i].calc_res(self.pred_mat[:,i], df, ko)
                            for i in range(len(self.models))]).T
        self.res_mat = res_mat[keep_rows, :]

    def super_region_positions(self, indices, sReg, df, ko):
        """
        (self, str, data frame, data frame)

        Given a string representing a valid super region, a data frame of cause
        data and a knock out data frame returns a dictionary with each position
        for that super region in the training and in the ful data set with null
        data removed.
        """
        logger.info("Getting super-region positions.")
        full = pd.concat([df, ko], axis=1)
        full = full.reset_index(drop=True)
        full_rows = full.iloc[indices, :].index.values
        train = full[full.ix[:, -3]]
        train = train.reset_index(drop=True)
        train_rows = train[train.super_region == sReg].index.values
        return {"full_rows": full_rows, "train_rows": train_rows}

    def single_st(self, indices, sReg, df, ko, omega_age_smooth, lambda_time_smooth,
                  lambda_time_smooth_nodata, zeta_space_smooth,
                  zeta_space_smooth_nodata):
        """
        Applies a single instance of spacetime weighting for a single super
        region. Results are stored in the self.st_smooth_mat matrix in the
        appropriate rows for that super region.
        """
        logger.info("Applying single instance of spacetime smoothing.")
        pos = self.super_region_positions(indices, sReg, df, ko)
        st = ST.spacetime(
            indices, sReg, df, ko, omega_age_smooth, lambda_time_smooth,
            lambda_time_smooth_nodata, zeta_space_smooth,
            zeta_space_smooth_nodata
        )
        residuals = self.res_mat[pos["train_rows"], :]
        ln_rate_adj = df[ko.ix[:, 0]]["ln_rate"].values[pos["train_rows"]]
        new_res = np.dot(st.T, residuals)
        self.st_smooth_mat[pos["full_rows"], :] = new_res
        new_simple_st = np.dot(st.T, ln_rate_adj)
        self.simple_st_ln_rate[pos["full_rows"]] = new_simple_st + self.simple_st_ln_rate[pos["full_rows"]]
        mini = df.ix[indices, ["pop", "envelope"]]
        new_simple_st = np.exp(new_simple_st) * \
                        (mini["pop"].values / mini["envelope"].values)
        new_simple_st[new_simple_st >= 1] = 0.9999
        new_simple_st = np.log(new_simple_st / (1. - new_simple_st))
        self.simple_st_lt_cf[pos["full_rows"]] = new_simple_st + self.simple_st_lt_cf[pos["full_rows"]]

    def spacetime_predictions(self, df, ko, omega_age_smooth, lambda_time_smooth,
                              lambda_time_smooth_nodata, zeta_space_smooth,
                              zeta_space_smooth_nodata):
        """
        Applies space time to all super regions in sequence. Only regions which
        appear in the training set have spacetime smoothing applied to them.
        """
        chunk_size = 5000
        self.st_smooth_mat = np.zeros(self.pred_mat.shape).astype("float32")
        super_regions = df[ko.ix[:, 0]]["super_region"].drop_duplicates().values
        for sr in super_regions:
            logger.info(f"Working on super region {sr} out of {super_regions}")
            df_sub = df.loc[df.super_region == sr, :]
            size = df_sub.shape[0]
            num_chunks = int(np.ceil(float(size)/chunk_size))
            idx = df_sub.index.values
            indices = [idx[range(chunk_num*chunk_size,
                                 min((chunk_num+1)*chunk_size, size))]
                       for chunk_num in range(num_chunks)]
            for chunk_num in range(num_chunks):
                logger.info(f"Working on chunk number {chunk_num} out of {num_chunks} in super region {sr}")
                self.single_st(
                    indices[chunk_num], sr, df, ko, omega_age_smooth,
                    lambda_time_smooth, lambda_time_smooth_nodata,
                    zeta_space_smooth, zeta_space_smooth_nodata
                )
        self.st_smooth_mat = self.pred_mat + self.st_smooth_mat

    def reset_residuals(self, response_list, ko, df):
        full = pd.concat([df, ko], axis=1)
        full = full.reset_index(drop=True)
        indices = full[full.ix[:,-3]].index.values
        self.res_mat = self.st_smooth_mat[indices,:] - \
                       df[(ko.ix[:,0])][response_list].values
        self.pred_mat = None

    def get_lin_mod_parameters(self, draw_number):
        """
        (self, list of ints) -> list of dicts

        Extracts all the stuff necessary for making predictions using draws
        from the variance covariance matrix. Should be able to make draws based
        off of the number of times we use a model for a particular ensemble.
        """
        draws = [np.repeat(i, draw_number[i]) for i in range(len(draw_number))]
        draws = [i for j in draws for i in j]
        mv = [{"fix_eff": self.models[j].fix_eff,
               "ran_eff": self.models[j].ran_eff,
               "vcov": self.models[j].vcov,
               "variables": self.models[j].variables} for j in draws]
        return mv

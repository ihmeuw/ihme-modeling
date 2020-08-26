'''
'''

import pandas as pd
import numpy as np


def dic2df(dic):
    '''
    (dict) -> data frame

    This function takes a dictionary that represents a model built in
    R and turns it into a data frame that we can use for prediction in
    a manageable way. The function also re-indexes the data frame so that
    it is indexed on the _row value and makes sure that the value doesn't
    have any leading underscores that we had to create in R for super region.
    '''
    df = pd.DataFrame(dic)
    if df["_row"].values[0].startswith("_"):
        df._row = df["_row"].map(lambda x: x[1:])
    df = df.set_index("_row")
    return df


class Model:
    def __init__(self, d, response):
        self.vcov = np.array(d["vcov"])
        self.fix_eff = dic2df(d["fix_eff"])
        self.ran_eff = {k: dic2df(d["ran_eff"][k]) for k in list(d["ran_eff"].keys())}
        self.random_intercepts = list(self.ran_eff.keys())
        self.variables = self.fix_eff.index.values
        self.response = response
        self.RMSE = np.NaN
        self.beta_draw = self.fix_eff.values.ravel()
        self.gpr_dict = None

    def __repr__(self):
        return "Variables included in model: " + str(self.variables[1:])

    def make_draw(self, return_vector=False):
        '''
        (self) -> None

        Makes a draw from the variance covariance matrix to change the values
        of self.beta_draw. Every time this function is run the self.beta_draw
        values are updated. Note that the initial values of the self.beta_draw
        are simply the mean values as given in self.fix_eff.
        '''
        self.beta_draw = np.dot(np.linalg.cholesky(self.vcov),
                                np.random.normal(size=len(self.vcov))) + \
                         self.fix_eff.values.ravel()
        if return_vector:
            return self.beta_draw

    def adjust_predictions(self, vector, linear_floor, df_sub):
        '''
        (self, array, float, data frame)

        Given an 1-D array "vector" which is a set of predictions adjusts the
        upper and lower limits of the vector so now value is outside of the range.
        linear_floor and df_sub are used to calculate wether a prediction is
        outside of the range.
        '''
        if self.response == "ln_rate":
            vector[vector < np.log(linear_floor/100000.)] = \
                np.log(linear_floor/100000.)
            ceiling = np.log((df_sub["envelope"] / df_sub["pop"]).values)
            need_replace = (vector > ceiling).astype(np.int8)
            vector = (vector * 0**need_replace) + (ceiling * need_replace)
        elif self.response == "lt_cf":
            x = np.log((linear_floor/100000.) / (df_sub["envelope"]/df_sub["pop"]))
            floor = x / (1 - x)
            need_replace = (vector < floor).astype(np.int8)
            vector = (vector * 0**need_replace) + (floor * need_replace)
        return vector


    def make_prediction(self, df, ko, linear_floor):
        '''
        (self, data frame, data frame) -> array

        Takes in two data frames, "df" is the full data frame used in the
        course of the analysis and ko is a data frame indicating which rows are
        used for training, and different phases of testing. In addition to
        returning an array of predicted values, the self.RMSE value is also
        updated to show the out of sample predictive validity on test set 1.
        '''
        df_sub = df.loc[(ko.ix[:,0]) | (ko.ix[:,1]) | (ko.ix[:,2])]
        fix = (np.dot(df_sub[self.variables[1:]], self.fix_eff.values[1:]) +
               self.fix_eff.values[0]).ravel()
        ran = np.array([self.ran_eff[x].loc[df_sub[x]].values.ravel() for
                        x in list(self.ran_eff.keys())]).sum(0)
        df_sub["predictions"] = self.adjust_predictions(fix+ran, linear_floor,
                                                        df_sub)
        df_out = df_sub.loc[ko[ko.ix[:,1]].index]
        N = len(df_out)
        self.RMSE = np.linalg.norm(df_out["predictions"].values -
                                   df_out[self.response].values) / np.sqrt(N)
        return df_sub["predictions"].values

    def calc_res(self, predictions, df, ko):
        '''
        (array, data frame) -> array

        Given an array of predictions and a data frame "df" returns an array
        of residuals for the given predictions.
        '''
        df_sub = df[(ko.ix[:,0]) | (ko.ix[:,1]) | (ko.ix[:,2])]
        return df_sub[self.response].values - predictions

    def make_draw_prediction(self, df, linear_floor, ko, new_draw=True):
        '''
        (self, data frame, bool) -> array

        Similar to the make predictions however there is no updating of the
        self.RMSE value. In addition rather than using the mean values of the
        betas a draw is made from the variance covariance matrix and those
        values of beta are used instead. "new_draw" indicates whether you
        want to make a new draw from the variance covariance matrix are use
        the existing set of values stored in self.beta_draw.
        '''
        if new_draw:
            self.make_draw()
        df_sub = df[(ko.ix[:,0]) | (ko.ix[:,1]) | (ko.ix[:,2])]
        fix = (np.dot(df_sub[self.variables[1:]], self.beta_draw[1:]) +
               self.beta_draw[0]).ravel()
        ran = np.array([self.ran_eff[x].loc[df_sub[x]].values.ravel() for
                        x in list(self.ran_eff.keys())]).sum(0)
        return self.adjust_predictions(fix+ran, linear_floor, df_sub)
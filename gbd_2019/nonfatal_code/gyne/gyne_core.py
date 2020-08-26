from __future__ import division
from scipy.special import expit
import os
import numpy as np
import pandas as pd
import argparse
import random
import copy
import re
from job_utils import draws, parsers
from db_queries import get_covariate_estimates


class Gynecological(draws.SquareImport):

    def __init__(self, me_map, decomp_step, **kwargs):
        # super init
        super(Gynecological, self).__init__(decomp_step, **kwargs)

        # store data by me in this dict key=me_id
        self.me_map = me_map
        self.me_dict = {}

        #import every input and create a dictionary of dataframes
        for mapper_key, mapper in me_map.items():
            inputs = mapper.get("srcs",{})
            for src_key, me_id in inputs.items():
                self.me_dict[me_id] = self.import_square(
                    "modelable_entity_id", me_id, source="epi")

    def draw_beta(self, mean, lower, upper):
        np.random.seed()
        sd = (upper - lower) / (2 * 1.96)
        sample_size = mean * (1 - mean) / sd ** 2
        alpha = mean * sample_size
        beta = (1 - mean) * sample_size
        draws = np.random.beta(alpha, beta, size=1000)
        return draws
    
    @staticmethod
    def get_symptomatic_coefficient():
        """
        Obtains the factor by which you can derive Symptomatic Fibroid prevalence from Total Fibroid prevalence.
        It's the inverse logit of the coefficient on the MR-BRT model.
        """

        mrbrt_coefficient_location = "FILEPATH"
        mrbrt_output = pd.read_csv(mrbrt_coefficient_location, index_col='x_cov')
        symptomatic_logit_coeff = mrbrt_output.beta_soln.loc['cv_symptomatic']
        return expit(symptomatic_logit_coeff)

    def split_total_fibroids(self):
        """
        Splits Total Fibroids into symptomatic and asymptomatic
        """

        total_fibroids_me_id = self.me_map["uterine_fibroids"]["srcs"]["total"]
        asymp_fibroids_me_id = self.me_map["uterine_fibroids"]["trgs"]["asymp"]
        symp_fibroids_me_id = self.me_map["uterine_fibroids"]["trgs"]["symp"]

        sym_coeff = self.get_symptomatic_coefficient()
        total_fibroids_df = self.me_dict[total_fibroids_me_id]
        asymptomatic_factor = 1 / (1 + sym_coeff)
        asymp_fibroids_df = total_fibroids_df * asymptomatic_factor
        symp_fibroids_df = total_fibroids_df - asymp_fibroids_df

        self.me_dict[asymp_fibroids_me_id] = asymp_fibroids_df
        self.me_dict[symp_fibroids_me_id] = symp_fibroids_df
        
    def adjust_pms(self):
        '''Adjusts PMS (Pre-menstrual Syndrome) cases for pregnancy prevalence and incidence '''
        pms_key = self.me_map["pms"]["srcs"]["tot"]
        adj_key = self.me_map["pms"]["trgs"]["adj"]

        covariate_index_dimensions = ['age_group_id', 'location_id', 'sex_id', 'year_id']
        
        pms_df = self.me_dict[pms_key]
        asfr_df = get_covariate_estimates(13, decomp_step=self.decomp_step)
        sbr_df = get_covariate_estimates(2267, decomp_step=self.decomp_step)

        asfr_df = asfr_df.filter(covariate_index_dimensions + ['mean_value'])
        # Assumes the still birth rates covariate is reported for all_ages 
        # and both sexes. Hence we ignore that and merge only on loc and year
        sbr_df = sbr_df.filter(['location_id', 'year_id', 'mean_value'])

        asfr_df.rename(columns={'mean_value':'asfr_mean'}, inplace=True)
        sbr_df.rename(columns={'mean_value':'sbr_mean'}, inplace=True)

        prop_df = asfr_df.merge(sbr_df, how='inner', 
            on=['location_id', 'year_id'])
        prop_df[prop_df.sex_id == 1].sbr_mean = 0

        prop_df['prop'] = (prop_df.asfr_mean + 
            (prop_df.asfr_mean * prop_df.sbr_mean)) * 46/52
        prop_df.set_index(covariate_index_dimensions, inplace=True)

        adj_df = pms_df.copy()
        adj_df.reset_index(level='measure_id', inplace=True)
        adj_df = adj_df.merge(prop_df.prop, how='inner', left_index=True, right_index=True)

        for col in self.draw_cols:
            adj_df[col] = adj_df[col] * (1 - adj_df.prop)
        
        adj_df.drop(columns='prop', inplace=True)

        self.me_dict[adj_key] = adj_df

##############################################################################
# function to run core code
##############################################################################

def gyne_data(me_map, location_id, out_dir, decomp_step):
    # retrieve default dimensions needed to initialize draws.SquareImport class, then change as needed
    dim = Gynecological.default_idx_dmnsns

    # production dimensions
    dim["location_id"] = [location_id]
    dim["sex_id"] = [2]

    # initialize instance of Congenital class
    gyne = Gynecological(me_map=me_map, decomp_step=decomp_step, idx_dmnsns=dim)
    gyne.split_total_fibroids()
    gyne.adjust_pms()
    
    for _, mapper in me_map.items():
        outputs = mapper.get("trgs", {})
        for _, me_id in outputs.items():
            fname = str(location_id) + ".h5"
            out_df = gyne.me_dict[me_id].reset_index()
            out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), key="draws", format="table", data_columns=dim.keys())

##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", type=parsers.json_parser)
    parser.add_argument("location_id", help="which year to use", type=int)
    parser.add_argument("out_dir", help="root directory to save stuff")
    parser.add_argument("decomp_step", help='decomp step')
    args = vars(parser.parse_args())
    
    # call function
    gyne_data(me_map=args["me_map"], location_id=args["location_id"], out_dir=args["out_dir"], decomp_step=args['decomp_step'])

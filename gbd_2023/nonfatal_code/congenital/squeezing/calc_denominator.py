from __future__ import division
import numpy as np
import pandas as pd
from numpy.random import normal
from db_queries import get_life_table
from db_queries import get_covariate_estimates
from db_queries import get_demographics
from db_queries import get_envelope
from gbd.estimation_years import estimation_years_from_gbd_round_id
from gbd.gbd_round import gbd_round_from_gbd_round_id
import collections
import glob
import os
import re
import argparse

class Neonatal_denominator():

    def __init__(self, out_dir, location_id, gbd_round_id, decomp_step):
        self.location_id = location_id
        self.birth_path =  ("FILEPATH")
        run_id = 314
        self.mort_path = ('FILEPATH/{run_id}/'
                            'life_tables'.format(run_id=run_id))
        self.out_dir = out_dir
        self.gbdr = gbd_round_id
        self.decomp_step = decomp_step

        self.dim_births = {
            'year_id': list(range(min(estimation_years_from_gbd_round_id(7)), max(estimation_years_from_gbd_round_id(7))+1, 1)),#estimation_years_from_gbd_round_id(self.gbdr),
            'sex_id':[1, 2],
            'location_id':self.location_id
        }
        self.dim_mort = {
            'age_group_id':[2, 3],
            'year_id': list(range(min(estimation_years_from_gbd_round_id(7)), max(estimation_years_from_gbd_round_id(7))+1, 1)),#estimation_years_from_gbd_round_id(self.gbdr),
            'sex_id':[1, 2],
            'location_id':self.location_id
        }

        self.columns = collections.OrderedDict()
        for prefix in ['births', 'qx', 'pop_survive', 'life_years', 
            'mort_rate']:
            self.columns['{p}_enn'.format(p=prefix)] = ['{p}_{i}_enn'.format(
                p=prefix, i=i) for i in range(0, 1000)]
            self.columns['{p}_lnn'.format(p=prefix)] = ['{p}_{i}_lnn'.format(
                p=prefix,i=i) for i in range(0, 1000)]

        self.lv_bir_frame = pd.DataFrame()
        self.qx_frame = pd.DataFrame()


    def flatten(self, df):
        '''Takes a dataframe with multiIndexed columns and returns the same 
        dataframe with the columns flattened '''
        col_list = list(df.columns)
        i = 0
        for col_tuple in col_list:
            # convert everything to string
            str_tuple = tuple(map(str, col_tuple))
            # join the strings in the tuple to create new column name
            cn_string = "_".join(str_tuple)
            # reassign to the column list to preserve order and column 
            # information
            col_list[i] = cn_string
            i += 1
        # delete all levels except bottom (flatten) to help with mergeing 
        # later on
        btm_lvl = df.columns.nlevels-1
        df.columns = df.columns.droplevel(level=[x for x in range(0,btm_lvl)])
        # rename resulting columns with new combined names
        df.columns = col_list
        df.reset_index(drop=False, inplace=True)
        return df


    def get_live_births_summaries(self):

        lvbrth_cov_id = 1106 # live births by sex
        births = get_covariate_estimates(covariate_id=lvbrth_cov_id,
             gbd_round_id=self.gbdr, 
             location_id=self.dim_births['location_id'], 
             year_id=self.dim_births['year_id'], 
             sex_id=self.dim_births['sex_id'],
             decomp_step="step2")
        births = births[['location_id','year_id','sex_id','mean_value']]
        births.rename(columns={'mean_value':'births'}, inplace=True)
        
        self.lv_bir_frame = births


    def get_mortality_summaries(self):
        """ pull mortality estimates. qx is the probability of dying 
        between ages x and x+1 """
        df = get_life_table(age_group_id=self.dim_mort['age_group_id'], 
            location_id=self.dim_mort['location_id'], 
            year_id=self.dim_mort['year_id'], sex_id=self.dim_mort['sex_id'], 
            with_shock=1, with_hiv=1, life_table_parameter_id=3, 
            gbd_round_id=7, status='best', decomp_step='step2')
        df = df[list(self.dim_mort.keys())+['mean']]
        df.rename(columns={'mean':'qx'}, inplace=True)

        self.qx_frame = df


    def calc_time_lived_by_neonates_with_summaries(self):
        """ Calculate time lived by neonates in the population
        
        ax = average time lived by those who die in the age 
        NID 307273: "use a_enn = (0.6 + 0.4*3.5)/365  this assumes that 60% of 
            early neonates who die do so in the first day of life and that the 
            rest die, on average, in the middle of the period"
        NID 307274: "use a_lnn = 7/365, assuming that on average late neonates 
            die in the first 7 days of the age period." """

        a_enn = ((0.6 + 0.4*3.5) / 365.)
        a_lnn = 7./365.
        age_start_enn = 0
        age_end_enn = 7./365.
        age_start_lnn = 7./365.
        age_end_lnn = 28./365.

        bqx = pd.merge(self.lv_bir_frame, self.qx_frame, on=list(
            self.dim_births.keys()), how='left', indicator=True)
        assert (bqx._merge=='both').all()
        bqx.drop('_merge',axis=1, inplace=True)
        
        # rename age groups and reshape to be wide on age_group
        bqx['age_group_id'] = bqx['age_group_id'].replace(2,'enn')
        bqx['age_group_id'] = bqx['age_group_id'].replace(3,'lnn')

        bqx = bqx.set_index(list(self.dim_mort.keys()))
        
        """ Unstack age_group_id and from this point forward use tuples to 
        select columns from the multiindex, e.g. 
        births_and_qx[('births_0', 'enn')], then flatten for export """
        bqx = bqx.unstack('age_group_id')

        """ calc number of infants until day 7 (this is the starting population 
        for the LNN period) """
        bqx[('pop_survive','enn')] = bqx[('births','enn')] * \
            (1-bqx[('qx', 'enn')])
        
        """ calc number of infants until day 28 (this is the starting 
        population for the PNN period) """
        bqx[('pop_survive','lnn')] = bqx[('pop_survive','enn')] * \
            (1-bqx[('qx','lnn')])
        
        # calc life-years in early neonatal pop
        bqx[('life_years','enn')] = bqx[('births','enn')] * (a_enn * \
            bqx[('qx','enn')]) + bqx[('births','enn')] * ((age_end_enn - \
                age_start_enn) * (1-bqx[('qx','enn')]))
        # calc life-years in late neonatal pop
        bqx[('life_years','lnn')] = bqx[('pop_survive','enn')] * (a_lnn * \
            bqx[('qx','lnn')]) + ((age_end_lnn - age_start_lnn) * \
            (1-bqx[('qx','lnn')])) * bqx[('pop_survive','enn')]
        
        # calc all-cause mortality rate for early neonatal pop
        bqx[('mort_rate','enn')] = (bqx[('births','enn')] - \
            bqx[('pop_survive','enn')]) / bqx[('life_years','enn')]
        
        # calc all-cause mortality rate for late neonatal pop
        bqx[('mort_rate','lnn')] = (bqx[('pop_survive','enn')] - \
            bqx[('pop_survive','lnn')]) / bqx[('life_years','lnn')]
        bqx = self.flatten(bqx)
        
        fname = "neonatal_denominators_with_mort_rate_{0}.csv".format(
            self.year_id)
        bqx.to_csv(os.path.join(self.out_dir, fname), encoding='utf-8', 
            index=False)


    def get_live_births_draws(self):

        df = pd.read_csv(os.path.join(
            self.birth_path,"{loc}_reporting_draws.csv".format(
                loc=self.location_id)))
        
        # filter on dismod year_ids and sex_ids we want
        df = df[(df.year_id.isin(self.dim_births['year_id'])) & \
            (df.sex_id.isin(self.dim_births['sex_id'])) & \
            (df1.age_group_id == 169)]

        # set multiindex, reshape sim to wide, rename births_
        birth_cols = ["births_{i}".format(i=i) for i in range(0, 1000)]
        df.loc[:, 'location_id'] = self.location_id
        df.drop(['age_group_id'], axis=1, inplace=True)
        df = df.set_index(list(self.dim_births.keys())+['draw'])
        df = df.unstack('draw')
        df.columns = birth_cols
        
        self.lv_bir_frame = df.reset_index()

    def get_live_births_fake_draws(self):
        lvbrth_cov_id = 1106 # live births by sex
        births = get_covariate_estimates(covariate_id=lvbrth_cov_id,
             gbd_round_id=self.gbdr, 
             location_id=self.dim_births['location_id'], 
             year_id=self.dim_births['year_id'], 
             sex_id=self.dim_births['sex_id'],
             decomp_step=self.decomp_step)
        #calculate average of difference between mean and upper/lower bound
        births['diff_low'] = births['mean_value'] - births['lower_value']
        births['diff_upp'] = births['upper_value'] - births['mean_value']
        births['diff_avg'] = (births['diff_low'] + births['diff_upp'])/2
        births['stdev_est'] = births['diff_avg'] / 1.96
        births = births[['location_id','year_id','sex_id',
            'mean_value', 'stdev_est']]
        #Create draws using
        num_obs = len(births['mean_value'])
        num_draws = 1000
        birth_cols = ["births_{i}".format(i=i) for i in range(0, num_draws)]
        draw_array = normal(births['mean_value'], births['stdev_est'], 
            (num_draws,num_obs)).T
        draw_df = pd.DataFrame(data=draw_array, columns=birth_cols)
        births = pd.concat([births, draw_df], axis=1)
        births.drop(columns=['mean_value', 'stdev_est'], inplace=True)
        #births.rename(columns={'mean_value':'births'}, inplace=True)
        self.lv_bir_frame = births

    def get_mortality_draws(self):
        """ Pull mortality estimates. Draws are saved in individual files by 
        location_id, and are long on draw. qx is the probability of dying 
        between ages x and x+1 """
        df = pd.read_csv(os.path.join(self.mort_path, "lt_{ihme}.csv".format(
            ihme=str(self.location_id))))
        
        """ filter flat file for dismod years and sex_ids/age_group_ids we're 
        interested in """
        df = df[(df.location_id==self.dim_mort['location_id']) & \
            (df.year_id.isin(self.dim_mort['year_id'])) & \
            (df.age_group_id.isin(self.dim_mort['age_group_id'])) & \
            (df.sex_id.isin(self.dim_mort['sex_id']))]
        df = df[list(self.dim_mort.keys())+['draw','qx']]

        # set multiindex, reshape draw to wide, rename qx_
        qx_cols = ["qx_{i}".format(i=i) for i in range(0, 1000)]
        df = df.set_index(list(self.dim_mort.keys())+['draw'])
        df = df.unstack('draw')
        df.columns = qx_cols

        self.qx_frame = df.reset_index()
    
    def get_mortality_fake_draws(self):
        p_death = get_life_table(
            gbd_round_id=self.gbdr,
            decomp_step=self.decomp_step,
            sex_id=self.dim_mort['sex_id'],
            location_id=self.dim_mort['location_id'],
            year_id=self.dim_mort['year_id'],
            age_group_id=self.dim_mort['age_group_id'],
            with_shock=True,
            with_hiv=True,
            with_ui=True,
            life_table_parameter_id=3 ## qx, probability of death
        )
        #calculate average of difference between mean and upper/lower bound
        p_death['diff_low'] = p_death['mean'] - p_death['lower']
        p_death['diff_upp'] = p_death['upper'] - p_death['mean']
        p_death['diff_avg'] = (p_death['diff_low'] + p_death['diff_upp'])/2
        p_death['stdev_est'] = p_death['diff_avg'] / 1.96
        p_death = p_death[['location_id','year_id','sex_id','age_group_id',
            'mean', 'stdev_est']]
        #Create draws using normal dist, mean and estimated stdev
        num_obs = len(p_death['mean'])
        num_draws = 1000
        qx_cols = ["qx_{i}".format(i=i) for i in range(0, num_draws)]
        draw_array = normal(p_death['mean'], p_death['stdev_est'], 
            (num_draws,num_obs)).T
        draw_df = pd.DataFrame(data=draw_array, columns=qx_cols)
        p_death = pd.concat([p_death, draw_df], axis=1)
        p_death.drop(columns=['mean', 'stdev_est'], inplace=True)
        #births.rename(columns={'mean_value':'births'}, inplace=True)
        self.qx_frame = p_death

    def calc_time_lived_by_neonates_draws(self):
        """ Calculate time lived by neonates in the population
        
        ax = average time lived by those who die in the age 
        NID 307273: "use a_enn = (0.6 + 0.4*3.5)/365  this assumes that 60% of 
            early neonates who die do so in the first day of life and that the 
            rest die, on average, in the middle of the period"
        NID 307274: "use a_lnn = 7/365, assuming that on average late neonates 
            die in the first 7 days of the age period."
        """

        a_enn = ((0.6 + 0.4*3.5) / 365.)
        a_lnn = 7./365.
        age_start_enn = 0
        age_end_enn = 7./365.
        age_start_lnn = 7./365.
        age_end_lnn = 28./365.

        bqx = pd.merge(self.lv_bir_frame, self.qx_frame, 
            on=list(self.dim_births.keys()), how='left', indicator=True)
        assert (bqx._merge=='both').all()
        bqx.drop('_merge',axis=1, inplace=True)
        
        # rename age groups and reshape to be wide on age_group
        bqx['age_group_id'] = bqx['age_group_id'].replace(2,'enn')
        bqx['age_group_id'] = bqx['age_group_id'].replace(3,'lnn')

        bqx = bqx.set_index(list(self.dim_mort.keys()))
        
        """ unstack age_group_id and from this point forward use tuples to 
        select columns from the multiindex, e.g. bqx[('births_0', 'enn')], 
        then flatten for export """
        bqx = bqx.unstack('age_group_id')

        for col in range(0, 1000):
            """ calc number of infants until day 7 (this is the starting 
            population for the LNN period) """
            bqx[('pop_survive_{i}'.format(i=col),'enn')] = bqx[
                ('births_{i}'.format(i=col),'enn')] * \
                (1-bqx[('qx_{i}'.format(i=col), 'enn')])
            
            """ calc number of infants until day 28 (this is the starting 
            population for the PNN period) """
            bqx[('pop_survive_{i}'.format(i=col),'lnn')] = bqx[
                ('pop_survive_{i}'.format(i=col),'enn')] * \
                (1-bqx[('qx_{i}'.format(i=col),'lnn')])
            
            # calc life-years in early neonatal pop
            bqx[('life_years_{i}'.format(i=col),'enn')] = bqx[
                ('births_{i}'.format(i=col),'enn')] * \
                (a_enn * bqx[('qx_{i}'.format(i=col),'enn')]) + \
                bqx[('births_{i}'.format(i=col),'enn')] * \
                ((age_end_enn - age_start_enn) * \
                    (1-bqx[('qx_{i}'.format(i=col),'enn')]))
            
            # calc life-years in late neonatal pop
            bqx[('life_years_{i}'.format(i=col),'lnn')] = bqx[
                ('pop_survive_{i}'.format(i=col),'enn')] * \
                (a_lnn * bqx[('qx_{i}'.format(i=col),'lnn')]) + \
                ((age_end_lnn - age_start_lnn) * \
                    (1-bqx[('qx_{i}'.format(i=col),'lnn')])) * \
                bqx[('pop_survive_{i}'.format(i=col),'enn')]
            
            # calc all-cause mortality rate for early neonatal pop
            bqx[('mort_rate_{i}'.format(i=col),'enn')] = (bqx[
                ('births_{i}'.format(i=col),'enn')] - \
                bqx[('pop_survive_{i}'.format(i=col),'enn')]) / \
                bqx[('life_years_{i}'.format(i=col),'enn')]
            
            # calc all-cause mortality rate for late neonatal pop
            bqx[('mort_rate_{i}'.format(i=col),'lnn')] = (bqx[
                ('pop_survive_{i}'.format(i=col),'enn')] - \
                bqx[('pop_survive_{i}'.format(i=col),'lnn')]) / \
                bqx[('life_years_{i}'.format(i=col),'lnn')]
        
        bqx = self.flatten(bqx)
        gbd_year = gbd_round_from_gbd_round_id(self.gbdr)
        fname = f"neonate_denom_with_mort_rate_gbd{gbd_year}_{self.location_id}.csv"
        
        """order location_id location_name year_id sex_id births_* 
        pop_surv_enn* pop_surv_lnn* qx_enn* qx_lnn* ly_enn* ly_lnn* 
        mort_rate_enn* mort_rate_lnn* """
        bqx = bqx[['location_id','year_id','sex_id']+\
            [y for x in list(self.columns.values()) for y in x]]
        bqx.to_csv(os.path.join(self.out_dir, fname), encoding='utf8', 
            index=False)


    def run(self):
        self.get_live_births_fake_draws()
        self.get_mortality_fake_draws()
        self.calc_time_lived_by_neonates_draws()


#############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("out_dir", 
        help="the directory where all the output files should go")
    parser.add_argument("location_id", 
        help="the location_id that will be used in the calculations", type=int)
    parser.add_argument("gbd_round_id", type=int)
    parser.add_argument("decomp_step", type=str)
    args = vars(parser.parse_args())
    
    # instantiate class
    neonate = Neonatal_denominator(out_dir=args["out_dir"], 
        location_id=args["location_id"], gbd_round_id=args["gbd_round_id"],
        decomp_step=args["decomp_step"])
    neonate.run()
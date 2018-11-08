from __future__ import division
import os
import numpy as np
import pandas as pd
import argparse
from job_utils import draws, parsers
from job_utils import getset
import copy
import re
from datetime import datetime
import xlsxwriter as xl

"""
This part of the code:
1. Grabs DisMod prevalence results for each congenital cause at the envelope 
    and subcause levels
2. Transforms subcause data in some way
    a. cong_neural
        * Calculates full life course prevalence for Anencephaly using 
            birth prevalence from Dismod and denominator from
            calc_denominator.py
        * Squeezes Spina Bifida and Encephalocele so that the sum of the 
            subcauses match the cong_neural envelope
    b. cong_heart and cong_msk
        * Sums draws for each subcause together 
        * Uses the subcause sum and age/sex specific prevalence ratios
            from Marketscan all-patient data to calculate prevalence of
            residual ('other') subcauses
        * Copies subcauses to new ids for carry-through to downstream
            GBD processes
    c. cong_chromo and cong_digestive
        * Turns erroneous sex data form Dismod to zero for sex-specifc causes 
        * Squeezes subcauses to 90% of the relavent envelope
        * Applies 10% of envelope + residual of subcause sum if sum is less 
            than 90% of envelope to relavent 'other' subcause
3. Outputs hdf files for upload via congenital_save.py
"""

class Congenital(draws.SquareImport):

    def __init__(self, me_map, cause_name, **kwargs):
        # super init
        super(Congenital, self).__init__(**kwargs)

        # store data by me_id in this dict 
        # key=me_id
        self.me_map = me_map
        self.cause_name = cause_name
        self.me_dict = {}

        #import every input and create a dictionary of dataframes
        for mapper_key, mapper in me_map.items():
            inputs = mapper.get("srcs",{})
            for src_key, me_id in inputs.items():
                # filter out bundles and the Other sub_cause
                if src_key != "bundle" and mapper_key != "Other":
                ''' The .import_square() method of the SquareImport class 
                is what will call the draws shared function.
                It grabs draws for one me_id at a time, shapes the dataframe 
                for downstream manipulation , and stores each dataframe
                in a dictionary '''
                    self.me_dict[me_id] = self.import_square(
                        gopher_what={"modelable_entity_id": me_id},
                        source="epi")
        if self.cause_name == "cong_chromo":
            self.zero_out_sexes()

    def zero_out_sexes(self):
        # Dismod produces results for both sexes regardless of whether
        # or not the cause is sex-specific
        # need to zero erroneous sexes
        turner_key = self.me_map["Turner syndrome"]["srcs"]["tot"]
        kline_key = self.me_map["Klinefelter syndrome"]["srcs"]["tot"]

        # zero out males for turner syndrome
        turner = self.me_dict[turner_key].copy()
        turner.loc[turner.query('sex_id == 1').index,:] = 0.
        self.me_dict[turner_key] = turner

        # zero out females for klinefelter syndrome
        kline = self.me_dict[kline_key].copy()
        kline.loc[kline.query('sex_id == 2').index,:] = 0.
        self.me_dict[kline_key] = kline
    
    def get_date_stamp(self):
        '''returns a datestamp for output files'''
        date_regex = re.compile('\W')
        date_unformatted = str(datetime.now())[0:10]
        date = date_regex.sub('_', date_unformatted)
        return date

    def calc_anencephaly(self):
        #######################################################################
        # denominator
        #######################################################################
        # read in denominator dataframe for given year
        location = self.idx_dmnsns['location_id'][0]
        denom = pd.read_csv("FILEPATH".format(location))
        denom_idx_cols = ['year_id','sex_id','location_id']
        denom_data_cols = \
            ['births_{}_enn'.format(draw) for draw in range(1000)] + \
            ['births_{}_lnn'.format(draw) for draw in range(1000)] + \
            ['life_years_{}_enn'.format(draw) for draw in range(1000)] + \
            ['life_years_{}_lnn'.format(draw) for draw in range(1000)]
        
        #######################################################################
        # calc numerator
        #######################################################################
        # columns on which calcuations are performed
        draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]

        # Anencephaly life table calculations
        # NID 296668
        # File with calculations here: "FILEPATH"
        ly_per_birth_enn = 0.00508204
        ly_per_birth_lnn = 0.001325096
        qx_enn_anenceph = 0.800653595

        '''get anencephaly draws (from dismod) and reset index to work with 
        denom dataframe'''
        anenceph_dismod_id = self.me_map["Anencephaly"]["srcs"]["dismod"]
        anencephaly = self.me_dict[anenceph_dismod_id].copy()
        anencephaly = anencephaly.reset_index()
        
        # Early neonatal
        enn_anenceph = pd.DataFrame()
        enn_anenceph = anencephaly.loc[anencephaly['age_group_id']==164,:]
        enn_anenceph = pd.merge(enn_anenceph, denom[denom_idx_cols+
            denom_data_cols], on=denom_idx_cols, how='left', indicator=True)
        assert (enn_anenceph._merge=='both').all()
        enn_anenceph.drop('_merge',axis=1, inplace=True)
        
        for col in range(0, 1000):
            enn_anenceph['draw_{i}'.format(i=col)] = (
                enn_anenceph['draw_{i}'.format(i=col)] * \
                enn_anenceph['births_{i}_enn'.format(i=col)] * \
                ly_per_birth_enn) / enn_anenceph['life_years_{i}_enn'.format(i=col)]
        enn_anenceph.loc[:,'age_group_id'] = 2

        # Late neonatal
        lnn_anenceph = pd.DataFrame()
        lnn_anenceph = anencephaly.loc[anencephaly['age_group_id']==164,:]
        lnn_anenceph = pd.merge(lnn_anenceph, 
            denom[denom_idx_cols+denom_data_cols], 
            on=denom_idx_cols, how='left', indicator=True)
        assert (lnn_anenceph._merge=='both').all()
        lnn_anenceph.drop('_merge',axis=1, inplace=True)

        for col in range(0, 1000):
            lnn_anenceph['draw_{i}'.format(i=col)] = (
                lnn_anenceph['draw_{i}'.format(i=col)] * \
                lnn_anenceph['births_{i}_lnn'.format(i=col)] * \
                (1-qx_enn_anenceph) * ly_per_birth_lnn) / \
                lnn_anenceph['life_years_{i}_lnn'.format(i=col)]
        lnn_anenceph.loc[:,'age_group_id'] = 3

        # Combine birth, enn & lnn, reset multiindex, and format for
        # save_results. Fill in all age_group_ids other than 164, 2 and 3 as
        # zero's for prevalence. Age group 164 should be from Dismod (same as 
        # original df)
        new_anencephaly = pd.concat([enn_anenceph,lnn_anenceph],ignore_index=True)
        new_anencephaly = new_anencephaly[self.idx_dmnsns.keys()+draw_cols]
        new_anencephaly.loc[:,'measure_id'] = 5
        dismod_anencephaly_164 = anencephaly.loc[anencephaly['age_group_id']==164,:]
        new_anencephaly = pd.concat([new_anencephaly, dismod_anencephaly_164], 
            ignore_index=True)
        new_anencephaly = new_anencephaly.set_index(self.idx_dmnsns.keys())
        new_anencephaly = pd.concat([self.index_df, new_anencephaly], axis=1)
        new_anencephaly.fillna(value=0, inplace=True)

        #Add new prevalence calculation to dataframe dictionary
        anenceph_custom_id = self.me_map["Anencephaly"]["trgs"]["custom"]
        self.me_dict[anenceph_custom_id] = new_anencephaly

    def get_other_props(self):
        other_bundle_id =  self.me_map["Other"]["srcs"]["bundle"]
        fname = "FILENAME".format(self.cause_name,other_bundle_id)
        directory = "FILEPATH"
        other_props = pd.read_excel(os.path.join(directory,fname))
        other_props.drop(['sex', 'age_start', 'age_end', 'cases', 'other_cases', 
            'cause'], axis=1, inplace=True)
        return other_props

    def squeeze(self):
        # copy over the unsqueezed data to the squeezed me_ids
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":
                # src_me = unsqueezed data
                src_me = self.me_dict[mapper["srcs"]["tot"]].copy(deep=True)
                outputs = mapper.get("trgs", {})
                for imp, me_id in outputs.items():
                    if imp == "sqzd":
                        self.me_dict[me_id] = src_me
        
        # aggregate subgroups
        sub_causes = pd.DataFrame()
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":
                outputs = mapper.get("trgs", {})
                for imp, me_id in outputs.items():
                    if imp == "sqzd":
                        sub_causes = pd.concat([sub_causes, self.me_dict[me_id]])

        # collapse on keys 
        sigma_sub_causes = sub_causes.groupby(level=self.idx_dmnsns.keys()).sum()
        
        if self.cause_name in ['cong_heart','cong_msk']:
            key_other = self.me_map["Other"]["trgs"]["sqzd"]
            props = self.get_other_props()
            props.loc[:,'complement_prop_other'] = (1-props['prop_other'])
            odf = sigma_sub_causes.reset_index()
            odf = pd.merge(odf, props[[c for c in props.columns if c not in ['prop_other']]], 
                how='left', on=['age_group_id', 'sex_id'],indicator=True)
            assert (odf._merge=='both').all()
            odf[self.draw_cols] = odf[self.draw_cols].divide(odf.complement_prop_other, axis=0) -\
                odf[self.draw_cols]
            odf.drop(['complement_prop_other','_merge'], axis=1, inplace=True)
            # set index back to way it was before
            odf = odf.set_index(self.idx_dmnsns.keys())
            odf = odf[self.draw_cols]
            odf = pd.concat([self.index_df, odf], axis=1)
            self.me_dict[key_other] = odf
        
        else:
            # get total env
            env_id = self.me_map["env"]["srcs"]["tot"]
            cause_env = self.me_dict[env_id].copy(deep=True)
            adj_env = pd.DataFrame()

            if self.cause_name != 'cong_neural':
                # reserve 90% so some is left over for 'other'
                adj_env = cause_env * .90
            else:
                # these are aliased now, 
                # whatever happens to adj_env, also happens to cause_env
                adj_env = cause_env

            '''Squeeze the summed subcauses that are bigger than 10% of the 
            envelope so they match the envelope. 
            sqz_bool is a df of boolean values that return True for 
            those data points that are bigger than 10% of the envelope and 
            False for data points that are less than 10% of the envelope .'''
            for sub_group, mapper in self.me_map.items():
                if mapper["type"] == "sub_group":
                    outputs = mapper.get("trgs", {})
                    for imp, me_id in outputs.items():
                        if imp == "sqzd":
                            to_scale = self.me_dict[me_id]
                            if self.cause_name in ['cong_chromo','cong_digestive']:
                                sqz_bool = (sigma_sub_causes > adj_env)
                                scaled = (
                                    to_scale[sqz_bool] * adj_env[sqz_bool] /
                                    sigma_sub_causes[sqz_bool])
                                scaled.fillna(to_scale[~sqz_bool], inplace=True)

                                # calculate 'Other'
                                key_other = self.me_map["Other"]["trgs"]["sqzd"]
                                sqzd = sigma_sub_causes[~sqz_bool].fillna(adj_env[sqz_bool])
                                other = cause_env - sqzd
                                self.me_dict[key_other] = other
                                assert (self.me_dict[key_other] < 0.0).any().any() == False
                                # reassign scaled value to med_id in dictionary of dataframes
                                assert (scaled[self.draw_cols] > 1.0).any().any() == False
                                assert (scaled[self.draw_cols] < 0.0).any().any() == False
                                assert scaled.isnull().values.any() == False
                                self.me_dict[me_id] = scaled
                            
                            elif self.cause_name == 'cong_neural':
                                scaled = (to_scale * adj_env / sigma_sub_causes)
                                scaled.fillna(0, inplace=True)
                                # reassign scaled value to med_id in dictionary of dataframes
                                assert (scaled[self.draw_cols] > 1.0).any().any() == False
                                assert (scaled[self.draw_cols] < 0.0).any().any() == False
                                assert scaled.isnull().values.any() == False
                                self.me_dict[me_id] = scaled



##############################################################################
# function to run core code
##############################################################################

def congenital_data(me_map, location_id, out_dir, cause_name):
    
    # retrieve default dimensions needed to initialize draws.SquareImport class, 
    # then change as needed
    dim = Congenital.default_idx_dmnsns

    # production dimensions
    # Add age group "Birth" (id 164)
    # because there are currently no age_group_set_ids that contain 164
    dim['age_group_id'] = [164] + dim['age_group_id']
    dim["location_id"] = location_id
    dim["measure_id"] = [5]
    dim["sex_id"] = [1, 2]

    # initialize instance of Congenital class
    congenital_cause = Congenital(me_map=me_map, 
        cause_name=cause_name, idx_dmnsns=dim)
    if cause_name == "cong_neural":
        congenital_cause.calc_anencephaly()
    congenital_cause.squeeze()

    # save results to disk
    for mapper_key, mapper in me_map.items():
        outputs = mapper.get("trgs", {})
        for imp, me_id in outputs.items():
            fname = str(location_id[0]) + ".h5"
            out_df = congenital_cause.me_dict[me_id].reset_index()
            out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), 
                key="draws", format="table", data_columns=dim.keys())
##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", 
        type=parsers.json_parser)
    parser.add_argument("out_dir", help="root directory to save stuff")
    parser.add_argument("location_id", help="which location to use", type=int)
    parser.add_argument("cause_name", help="which cause to graph")
    args = vars(parser.parse_args())
    
    # call function
    congenital_data(me_map=args["me_map"], out_dir=args["out_dir"], 
        location_id=[args["location_id"]], cause_name=args["cause_name"])
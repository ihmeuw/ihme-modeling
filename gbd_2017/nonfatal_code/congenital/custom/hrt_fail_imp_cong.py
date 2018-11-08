from __future__ import division
import os
import numpy as np
import pandas as pd
import argparse
from job_utils import draws, parsers
from job_utils import getset
import copy
import subprocess
from db_queries import get_demographics


class HrtFailImpCong(draws.SquareImport):

    def __init__(self, me_map, cause_name, **kwargs):
        # super init
        super(HrtFailImpCong, self).__init__(**kwargs)

        # store data by me in this dict 
        # key=me_id, val=dataframe
        self.me_map = me_map
        self.cause_name = cause_name
        self.csmr_dict = {}
        self.hrtf_dict = {}
        self.sqz_dict = {}
        self.out_dict = {}

        ''' The .import_square() method of the SquareImport class 
        is what will call the get_draws shared function. It grabs draws for 
        one me_id at a time and shapes the dataframe for downstream 
        manipulation. '''
        for mapper_key, mapper in me_map.items():
            inputs = mapper.get("srcs",{})
            for key, me_id in inputs.items():
                if (mapper_key == "Other")&(key == "tot"):
                    pass
                elif key == "tot":
                    '''the import_square function sets up a multiindex based 
                    off the values in the dimension dictionary first given to
                    it. Since we have to use a different measure_id to grab 
                    csmr we can't self.import_square because it will call 
                    df = pd.concat([self.index_df, df], axis=1) with the 
                    measure_id we first used. Instead, we need to instantiate 
                    a new instance of the SquareImport class with the heart 
                    failure dimensions, and use the import_square method from 
                    that object '''
                    dim_csmr = copy.deepcopy(self.idx_dmnsns)
                    # 15=cause-specific mortality rate (csmr)
                    dim_csmr["measure_id"] = [15]
                    csmr = draws.SquareImport(idx_dmnsns=dim_csmr)
                    self.csmr_dict[me_id] = csmr.import_square(
                        gopher_what={"modelable_entity_id": me_id},
                        source="epi")
                elif key == "sqzd":
                    self.sqz_dict[me_id] = self.import_square(
                        gopher_what={"modelable_entity_id": me_id},
                        source="epi")
                else:
                    self.hrtf_dict[me_id] = self.import_square(
                        gopher_what={"modelable_entity_id": me_id},
                        source="epi")


    def create_draws(self, mean, lower, upper):
        '''For the purpose of severity splits'''
        sd = (upper - lower) / (2 * 1.96)
        sample_size = mean * (1 - mean) / sd ** 2
        alpha = mean * sample_size
        beta = (1 - mean) * sample_size
        draws = np.random.beta(alpha, beta, size=1000)
        return draws


    # NIDS 261403, 292275, 292273, and 292237
    # For VSD/ASD only, split into proportion asymptomatic: 
    # 45% asymptomatic, lower=0.432728512, upper=0.465528952; 
    # uncertainty information here: "FILEPATH"
    # Save the 45% in the asymptomatic me_id.
    # Return the other 55% and use it in the remaining heart failure 
    # calculations for the VSD/ASD model
    def calc_vsd_asd_asymptomatic(self):
        vsd_asd_id = self.me_map["Ventricular septal defect and atrial septal defect"]["srcs"]["sqzd"]
        sqzd = self.sqz_dict[vsd_asd_id].copy(deep=True)

        asymptomatic = sqzd.copy(deep=True)
        asymp_props = self.create_draws(mean=0.45, lower=0.4327, upper=0.4655)
        asymptomatic[self.draw_cols] = asymptomatic[self.draw_cols].mul(asymp_props, axis=1)
        asymp_id = self.me_map["Ventricular septal defect and atrial septal defect"]["trgs"]["asymp_vsdasd"]
        self.out_dict[asymp_id] = asymptomatic
        return sqzd - asymptomatic


    def calc_heart_failure(self):
        # aggregate csmr subgroups
        df_list = []
        for me_id in self.csmr_dict.keys():
            df_list.append(self.csmr_dict[me_id])
        csmr_sub_causes = pd.concat(df_list)
        # collapse on keys 
        sigma_csmr_sub_causes = csmr_sub_causes.groupby(
            level=self.idx_dmnsns.keys()).sum()

        # calc proportions for csmr me_ids
        csmr_prop_dict = {}
        for me_id in self.csmr_dict.keys():
            prop = self.csmr_dict[me_id] / sigma_csmr_sub_causes
            prop.fillna(0, inplace=True)
            assert (prop[self.draw_cols] > 1.0).any().any() == False
            assert (prop[self.draw_cols] < 0.0).any().any() == False
            assert prop.isnull().values.any() == False

            # reindex prop df so that measure_id is 5 instead of 15, 
            # that way it can be joined with the other dataframes 
            # (everything else stays the same, only the measure_id changes)
            prop = prop.reset_index()
            prop.loc[:,'measure_id'] = 5
            prop = prop.set_index(self.idx_dmnsns.keys())
            prop = prop[self.draw_cols]
            prop = pd.concat([self.index_df, prop], axis=1)
            prop.fillna(value=0, inplace=True)
            csmr_prop_dict[me_id] = prop

        # incorporate heart failure severities
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":

                for severity in ["asymp", "mild", "moderate", "severe"]:
                    split_trg_id = self.me_map[sub_group]['trgs'][severity]
                    hf_src_id = self.me_map['heart failure']['srcs'][severity]
                    csmr_id = self.me_map[sub_group]['srcs']['tot']

                    # At draw level, multiply heart failure severity by
                    # CSMR proportion and save to the target 
                    # modelable_entity_id
                    self.out_dict[split_trg_id] = (self.hrtf_dict[hf_src_id] * \
                        csmr_prop_dict[csmr_id])

                # aggregate congenital heart failure severity groups
                severity_df = pd.DataFrame()
                for severity in ["mild", "moderate", "severe"]:
                    split_trg_id = self.me_map[sub_group]['trgs'][severity]
                    severity_df = pd.concat([severity_df, 
                        self.out_dict[split_trg_id]])
                # collapse on keys
                sigma_severity_df = severity_df.groupby(
                    level=self.idx_dmnsns.keys()).sum()
                
                # calc non heart failure portion and save to the target
                # modelable_entity_id
                sqzd_id = self.me_map[sub_group]['srcs']['sqzd']
                if str(sqzd_id) == '10937':
                    sqzd_df = self.calc_vsd_asd_asymptomatic()
                else: 
                    sqzd_df = self.sqz_dict[sqzd_id].copy(deep=True)
                
                residual = sqzd_df - sigma_severity_df
                residual[residual<0.] = 0.
                assert (residual < 0.0).any().any() == False
                assert residual.isnull().values.any() == False
                non_hf_trg_id = self.me_map[sub_group]['trgs']['none']
                self.out_dict[non_hf_trg_id] = residual

        '''copy cong_heart sqzd other into "15756 Congenital heart disease 
        due to other congenital cardiovascular anomalies before ID 
        severity split" for carry-through to downstream GBD processes.'''
        other_none_id = self.me_map["Other"]['trgs']['none']
        other_sqzd_id = self.me_map["Other"]['srcs']['sqzd']
        self.out_dict[other_none_id] = self.sqz_dict[other_sqzd_id].copy()

##############################################################################
# function to run core code
##############################################################################

def heart_failure_data(me_map, location_id, out_dir, cause_name):
    # retrieve default dimensions needed to initialize draws.SquareImport 
    # class, then change as needed
    dim = HrtFailImpCong.default_idx_dmnsns
    # production dimensions
    dim["location_id"] = location_id
    dim["measure_id"] = [5]
    dim["sex_id"] = [1, 2]

    # initialize instance of 
    # heart-failure-impairment-due-to-congenital-causes class
    hf_impairment = HrtFailImpCong(me_map=me_map, cause_name=cause_name, 
        idx_dmnsns=dim)
    hf_impairment.calc_heart_failure()

    # save results to disk
    for me_id in hf_impairment.out_dict.keys():
        fname = str(location_id[0]) + ".h5"
        out_df = hf_impairment.out_dict[me_id].reset_index()
        out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), key="draws", 
            format="table", data_columns=dim.keys())

##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", 
        type=parsers.json_parser)
    parser.add_argument("out_dir", help="root directory to save stuff")
    parser.add_argument("location_id", help="which location to use", 
        type=parsers.int_parser)
    parser.add_argument("cause_name", help="which cause to graph")
    args = vars(parser.parse_args())
    
    # call function
    heart_failure_data(me_map=args["me_map"], out_dir=args["out_dir"], 
        location_id=[args["location_id"]], cause_name=args["cause_name"])
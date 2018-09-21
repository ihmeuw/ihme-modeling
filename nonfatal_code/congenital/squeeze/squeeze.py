"""
This part of the code proportionally transforms (squeezes) DisMod subcause
data so they sum to the envelope cause. This code will also calculate anencephaly prevelence
for early and late neonatal age groups, split out congenital heart failure models from
the total heart failure model, and prep data for visualization with ggplot.

"""
from __future__ import division
import os
import numpy as np
import pandas as pd
import argparse
from job_utils import draws, parsers
from job_utils import getset
from db_queries import get_outputs
os.chdir(os.path.dirname(os.path.realpath(__file__)))
import logging
import copy
import subprocess
import shutil
import glob


class Congenital(draws.SquareImport):

    def __init__(self, me_map, cause_name, **kwargs):
        # super init
        super(Congenital, self).__init__(**kwargs)

        # store data by me in this dict key=me_id
        self.me_map = me_map
        self.cause_name = cause_name
        self.me_dict = {}

        #import every input and create a dictionary of dataframes
        logging.info("Begin me_map loop")
        for mapper_key, mapper in me_map.items():
            inputs = mapper.get("srcs",{})
            for src_key, me_id in inputs.items():
                #filter out bundles and the Other sub_cause
                if src_key != "bundle" and mapper_key != "Other":
                # The .import_square() method of the SquareImport class 
                # (created by central_comp)
                # is what will call the draws shared function.
                # It grabs draws for one me_id at a time, shapes the dataframe for downstream manipulation, and stores each dataframe
                # in a dictionary
                    logging.info(me_id)
                    self.me_dict[me_id] = self.import_square(
                        gopher_what={"modelable_entity_ids": [me_id]},
                        source="dismod")
    
    def calc_anencephaly(self):
        # read in denominator dataframe 
        denom = pd.read_csv("{FILEPATH}")
        # rest of the code is currently parallelized by year but denominator data is not
        # filter denomintor data to match year_id in use
        denom = denom[denom['year_id']==self.idx_dmnsns['year_id']]
        
        # dimensions for index
        dim_denom = {
            'year_id': self.idx_dmnsns['year_id'],
            'sex_id':[1, 2],
            'location_id':denom['location_id'].unique().tolist()
        }
        denom = denom.set_index(dim_denom.keys())
        
        ###################################
        # calc numerator
        ###################################
        # columns on which calcuations are performed
        draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]
        birth_cols = ["births_{i}".format(i=i) for i in range(0, 1000)]
        anenceph_prev_enn_cols = ["anenceph_prev_enn_{i}".format(i=i) for i in range(0, 1000)]
        anenceph_prev_lnn_cols = ["anenceph_prev_lnn_{i}".format(i=i) for i in range(0, 1000)]
        ly_enn_cols = ["ly_enn_{i}".format(i=i) for i in range(0, 1000)]
        ly_lnn_cols = ["ly_lnn_{i}".format(i=i) for i in range(0, 1000)]

        # import file with anencephaly life table calculations
        # grab variables
        df = pd.read_excel(io="{FILEPATH}", sheetname="AS")
        ly_per_birth_enn = df.loc[8, 'Unnamed: 10']
        ly_per_birth_lnn = df.loc[10, 'Unnamed: 10']
        qx_enn_anenceph = df.loc[6, 'prop dying']
        
        # get anencephaly draws(pre-sqz) and reset index to work with denom dataframe
        anencephaly_id = self.me_map["Anencephaly"]["srcs"]["tot"]
        anencephaly = self.me_dict[anencephaly_id].copy()
        anencephaly = anencephaly.reset_index().set_index(dim_denom.keys())
        
        # Early neonatal
        enn_anenceph = pd.DataFrame()
        enn_anenceph = anencephaly[anencephaly['age_group_id']==164]
        assert (enn_anenceph.index == denom.index).all(), "Index not the same"
        enn_anenceph = pd.merge(enn_anenceph, denom[birth_cols], left_index=True, right_index=True)
        for col in range(0, 1000):
            enn_anenceph["ly_enn_anenceph_{i}".format(i=col)] = enn_anenceph["draw_{i}".format(i=col)] * enn_anenceph["births_{i}".format(i=col)] * ly_per_birth_enn
        enn_anenceph = pd.merge(enn_anenceph, denom[ly_enn_cols], left_index=True, right_index=True)
        for col in range(0, 1000):
            enn_anenceph["anenceph_prev_enn_{i}".format(i=col)] = enn_anenceph["ly_enn_anenceph_{i}".format(i=col)] / enn_anenceph["ly_enn_{i}".format(i=col)]
        enn_anenceph = enn_anenceph[anenceph_prev_enn_cols]
        enn_anenceph.columns = draw_cols
        enn_anenceph['age_group_id'] = 2

        # Late neonatal
        lnn_anenceph = pd.DataFrame()
        lnn_anenceph = anencephaly[anencephaly['age_group_id']==164]
        assert (lnn_anenceph.index == denom.index).all(), "Index not the same"
        lnn_anenceph = pd.merge(lnn_anenceph, denom[birth_cols], left_index=True, right_index=True)
        for col in range(0, 1000):
            lnn_anenceph["ly_lnn_anenceph_{i}".format(i=col)] = lnn_anenceph["draw_{i}".format(i=col)] * lnn_anenceph["births_{i}".format(i=col)] * (1-qx_enn_anenceph) * ly_per_birth_lnn
        lnn_anenceph = pd.merge(lnn_anenceph, denom[ly_lnn_cols], left_index=True, right_index=True)
        for col in range(0, 1000):
            lnn_anenceph["anenceph_prev_lnn_{i}".format(i=col)] = lnn_anenceph["ly_lnn_anenceph_{i}".format(i=col)] / lnn_anenceph["ly_lnn_{i}".format(i=col)]
        lnn_anenceph = lnn_anenceph[anenceph_prev_lnn_cols]
        lnn_anenceph.columns = draw_cols
        lnn_anenceph['age_group_id'] = 3

        # Combine birth, enn & lnn, reset multiindex, and format for save_results
        # fill in all age groups other than 164, 2 and 3 as zero's for prevalence
        # 164 should be from DisMod (same as original df)
        new_anencephaly = pd.DataFrame()
        new_anencephaly = pd.concat([enn_anenceph,lnn_anenceph])
        new_anencephaly["measure_id"] = 5
        dismod_anencephaly_164 = anencephaly[anencephaly['age_group_id']==164]
        new_anencephaly = pd.concat([new_anencephaly, dismod_anencephaly_164])
        new_anencephaly = new_anencephaly.reset_index().set_index(self.idx_dmnsns.keys())
        new_anencephaly = pd.concat([self.index_df, new_anencephaly], axis=1)
        new_anencephaly.fillna(value=0, inplace=True)

        #Add new prevalence calculation to dataframe dictionary
        self.me_dict[anencephaly_id] = new_anencephaly

    # Calculate proportion 'other' from hospital and marketscan data
    def calc_other(self):
        odf = pd.DataFrame()
        srcdfs = pd.DataFrame()
        list_srcdfs = []
        levels = ['location_id', 'year_start','year_end', 'sex', 'age_start', 'age_end']
        columns_we_need = ['sex','age_start','age_end','cases', 'other_cases']

        # collect data
        for mapper_key, mapper in self.me_map.items():
            # don't include the envelope
            if mapper["type"] == "sub_group" or mapper["type"] =="other":
                # only include the source bundles (not targets)
                outputs = mapper.get("srcs", {})
                for src_key, bundle_num in outputs.items():
                    if src_key == "bundle":
                        # bring in claims data
                        path = "{FILEPATH}".format(self.cause_name, str(bundle_num))
                        fname = "{FILENAME}".format(str(bundle_num))
                        temp_path = os.path.join(path, fname)
                        if os.path.exists(temp_path):
                            
                            df = pd.read_excel(temp_path)
                            list_srcdfs.append(df)

                            if mapper_key == "Other":
                                # trim data by selecting columns
                                odf = df[levels + ['cases']]
                                # rename columns for merge later
                                odf.rename(columns={'cases' : 'other_cases'}, inplace=True)
                        else:
                            # exception text should be printed to error file
                            # designated in qsub call
                            raise Exception("Data not available for bundle {0} in latest round of hospital data or fname has not been updated appropriately".format(str(bundle_num)))

        # join data from other bundles within cause
        srcdfs = pd.concat(list_srcdfs, axis=0, join='inner', levels=levels, ignore_index=True)
        assert int(self.me_map['env']['srcs']['bundle']) not in srcdfs['bundle_id'].unique()
        srcdfs = srcdfs[levels + ['cases']] # trim data by selecting columns

        #sum cases and merge in other data
        sigma_srcdfs = srcdfs.groupby(by=levels).sum().reset_index()
        merge_sigma_srcdfs = sigma_srcdfs.merge(odf, how='inner', on=levels) # 'inner' takes the intersection of each df

        # collapse to 'sex','age_start','age_end' combinations and trim
        sigma = merge_sigma_srcdfs.groupby(by=['sex','age_start','age_end']).sum().reset_index()
        sigma = sigma[columns_we_need]

        # calculate proportion of other for each age/sex category
        sigma['prop_other'] = sigma['other_cases'] / sigma['cases']
        assert (sigma['prop_other'] > 1.0).any().any() == False
        sigma['cause'] = self.cause_name

        # merge in age_group_ids
        age_df =  pd.read_csv("{FILEPATH}")
        # age_group_years_end in database do not match age_end in hospital data so only merge on age_start using the desired age_group_id_set
        # this strategy will match hospital data for 0-1 to age_group_id 2
        age_df.rename(columns={'age_group_years_start': 'age_start', 'age_group_years_end': 'age_end'}, inplace=True)
        age_df.drop('age_end', axis=1, inplace=True)
        age_sigma = sigma.merge(age_df, on=['age_start'])

        # hospital data does not have neonate age groups
        # copy 0-1 proportion to gbd age_group_ids 164, 3 and 4 (2 is already assigned)
        neonate_groups = [164, 3, 4]
        add_to_hospital = pd.DataFrame()
        for age in neonate_groups:
            duplicate = age_sigma[age_sigma['age_start']==0]
            duplicate.loc[:,'age_group_id'] = age
            add_to_hospital = add_to_hospital.append(duplicate)

        age_sigma = age_sigma.append(add_to_hospital,ignore_index=True)

         # add in sex_ids and trim
        age_sigma.loc[:,'sex_id'] = [2 if x == "Female" else 1 for x in age_sigma.loc[:,'sex']]
        # export csv for sanity check
        age_sigma.to_csv("{FILEPATH}".format(self.cause_name), index=False, encoding='utf-8')
        age_sigma.drop(['sex', 'age_start', 'age_end', 'cases', 'other_cases', 'cause'], axis=1, inplace=True)

        return age_sigma
    
    def squeeze(self):
        # Add fullmod_ME_after_squeeze me_ids to dictionary and copy over the unsqueezed data
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":
                #src_me = unsqueezed data
                src_me = self.me_dict[mapper["srcs"]["tot"]].copy()
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
        
        # get total env
        env_id = self.me_map["env"]["srcs"]["tot"]
        cause_env = self.me_dict[env_id].copy()
        adj_env = pd.DataFrame()

        # calc other and save to post-squeeze me_id
        if "Other" in self.me_map:
            # calculate other and add it to the dictionary of modelable entity dataframes
            key_other = self.me_map["Other"]["trgs"]["sqzd"]
            
            # get age/sex split proportions for other
            props = self.calc_other()
            
            #merge in proportions
            #cause_env is a MultiIndex df, props is not
            #can't do a straight merge witout considering indices
            cause_env = cause_env.reset_index()
            odf = cause_env.merge(props, how='inner')
            odf[self.draw_cols] = odf[self.draw_cols].mul(odf.prop_other, axis=0)
            odf.drop('prop_other', axis=1, inplace=True)
            
            # set index back to way it was before
            odf = odf.set_index(self.idx_dmnsns.keys())
            odf = odf[self.draw_cols]
            odf = pd.concat([self.index_df, odf], axis=1)
            self.me_dict[key_other] = odf
            cause_env = self.me_dict[self.me_map["env"]["srcs"]["tot"]].copy()
            adj_env = cause_env - odf
        
        else:
            # these are aliased now, whatever happens to adj_env, also happens to cause_env
            adj_env = cause_env
        adj_env.to_csv("{FILEPATH}".format(self.cause_name), encoding='utf-8')

        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":
                outputs = mapper.get("trgs", {})
                for imp, me_id in outputs.items():
                    if imp == "sqzd":
                        to_scale = self.me_dict[me_id]
                        scaled = (to_scale * adj_env / sigma_sub_causes)
                        scaled.fillna(0, inplace=True)
                        # reassign scaled value to med_id in dictionary of dataframes
                        assert (scaled[self.draw_cols] > 1.0).any().any() == False
                        assert (scaled[self.draw_cols] < 0.0).any().any() == False
                        assert scaled.isnull().values.any() == False
                        self.me_dict[me_id] = scaled
                        
    # NIDS 261403, 292275, 292273, and 292237
    # For VSD/ASD only, split into proportion asymptomatic: 45% asymptomatic,  lower=0.4327, upper=0.4655;
    # Save the 45% in the asymptomatic me_id.
    # Return the other 55% and use it in the remaining heart failure calculations for the VSD/ASD model
    def calc_vsd_asd_asymptomatic(self):
        vsd_asd_id = self.me_map["Ventricular septal defect and atrial septal defect"]["trgs"]["sqzd"]
        sqzd = self.me_dict[vsd_asd_id].copy()

        asymptomatic = sqzd.copy()
        asymp_props = self.create_draws(mean=0.45, lower=0.4327, upper=0.4655)
        asymptomatic[self.draw_cols] = asymptomatic[self.draw_cols].mul(asymp_props, axis=1)
        asymp_id = self.me_map["Ventricular septal defect and atrial septal defect"]["trgs"]["asymptomatic"]
        self.me_dict[asymp_id] = asymptomatic
        return sqzd - asymptomatic

    def create_draws(self, mean, lower, upper):
        '''For the purpose of severity splits'''
        sd = (upper - lower) / (2 * 1.96)
        sample_size = mean * (1 - mean) / sd ** 2
        alpha = mean * sample_size
        beta = (1 - mean) * sample_size
        draws = np.random.beta(alpha, beta, size=1000)
        return draws
    
    def calc_heart_failure(self):
        # grab dimensions used in this class instantiation 
        # modify as necessary to get CSMR (cause-specific mortality rates)
        # since code is parallelized by year, year_id should already be reduced to one year
        dim_hf = copy.deepcopy(self.idx_dmnsns)
        dim_hf["measure_id"] = [15] # cause-specific mortality rate (csmr)
        
        # the import_square function sets up a multiindex based off the values in the dimension dictionary first passed in
        # since we have to use a different measure_id to grab csmr we can't self.import_square because it will call df = pd.concat([self.index_df, df], axis=1) with the measure_id we first used.
        # instead, we need to instantiate a new instance of the SquareImport class with the heart failure dimensions, and use the import_square method from that 
        heart_failure = draws.SquareImport(idx_dmnsns=dim_hf)
        
        # get draws for csmr measure_id and src me_ids
        csmr_dict = {}
        csmr_prop_dict = {}
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":
                inputs = mapper.get("srcs",{})
                for src_key, me_id in inputs.items():
                    if src_key != "bundle":
                        csmr_dict[me_id] = heart_failure.import_square(gopher_what={"modelable_entity_ids": [me_id]}, source="dismod")
                        
        # aggregate csmr subgroups
        # measure_id = 15
        csmr_sub_causes = pd.DataFrame()
        for me_id in csmr_dict.keys():
            csmr_sub_causes = pd.concat([csmr_sub_causes, csmr_dict[me_id]])
        # collapse on keys 
        sigma_csmr_sub_causes = csmr_sub_causes.groupby(level=self.idx_dmnsns.keys()).sum()

        # calc proportions for csmr me_ids
        for me_id in csmr_dict.keys():
            # csmr_dict[me_id] is not changed in this process, no copy is necessary
            prop = csmr_dict[me_id] / sigma_csmr_sub_causes
            prop.fillna(0, inplace=True)
            assert (prop[self.draw_cols] > 1.0).any().any() == False
            assert (prop[self.draw_cols] < 0.0).any().any() == False
            assert prop.isnull().values.any() == False

            # reindex prop df so that measure_id is 5 instead of 15, that way it can be joined with the other dataframes (everything else stays the same, only the measure_id changes)
            prop = prop.reset_index()
            prop.loc[:,'measure_id'] = 5
            prop = prop.set_index(self.idx_dmnsns.keys())
            prop = prop[self.draw_cols]
            prop = pd.concat([self.index_df, prop], axis=1)
            csmr_prop_dict[me_id] = prop

        # incorporate heart failure severities
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":

                for severity in ["mild", "moderate", "severe"]:
                    split_trg_id = self.me_map[sub_group]['trgs'][severity]
                    hf_src_id = self.me_map['heart failure']['srcs'][severity]
                    csmr_id = self.me_map[sub_group]['srcs']['tot']

                    # At draw level, multiply heart failure severity X CSMR proportion -> save to the target_modelable_entity_id
                    self.me_dict[split_trg_id] = (self.me_dict[hf_src_id] * csmr_prop_dict[csmr_id])
                    assert self.me_dict[split_trg_id]['draw_0'][0] == (self.me_dict[hf_src_id]['draw_0'][0] * csmr_df['draw_0'][0])
                    
                # aggregate congenital heart failure severity groups
                severity_df = pd.DataFrame()
                for severity in ["mild", "moderate", "severe"]:
                    split_trg_id = self.me_map[sub_group]['trgs'][severity]
                    severity_df = pd.concat([severity_df, self.me_dict[split_trg_id]])
                # collapse on keys
                sigma_severity_df = severity_df.groupby(level=self.idx_dmnsns.keys()).sum()
                
                # calc non heart failure portion and save to the target_modelable_entity_id
                sqzd_id = self.me_map[sub_group]['trgs']['sqzd']
                non_sqzd = self.me_map[sub_group]['srcs']['tot']
                if str(sqzd_id) == '10937':
                    sqzd_df = self.calc_vsd_asd_asymptomatic()
                else: 
                    sqzd_df = self.me_dict[sqzd_id].copy()
                residual = sqzd_df - sigma_severity_df
                residual[residual<0] = 0
                assert (residual < 0.0).any().any() == False
                assert residual.isnull().values.any() == False
                assert residual['draw_0'][0] == (sqzd_df['draw_0'][0] - sigma_severity_df['draw_0'][0])
                non_hf_trg_id = self.me_map[sub_group]['trgs']['none']
                self.me_dict[non_hf_trg_id] = residual

        # copy cong_heart sqzd other into "15756 Congenital heart disease due to other congenital cardiovascular anomalies before ID severity split". 
        # no other calculations are necessary at this point in time.
        other_none_id = self.me_map["Other"]['trgs']['none']
        other_sqzd_id = self.me_map["Other"]['trgs']['sqzd']
        self.me_dict[other_none_id] = self.me_dict[other_sqzd_id].copy()

##############################################################################
# function to run squeeze
##############################################################################

def congenital_data(me_map, year_id, out_dir, cause_name):
    
    # retrieve dUSERt dimensions needed to initialize draws.SquareImport class, then change as needed
    dim = Congenital.dUSERt_idx_dmnsns
    
    # production dimensions
    # Add age group "Birth" (164)
    # note that there currently are no age_group_set_ids that contain 164
    # age_group_id is pulled in as a pandas Series from the database
    # need to use Series syntax to add an item
    dim['age_group_id'] = dim['age_group_id'].append(pd.Series([164]), ignore_index=True)
    dim["year_id"] = year_id
    dim["measure_id"] = [5]
    dim["sex_id"] = [1, 2]

    # initialize instance of Congenital class
    congenital_cause = Congenital(me_map=me_map, cause_name=cause_name, idx_dmnsns=dim)
    # need to adjust anencephaly prevalence before squeeze
    if cause_name == "cong_neural": 
        congenital_cause.calc_anencephaly()
    congenital_cause.squeeze()
    if cause_name == "cong_heart": 
        congenital_cause.calc_heart_failure()

    # save results to disk
    full_data_set = pd.DataFrame()
    df_list = []
    draw_list = ["draw_{i}".format(i=i) for i in range(0, 1000)]
    # for mapper in me_map.values():
    for mapper_key, mapper in me_map.items():
        outputs = mapper.get("trgs", {})
        for imp, me_id in outputs.items():
            fname = str(year_id[0]) + ".h5"
            out_df = congenital_cause.me_dict[me_id].reset_index()
            out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), key="data",
                format="table", data_columns=dim.keys())
            if imp == "sqzd":
                # calc ave of draws for each row (axis=1) and drop all other draw columns
                # to save space
                out_df["ave"] = out_df[draw_list].mean(axis=1)
                out_df.drop(draw_list, axis=1, inplace=True)
                if mapper_key == "Other":
                    out_df["description"] = mapper_key + " " + cause_name
                else:
                    out_df["description"] = mapper_key
                out_df["state"] = "sqzd"
                #select only the ages we want to graph
                out_df = out_df[out_df.age_group_id.isin([164, 2, 3, 4, 6, 13])]
                df_list.append(out_df)
        outputs = mapper.get("srcs", {})
        for src_key, me_id in outputs.items():
            if src_key == "tot" and mapper_key != "Other":
                out_df = congenital_cause.me_dict[me_id].reset_index()
                # calc ave of draws for each row (axis=1) and drop all other draw columns
                # to save space
                out_df["ave"] = out_df[draw_list].mean(axis=1)
                out_df.drop(draw_list, axis=1, inplace=True)
                out_df["description"] = mapper_key
                out_df["state"] = "raw"
                out_df = out_df[out_df.age_group_id.isin([164, 2, 3, 4, 6, 13])]
                df_list.append(out_df)
    full_data_set = pd.concat(df_list)
    full_data_set.to_csv(os.path.join(out_dir, 'graphs', '{0}_{1}.csv'.format(cause_name, year_id)), encoding='utf-8')
    
##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", type=parsers.json_parser)
    parser.add_argument("out_dir", help="root directory to save stuff")
    parser.add_argument("year_id", help="which year to use", type=parsers.int_parser)
    parser.add_argument("cause_name", help="which cause to graph")
    args = vars(parser.parse_args())
    
    # call function
    congenital_data(me_map=args["me_map"], out_dir=args["out_dir"], year_id=[args["year_id"]], cause_name=args["cause_name"])


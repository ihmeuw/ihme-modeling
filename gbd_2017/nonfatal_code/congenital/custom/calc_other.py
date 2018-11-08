from __future__ import division
import numpy as np
import pandas as pd
import os
import argparse
from job_utils import parsers

def get_exemplar_df():
    """Creates a template index for square dataset. Uses claims data from an 
    acause we know is 'square' in terms of age to create an exemplar 
    multiindex."""
    exemplar = pd.read_excel("FILEPATH")
    exemplar['age_end'] = exemplar['age_end'].map('{:.3f}'.format)
    dim_exemplar = {
        'location_id':exemplar.location_id.unique().tolist(),
        'year_start':exemplar.year_start.unique().tolist(),
        'sex': exemplar.sex.unique().tolist(),
        'age_start': exemplar.age_start.unique().tolist()
    }
    idx = pd.MultiIndex.from_product(
        dim_exemplar.values(),
        names=dim_exemplar.keys())
    index_df = pd.DataFrame(index=idx).reset_index()
    merge_cols = exemplar[['year_start', 'year_end', 'age_start', 'age_end']]
    merge_cols.drop_duplicates(keep='first',inplace=True)
    index_df = pd.merge(index_df, merge_cols, how='left')
    index_df = index_df[['location_id','year_start','year_end','sex',
        'age_start','age_end']]
    assert (index_df.duplicated(keep=False).any().any()) == False
    return index_df

def calc_prop_other(me_map):
    """Uses marketscan claims data to calcuate the proportion of cases that 
    fall under the 'other' category for a given acause. 
    Exports a csv with case and proportion information for GBD age groups 
    ranging from birth to 95+"""
    cause_name = me_map['name']['type']
    print("{} start.".format(cause_name))
    odf = pd.DataFrame()
    srcdfs = pd.DataFrame()
    list_srcdfs = []
    levels = ['location_id', 'year_start','year_end', 'sex', 'age_start', 
        'age_end']
    columns_we_need = ['sex','age_start','age_end','cases', 'other_cases']
    age_df =  pd.read_csv("FILEPATH")
    age_set = set(age_df.age_group_id.tolist()+[164])

    # collect data
    for mapper_key, mapper in me_map.items():
        # don't include the envelope
        if mapper["type"] == "sub_group" or mapper["type"] =="other":
            # only include the source bundles (not targets)
            outputs = mapper.get("srcs", {})
            for src_key, bundle_num in outputs.items():
                if src_key == "bundle":
                    # handle cong_chromo acause exceptions
                    if int(bundle_num)==436: acause='cong_downs'
                    elif int(bundle_num)==437: acause='cong_turner'
                    elif int(bundle_num)==438: acause='cong_klinefelter'
                    else: acause = cause_name
                    # bring in claims data
                    path = "FILEPATH".format(acause, str(bundle_num))
                    # This file excludes taiwan MS data as taiwan data was 
                    # added into GBD2017 separately and later
                    fname = "FILENAME".format(str(bundle_num))
                    temp_path = os.path.join(path, fname)
                    if os.path.exists(temp_path):
                        df = pd.read_excel(temp_path)
                        # trim data by selecting columns
                        assert int(me_map['env']['srcs']['bundle']) not in df['bundle_id'].unique()
                        df = df[levels + ['cases']]
                        df['age_end'] = df['age_end'].map('{:.3f}'.format)
                        
                        # handle sex-specific acause exceptions so the data 
                        # doesn't get kicked out during the merge
                        if acause == 'cong_turner':
                            # duplicate female data, 
                            # set sex to male and cases to 0 in the duplicate
                            duplicate = df.copy()
                            duplicate.loc[:,'sex'] = 'Male'
                            duplicate.loc[:,'cases'] = 0
                            df = df.append(duplicate)
                        if acause == 'cong_klinefelter':
                            # duplicate male data, 
                            # set sex to female and cases to 0 in the duplicate
                            duplicate = df.copy()
                            duplicate.loc[:,'sex'] = 'Female'
                            duplicate.loc[:,'cases'] = 0
                            df = df.append(duplicate)
                        
                        # handle cong_chromo age missingness
                        if cause_name == 'cong_chromo':
                            index_df = get_exemplar_df()
                            df = pd.merge(index_df, df, how='left', on=levels)
                            df.fillna(value=0, inplace=True)
                        list_srcdfs.append(df)

                        if mapper_key == "Other":
                            # trim data by selecting columns
                            odf = df.copy()
                            # rename columns for merge later
                            odf.rename(columns={'cases' : 'other_cases'}, inplace=True)
                            trunc_fname = fname.strip(".xlsx")
                    else:
                        # exception text should be printed to error file
                        # designated in qsub call
                        raise Exception("Data not available for bundle {0} in latest round of claims data or fname has not been updated appropriately".format(str(bundle_num)))

    '''Use inner merge to select for 'location_id', 'year_start','year_end', 
    'sex', 'age_start', and 'age_end' in all bundles. Reshape for sum step'''
    srcdfs = list_srcdfs[0]
    for i in range(1, len(list_srcdfs)):
        srcdfs = srcdfs.merge(list_srcdfs[i], how='inner', on=levels, 
            suffixes=['', '_{}'.format(i)])
    srcdfs = pd.melt(srcdfs, id_vars=levels, 
        value_vars=[c for c in srcdfs.columns if 'cases' in c], 
        value_name='cases')
    srcdfs.drop('variable', axis=1, inplace=True)
    
    # sum cases and merge in 'other' data
    sigma_srcdfs = srcdfs.groupby(by=levels).sum().reset_index()
    merge_sigma_srcdfs = sigma_srcdfs.merge(odf, how='inner', on=levels)

    # collapse to 'sex','age_start','age_end' combinations and trim
    sigma = merge_sigma_srcdfs.groupby(by=['sex','age_start','age_end']).sum().reset_index()
    sigma = sigma[columns_we_need]

    # calculate proportion of other for each age/sex category
    sigma['prop_other'] = sigma['other_cases'] / sigma['cases']
    sigma.loc[:,'prop_other'] = sigma.loc[:,'prop_other'].fillna(0)
    assert (sigma['prop_other'] > 1.0).any().any() == False
    assert (sigma['prop_other'].isnull()).any().any() == False
    sigma.loc[:,'cause'] = cause_name

    ''' Merge in age_group_ids. age_group_years_end in database do not match 
    age_end in hospital data so only merge on age_start using the desired 
    age_group_id_set. This strategy will match hospital data for 0-1 to 
    age_group_id 2 '''
    age_df.rename(columns={'age_group_years_start': 'age_start', 
        'age_group_years_end': 'age_end'}, inplace=True)
    age_df.drop('age_end', axis=1, inplace=True)
    age_sigma = sigma.merge(age_df, on=['age_start'])

    ''' Hospital data does not have neonate age groups.
    Copy 0-1 proportion to IHME age_group_ids 164, 3 and 4 
    (2 is already assigned) '''
    neonate_groups = [164, 3, 4]
    add_to_hospital = pd.DataFrame()
    for age in neonate_groups:
        duplicate = age_sigma.loc[age_sigma.age_start==0]
        duplicate.loc[:,'age_group_id'] = age
        add_to_hospital = add_to_hospital.append(duplicate)

    age_sigma = age_sigma.append(add_to_hospital,ignore_index=True)
    
    # check to make sure all ages are represented
    set2 = set(age_sigma.age_group_id.tolist())
    assert age_set==set2
    
    # add in sex_ids and trim
    age_sigma.loc[:,'sex_id'] = [2 if x == "Female" else 1 for x in age_sigma.loc[:,'sex']]
    # export for use in congenital_core.py
    age_sigma.to_excel("FILEPATH".format(cause_name, trunc_fname), 
        index=False, encoding='utf-8')
    return age_sigma

##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", 
        type=parsers.json_parser)
    args = vars(parser.parse_args())

    # call function
    calc_prop_other(me_map=args["me_map"])
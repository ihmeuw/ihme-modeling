from __future__ import division
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import os
import argparse
from job_utils import parsers
from elmo.run.bundle_version import get_bundle_version

def get_exemplar_df():
    """Creates a template index for square dataset 
    Create the multiindex using claims data from an acause we know is 
    'square' in terms of age (or confirm with the hospital team the 
    demographics that should be in all speadsheets they export
    and build the template from their advice)"""
    exemplar = pd.read_excel(("FILEPATH"))
    exemplar['age_end'] = exemplar['age_end'].map('{:.3f}'.format)
    dim_exemplar = {
        'location_id':exemplar.location_id.unique().tolist(),
        'year_start':exemplar.year_start.unique().tolist(),
        'sex': exemplar.sex.unique().tolist(),
        'age_start': exemplar.age_start.unique().tolist()
    }
    idx = pd.MultiIndex.from_product(
        list(dim_exemplar.values()),
        names=list(dim_exemplar.keys()))
    index_df = pd.DataFrame(index=idx).reset_index()
    merge_cols = exemplar[['year_start', 'year_end', 'age_start', 'age_end']]
    merge_cols.drop_duplicates(keep='first',inplace=True)
    index_df = pd.merge(index_df, merge_cols, how='left')
    index_df = index_df[['location_id','year_start','year_end','sex',
        'age_start','age_end']]
    assert (index_df.duplicated(keep=False).any().any()) == False
    return index_df


def get_cartesian_product_asly(locations):
    ages = get_exemplar_df()[['age_start', 'age_end']].drop_duplicates()
    years = pd.Series([x for x in range(2000, 2017)], name='year_start').to_frame()
    sexes=pd.DataFrame({'sex': ['Male', 'Female']})
    locations=pd.Series(locations, name='location_id').to_frame()
    for df in [ages, years, sexes, locations]:
        df['temp_key'] = 0
    temp_1 = pd.merge(ages, years, how='outer', on='temp_key')
    temp_2 = pd.merge(sexes, locations, how='outer', on='temp_key')
    product = pd.merge(temp_1, temp_2, how='outer', on='temp_key')
    product['year_end'] = product['year_start']
    product.drop(columns=['temp_key'], inplace=True)
    return product

def add_zero_prevalence_for_old_ages(cause_df, age_cutoff):
    """
    Given a cases dataframe, adds ages with zero prevalence that are not 
    included in the data. Used to have a fuller cases dataframe that can
    be merged without losing data.
    Args:
        cause_df: The dataframe on which zero prevalence will be imputed.
                  Note: It's assumed this dataframe does not include 
                  cases for ages above the cutoff.
        age_cutoff: The age above which prevalence will be imputed as 0
    
    Returns:
        The resulting dataframe with zero prevalence ages imputed.
    """
    original_cases = cause_df.cases.sum()
    old_age_df =  pd.read_csv("/ihme/mnch/congenital/age_groups.csv")
    old_age_df = old_age_df[old_age_df.age_group_years_start >= age_cutoff]

    df_to_merge = cause_df[cause_df.age_start >=age_cutoff]
    df_to_merge['temp_key'] = 0
    old_age_df['temp_key'] = 0
    mrgd = df_to_merge.merge(old_age_df, on=['temp_key'], how='outer')

    mrgd = mrgd[mrgd.age_group_years_start != mrgd.age_start]
    mrgd.drop(columns=['temp_key', 'age_group_id', 'age_start', 'age_end'], inplace=True)
    mrgd.rename(columns={'age_group_years_start': 'age_start', 
         'age_group_years_end': 'age_end'}, inplace=True)
    mrgd['cases'] = 0
    result = pd.concat([cause_df, mrgd])
    assert original_cases == result.cases.sum()
    return result

def extend_prop_other_from_age_onwards(age_start_to_extend, df_to_extend):
    """
    Given a dataframe and a specific age, duplicates the information on the TODO COMPLETE
    """
    to_extend = df_to_extend.copy()
    age_to_extend = age_start_to_extend
    ages_to_add =  pd.read_csv("/ihme/mnch/congenital/age_groups.csv")
    ages_to_add = ages_to_add[ages_to_add.age_group_years_start > age_to_extend]
    ages_to_add.drop(columns=['age_group_id'], inplace=True)
    ages_to_add['temp_key'] = 0
    ages_to_add.rename(columns={'age_group_years_start':'age_start', 'age_group_years_end':'age_end'}, inplace=True)

    rows_to_extend = to_extend[to_extend['age_start'] == age_to_extend]
    rows_to_extend.drop(columns=['age_start', 'age_end'], inplace=True)
    rows_to_extend['temp_key'] = 0

    extension = pd.merge(ages_to_add, rows_to_extend, on='temp_key', how='outer')
    extension.drop(columns=['temp_key'], inplace=True)

    not_extended = to_extend[to_extend['age_start'] <= age_to_extend]
    result = pd.concat([not_extended, extension])
    return result

def calc_prop_other(me_map, cause_name):
    """Uses marketscan claims data to calculate the proportion of cases that 
    fall under the 'other' category for a given acause. Exports a csv with 
    case, sex, and proportion information for GBD age groups ranging from 
    birth to 95+"""
    print("{} start.".format(cause_name))
    odf = pd.DataFrame()
    srcdfs = pd.DataFrame()
    list_srcdfs = []
    levels = ['location_id', 'year_start','year_end', 'sex', 'age_start', 
        'age_end']
    columns_we_need = ['sex','age_start','age_end','cases', 'other_cases']
    age_df =  pd.read_csv("/ihme/mnch/congenital/age_groups.csv")
    age_set = set(age_df.age_group_id.tolist()+[164])

    # for assertion test
    mapdf = json_normalize(me_map)
    env = mapdf.filter(regex=(".*envelope.*bundle_id.*"))
    env_bundle = env.values.item()

    # collect data
    for grp, grp_dict in me_map.items():
        # don't include the envelope
        if grp != 'envelope':
            for ns in grp_dict.keys(): # name short
                bundle_num = grp_dict[ns]['srcs']['bundle_id']
                acause = grp_dict[ns]['acause']
                # bring in claims data
                path = ("FILEPATH")
                fname = "{}_claims.xlsx".format(
                str(bundle_num))
                temp_path = os.path.join(path, fname)

                df = pd.read_excel(temp_path)
                if len(df) > 0:
                    # trim data by selecting columns
                    assert int(env_bundle) not in df['bundle_id'].unique()
                    df = df[levels + ['cases']]
                    df['age_end'] = df['age_end'].map('{:.3f}'.format)
                    
                    # handle sex-specific acause exceptions so they data 
                    # doesn't get kicked out during the merge
                    if acause == 'cong_turner':
                        # duplicate female data, set sex to male and cases 
                        # to 0 in the duplicate
                        duplicate = df.copy()
                        duplicate.loc[:,'sex'] = 'Male'
                        duplicate.loc[:,'cases'] = 0
                        df = df.append(duplicate)
                    if acause == 'cong_klinefelter':
                        # duplicate male data, set sex to female and cases 
                        # to 0 in the duplicate
                        duplicate = df.copy()
                        duplicate.loc[:,'sex'] = 'Female'
                        duplicate.loc[:,'cases'] = 0
                        df = df.append(duplicate)
                    if acause == 'cong_downs':
                        # Impute zero prevalence for 65 and up
                        duplicate = df.copy()
                        df = add_zero_prevalence_for_old_ages(duplicate, 65)
                    # handle cong_chromo age missingness
                    if cause_name == 'cong_chromo':
                        index_df = get_cartesian_product_ages_and_years(df.location_id.unique())#get_exemplar_df()
                        df = pd.merge(index_df, df, how='left', on=levels)
                        df.fillna(value=0, inplace=True)
                    
                    list_srcdfs.append(df)

                    if grp == "other":
                        odf = df.copy()
                        # rename columns for merge later
                        odf.rename(columns={'cases' : 'other_cases'}, 
                            inplace=True)
                        trunc_fname = fname.strip(".xlsx")
                else:
                    # exception text should be printed to error file
                    # designated in qsub call
                    raise Exception(("Data not available for bundle {0} in"
                        " latest round of claims data or fname has not"
                        " been updated appropriately").format(str(
                            bundle_num)))

    # manipulate data
    srcdfs = list_srcdfs[0]
    for i in range(1, len(list_srcdfs)):
        srcdfs = srcdfs.merge(list_srcdfs[i], how='inner', on=levels, 
            suffixes=['', '_{}'.format(i)])
    srcdfs = pd.melt(srcdfs, id_vars=levels, 
        value_vars=[c for c in srcdfs.columns if 'cases' in c], 
        value_name='cases')
    srcdfs.drop('variable', axis=1, inplace=True)
    
    # sum cases and merge in other data
    sigma_srcdfs = srcdfs.groupby(by=levels).sum().reset_index()
    
    # 'inner' takes the intersection of each df
    merge_sigma_srcdfs = sigma_srcdfs.merge(odf, how='inner', on=levels)

    # collapse to 'sex','age_start','age_end' combinations and trim
    sigma = merge_sigma_srcdfs.groupby(by=[
        'sex','age_start','age_end']).sum().reset_index()
    sigma = sigma[columns_we_need]

    # calculate proportion of other for each age/sex category
    sigma['prop_other'] = sigma['other_cases'] / sigma['cases']
    sigma.loc[:,'prop_other'] = sigma.loc[:,'prop_other'].fillna(0)
    assert (sigma['prop_other'] > 1.0).any().any() == False
    assert (sigma['prop_other'].isnull()).any().any() == False
    sigma.loc[:,'cause'] = cause_name

    """Merge in age_group_ids
    age_group_years_end in database do not match age_end in hospital data so 
    only merge on age_start using the desired age_group_id_set.
    This strategy will match hospital data for 0-1 to age_group_id 2."""
    age_df.rename(columns={'age_group_years_start': 'age_start', 
        'age_group_years_end': 'age_end'}, inplace=True)
    age_df.drop('age_end', axis=1, inplace=True)
    age_sigma = sigma.merge(age_df, on=['age_start'])

    """Hospital data does not have neonate age groups.
    Copy 0-1 proportion to IHME age_group_ids 164, 3 and 4 
    (2 is already assigned)."""
    neonate_groups = [164, 3, 4]
    add_to_hospital = pd.DataFrame()
    for age in neonate_groups:
        duplicate = age_sigma.loc[age_sigma.age_start==0]
        # trying .loc syntax for slicing
        duplicate.loc[:,'age_group_id'] = age
        add_to_hospital = add_to_hospital.append(duplicate)

    age_sigma = age_sigma.append(add_to_hospital,ignore_index=True)
    
    #check to make sure all ages are represented
    set2 = set(age_sigma.age_group_id.tolist())
    assert age_set==set2
    
    # add in sex_ids and trim
    age_sigma.loc[:,'sex_id'] = [
        2 if x == "Female" else 1 for x in age_sigma.loc[:,'sex']]
    # export excel for sanity check
    age_sigma.to_excel(("/home/j/WORK/12_bundle/cong/01_input_data/"
            "06_proportion_other/{0}_other_props_from_subcause_{1}.xlsx".format(
            cause_name, trunc_fname)), index=False, encoding='utf-8')
    print("{} done!".format(cause_name))
    age_sigma.drop(['sex', 'age_start', 'age_end', 'cases', 'other_cases', 
        'cause'], axis=1, inplace=True)
    return age_sigma

##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", 
        type=parsers.json_parser)
    parser.add_argument("cause_name", 
        help="the name given to the collection of causes in the me_map")
    args = vars(parser.parse_args())

    # call function
    calc_prop_other(me_map=args["me_map"], cause_name=args["cause_name"])

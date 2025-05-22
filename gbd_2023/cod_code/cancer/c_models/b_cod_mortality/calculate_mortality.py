
# -*- coding: utf-8 -*-
'''
Description: Calculates cancer mortality by cause for the CoD database
Contributors: INDIVIDUAL_NAME
'''
from cancer_estimation.py_utils import common_utils as utils
from cancer_estimation.c_models.b_cod_mortality import tests
from cancer_estimation._database import cdb_utils as cdb
import pandas as pd
from cancer_estimation.py_utils import(
    test_utilities as test_utils,
    modeled_locations
)
from cancer_estimation.registry_pipeline.cause_mapping import recode
from cancer_estimation.b_staging import staging_functions as staging
from time import time

from datetime import (datetime, date, timedelta)
today = date.today().strftime("%Y_%m_%d")
temp_dir = f"FILEPATH"


def get_uid_columns():
    return(['location_id', 'year_id', 'sex_id', 'age_group_id', 'acause'])


def get_current_mi_results(cod_mortality_version_id):
    ''' Returns the current/best compiled MI results
    '''
    db_link = cdb.db_api()
    mor_config = db_link.get_table('cod_mortality_version')

    # get mir_model_version_id
    mir_id = int(mor_config.loc[mor_config['cod_mortality_version_id']
                            == cod_mortality_version_id, 'mir_model_version_id'])
    print(f'using mir model version id {mir_id}')
    mi_path = utils.get_path(process='mir_model', key='compiled_mir_results')

    # replace suffix with model_version_id
    mi_path = mi_path.replace('<mir_model_version_id>', str(mir_id))
    print(f"Reading from {mi_path}")
    compiled_mi_df = pd.read_csv(mi_path)
    print(f"Finished reading from {mi_path}")
    return compiled_mi_df


def load_mi_estimates(cod_mortality_version_id):
    ''' returns the compiled MIR model results with the provided suffix,
            formatted for use with incidence selected for the CoD upload
    '''
    print(f"    formatting mir estimates, using v{cod_mortality_version_id}...")
    uid_columns = get_uid_columns()
    required_columns = uid_columns + ['mi_ratio']
    # load and subset data
    df = get_current_mi_results(cod_mortality_version_id)
    df.rename(columns={'sex': 'sex_id', 'year': 'year_id'}, inplace=True)
    df = df.loc[:, required_columns]
    # add extended age groups
    extended_ages = [30, 31, 32, 235]
    if not any(a in df['age_group_id'].unique() for a in extended_ages):
        eightyplus = df.loc[df['age_group_id'] == 21, :].copy()
        for a in extended_ages:
            eightyplus.loc[:, 'age_group_id'] = a
            df = df.append(eightyplus)
    df = df.loc[df['age_group_id'] != 21, :]
    return(df)


def merge_by_cause(inc_data, mir_data):
    ''' Returns a square dataset of mortality estimates, calculated by dividing
            incidence by mir.
    '''
    print("    merging inc with mir...")
    standard_uid_columns = get_uid_columns()
    uid_cols = standard_uid_columns +  ['country_id', 'orig_acause', 'orig_sex']
    # include cause exceptions
    inc_data['orig_sex'] = inc_data['sex_id']
    inc_data['orig_acause'] = inc_data['acause']
    inc_data.loc[inc_data['acause'].eq('neo_meso') | inc_data['acause'].eq('neo_nasopharynx'), 'sex_id'] = 1 
    inc_data = inc_data[inc_data['year_id'].isin(mir_data['year_id'].unique())]
    # merge with mir, first subsetting to avoid adding additional cause-data-years
    subset = inc_data[uid_cols].drop_duplicates()
    output = inc_data.merge(mir_data, how='left', 
            on=['location_id','year_id','sex_id','age_group_id','acause'], indicator=True)
    inc_data_unmatched = output[output['_merge'] == "left_only"]
    if len(inc_data_unmatched) > 0:
        num_unmatched = len(inc_data_unmatched)
        print(f"WARNING -- in merge_by_cause -- {num_unmatched} rows of incidence data were not matched with MIRs")
        fp = f"{temp_dir}inc_data_unmatched_with_mir_data.csv"
        print(f"Saving unmatched data to {fp}")
        inc_data_unmatched.to_csv(fp)
        print("Removing unmatched data in order to continue")
        output = output[output['_merge'] == "both"]
    else:
        num_unmatched = 0
    output.loc[:, 'sex_id'] = output['orig_sex']
    output.loc[:, 'acause'] = output['orig_acause']
    output = output.drop(['orig_sex', 'orig_acause'], axis=1)
    output.drop_duplicates(inplace=True)
    if (len(output) + num_unmatched) != len(inc_data):
        raise Exception("Error when merging with mir estimates -- sorry I can't be more specific about why")
    # Check whether mi_ratio has a value wherever cases exist
    cases_with_null_mirs = output[output['cases'].notnull() & output['mi_ratio'].isnull()]
    if len(cases_with_null_mirs) > 0:
        print("Some rows have values for 'cases' but not values for 'mi_ratio'")
        print(cases_with_null_mirs)
        raise Exception("Some rows have values for 'cases' but not values for 'mi_ratio'")
    duplicates_selector = output.duplicated(standard_uid_columns)
    if duplicates_selector.any():
        print(f"After merge with mirs, duplicates exist by these UID columns: {standard_uid_columns}")
        print(output[duplicates_selector])
        raise Exception(f"After merge with mirs, duplicates exist by these UID columns: {standard_uid_columns}")
    return(output)


def calculate_mortality(inc_data, cod_mortality_version_id):
    ''' Loads MIR then applies it to incidence data to caluculate mortality
            by uid
    '''
    print("Calculating mortality...")
    mir_data = load_mi_estimates(cod_mortality_version_id)
    df = merge_by_cause(inc_data, mir_data)
    df['deaths'] = df['cases']*df['mi_ratio']
    has_nulls =  df['deaths'].isnull() | df['cases'].isnull()
    if has_nulls.any():
        print('WARNING: there are some rows with null cases, null deaths, or both.')
        fp = f"{temp_dir}data_missing_mortality_estimates.csv"
        print(f"Saving those data to {fp}")
        df[has_nulls].to_csv(fp)
        print("For now only, removing those observations and continuing")
        df = df[~has_nulls]
    df = df.drop(['cases', 'mi_ratio', 'rate'], axis=1, errors='ignore')
    return(df)


def run(cod_mortality_version_id):
    ''' Runs merge between incidence and MIR estimates to generate mortaliy estimate
            output
    '''
    output_file = utils.get_path(
        "mortality_estimates", process='cod_mortality')
    input_file = utils.get_path("projected_incidence", process="cod_mortality")
    df = pd.read_csv(input_file)

    drop_loc_ids = [332, 338, 339, 363, 370, 387]
    df = df.loc[~df['location_id'].isin(drop_loc_ids),] 
    df = calculate_mortality(df, cod_mortality_version_id)
    # Validate and save
    df.to_csv(output_file, index=False)
    print("    deaths calculated.")
    return(df)


if __name__ == "__main__":
    import sys
    # sys.argv[1] is the mortality pipeline run id
    run(int(sys.argv[1]))

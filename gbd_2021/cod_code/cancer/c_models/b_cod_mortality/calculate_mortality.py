
'''
Description: Calculates cancer mortality by cause for the CoD database
Contributors: USERNAME
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
    print('using mir model version id...{}'.format(mir_id))
    mi_path = utils.get_path(process='mir_model', key='compiled_mir_results')

    # replace suffix with model_version_id
    mi_path = mi_path.replace('<mir_model_version_id>', str(mir_id))
    compiled_mi_df = pd.read_csv(mi_path)
    return compiled_mi_df


def load_mi_estimates(cod_mortality_version_id):
    ''' returns the compiled MIR model results with the provided suffix,
            formatted for use with incidence selected for the CoD upload
        -- Inputs
            location_ids : list of location_ids to return
            years : list of years to return
            ages : list of ages to return
    '''
    print("    formatting mir estimates...")
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
            incidence by mir. Updates cause and sex to handle exceptions where
            some models work better than others
    '''
    print("    merging inc with mir...")
    uid_cols = get_uid_columns() +  ['country_id', 'orig_acause', 'orig_sex']
    # include cause exceptions
    inc_data['orig_sex'] = inc_data['sex_id']
    inc_data['orig_acause'] = inc_data['acause']
    inc_data.loc[inc_data['acause'].eq('neo_meso') | inc_data['acause'].eq('neo_nasopharynx'), 'sex_id'] = 1
    inc_data = inc_data[inc_data['year_id'].isin(mir_data['year_id'].unique())]
    # merge with mir, first subsetting to avoid adding additional cause-data-years
    #      Note: merge with mir should fill-in missing age categories only
    subset = inc_data[uid_cols].drop_duplicates()

    output = inc_data.merge(mir_data, how='left', 
            on=['location_id','year_id','sex_id','age_group_id','acause'], indicator=True)

    output.loc[:, 'sex_id'] = output['orig_sex']
    output.loc[:, 'acause'] = output['orig_acause']
    output = output.drop(['orig_sex', 'orig_acause'], axis=1)
    output.drop_duplicates(inplace=True)
    assert len(output) == len(
        inc_data), "Error when merging with mir estimates"
    # Ensure that mi_ratio has value wherever cases exist
    assert not output.loc[output['cases'].notnull() & output['mi_ratio'].isnull(), 'mi_ratio'].any(), \
        "NULL MIR estimates"
    assert not output.loc[output.duplicated(get_uid_columns()), :].any().any(), "Duplicates exist after merge with mir"
    return(output)


def calculate_mortality(inc_data, cod_mortality_version_id):
    ''' Loads MIR then applies it to incidence data to caluculate mortality
            by uid
    '''
    print("Calculating mortality...")
    mir_data = load_mi_estimates(cod_mortality_version_id)
    df = merge_by_cause(inc_data, mir_data)
    df['deaths'] = df['cases']*df['mi_ratio']
    assert not df.loc[df['deaths'].isnull() & df['cases'].notnull(), :].any().any(), \
        "Missing mortality estimates at end of calculate_mortality"
    df = df.drop(['cases', 'mi_ratio', 'rate'], axis=1, errors='ignore')
    return(df)


def run(cod_mortality_version_id):
    ''' Runs merge between incidence and MIR estimates to generate mortaliy estimate
            output
        Arguments:
            cod_mortality_version_id : int
                - table id that refers to cod_mortality_version table in Cancer's
                database. This ID helps select the correct mir_model_version and
                staged_incidence_version
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
    run(int(sys.argv[1]))

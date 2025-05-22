# -*- coding: utf-8 -*-
'''
Description: Contains core classes and functions used throughout the nonfatal
    pipeline.
Contents:
    nonfatalDataset : class for accessing metadata for nonfatal datasets
    get_folder : returns
How To Use:
Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME
'''

import os
from db_queries import get_location_metadata
import cancer_estimation.py_utils.common_utils as utils
import cancer_estimation._database.cdb_utils as cdb
import pandas as pd
import numpy as np
from numpy import nan, inf
from copy import copy
import time
import re
import datetime
try:
    from functools import lru_cache
except:
    from functools32 import lru_cache


class DataMissingError(Exception):
    def __init__(self, message="Data is missing"):
        self.message = message
        super().__init__(self.message)


class nonfatalDataset():
    ''' For accessing metadata specific to the datasets being processed
    '''
    uid_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id']
    num_draws = 1000
    draw_cols = ['draw_{}'.format(i) for i in range(0, num_draws)]
    decomp_draws = ['draw_{}'.format(i) for i in range(0,100)]
    max_survival_months = 120
    valid_steps = ['mortality', 'mir', 'incidence', 'access_to_care',
                   'survival', 'prevalence', 'dismod_inputs', 'final_results',
                   'upload', 'split']
    estimated_age_groups = utils.get_gbd_parameter('current_age_groups') 
    annual_years = list(range(utils.get_gbd_parameter('min_year_epi'), utils.get_gbd_parameter('max_year') + 1)) 
    estimation_years = utils.get_gbd_parameter('estimation_years') 
    constants = {}

    def __init__(self, step_name="", cause_indicator=""):
        ''' -- Inputs
                cause_indicator = acause, me_id, or bundle_id associated with 
                    this step-instance
        '''
        if step_name != "":
            self.__validate_step(step_name)
            self.step_name = step_name
            self.__set_uid_cols()
            self.__set_estimated_ages()
            self.output_folder = get_folder(self.step_name, cause_indicator)
            utils.ensure_dir(self.output_folder)

    def get_output_file(self, output_id):
        ''' Returns the output file specifc to the output_id for the dataset instance
            --- Inputs
                output_id : any unique identifier for the pipeline output (ie.
                    location_id, modelable_entity_id, etc.)
        '''
        this_file = "{}/{}.csv".format(self.output_folder, output_id)
        utils.ensure_dir(this_file)
        return(this_file)

    def __validate_step(self, step_name):
        ''' Prevents the class from being instantiated with a typo
        '''
        assert step_name in self.valid_steps, \
            "Error: invalid step_name \'{}\'".format(step_name)

    def __set_uid_cols(self):
        ''' Updates the uid list for the instance if required
        '''
        uid_col_update = {
            'access_to_care': [c for c in self.uid_cols if c != 'age_group_id'],
            'survival': self.uid_cols + ['survival_month'],
            'prevalence': self.uid_cols+['me_tag']
        }
        if self.step_name in uid_col_update.keys():
            self.uid_cols = uid_col_update[self.step_name]
        return(None)

    def __set_estimated_ages(self):
        ''' Updates the list of estimated ages for the instance if required
        '''
        if self.step_name == "access_to_care":
            self.estimated_age_groups = [27]
        return(None)


def dataset_status(since: datetime.datetime):
    steps = ['incidence', 'survival', 'prevalence', 'final_results']
    causes = modeled_cause_list()

    list_of_step_cause_locs_present = []
    list_of_step_cause_locs_absent = []
    for step in steps:
        print(step)
        for acause in causes:
            path = get_folder(step, acause)
            for root, dirs, files in os.walk(path, topdown=True):
                # For now, exclude an analysis of bundle_id contents
                if re.search(f'\d+$', root):
                    print(f'skipping: {root}')
                    continue

                if files: 
                    for i, file in enumerate(files):                    
                        c_time = os.path.getctime(os.path.join(root, file))
                        timestamp = datetime.datetime.fromtimestamp(c_time)
                        if timestamp >= since:
                            list_of_step_cause_locs_present.append([step, acause, file, timestamp])
                        else:
                            list_of_step_cause_locs_absent.append([step, acause, None, timestamp])
                            # if step!='incidence':
                            #     print(step, acause, None, timestamp)
                            #     print()
                else:
                    list_of_step_cause_locs_absent.append([step, acause, None, None])


    # Appending the rows where file is None will allow the groupby to identify the causes for which there is no loc for the associated acause
    [list_of_step_cause_locs_present.append(row) for row in list_of_step_cause_locs_absent]
    status_df = pd.DataFrame(list_of_step_cause_locs_present, columns=['step', 'acause', 'file', 'timestamp'])
    status_df = status_df.groupby(['step', 'acause']).agg({'file':'nunique'}).reset_index().pivot(index='acause', columns='step', values='file')

    status_df.reset_index(inplace=True)


    status_df = status_df[['acause', 'incidence', 'survival', 'prevalence', 'final_results']]
    status_df = status_df.astype(str)
    
    # status_df.incidence = status_df.incidence.apply(create_flag, axis=1)
    status_df.incidence = status_df.incidence.apply(lambda row: create_flag(row))
    status_df.survival = status_df.survival.apply(lambda row: create_flag(row))
    status_df.prevalence = status_df.prevalence.apply(lambda row: create_flag(row))
    status_df.final_results = status_df.final_results.apply(lambda row: create_flag(row))

    # Write to file not std out:      utils.get_path
    print('\n\n', status_df)

    return status_df

def create_flag(row):
    # Replace 843 with a function call to get number of modeled locations
    # Use get_location_metadata -- use a flag to determine which locations are modeled/not modeled.
    if row != '843':
        replaced_row = f'--> {row}'
        return replaced_row
    else:
        return row

def get_folder(step_name, acause_or_meID):
    ''' Returns the data folder for the specified step
    '''
    assert step_name in nonfatalDataset.valid_steps, \
        "Invalid step_name sent to get_folder"
    if step_name in ['mortality', 'mir', 'incidence']:
        main_output_folder = utils.get_path(step_name + "_draws_output", 
                                        process='nonfatal_model')
    else:
        nd_folder = utils.get_path("this_nonfatal_model", process="nonfatal_model")
        main_output_folder = "{}/{}".format(nd_folder, step_name)
    this_output_folder = "{}/{}".format(main_output_folder,acause_or_meID)
    return(this_output_folder)


@lru_cache()
def get_columns(key, draw_number=-1):
    ''' Returns either a single  name or a list of names of columns 
    '''
    draw_cols = nonfatalDataset().draw_cols
    decomp_draws = nonfatalDataset().decomp_draws
    column_dict = {
        'draw_cols': draw_cols,
        'decomp_draw_cols' : decomp_draws,
        'mir': [d.replace('draw', 'mir') for d in draw_cols],
        'mortality': [d.replace('draw', 'deaths') for d in draw_cols],
        'incidence': [d.replace('draw', 'inc') for d in draw_cols],
        'access_to_care': [d.replace('draw', 'atc') for d in draw_cols],
        'relative_survival': [d.replace('draw','surv_rel') for d in draw_cols],
        'absolute_survival': [d.replace('draw','surv_abs') for d in draw_cols],
        'scaled_survival': [d.replace('draw','scaled_surv') for d in draw_cols],
        'incremental_mortality': [d.replace('draw','incr_mort') for d in draw_cols],
        'relative_survival_draws': [d.replace('draw', 'surv_rel') for d in draw_cols],
        'absolute_survival_draws': [d.replace('draw', 'surv_abs') for d in draw_cols],
        'incremental_mortality_draws': [d.replace('draw', 'mort_incr') for d in draw_cols],
        'prevalence': [d.replace('draw', 'prev') for d in draw_cols],
        'decomp_mir': [d.replace('draw','mir') for d in decomp_draws],
        'decomp_scaled_survival' : [d.replace('draw', 'scaled_surv') for d in decomp_draws],
        'decomp_mortality': [d.replace('draw','deaths') for d in decomp_draws],
        'decomp_incidence': [d.replace('draw', 'inc') for d in decomp_draws],
        'decomp_access_to_care': [d.replace('draw', 'atc') for d in decomp_draws],
        'decomp_relative_survival': [d.replace('draw', 'surv_rel') for d in decomp_draws],
        'decomp_absolute_survival': [d.replace('draw', 'surv_abs') for d in decomp_draws], 
        'decomp_incremental_mortality': [d.replace('draw', 'incr_mort') for d in decomp_draws], 
        'decomp_prevalence': [d.replace('draw', 'prev') for d in decomp_draws],
        'final_results' : draw_cols
    }
    if draw_number >= 0:
        return ([d for d in column_dict[key] if d.endswith('_{}'.format(draw_number))][0])
    else:
        return(column_dict[key])


def get_modelable_entity_id(acause, me_tag):
    ''' Returns the modelable_entity_id (or IDs) corresponding to the acause and
        tag
        -- Inputs
            acause : GBD acause
            me_tag : me_tag associated with the data per the cancer_db
    '''
    if not isinstance(me_tag, list):
        me_tag = [me_tag]
    me_table = load_me_table()
    me_id = me_table.loc[me_table['acause'].eq(acause) &
                         me_table['me_tag'].isin(me_tag),
                         'modelable_entity_id']
    if len(me_id) == 0:
        return None
    elif len(me_id) == 1:
        return(int(me_id.item()))
    else:
        return(me_id.astype(int).tolist())


@lru_cache()
def list_location_ids():
    ''' Returns a list of the location_ids required by the Epi uploader
    '''
    release_id = utils.get_gbd_parameter("current_release_id")
    gbd_locs = get_location_metadata(location_set_id=35, release_id = release_id)
    estimation_locs = gbd_locs.loc[(gbd_locs.most_detailed == 1),
                                   'location_id'].unique().tolist()
    return(sorted(estimation_locs))


@lru_cache()
def modeled_cause_list():
    ''' Retuns a list of the acause values modeled by the nonfatal code
    '''
    me_table = load_me_table()
    return(me_table['acause'].unique().tolist())


@lru_cache()
def load_me_table():
    ''' Loads the full table of cancer-associated modelable entity_ids
    '''
    output_cols = ['acause', 'modelable_entity_id', 'modelable_entity_name',
                   'me_tag', 'cancer_model_type', 'bundle_id']
    #db_link = cdb.db_api('cancer_db')
    #me_table = db_link.get_table('cnf_model_entity')
    me_table = pd.read_csv('{}/cnf_model_entity.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache')))
    me_table = me_table.loc[me_table['is_active'].eq(1) & 
                            me_table['cancer_model_type'].str.startswith("custom_"), 
                            :]
    return(me_table[output_cols])


def get_expected_ages_sex(acause): 
    ''' Returns list of age_group_ids estimated for a given acause.
    '''

    def _get_young_ages(age): 
        ''' Takes an age_start value, and returns apprpriate age_group_ids for <5 years
        '''
        if age == 0: 
            young_ages = utils.get_gbd_parameter('young_ages_new')
        elif age == 1: 
            young_ages = [238, 34]
        elif age == 2: 
            young_ages = [34]
        else: 
            young_ages = [] 
        return young_ages

    release_id = utils.get_gbd_parameter('current_release_id')
    # subset cancer_db.registry_input_entity to be unique per cause 
    can_causes = pd.read_csv('{}/registry_input_entity.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache'))) \
        [['acause','cause_id','yld_age_start','yld_age_end', 'release_id','refresh','male','female','is_active']]

    can_causes = can_causes[can_causes['release_id'] == release_id]
    max_refresh = max(can_causes['refresh'].unique().tolist()) # max value without NA's 
    can_causes = can_causes.loc[can_causes['refresh'].eq(max_refresh), 
            ['acause','yld_age_start','yld_age_end','male','female']]
    assert len(can_causes) > 1, 'returned empty dataframe when retrieving nonfatal age restrictions'
    
    # get a list of all age_groups < 5 
    this_cause = can_causes.loc[can_causes['acause'].eq(acause), ]
    young_ages = _get_young_ages(int(this_cause['yld_age_start']))

    # pull all gbd_age_groups and remove all <5 year entries 
    can_ages = pd.read_csv('{}/cancer_age_conversion.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache')))
    no_young_ages = (can_ages['start_age'] >= 5) & \
                    (~can_ages['gbd_age_id'].isin(young_ages)) & \
                    (can_ages['start_age'] >= int(this_cause['yld_age_start']))
    less_than_age_end = (int(this_cause['yld_age_end']) >= (can_ages['start_age']))
    older_ages = can_ages.loc[no_young_ages & less_than_age_end, 'gbd_age_id'].unique().tolist()
    expected_ages = young_ages + older_ages
    
    # create list of expected sex_ids 
    sex_list = [] 
    if int(this_cause['male']): 
        sex_list +=[1] 
    if int(this_cause['female']): 
        sex_list +=[2] 
    return {'expected_ages': expected_ages, 'expected_sex': sex_list}



def validate_expected_ages(df, step, acause):
    ''' Returns dataframe with only data for the expected age_group set age_group_ids 
    '''
    # validate ages that should exist 
    age_sex_dict = get_expected_ages_sex(acause)
    
    # The block below addresses an iusse in GBD2022 in which MIRs were created for age_groupd_ids that we don't actually want to model
    if acause=='neo_ben_brain':
        age_sex_dict['expected_ages'].remove(2)
        age_sex_dict['expected_ages'].remove(3)

    # print(df.head())
    # print('columns: ', df.columns.tolist())
    print('unique age group ids: ', df['age_group_id'].unique().tolist())
    # df.to_csv(f'/ihme/homes/USERNAME/non_fatal/survival_data_{step}_{acause}.csv')
    expected_ages = age_sex_dict['expected_ages'] 
    missing_ages = set(expected_ages) - set(df['age_group_id'].unique().tolist())
    extra_ages = set(df['age_group_id'].unique().tolist()) - set(expected_ages)
    # if not os.path.isfile('/share/homes/USERNAME/nonfatal/ben_brain.csv'):
    #     df.to_csv('/share/homes/USERNAME/non_fatal/ben_brain.csv')
    print('expected_ages')
    assert  missing_ages == set(), \
        '{} ages are expected but are missing for {}'.format(missing_ages, acause) 
    # don't check for ages outside of age_restrictions for absolute survival 
    # this can be removed. however, function would need to alter data and only return data within age restrictions
    if (step == "survival"): 
        pass 
    else: 
        # assert no extra_ages exist, or that the only extra_age is the 0-4 age group

        assert (extra_ages == set()) | (extra_ages == set([1])), \
            'there are age groups outside of age restriction bounds. age groups outisde of restrictions: {}'.format(extra_ages)
    return True 


def validate_expected_sex(df, acause): 
    ''' Compares the expected sex values with what's present in the processed data
    '''
    age_sex_dict = get_expected_ages_sex(acause)
    expected_sex = age_sex_dict['expected_sex']
    missing_sex = set(expected_sex) - set(df['sex_id'].unique().tolist())
    assert missing_sex == set(), \
        '{} sex values are expected but are missing for {}'.format(missing_sex, acause)
    return True


def validate_data_square(df, step, acause, is_estimation_yrs, this_dataset): 
    ''' This function will verify if data is square - all combinations of sex-age-years
        are all present.
    '''
    if is_estimation_yrs: 
        expected_years = this_dataset.estimation_years
    else: 
        expected_years = this_dataset.annual_years
    expected_age_sex = get_expected_ages_sex(acause) 
    expected_ages = expected_age_sex['expected_ages']
    expected_sex = expected_age_sex['expected_sex'] 
    is_square = True
    # check if every combination of sex-age-year
    if (validate_expected_sex(df, acause)): 
        for this_sex in df['sex_id'].unique(): 
            sub = df.loc[df['sex_id'].eq(this_sex), ]
            if (validate_expected_ages(sub, step, acause)):
                for this_age in sub['age_group_id'].unique():
                    final_sub = sub.loc[sub['age_group_id'].eq(this_age), ]
                    print('checking years for sex: {}, age_group: {}'.format(this_sex, this_age))
                    years_present = final_sub['year_id'].unique() 
                    missing_years = set(expected_years) - set(years_present)
                    # assert that for this UID that are no missing years, and all expected sex and ages exist for a given acause
                    if (missing_years != set()) | (not validate_expected_sex(df, acause)) |  (not validate_expected_ages(sub, step, acause)): 
                        is_square=False
    return is_square 


def get_expected_years(is_estimation_yrs, this_dataset): 
    ''' Depending on whether GBD is running estimation_years or annual, this function 
        will take care of returning the correct year_ids for NF data processing.
    '''
    if is_estimation_yrs: 
        expected_years = this_dataset.estimation_years
    else: 
        expected_years = this_dataset.annual_years 
    return expected_years 


def check_zero_values_exist(df, step, acause): 
    ''' For all draw estimates, ensure that all estimates are non-zero
    '''
    if step == 'survival': 
        step = 'absolute_'+step
    draw_cols = get_columns(key=step)
    zero_cols = []
    tol = utils.get_gbd_parameter("pipe_zero_tol",
                            parameter_type = "nonfatal_parameters")
    for d in draw_cols:
        if any(np.isclose(df[d],0, atol = tol) | (df[d] <= 0)):
            zero_cols += [d]
    # exception locs to allow 0s to pass
    cur_loc = df['location_id'].values[0]
    except_locs = utils.get_gbd_parameter("zero_exception_locs", parameter_type = "nonfatal_parameters")
    if cur_loc in except_locs:
        zero_cols = []
        print("This is an exception location where 0s are allowed")
        uid_cols = [col for col in df.columns if col not in draw_cols]
        df_with_zeroes = df[uid_cols + zero_cols]
        save_columns_with_zeroes(df_with_zeroes, acause, step)
        return

    # 2024-08-16. these causes have "0"'s for certain steps. to allow the
    # pipeline to run to completion for release 16, we're saving any output with
    # 0s in it, for diagnosis, and not raising an error
    if (acause in ["neo_eye", "neo_eye_other", "neo_neuro", "neo_ben_brain"]) & (len(zero_cols) > 0):
        print(f"We've encountered 'zeroes' (or nearly zeroes) in data that we're accepting for release 16.")
        uid_cols = [col for col in df.columns if col not in draw_cols]
        df_with_zeroes = df[uid_cols + zero_cols]
        save_columns_with_zeroes(df_with_zeroes, acause, step)
        return
    # if cur_loc in [514, 50, 570, 351]:
    #     df.to_csv(f'/ihme/homes/USERNAME/non_fatal/{acause}_{cur_loc}.csv')
    assert len(zero_cols) == 0, \
        'negative values, zeros, or positive values within {} of zero exist for the following columns: {}'.format(tol, zero_cols)


def check_empty_saved_dataset(filename : str) -> None:
    ''' Takes a filename as a string and checks that its non-empty
    '''
    try:
        pd.read_csv(filename)
    except pd.errors.EmptyDataError:
        print('Empty csv file! for {}'.format(filename))


def save_outputs(step="[name]", df=pd.DataFrame(), acause="[acause]",
                 me_id=0, other_id=0, skip_testing=False, is_estimation_yrs=True,
                 location_id=None):
    ''' Saves validated outputs in the in the output folder for the step
        -- Notes
            use me_id argument to save data separately my modelable_entity_id
    '''
    print(f'save_outputs called!')
    assert me_id == other_id == 0 or me_id != 0, "Error in arguments"
    this_step = nonfatalDataset(step, acause)
    if not skip_testing:  # Only skip testing for epi uploads
        test_outputs(df, this_step, is_estimation_yrs, location_id)
        assert validate_data_square(df, step, acause, is_estimation_yrs, this_step), \
            'Data is not square. an age, sex, or year is missing from the output.'
        check_zero_values_exist(df, step, acause)
    loc_ids = df['location_id'].unique().tolist()
    for l_id in loc_ids:
        output_file = this_step.get_output_file(l_id)
        if me_id != 0:
            output_file = this_step.get_output_file(
                '{}/{}_{}'.format(me_id, other_id, l_id))
        try:
            os.remove(output_file)
        except:
            pass
        df.loc[df['location_id'] == l_id, ].to_csv(output_file, index=False, encoding='utf-8')
        check_empty_saved_dataset(output_file)
    if len(loc_ids) > 1 and me_id == 0:
        df.to_csv(this_step.get_output_file("all"), index=False)
        check_empty_saved_dataset(this_step.get_output_file("all"))

    print("Output data saved...")
    return(None)


def save_duplicates(df, acause) -> None:
    nf_scratch_dir = utils.get_path(process='nonfatal_model', key='this_nonfatal_model')
    duplicate_uids_reports = f'{nf_scratch_dir}/duplicate_uids_reports/{acause}/'
    utils.ensure_dir(duplicate_uids_reports)
    job_id = os.getenv("SLURM_JOB_ID")
    filename = f"{duplicate_uids_reports}{job_id}.csv"
    df.to_csv(filename)
    return None


def save_columns_with_zeroes(df, acause, step) -> None:
    nf_scratch_dir = utils.get_path(process='nonfatal_model', key='this_nonfatal_model')
    duplicate_uids_reports = f'{nf_scratch_dir}/columns_with_zeroes_reports/{acause}/'
    utils.ensure_dir(duplicate_uids_reports)
    job_id = os.getenv("SLURM_JOB_ID")
    array_job_id = os.getenv("SLURM_ARRAY_TASK_ID")
    filename = f"{duplicate_uids_reports}{step}_{job_id}_{array_job_id}.csv"
    print(f"Writing data with zeroes to {filename}")
    df.to_csv(filename)
    return None


def test_outputs(df=pd.DataFrame(), this_dataset=nonfatalDataset(), is_estimation_yrs=True, location_id=None):
    ''' Runs basic validation for a dataframe: no values are null, data exist,
            no uids are duplicated, data exist, data are present for all required
            age_group_ids, year_ids, 
    '''
    assert len(df) > 0, f"ERROR: empty dataframe for location {location_id} sent to save_outputs"
    assert not df.isnull().any().any() and not df.isin([nan, inf]).any().any(), \
        "ERROR: undefined values exist in the output dataframe"

    test_for_expected_years(df, this_dataset.estimation_years, this_dataset.annual_years, is_estimation_yrs)

    # test whether there are multiple rows that have the same values for the UIDs
    df['is_duplicate'] = df.duplicated(subset=this_dataset.uid_cols, keep=False)
    if df['is_duplicate'].any():
        print(f"there are duplicate combinations of {this_dataset.uid_cols}")
        print(f"the duplicates are for the location(s) {df['location_id'].unique()}")
        non_uid_cols = [col for col in df.columns if col not in this_dataset.uid_cols]
        df = df[this_dataset.uid_cols + non_uid_cols]
        print(df.loc[df['is_duplicate'], this_dataset.uid_cols])
        print(df[df['is_duplicate']])
        save_duplicates(df.loc[df['is_duplicate'], this_dataset.uid_cols], this_dataset.cause_indicator)
        assert not df.duplicated(this_dataset.uid_cols).any(), \
            'ERROR: duplicate uids exist in the output dataframe'

    assert df['location_id'].ge(1).all(), "Location_ids <= 0 in dataset"

def test_for_expected_years(df, estimation_years, annual_years, is_estimation_yrs):
    if is_estimation_yrs: 
        missing_years = set(estimation_years) - set(df['year_id'].unique().tolist())
        if not isinstance(missing_years, set):
            missing_years = {missing_years}
        if len(missing_years) > 0:
            raise DataMissingError(f"ERROR: expecting the years {estimation_years}, but the data is missing {missing_years}")
    else:
        missing_years = set(annual_years) - set(df['year_id'].unique().tolist())
        if not isinstance(missing_years, set):
            missing_years = {missing_years}
        if len(missing_years) > 0:
            raise DataMissingError('ERROR: expecting the years {}-{}, but the output is missing {}'.format(min(annual_years), max(annual_years), missing_years))

def get_run_record(cnf_model_version_id):
    ''' Loads the entry for the specified version_id from the cnf_model_version_record (where
            "cnf" = "cancer non-fatal")
    '''
    #db_link = cdb.db_api("cancer_db")
    all_records = pd.read_csv('{}/cnf_model_version.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache')))
    #db_link.get_table('cnf_model_version')
    this_record = all_records.loc[
        all_records['cnf_model_version_id'] == cnf_model_version_id,: ].reset_index()
    assert this_record.notnull().all().all() and len(this_record) == 1, \
        "Canceling run {}. Missing or duplicate entries in the cnf_model_version table for this run.".format(cnf_model_version_id)\
        + "\nDo not refresh nonfatal models without complete run information ."
    return(this_record)



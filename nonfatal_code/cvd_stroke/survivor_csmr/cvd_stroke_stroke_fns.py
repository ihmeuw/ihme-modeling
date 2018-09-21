import time
import re
from datetime import datetime
import submitter
import db_queries
from db_tools.ezfuncs import query
from db_queries import get_location_metadata
import os
import numpy as np


def get_summary_stats(df, index_cols, central_stat):
    '''Args: df--must have only draws as columns
             index_cols --list of strings that df is indexed on
             central_stat--'mean' or 'median' '''
    if central_stat == 'mean':
        df = df.transpose().describe(
            percentiles=[.025, .975]).transpose()[['mean', '2.5%', '97.5%']]
        df.rename(
            columns={'2.5%': 'lower', '97.5%': 'upper'}, inplace=True)
    else:
        df = df.transpose().describe(percentiles=[.025, .5, .975]).transpose(
            )[['2.5%', 'median', '97.5%']]
        df.rename(columns={'2.5%': 'lower', '50%': 'median', '97.5%': 'upper'},
                  inplace=True)
    df.index.rename(index_cols, inplace=True)
    return df


def wait_dismod(me1, me2=None):
    # get status_ids of model_versions passed in
    
    if me2 is not None:
	    q = ('SELECT modelable_entity_id, model_version_id, '
				  'model_version_status_id, date_inserted FROM '
                  'epi.model_version WHERE modelable_entity_id '
                  'IN(%s,%s) AND gbd_round_id = 4 AND '
                  'model_version_status_id=1' %(me1,me2))
    else:
        q = ('SELECT modelable_entity_id, model_version_id, '
				  'model_version_status_id, date_inserted FROM '
                  'epi.model_version WHERE modelable_entity_id '
                  '= %s AND gbd_round_id = 4 AND '
                  'model_version_status_id=1' % me1)
    while True:
        status = query(q, conn_def="epi")
        df1 = status[status.modelable_entity_id == me1]
        df1 = df1.sort(['date_inserted'], ascending=False).reset_index(drop=True)
        mv1 = df1.loc[0, 'model_version_id']
        if me2 is not None:
            df2 = status[status.modelable_entity_id == me2]
            df2 = df2.sort(['date_inserted'], ascending=False).reset_index(drop=True)
            mv2 = df2.loc[0, 'model_version_id']
        break
    if me2 is not None:
        return mv1, mv2
    else:
        return mv1

        # # if dismod models are running, wait for them
        # status['date_inserted'] = status['date_inserted'].astype(datetime)		
        # if me2 is not None:
            # df1 = status[status.modelable_entity_id == me1]
            # df1 = df1.sort(['date_inserted'], ascending=False).reset_index(
                # drop=True)
            # status1 = df1.loc[0, 'model_version_status_id']
            # df2 = status[status.modelable_entity_id == me2]
            # df2 = df2.sort(['date_inserted'], ascending=False).reset_index(
                # drop=True)
            # status2 = df2.loc[0, 'model_version_status_id']
            # status_list = [status1, status2]
        # else:
            # df1 = status[status.modelable_entity_id == me1]
            # df1 = df1.sort(['date_inserted'], ascending=False).reset_index(
                # drop=True)
            # status1 = df1.loc[0, 'model_version_status_id']
            # status_list = [status1]
        # if 0 in status_list:
            # time.sleep(600)
        # # otherwise use most_recent
        # else:
            # # get first model's model version
            # df1 = status[status.modelable_entity_id == me1]
            # df1 = df1.sort(['date_inserted'], ascending=False).reset_index(
                # drop=True)
            # mv1 = df1.loc[0, 'model_version_id']
            # if me2 is not None:
                # # get second model's model version
                # df2 = status[status.modelable_entity_id == me2]
                # df2 = df2.sort(['date_inserted'], ascending=False).reset_index(
                    # drop=True)
                # mv2 = df2.loc[0, 'model_version_id']
            # break



def wait(pattern, seconds):
    '''
    Description: Pause the master script until certain sub-jobs are finished.

    Args:
        1. pattern: the pattern of the jobname that you want to wait for
        2. minutes: number of minutes you want to wait

    Output:
        None, just pauses the script
    '''
    seconds = int(seconds)
    while True:
        qstat = submitter.qstat()
        if qstat['name'].str.contains(pattern).any():
            print time.localtime()
            time.sleep(seconds)
            print time.localtime()
        else:
            break


def get_locations():
    '''
    Description: get list of locations to iterate through for every part of the
    maternal custom process, down to one level of subnationals

    Args: None

    Output: (list) location_ids
    '''
    #### location_set_id was "35" but it should be "2" for GBD computation
    locations_df = get_location_metadata(location_set_id=35, gbd_round_id=4)  
    #### is_estimate = 1 should be filtered	
    locations = (locations_df[(locations_df['most_detailed'] == 1) &
							  (locations_df['is_estimate'] == 1)][
                 'location_id'].tolist())
    #locations = (locations_df[(locations_df['level'] == 1)|(locations_df['level'] == 3)]['location_id'].tolist())

    return locations


def get_time():
    '''
    Description: get timestamp in a format you can put in filepaths

    Args: None

    Output: (string) date_str: string of format '{year}_{month}_{day}_{hour}'
    '''
    date_regex = re.compile('\W')
    date_unformatted = str(datetime.now())[0:13]
    date_str = date_regex.sub('_', date_unformatted)
    return date_str


def filter_cols():
    '''
    Description: Returns a list of the only columns needed for doing math
    on data frames within the maternal custom code. This is used to subset
    dataframes to only keep those columns.

    Args: None

    Output: (list) columns names: age_group_id and draws_0 - draw_999
    '''
    usecols = ['age_group_id']
    for i in range(0, 1000, 1):
        usecols.append("draw_%d" % i)
    return usecols


def check_dir(filepath):
    '''
    Description: Checks if a file path exists. If not, creates the file path.

    Args: (str) a file path

    Output: (str) the file path that already existed or was created if it
    didn't already exist
    '''

    if not os.path.exists(filepath):
        os.makedirs(filepath)
    else:
        pass
    return filepath


def assign_row_nums(df, me):
    """Fills in missing row_nums in input dataframe

        Args:
            df (object): pandas dataframe object of input data
            engine (object): ihme_databases class instance with dUSERt
                engine set
            me_id (int): modelable_entity_id for the dataset in memory

        Returns:
            Returns a copy of the dataframe with the row_nums filled in,
            increment strarting from the max of the database for the given
            modelable_entity.
        """
    q = ('SELECT seq, location_id,year_start,year_end,age_start,age_end, '
         'sex_id, measure_id FROM '
         'epi.bundle_dismod WHERE bundle_id = %s' % (me))

	
    # fill in new row_nums that are currently null
    nullCond = df.seq.isnull()
    lengthMissingRowNums = len(df[nullCond])
    if lengthMissingRowNums == 0:
        return df
    else:
        seqs = query(q, conn_def="epi")
        if len(seqs) == 0:  # if this is the first upload
            num = len(df)
            df['seq'] = range(1, num + 1)
        else:  # if this data should replace old data
            seqs.age_start.replace(0.0191781, 0.01917808, inplace=True)
            seqs.age_start.replace(0.0767123, 0.07671233, inplace=True)
            seqs.age_end.replace(0.0191781, 0.01917808, inplace=True)
            seqs.age_end.replace(0.0767123, 0.07671233, inplace=True)
            df.drop('seq', axis=1, inplace=True)
            df = df.merge(seqs, on=[
                'location_id', 'year_start', 'year_end', 'age_start',
                'age_end', 'sex_id', 'measure_id'], how='left')
            df_null = df[df.seq.isnull()]
            if len(df_null) > 0:
                df.dropna(subset=['seq'], how='all', inplace=True)
                max_seq = seqs.seq.max() + 1
                new_max = ((len(df_null)) + max_seq)
                df_null['seq'] = range(max_seq, new_max)
                df = df.append(df_null)

        # check if row nums assigned properly
        assert not any(df.seq.duplicated()), '''
            Duplicate row numbers assigned'''
        return df
		
def sex_fix(sex_id):
    if sex_id == 1:
        sex = 'Male'
    else:
	    sex = 'Female'
    return sex

def add_uploader_cols(epi_input):
    q = ('SELECT age_group_id, age_group_years_start AS age_start, '
             'age_group_years_end AS age_end '
             'FROM shared.age_group')
    age_df = query(q, conn_def="cod")
    epi_input = epi_input.merge(age_df, on='age_group_id', how='inner')
    epi_input['year_start'] = epi_input['year_id']
    epi_input['year_end'] = epi_input['year_id']
    epi_input['uncertainty_type'] = np.NAN
    epi_input['uncertainty_type_id'] = 3
    epi_input['uncertainty_type_value'] = 95
    epi_input['input_type'] = np.NAN
    epi_input['input_type_id'] = 2
    epi_input['representative_id'] = np.NAN
    epi_input['representative_name'] = "Nationally representative only"
    epi_input['urbanicity_type'] = "Mixed/both"
    epi_input['urbanicity_type_id'] = 0
    epi_input['recall_type'] = "Lifetime"
    epi_input['recall_type_id'] = np.NAN
    epi_input['recall_type_value'] = np.NAN
    epi_input['unit_type'] = "Person"
    epi_input['location_name'] = np.NAN
    epi_input['unit_type_id'] = 1
    epi_input['response_rate'] = np.NAN
    epi_input['unit_value_as_published'] = 1
    epi_input['outlier_type_id'] = 0
    epi_input['underlying_nid'] = np.NAN
    epi_input['source_type'] = "Mixed or estimation"
    epi_input['smaller_site_unit'] = np.NAN
    epi_input['site_memo'] = np.NAN
    epi_input['sex_issue'] = np.NAN
    epi_input['year_issue'] = np.NAN
    epi_input['age_demographer'] = np.NAN
    epi_input['extractor'] = np.NAN
    epi_input['source_type_id'] = 36
    epi_input['sampling_type'] = np.NAN
    epi_input['sampling_type_id'] = np.NAN
    epi_input['design_effect'] = np.NAN
    epi_input['inserted_by'] = os.environ.get("USER")
    epi_input['last_updated_by'] = os.environ.get("USER")
    epi_input["note_modeler"] = np.NAN
    epi_input["note_SR"] = np.NAN
    epi_input['cases'] = np.NAN
    epi_input['sample_size'] = np.NAN
    epi_input['effective_sample_size'] = np.NAN
    epi_input['standard_error'] = np.NAN
    epi_input['age_issue'] = np.NAN
    epi_input['seq'] = np.NAN
    epi_input['seq_parent'] = np.NAN
    return epi_input

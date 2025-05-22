# -*- coding: utf-8 -*-
'''
Description: Uploads data to the CoD database table 'data'.

Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME, INDIVIDUAL_NAME
'''

# ........................................................................................

# import libraries 
import pandas as pd
import numpy as np
from cancer_estimation.py_utils import common_utils as utils
from cancer_estimation._database import cdb_utils as cdb
from db_queries import get_cause_metadata
import cancer_estimation.py_utils.response_validator as rv
from datetime import datetime
import getpass
import sys
import db_tools_core
from db_tools import loaders
import time
import argparse
import logging
import os

# ........................................................................................

# define globals

# the ID used to link data_versions to a specific GBD round / release. needs updating
# each round
PROJECT_ID = 15

RELEASE_ID = utils.get_gbd_parameter('current_release_id')
PREVIOUS_RELEASE_ID = utils.get_gbd_parameter('previous_release_id')

# these are needed by the CoD db, when adding to the data_version_id table
CANCER_REGISTRY_SOURCE_ID = 68 # from the cod.source table
GENERIC_CANCER_NID = utils.get_gbd_parameter('generic_cancer_nid')
DATA_TYPE_ID = 2 # as used in the code
TEMP_UPLOAD_STATUS = 2 # per communication with CoD
BEST_STATUS = 1 # per communication with CoD
NOT_BEST_STATUS = 0 # per communication with CoD
TOOL_TYPE_ID = 9 # as used in the code
GENERIC_SITE_ID = 2 # as used in the code

PROD_DB = "prodcod"
TEST_DB = "testcod"
COD_DBS = [PROD_DB, TEST_DB]
SCHEMA = "cod"

# row is created for each run of any claude phase
CLAUDE_LAUNCH_SET = "claude_launch_set"
# metadata associated with data points in cod.data via data_version_id
DATA_VERSION_TABLE = "data_version"
# unique data points by age/sex/location/year/cause/site, identified by data_id
DATA_TABLE = "data"

COD_UPLOAD_DIR = ADDRESS

DATA_ID_COLNAME = 'data_id'

NUM_DATA_POINTS = None

# ........................................................................................

# set up logging
level = logging.INFO
fmt = ("%(levelname)s %(asctime)s %(name)s %(funcName)s: "
        "%(message)s")
datefmt = "%y-%m-%d %H:%M:%S"
root_handler = logging.StreamHandler(sys.stderr)
root_handler.setFormatter(logging.Formatter(fmt, datefmt))
logging.root.addHandler(root_handler)
logging.root.setLevel(level)


# ........................................................................................

def get_uid_cols():
    ''' Returns a list of unique identifier columns
    '''
    return(['location_id', 'year_id', 'sex_id', 'age_group_id'])


def get_update_settings():
    ''' Create values for the columns that keep track of who updates what, and the date.
    '''
    update_settings = { 'date_inserted': datetime.now().replace(microsecond=0),
                        'inserted_by' : getpass.getuser(),
                        'last_updated': datetime.now().replace(microsecond=0),
                        "last_updated_by": getpass.getuser(),
                        "last_updated_action": "INSERT"}
    return(update_settings)


def get_cod_description(): 
    ''' Pulls notes from cod_mortality_version table from the cancer database
    '''
    db_link = cdb.db_api() 
    cod_df = db_link.get_table('cod_mortality_version')
    max_id = cod_df['cod_mortality_version_id'].max()
    desc = cod_df.loc[cod_df['cod_mortality_version_id'].eq(max_id), 'notes'].reset_index()
    return (desc['notes'][0])


def add_cols_with_zeroes(df : pd.DataFrame, cols : list) -> pd.DataFrame:
    ''' Add columns and populate them with zeros; the columns get populated with real data later on.
    '''
    nrow = len(df)
    zeroes_df = pd.DataFrame(0, index=range(nrow), columns=cols)
    df = pd.concat([df, zeroes_df], axis=1)
    return df


def ensure_acause_is_present(df : pd.DataFrame) -> pd.DataFrame:
    acause = 'acause'
    if acause not in df.columns:
        cols_to_keep = list(df.columns) + [acause]
        try:
            cause_metadata = get_cause_metadata(3, release_id=RELEASE_ID)
        except ValueError:
            cause_metadata = get_cause_metadata(3, release_id=PREVIOUS_RELEASE_ID)
        df = df.merge(cause_metadata, how='left', on='cause_id')
        df = df[cols_to_keep]
    return df


def remove_data_id_column_name(col_names : list) -> list:
    col_names = [col for col in col_names if col != DATA_ID_COLNAME]
    return col_names


def check_for_missing_columns(df, cod_cols):
    missing_cols = set(cod_cols).difference(df.columns)
    if missing_cols:
        raise KeyError(f'These columns are missing from the dataset: {missing_cols}')


def check_for_missing_data_columns(df : pd.DataFrame, cod_cols : list) -> None:
    cod_cols = remove_data_id_column_name(cod_cols)
    check_for_missing_columns(df, cod_cols)


def get_max_round_id(database):
    query_string = f'''select max(gbd_round_id) as gbd_round_id
                    from cod.{DATA_VERSION_TABLE}'''
    with db_tools_core.session_scope(database) as sesh:
        max_round_id = db_tools_core.query_2_df(query_string, sesh)
    max_round_id = max_round_id['gbd_round_id'][0]
    return max_round_id


def get_data_version_id_cols(database, launch_set_id): 
    gbd_round_id = utils.get_gbd_parameter('current_gbd_round')
    # now make sure we have the correct value
    max_round_id_in_cod = get_max_round_id(database)
    if gbd_round_id < max_round_id_in_cod:
        raise KeyError(f'''The 'current_gbd_round' in your gbd_parameters.yaml file, {gbd_round_id},
is lower than the max round id in Cod's {DATA_VERSION_TABLE} table, {max_round_id_in_cod}.
Please check that you have the updated version of gbd_parameters.yaml.''')
    desc = get_cod_description()
    new_dv_entry = {'project_id': PROJECT_ID,
                    'gbd_round_id' : gbd_round_id,
                    'nid': GENERIC_CANCER_NID,
                    'data_type_id': DATA_TYPE_ID,
                    'status_start': datetime.now(),
                    'source_id': CANCER_REGISTRY_SOURCE_ID,
                    'launch_set_id': launch_set_id,
                    'description': desc,
                    'status' : TEMP_UPLOAD_STATUS,
                    'tool_type_id' : TOOL_TYPE_ID
                    }
    return new_dv_entry


def upload_data_version(database, launch_set_id):
    ''' Add a new row to the CoD 'data_version' table, identifying this version (or
        set of data, or bolus of data), that we're uploading.
    '''
    data_version_cols = get_data_version_id_cols(database, launch_set_id)
    colnames, values = get_colnames_and_values(data_version_cols)
    our_query = f'''
        insert into {DATA_VERSION_TABLE} ({colnames})
        values ({values});
        '''
    with db_tools_core.session_scope(database) as sesh:
        sesh.execute(our_query)
    return


def query_data_version(database, launch_set_id):
    ''' look in CoD's 'data_version' table, and get the max value of 'max_data_version_id'
    '''
    max_data_version_id = "max_data_version_id"
    query_string = f'''
        SELECT MAX(data_version_id) as max_data_version_id
        FROM cod.{DATA_VERSION_TABLE}
        WHERE launch_set_id={launch_set_id}
    '''
    with db_tools_core.session_scope(database) as sesh:
        max_id = db_tools_core.query_2_df(query_string, sesh)
    max_id = max_id[max_data_version_id][0]
    return max_id


def make_db_datestamp(): 
    ''' Create timestamp for CoD upload 
    ''' 
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clear_prev_data_version_status(database):
    """ Update CoD's 'data_version' table to:
            * update any row that is marked 'best' (i.e., has status 1) to status 'not best' (i.e., status 0).
            * update any row that is marked 'temporary' (i.e., has status 2) to status 'not best' (i.e., status 0).
    """
    date = make_db_datestamp()  
    update_query = f"""
        UPDATE cod.data_version
        SET status_end = "{date}", status={NOT_BEST_STATUS}
        WHERE nid = {GENERIC_CANCER_NID}
        AND project_id = {PROJECT_ID}
        AND source_id = {CANCER_REGISTRY_SOURCE_ID}
        AND data_type_id = {DATA_TYPE_ID}
        AND (status = {BEST_STATUS}) or (status = {TEMP_UPLOAD_STATUS})
    """
    with db_tools_core.session_scope(database) as sesh:
        sesh.execute(update_query)


def create_elapsed_time_string(start_time : float, end_time : float) -> str:
    # calculate the elapsed time in seconds
    elapsed_time_in_seconds = max(end_time - start_time, 6)
    # Convert seconds to minutes and remaining seconds
    minutes, remaining_seconds = divmod(elapsed_time_in_seconds, 60)
    # Convert the total minutes and the fraction of the next minute to a float
    total_minutes = minutes + remaining_seconds / 60.0
    # Format the output with one decimal place, including leading zero if necessary
    formatted_time = "{:0>4.1f}".format(total_minutes)
    return(formatted_time)


def mark_upload_as_best(database, launch_set_id):
    ''' Mark this upload as 'best', marking previous entries as 'not best'. We
        always mark each run as 'best'.
    '''
    clear_prev_data_version_status(database)
    update_query = f"""
        UPDATE cod.{DATA_VERSION_TABLE}
        SET status = {BEST_STATUS}
        WHERE launch_set_id = {launch_set_id}
        AND project_id = {PROJECT_ID}
        AND source_id = {CANCER_REGISTRY_SOURCE_ID}
        AND data_type_id = {DATA_TYPE_ID}
        AND nid = {GENERIC_CANCER_NID}
    """
    with db_tools_core.session_scope(database) as sesh:
        sesh.execute(update_query)

    return()


def check_upload_marked_best(database : str, launch_set_id : int) -> None:
    ''' Make sure the current upload is marked 'best'.
    '''
    query = f'''
        select status
        from cod.{DATA_VERSION_TABLE}
        where launch_set_id = {launch_set_id}
    '''
    with db_tools_core.session_scope(database) as sesh:
        current_upload = db_tools_core.query_2_df(query, sesh)
    upload_status = current_upload['status'][0]
    if upload_status == BEST_STATUS:
        logging.info("Current upload verified marked as 'best'")
    else:
        logging.error("Current upload not marked as 'best'. Check the log and make sure the upload finished and that the 'mark_upload_as_best' process finished.")


def remove_update_settings_cols(cod_cols):
    ''' From a list of columns, remove those that are 'update settings', i.e., username and dates.
    '''
    update_cols = get_update_settings()
    cod_cols = [col for col in cod_cols if col not in update_cols]
    return cod_cols


def check_columns(df, db):
    ''' Check that all required columns are present in the data before upload
    '''
    cod_cols = get_table_colnames(DATA_TABLE, db)
    # remove id and update cols
    cod_cols = remove_update_settings_cols(cod_cols)
    # check that all required columns are present
    check_for_missing_data_columns(df, cod_cols)
    return(None)


def prep_data(uid_cols, data_version_id, db, path_to_data):
    ''' to the data, add necessary columns (with zeroes for now), ensure that the 'acause'
        column is present, and add uid columns
    '''
    global NUM_DATA_POINTS

    df = pd.read_csv(path_to_data)
    df.loc[df['cf_raw'].isnull(), 'cf_raw'] = 0 
    # temporarily fill columns to 0 until post-age-sex and post-rdp mortality values are
    # available
    cf_cols = ['cf_rd', 'cf_corr', 'cf_cov']
    df = add_cols_with_zeroes(df, cf_cols)
    df = ensure_acause_is_present(df)
    df = df.loc[df['acause'].ne('cc_code'),] # drop cc_code. 
    prepped_df = add_required_columns(df, uid_cols, data_version_id, db)
    NUM_DATA_POINTS = len(df)
    return(prepped_df)


def add_required_columns(df, uid_cols, data_version_id, db):
    ''' adding in required columns for upload
    '''
    final_df = df.copy()
    final_df['data_version_id'] = data_version_id

    # rename column names
    final_df.rename(columns = {"national":"representative_id",
                                'sex':'sex_id',
                                'year':'year_id',
                                },inplace = True)

    # add temp cf columns
    cf_var_cols = ['cf_final_low_rd', 
                'cf_final_high_rd','cf_final_low_ss', 'cf_final_high_ss', 
                'cf_final_low_total', 'cf_final_high_total',
                'variance_rd_logit_cf', 'variance_rd_log_dr']
    final_df = add_cols_with_zeroes(final_df, cf_var_cols)

    # other columns that are constant 
    final_df['underlying_nid'] = np.nan
    final_df['source_id'] = CANCER_REGISTRY_SOURCE_ID # cancer default
    final_df['data_type_id'] = DATA_TYPE_ID # cancer registry

    # assign generic site id to all data for upload
    final_df['site_id'] = GENERIC_SITE_ID
    return(final_df)


def construct_cod_timestamp(): 
    '''
    '''
    now = datetime.now() 
    timestamp_str = '{yr}_{mon}_{day}_{hr}{min}{sec}'.format(yr=now.year,
                                                             mon=now.month,
                                                             day=now.day,
                                                             hr=now.hour,
                                                             min=now.minute,
                                                             sec=now.second)
    return timestamp_str                                                         


def retrieve_commit_hash(): 
    ''' Get the SHA-1 hash of the last commit in the current branch of the repo;
        this is used to document the state of the code when the upload was run
    '''
    import git
    repo = git.Repo(search_parent_directories=True)
    sha = repo.head.object.hexsha
    return sha


def get_colnames_and_values(launch_set_upload : dict) -> pd.DataFrame:
    columns = ', '.join(launch_set_upload.keys())
    values = ', '.join([f"'{v}'" for v in launch_set_upload.values()])
    return columns,values


def upload_launch_set_info(db):
    """ Upload a row of data to CoD's 'claude_launch_set' table, with identifying
        information about this run of the code."""
    launch_set_start = str(datetime.now())
    cod_stamp = construct_cod_timestamp() 
    c_hash = retrieve_commit_hash()
    launch_set_upload = {
        'cod_timestamp': cod_stamp,
        'username': getpass.getuser(),
        'launch_set_description': get_cod_description(),
        'commit_hash': c_hash,
        'launch_set_start': launch_set_start,
        'launch_set_end': ''
    }

    columns, values = get_colnames_and_values(launch_set_upload)
    our_query = f"INSERT INTO {CLAUDE_LAUNCH_SET} ({columns}) VALUES ({values})"

    with db_tools_core.session_scope(db) as sesh:
        res = sesh.execute(our_query)

    return()


def get_launch_set_id(db):
    ''' Get the id for the claude launch session we're running.
    '''
    username = getpass.getuser()
    max_launch_set_id = "max_launch_set_id"
    query_string = f'''
        SELECT MAX(launch_set_id) as {max_launch_set_id}
        FROM cod.{CLAUDE_LAUNCH_SET}
        WHERE username='{username}'
    '''
    with db_tools_core.session_scope(db) as sesh:
        max_id = db_tools_core.query_2_df(query_string, sesh)
    max_id = max_id[max_launch_set_id][0]
    if max_id is not None:
        return max_id
    raise Exception(f"Unable to get launch set id for user '{username}'. No entries for that username in cod.{CLAUDE_LAUNCH_SET}.")


def set_end_time_in_launch_set(database, launch_set_id):
    ''' update CoD's claude_launch_set table with the time the upload ended
    '''
    launch_set_end = make_db_datestamp()
    update_query = f"""
        UPDATE cod.{CLAUDE_LAUNCH_SET}
        SET launch_set_end = '{launch_set_end}'
        WHERE launch_set_id = {launch_set_id}"""
    with db_tools_core.session_scope(database) as sesh:
        sesh.execute(update_query)

    return()


def get_table_colnames(table, db):
    ''' for the table in db, get its columns
    '''
    query_string = f"SELECT * FROM {table} LIMIT 1"
    with db_tools_core.session_scope(db) as sesh:
        first_row_of_table = db_tools_core.query_2_df(query_string, sesh)
    table_columns = first_row_of_table.columns.values
    return table_columns


def execute_upload(df, db):
    global NUM_DATA_POINTS

    # order columns same as table
    cod_cols = get_table_colnames(DATA_TABLE, db)
    cod_cols = remove_update_settings_cols(cod_cols)
    cod_cols = remove_data_id_column_name(cod_cols)
    upload_df = df[cod_cols]
    # make sure the columns to upload are the same as what exists
    check_for_missing_data_columns(df, cod_cols)
    # get a session with the CoD database, create a loader object, indicate where
    # our upload directory is, have the loader stage the data frame, and then
    # have it upload to CoD
    with db_tools_core.get_session(db) as sesh:
        uploader = loaders.ToInfile(COD_UPLOAD_DIR)
        pd.set_option('mode.chained_assignment', None)
        uploader.stage_2_stagedir(upload_df)
        start_time = time.time()
        uploader.upload_stagedir(DATA_TABLE, SCHEMA, sesh, with_replace=False, commit=True)
        end_time = time.time()
        elapsed_time = create_elapsed_time_string(start_time, end_time)
        logging.info(f'Uploaded {NUM_DATA_POINTS} data points in {elapsed_time} minutes.')


def get_path_to_data_for_uploading():
    """ Get the default path from which to read the data that will be uploaded. """
    path_to_data = utils.get_path(key='cod_upload', process='cod_mortality')
    return path_to_data


def prompt_correct_database(database):
    ''' If the user is trying to upload to production, make sure they really want to do it,
        and if they don't, exit. Otherwise, do nothing, and the code will continue.
    '''
    yes = 'y'
    if database == PROD_DB:
        prompt = rv.response_validator(
            f"Are you sure you want to upload to {PROD_DB}?", {yes: 'yes', 'n': 'no'})
        if prompt == yes:
            return
        else:
            sys.exit()
    else: 
        pass


def check_all_entries_uploaded(database, data_version_id):
    global NUM_DATA_POINTS
    count_query = f'''
        SELECT COUNT(data_version_id) AS data_count
        FROM cod.{DATA_TABLE}
        WHERE data_version_id={data_version_id}
    '''
    with db_tools_core.session_scope(database) as sesh:
        data_count = db_tools_core.query_2_df(count_query, sesh)
    data_count = data_count['data_count'][0]
    if NUM_DATA_POINTS == data_count: 
        logging.info("All data points successfully uploaded")
    else: 
        logging.error(f'''Some data points were not uploaded. There were {NUM_DATA_POINTS} data points to upload,
but only {data_count} data points were found when reading from the Cod database. Please check what data points 
have not been uploaded successfully.''')
    return 


def check_previous_upload_not_best(database : str, previous_data_version_id : int) -> None:

    query = f'''
        select status
        from cod.{DATA_VERSION_TABLE}
        where data_version_id = {previous_data_version_id}
        and project_id = {PROJECT_ID}
        and source_id = {CANCER_REGISTRY_SOURCE_ID}
        and data_type_id = {DATA_TYPE_ID}
        and nid = {GENERIC_CANCER_NID}
        '''
    with db_tools_core.session_scope(database) as sesh:
        previous_status = db_tools_core.query_2_df(query, sesh)
    previous_status = previous_status['status'][0]
    if previous_status == NOT_BEST_STATUS:
        logging.info(f'''Previous upload, id {previous_data_version_id}, has been marked not-best''')
    else:
        logging.error(f'''Previous upload, id {previous_data_version_id}, has not been marked not-best. Please check the {DATA_VERSION_TABLE} and make sure the correct data version has been marked best''')


def check_no_upload_in_temp_status(database, launch_set_id):

    temp_status_query = f'''select status, data_version_id
        from cod.{DATA_VERSION_TABLE}
        WHERE project_id = {PROJECT_ID}
        AND source_id = {CANCER_REGISTRY_SOURCE_ID}
        AND data_type_id = {DATA_TYPE_ID}
        AND nid = {GENERIC_CANCER_NID}
        AND status = {TEMP_UPLOAD_STATUS}
        '''
    with db_tools_core.session_scope(database) as sesh:
        temp_status_df = db_tools_core.query_2_df(temp_status_query, sesh)
    if len(temp_status_df) > 0:
        data_version_ids = list(temp_status_df['data_version_id'])
        logging.error(f'''Some uploads are marked as temp with status being {TEMP_UPLOAD_STATUS}. Check the {database}.{DATA_VERSION_TABLE} table for row(s) with the data_version_id(s) {data_version_ids}''')
    else:
        logging.info(f"Verified that no uploads are still marked with temporary status {TEMP_UPLOAD_STATUS}.")


def query_previous_data_version_id(database : str) -> int:

    query = f'''
        select max(data_version_id) as max_data_version_id
        from cod.{DATA_VERSION_TABLE}
        where (project_id = {PROJECT_ID})
            and (source_id = {CANCER_REGISTRY_SOURCE_ID})
        '''
    with db_tools_core.session_scope(database) as sesh:
        previous_data_version = db_tools_core.query_2_df(query, sesh)
    previous_data_version = previous_data_version['max_data_version_id'][0]

    return previous_data_version


def check_path_to_data(path_to_data : str) -> None:
    ''' Test whether the user-supplied path actually exists.
    '''
    if os.path.exists(path_to_data):
        logging.info(f'Found the data file at {path_to_data}')
    else:
        raise FileNotFoundError(f'Not able to find a file at {path_to_data}. Check that the path is correct and try the upload again.')


def check_access_to_database(database : str) -> None:
    ''' Check whether the database can be accessed with the credentials available.
    '''
    try:
        with db_tools_core.session_scope(database):
            pass
    except:
        logging.error(f'Unable to access database {database}. Please check your .odbc.ini file; make sure that your\nusername and password are correct, and that "{database}" is present. For more information, go to\n"https://hub.ihme.washington.edu/display/CT/Setting+Up+Your+System".')
        sys.exit()
    else:
        logging.info(f'Access to database "{database}" verified.')


def run_cod_upload(database, path_to_data):
    ''' 
    ''' 
    logging.info("Beginning upload process")
    start_time_overall = time.time()

    global NUM_DATA_POINTS

    # quick safety check to ensure data gets uploaded to correct database
    prompt_correct_database(database)

    check_path_to_data(path_to_data)

    check_access_to_database(database)

    # get the data_version_id of the current best upload, so after we finish a new
    # upload, we can make sure that the current one has been set to not-best
    previous_data_version_id = query_previous_data_version_id(database)

    # upload a set of identifying information to a table in the CoD database, and
    # get a launch set id back
    upload_launch_set_info(database)
    launch_set_id = get_launch_set_id(database)
    logging.info(f"Launch set id is {launch_set_id}")

    # add a new row to the CoD 'data_version' table, and get the data_version_id that was created
    upload_data_version(database, launch_set_id)

    data_version_id = query_data_version(database, launch_set_id)
    logging.info(f"Data version id is {data_version_id}")
    
    # prep the data for upload
    logging.info("Prepping data for upload")
    df = prep_data(get_uid_cols(), data_version_id, database, path_to_data)
    # check columns
    check_columns(df, database)

    # and upload the data.
    logging.info("Uploading data")
    execute_upload(df, database)
    
    logging.info("Uploading the time the data upload completed")
    # update database statuses if no errors occur
    set_end_time_in_launch_set(database, launch_set_id)

    check_all_entries_uploaded(database, data_version_id)

    # mark this data as best, and "unmark" any previously bested data for this round
    mark_upload_as_best(database, launch_set_id)

    check_upload_marked_best(database, launch_set_id)

    check_no_upload_in_temp_status(database, launch_set_id)

    check_previous_upload_not_best(database, previous_data_version_id)

    end_time_overall = time.time()
    elapsed_time_overall = create_elapsed_time_string(start_time_overall, end_time_overall)
    logging.info(f'Upload process completed. Elapsed time: {elapsed_time_overall} minutes.')


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-db', '--database',
                        type=str,
                        default='testcod',
                        help='The (nick)name of the database as it appears in your .odbc.ini file')
    parser.add_argument('-ptd', '--path_to_data',
                        type=str,
                        default=get_path_to_data_for_uploading(),
                        help='The path to the file with the data to be uploaded')
    args = parser.parse_args()
    return (args)


if __name__ == "__main__":
    args = parse_args()
    run_cod_upload(args.database, args.path_to_data)

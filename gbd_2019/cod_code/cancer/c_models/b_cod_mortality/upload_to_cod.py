# -*- coding: utf-8 -*-
'''
Description: Uploads to cod database after grabbing required cols
How To Use: "python upload_to_cod.py <database_name>" 
    database_name : string - acceptable values: (NAME)
Contributors: NAME
'''

# import libraries 
import os
import pandas as pd
import numpy as np
from ast import literal_eval
from db_tools import ezfuncs as ez
from db_tools.ezfuncs import query, get_session, get_engine
from db_tools.loaders import Infiles
from FILEPATH import tests as tt
from cancer_estimation.py_utils import (
    data_format_tools as dft,
    common_utils as utils,
    modeled_locations,
    pydo
)
import FILEPATH as md
from cancer_estimation._database import cdb_utils as cdb
from db_queries import get_ids, get_location_metadata
import cancer_estimation.py_utils.response_validator as rv
from datetime import datetime
import itertools
import getpass
import subprocess


def get_uid_cols():
    ''' Returns a list of unique identifier columns
    '''
    return(['location_id', 'year_id', 'sex_id', 'age_group_id'])


def get_update_settings():
    ''' default values for the columns that keeps track of who/when updates what
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
    # grab description 
    db_link = cdb.db_api() 
    cod_df = db_link.get_table('cod_mortality_version')
    max_id = cod_df['cod_mortality_version_id'].max()
    desc = cod_df.loc[cod_df['cod_mortality_version_id'].eq(max_id), 'notes'].reset_index()
    
    return (desc['notes'][0])

def update_cancer_mortality_version(lsid): 
    ''' Updates cod_mortality_version table and inserts CoD's launch_set_id 
        for versioning purposes.
    '''
    # get max_id 
    db_link = cdb.db_api() 
    cod_df = db_link.get_table('cod_mortality_version')
    max_id = cod_df['cod_mortality_version_id'].max()
    
    # update Cancer's database with CoD's launch_set_id
    update_query='''
    UPDATE cancer_db.{tbl}
    cod_launch_set_id = {l_id} 
    WHERE cod_mortality_version_id={cod_id}
    '''.format(tbl='cod_mortality_version', l_id=int(lsid), cod_id=max_id)
    db_link = cdb.db_api()
    db_link.run_query(update_query)
    print('cancers cod_mortality_version table updated!')
    return 


def insert_new_sites(df, db): 
    ''' Function will update cod.site by adding entries, and will only do so 
        if the entry doesn't already exist. 
    '''

    # check for overlaps before uploading 
    name_df = df[['site_name']].drop_duplicates().dropna()
    unique_names = name_df['site_name'].unique()
    names_clause = "','".join(unique_names)
    names_query = """
        SELECT *
        FROM cod.site
        WHERE site_name IN ('{names_clause}')
    """.format(names_clause=names_clause)
    overlap = ez.query(names_query, conn_def=db)
    if len(overlap) > 0:
        raise AssertionError(
            "Conflicting site_name's already present: \n{overlap}".format(
            overlap=overlap)
        )
    engine = ez.get_engine(db)
    conn = engine.connect()
    df.to_sql('site', conn, if_exists='append', index=False)
    conn.close()
    print("Uploaded new {name_col}s: \n{name_df}".format(
        name_col='site', name_df=df))


def get_sites(db): 
    ''' Fetch metadata for site labels 
    '''
    query_string = '''
        SELECT site_id, site_name
        FROM cod.site
    '''
    db_link = cdb.db_api(db)
    conn = cdb.create_connection_string(db)
    site_df = cdb.execute_query(query_string, conn)

    # ensure empty string instead of null values 
    site_df.loc[(site_df['site_id'].eq(2)) & (site_df['site_name'].isnull()), 'site_name'] = ''
    return site_df 

def map_site_id(df, db):
    ''' Maps site/source information to Cancer Registry data. This function will 
        also add any new site/sources to cod.site if necessary
    '''

    # named site_name in database 
    df.rename(columns={'subdiv' : 'site_name'}, inplace=True)
    df['site_name'] = df['site_name'].fillna('') 

    # get site names from cod DB 
    db_sites = get_sites(db)
    unique_sites = df[['site_name']].drop_duplicates() 

    # make site_name both lower for merge. mysql site name is case-sensitive
    unique_sites['site_name_orig'] = unique_sites['site_name']
    unique_sites['site_name'] = unique_sites['site_name'].str.strip().str.lower()
    db_sites['site_name'] = db_sites['site_name'].str.strip().str.lower()

    # merge onto df 
    unique_sites = unique_sites.merge(db_sites, on=['site_name'], how='left')
    unique_sites['site_name'] = unique_sites['site_name_orig']
    unique_sites = unique_sites.drop('site_name_orig', axis=1)

    # find missing site entries 
    upload_sites = unique_sites[unique_sites['site_id'].isnull()]
    upload_sites = upload_sites[['site_name']].drop_duplicates()
    if len(upload_sites) > 0: 
        print(
            "No site_id for sites {}".format(upload_sites.site_name.unique())
        )
        print("Uploading new sites...")
        # if any, upload them
        insert_new_sites(upload_sites, db)

        # refresh db_sites
        db_sites = get_sites(db)
        unique_sites = unique_sites.drop('site_id', axis=1)
        unique_sites = unique_sites.merge(db_sites, on='site_name',how='left')
        tt.report_if_merge_fail(unique_sites, 'site_id', 'site_name')
    df = df.merge(unique_sites, on='site_name', how='left')
    return df


def get_data_version_id_cols(launch_set_id): 
    ''' Returns a dictionary with default values for columnns from data_version_id table 
    '''
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    desc = get_cod_description()
    new_dv_entry = {'gbd_round_id' : gbd_id,
                    'nid': 284465,
                    'underlying_nid' :np.nan ,
                    'data_type_id': 2,
                    'status_start': datetime.now(),
                    'source_id': 68,
                    'launch_set_id': launch_set_id,
                    'description': desc,
                    'status' : 2,
                    'tool_type_id' : 9
                    }
    return new_dv_entry


def upload_data_version(database, table, launch_set_id): 
    '''
    '''
    print('uploading new entry to data_version...')
    conn = cdb.create_connection_string(database) 
    data_version_cols = get_data_version_id_cols(launch_set_id)
    df = pd.DataFrame(data=data_version_cols, index=[0])
    df.to_sql(table, conn, if_exists='append',index=False)
    return

def query_data_version(database, table, launch_set_id):
    '''
    '''
    query_string = '''
        SELECT MAX(data_version_id)
        FROM cod.{db_table}
        WHERE launch_set_id={lsid}
    '''.format(db_table=table,lsid=launch_set_id)
    db_link = cdb.db_api(database)
    conn = cdb.create_connection_string(database)
    max_id = cdb.execute_query(query_string, conn)
    print('DATA VERSION ID SET TO...{}'.format(max_id)) 
    return max_id


def make_db_datestamp(): 
    ''' Create timestamp for CoD upload 
    ''' 
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clear_prev_data_version_status(database, table):
    """Update the data_version table.
    """
    date = make_db_datestamp()  
    can_nid = utils.get_gbd_parameter('generic_cancer_nid')
    update_query = """
        UPDATE cod.{tbl}
        SET status_end = "{dt}", status='0'
        WHERE nid={nid}
        AND status=1
    """
    #conn_string = cdb.create_connection_string('testcod')
    engine = get_engine(conn_def=table)
    conn = engine.connect()

    res = conn.execute(update_query.format(
        tbl=table,
        dt=date,
        nid=can_nid 
    ))

    conn.close()


def update_data_version(database, table, launch_set_id):
    '''
    '''
    clear_prev_data_version_status(database, table)
    update_query = """
        UPDATE cod.{tbl}
        SET status='1'
        WHERE launch_set_id = {lsid}
    """.format(
        tbl=table,
        lsid=launch_set_id
    )
    db_link = cdb.db_api(database)
    db_link.run_query(update_query)

    print('updated data version table!!')
    return()

def check_columns(df, table, db):
    ''' checks that all required columns are present before upload
    '''
    db_link = cdb.db_api(db)
    cod_cols = load_db_cols(table,db)

    # remove id and update cols from check 
    cod_cols = list(cod_cols[1:,]) 
    update_cols = get_update_settings()
    for col in update_cols.keys():
        cod_cols.remove(col)
    assert set(cod_cols).difference(set(df.columns)) == set(), \
        "Missing columns in dataset: \n{}".format(
            set(cod_cols).difference(df.columns))
    return(None)


def prep_data(uid_cols, data_version_id, db):
    ''' getting sample size which is total deaths by sex, year, loc and age 
    + cc_code by that specific acause
    as well as cause fractions which is deaths/sample_size
    '''
    print('prepping input...')
    df = pd.read_csv("{}/final_output.csv".format(
                            utils.get_path("mortality_model", 
                            base_folder = "storage")))
    df.loc[df['cf_raw'].isnull(), 'cf_raw'] = 0 
    # teporarily fill columns to 0 until post-age-sex 
    # and post-rdp mortality values are available
    cf_cols = ['cf_rd', 'cf_corr'] 
    for c in cf_cols: 
        df[c] = 0
    df = df.loc[df['acause'].ne('cc_code'),] # drop cc_code. 
    prepped_df = add_required_columns(df, uid_cols, data_version_id, db)
    return(prepped_df)


def add_required_columns(df, uid_cols, data_version_id, db):
    ''' adding in required columns for upload
    '''
    print('adding  required coluuns...')
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
    for col in cf_var_cols:
        final_df[col] = 0

    # add cause_id 
    cancer_link = cdb.db_api()
    gbd_id = utils.get_gbd_parameter('current_gbd_round')

    # other columns that are constant 
    final_df['underlying_nid'] = np.nan
    final_df['source_id'] = 68 # cancer default 
    final_df['data_type_id'] = 2 # cancer registry

    # add site labels. upload to site table if new sources are present 
    final_df = map_site_id(final_df, db)
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
    '''
    '''
    import git
    repo = git.Repo(search_parent_directories=True)
    sha = repo.head.object.hexsha
    return sha


def upload_launch_set(db, tb):
    """Upload the launch_set_id to the launch_set_table."""
    print('assigning launch_set_id...')
    conn = cdb.create_connection_string(db)
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
    df = pd.DataFrame(data=launch_set_upload, index=[0])
    df.to_sql('claude_launch_set', conn, if_exists='append', index=False)
    print('new claude launch set entry submitted!!')
    return() 

def query_launch_set_id(db,tb): 
    ''' Looks for the current max id in claude_launch_set id per user
    '''
    query_string = '''
        SELECT MAX(launch_set_id)
        FROM cod.{db_table}
        WHERE username='{usr}'
    '''.format(db_table=tb,usr=getpass.getuser())
    db_link = cdb.db_api(db)
    conn = cdb.create_connection_string(db)
    max_id = cdb.execute_query(query_string, conn)
    print('LAUNCH SET ID SET TO...{}'.format(max_id)) 
    return max_id

def update_launch_set(database, table, launch_set_id):
    '''
    '''
    launch_set_end = make_db_datestamp()
    update_query = """
        UPDATE cod.{tbl}
        SET launch_set_end = '{lse}'
        WHERE launch_set_id = {lsid}
    """.format(
        tbl=table, lse=launch_set_end,
        lsid=launch_set_id
    )
    db_link = cdb.db_api(database)
    db_link.run_query(update_query)

    print('updated launch_set_end to...{}'.format(launch_set_end))
    return()


def load_db_cols(table, db): 
    '''
    '''
    conn = cdb.create_connection_string(db)
    eng_conn = cdb.load_engine(conn)
    query_string = "SELECT * FROM {tbl} LIMIT 1".format(tbl=table)
    table = pd.read_sql(query_string, eng_conn)
    cod_cols = table.columns.values
    return cod_cols

def execute_upload(df, table, db):
    '''Uploads our table to cod data. This will load in the columns from the 
        database you want to upload to, and re-orders the dataframe to the 
        approrpriate order 
    '''
    print('executing upload...')
    db_link = cdb.db_api(db)

    # order columns same as table
    cod_cols = load_db_cols(table,db)
    cod_cols = list(cod_cols[1:,])
    update_cols = get_update_settings()
    for col in update_cols.keys():
        cod_cols.remove(col)
    upload_df = df[cod_cols]

    # ASSERT columns to upload are the same as what exists 
    assert set(cod_cols).difference(set(df.columns)) == set(), \
        "Missing columns in dataset: \n{}".format(
            set(cod_cols).difference(df.columns))
    
    # upload dataframe 
    write_table_to_csv(upload_df, table)
    upload_to_db(table, db)


def upload_to_db(table, db):
    """Upload a csv to the cod database."""
    #determine schema (uploading to cod or cancer)
    upload_path = _get_upload_path(table)
    sesh = get_session(db)
    inf = Infiles(table=table, schema='cod', session=sesh)
    inf.infile(path=upload_path, with_replace=False, commit=True)


def write_table_to_csv(df, table):
    """Get the output directory for claude data."""
    upload_path = _get_upload_path(table)
    df.to_csv(upload_path, index=False)


def _get_upload_path(table):
    """Get the path to write the file to."""
    upload_path = utils.get_path(key='cod_upload', process='cod_mortality')
    return upload_path


def prompt_correct_database(database):
    ''' Prompts user to check if the chosen database is correct
    '''
    if database == "prodcod":
        prompt = rv.response_validator(
            "You sure you want to upload to prodcod", {0: 'no', 1: 'yes'})
        if prompt == '1':
            print('Running upload phase to prodcod!!')
            return
        else:
            sys.exit()
    else: 
        print('Running upload phase to...{}'.format(database))
        pass


def check_all_entries_uploaded(database, table, data_version_id):
    '''
    '''
    upload_file = pd.read_csv(_get_upload_path('data'))
    count_query = '''
        SELECT COUNT(data_version_id)
        FROM cod.{tbl}
        WHERE data_version_id={d_id}
    '''.format(tbl=table, d_id=data_version_id)
    db_link = cdb.db_api(database)
    conn = cdb.create_connection_string(database)
    data_count = cdb.execute_query(count_query, conn)
    if len(upload_file) == data_count: 
        print("ALL DATA POINTS SUCCESSFULLY UPLOADED")
    else: 
        print("ERROR: SOME datapoints were not uploaded. Please check what data points \
        have not been uploaded successfully.")
    return 


def run_cod_upload(database='testcod'):
    ''' 
    ''' 
    # quick safety check to ensure data gets uploaded to correct database
    prompt_correct_database(database)

    # assign launch_set_id for CoD's versioning system
    upload_launch_set(database, 'claude_launch_set')
    launch_set_id = query_launch_set_id(database, 'claude_launch_set')
    upload_data_version(database, 'data_version', launch_set_id)
    data_version_id = query_data_version(database, 'data_version', launch_set_id)
    
    # update cancer table with launch_set_id 
    update_cancer_mortality_version(launch_set_id)
    
    # load input, rename, calculate sample_size
    df = prep_data(get_uid_cols(), data_version_id, db) 

    # check columns and upload final data 
    print('uploading final data...')
    check_columns(df, 'data', database)
    execute_upload(df, "data", database)
    
    # update database statuses if no errors occur
    update_launch_set(database, 'claude_launch_set', launch_set_id)
    update_data_version(database,'data_version',launch_set_id)

    check_all_entries_uploaded(database, 'data', data_version_id)
    print('Cancer -> CoD Upload DONE')


if __name__ == "__main__":
    import sys 
    db = sys.argv[1]
    run_cod_upload(db)
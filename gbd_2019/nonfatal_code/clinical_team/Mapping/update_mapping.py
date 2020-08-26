'''
Contains a set of functions that alter the clinical mapping database. A user may
create, and update records within the database. These actions are of course validated
by test functions. Currently there are no methods to delete icgs, cause codes or bundles,
since a modeler can only submit a remapping request.
All reading of the database is handled in a seperate module.
'''

import pandas as pd
import numpy as np
import mysql.connector as mariadb
from sqlalchemy import create_engine
import getpass
import warnings
import datetime
import re
import os
import sys
import glob

user = getpass.getuser()

repo_dir = ['Functions', 'Mapping', "FILEPATH"]
base = r"FILEPATH".format(user)
list(map(sys.path.append, [base + e for e in repo_dir]))

import hosp_prep

import clinical_mapping as cm
import ipdb

def engine_con():
    '''
    Pandas already has built in connectivity with SQLAlchamey, as such it is more
    efficent to this method.

    '''
    user, password, host = get_creds()
    engine = create_engine ('mysql://{user}:{password}@{host}:3306'\
                .format(user=user, password=password,host=host))
    return engine

def db_connection():
    '''
    Retrieves a user and associated permissions from a user created json file
    and establishes a connection to the clinical db. Connection is required
    to perform any operation on the database.

    Returns:
        mariadb_connection (obj)

    '''
    user, password, host = get_creds()
    mariadb_connection = mariadb.connect(user=user, password=password,
                                 database='clinical_mapping',
                                 host=host)
    return mariadb_connection

def get_creds():
    """
    Helper function of db_connection.
    Per the documentation on /database of the hospital repo, store your
    credentials to access the sandbox dB in a json file. This functions
    pulls your username, password and the sandbox host address
    """
    user = getpass.getuser()
    creds_path = "FILEPATH".format(user)
    try:
        creds = pd.read_json(creds_path).reset_index()
    except:
        assert False, "You must store your clinical data sandbox credentials in "\
            "FILEPATH"\
            "directory of the hospital repo for more info"
    user = creds.loc[creds['index']=='user', 'credentials'].iloc[0]
    password = creds.loc[creds['index']=='password', 'credentials'].iloc[0]
    host = creds.loc[creds['index']=='name', 'host'].iloc[0]

    return user, password, host


def update_table(df, tbl_name, merge='append', map_version=-1, dev=True):
    '''
    Append rows to a database table

    Args:
        df(pd.df): New or updated rows to be append to the desired table.

        tbl_name(str): Name of the table to be updated.

        merge(str): How pandas should handle exisiting table names when writting to the database
        set to 'append' as default. Can also take arguments 'replace' and 'fail'. As of right now
        there is no scenario in which these other two arguments would be envoked (we want a complete
        record of changes to the table)

        map_version(int): The version of the map to which these changes should be
        applied to. Set to -1 by default.

    '''

    engine = engine_con()
    df['map_version'] = map_version

    if dev:
        tbl_name = "{}_dev".format(tbl_name)
    df.to_sql(tbl_name, con=engine, if_exists=merge, schema='clinical_mapping', index=False)


def add_icg_ids(df):
    '''
    The nfc maps are inconsistent for bs_ids, that is different ids are assigned to the same
    icg across different versions of the maps. We enforce consistent bs_ids as well as adding
    new ids for new icgs

    Args:
        df(pd.df): Must have

    Return:
        pd.DataFrame
    '''
    maps = df.copy()
    dur = cm.get_clinical_process_data('icg_durations', prod=prod)


    dur.drop(['map_version', 'icg_measure'], axis = 1, inplace = True)


    df = maps.merge(dur, on="icg_name", how='left')
    msg =  'Some rows went missing'
    assert temp.shape[0] == df.shape[0], msg


    m_icgs = maps.icg_name.unique().tolist()
    d_icgs = d_cp.icg_name.unique().tolist()


    m = set(m_icgs) - set(d_icgs)
    print(len(m))
    msg = 'Mismatch of new ICGs'
    assert len(m) == df.loc[df.icg_id.isnull(),'icg_name'].unique().size, msg


    start = dur.icg_id.max() + 1
    stop = start + len(new_icg)
    new_icg = list(m)
    new_icg_id = np.arange(start, stop, 1)
    r = list(zip(new_icg, new_icg_id))

    for e in r:
        df.loc[df.icg_name == e[0], 'icg_id'] = e[1]

    msg = 'There are still ICGs without an ID'
    assert df.icg_id.isnull().sum() == 0, msg

    return df

def test_df(df, tbl_name):
    '''
    Generates a dataframe that is used to tested for data quality and ensure mapping rules.
    In addition, a second dataframe is produced and contains only the updated / new table
    rows.

    Args:
        df(pd.df): Dataframe that contains the column to be updated as well as the old and new
        values

        tbl_name(str): Name of the table that is being updated

    Returns:
        tbl: pd.DataFrame
        update_df: pd.DataFrame

    '''
    tbl = cm.get_clinical_process_data(tbl_name)
    ids = df.icg_id.unique().tolist()
    assert tbl[tbl.icg_ids.isin(ids)], 'ICG IDs are not present in the database table'



    df = df.replace(None, -1)
    tbl['update'] = 0
    for i, r in df.iterrows():
        col = r['col_name']
        tbl.loc[(tbl.icg_id == r['icg_id']) & (tbl[col] == r['old_val']),
        [col, 'update']] = [r['new_val'], 1]


    tbl = cast_to_int(tbl, tbl_name)



    tbl = tbl.replace(-1, np.nan)

    update_df = tbl[tbl.update == 1]
    update_df.drop(['update'], axis=1, inplace=True)
    tbl.drop(['update'], axis=1, inplace=True)
    tbl = remove_nulls(tbl)

    return tbl, update_df

def cast_to_int(tbl, tbl_name):
    '''
     Since the data is stored long we need to cast certain cols from obj to int
    '''

    if tbl_name.startswith('cause_code'):
        tbl['code_system_id'] = pd.to_numeric(tbl['code_system_id'])

    if tbl_name.startswith('icg_bundle'):
        tbl['bundle_id'] = pd.to_numeric(tbl['bundle_id'])

    if tbl_name.startswith('icg_durations'):
        tbl['icg_duration'] = pd.to_numeric(tbl['icg_duration'])

    return tbl

def insert_icg_helper(tbl_name, df):
    '''
    Helper function which assists in adding an icg to the database.

    Args:
        tbl_name(str) : mirrors the name of the table that is to be updated.

        df(pd.df) : DataFrame of the new values. Saved in a long format

    Return:
        result: Either a string or a list based on the results of the testing
        df_sub(pd.sub)
    '''

    tbl = cm.get_clinical_process_data(tbl_name)
    tbl_cols = tbl.columns.tolist()
    tbl_cols.remove('icg_id')
    tbl_v = tbl['map_version'].unique().item()

    df_sub = df[df.col_name.isin(tbl_cols)]
    df_sub = pd.pivot_table(df_sub, index='icg_id', values='value', columns='col_name',
            aggfunc=np.sum).reset_index()

    tbl = pd.concat([tbl, df_sub], sort=False).reset_index(drop=True)
    tbl['map_version'] = tbl_v
    tbl = cast_to_int(tbl, tbl_name)

    if tbl_name.startswith('cause_code'):
        result = cm.test_cc_icg_map(tbl)
    elif tbl_name.startswith('icg_bundle'):
        result = cm.test_icg_bundle(tbl)
    elif tbl_name.startswith('icg_durations'):
        result = cm.test_icg_vals(tbl, tbl_v)

    df_sub = cast_to_int(df_sub, tbl_name)
    return result, df_sub

def insert_icg(df, dev=True, update_version=False):
    '''
    Inserts a new ICG to the map. By default the map version is updated for any new
    insert. There are, however, instances where several inerts may be applied to the map
    so the user has the opportunity not to update the version. The ICG is used as the key
    to bind the new mappings.

    Args:
        df(pd.df): Contains the complete mapping of the icd, that is there at least one associated
        cause_code and bundle_id and measure / duration

        update_version(bol): Updates the entire map version to the next iteration for
        all tables. Defaults to False
    '''

    count = 0
    cols = ['cause_code', 'code_system_id', 'icg_name', 'icg_measure','icg_duration',
    'bundle_id', 'male', 'female', 'yld_age_start', ' yld_age_end']
    ids = df.icg_id.unique().tolist()


    for id in ids:
        r = df.loc[df.icg_id == id, 'col_name'].unique().tolist()
        assert cols == r, 'Missing columns for icg_id {}'.format(id)

    tbl_cur = cm.get_clinical_process_data('icg_durations_dev')
    try:
        tbl_new = cm.get_clinical_process_data('icg_durations_dev', map_version=-1)
        tbl = tbl = pd.concat([tbl_cur, tbl_new])
    except AssertionError:
        print('There are no pending icg inserts')
        tbl = tbl_cur

    icg_new = df.icg_id.unique().tolist()
    assert tbl[tbl.icg_id.isin(icg_new)].empty, 'You are adding ICGs that already exisit'

    tbls = ['cause_code_icg', 'icg_bundle', 'icg_durations', 'age_sex_restrictions']
    if dev:
        tbls = [e + '_dev' for e in tbls]

    insert_dict = {}
    for e in tbls:
        result, insert_df = insert_icg_helper(e, df)
        if type(result) == str:
            insert_dict[e] = insert_df
            count += 1
        else:
            return result



    if count == 3:
        for k, v in list(insert_dict.items()):
            update_table(df=v, tbl_name=k, map_version=-1)

    if update_version and count == 3:
        update_map_version(update_version)

def update_icg_to_bundle(df, dev=True, update_version=False):
    '''
    Updates the mapping between icg and bundle. The ICG must exist before any update can occur
    since the ICG is used as the primary key to update the mapping. However, a new bundle can
    be inserted using this method.

    Args:
        df(pd.df): Dataframe that contains the column to be updated as well as the old and new
        values

        update_version(bol): Updates the entire map version to the next iteration for
        the icg_bundle table. Defaults to False

    '''
    tbl_name = 'icg_to_bundle'

    if dev:
        tbl_name = tbl_name + '_dev'

    tbl, update_df = test_df(df, tbl_name)
    result = cm.test_icg_bundle(tbl)

    if type(result) == str:
        update_table(update_df, tbl_name)
        if update_version:
            update_map_version([tbl_name])

    else:
        return result

def update_cc_to_icg(df, dev=True, update_version=False):
    '''
    Updates the mapping between icg and bundle. The cause code and its associated code system id
    must exist before any update can occur since the cause code is used as
    the primary key to update the mapping.

    Args:
        df(pd.df): Dataframe that contains the column to be updated as well as the old and new
        values.

        update_version(bol): Updates the entire map version to the next iteration for
        the icg_bundle table. Defaults to False
    '''
    tbl_name = 'cause_code_icg'

    if dev:
        tbl_name = tbl_name + '_dev'

    if 'icg_measure' in df.col_name.unique():
        temp = df[df.col_name.isin(['icg_duration', 'icg_measure'])]
        result = update_duration(temp)
        if type(result) == list:
            return result

    tbl, update_df = test_df(df, tbl_name)
    result = cm.test_code_sys(tbl)
    if type(result) == str:
        update_table(tbl_name)
        if update_version:
            update_map_version([tbl_name])

    else:
        return result

def update_duration(df, dev=True, update_version=False):
    '''
    Updates an icd measure. If the icd is switiching measure than the duration
    must also be provided.

    Args:
        df(pd.df): Dataframe that contains the column to be updated as well as the old and new
        values. The df has four columns: icg_id, col_name, old_val, new_val

        update_version(bol): Updates the entire map version to the next iteration for
        the icg_bundle table. Defaults to False
    '''
    tbl_name = 'icg_durations'

    if dev:
        tbl_name = tbl_name + '_dev'

    if 'icg_measure' in df.col_name.unique():
        m_ids = df[df.col_name == 'icg_measure', 'icg_ids'].unique().tolist()
        d_ids = df[df.col_name == 'icg_duration', 'icg_ids'].unique().tolist()
        r = [e for e in d_ids if e not in m_ids]
        assert len(r) == 0, 'There is an incomplete update for icg(s) switching measures'

    tbl, update_df = test_df(df, tbl_name)
    result = cm.test_icg_vals(tbl)

    if type(result) is str:
        update_table(update_df, tbl_name)
        if update_version:
            update_map_version([tbl_name])
    else:
        return result

def update_age_sex_restriction(df, dev=True, update_version=False):
    tbl_name = 'age_sex_restrictions'
    if dev:
        tbl_name = tbl_name + '_dev'

    tbl, update_df = test_df(df, tbl_name)

    assert tbl.loc[tbl.yld_age_end > 95, 'yld_age_end'].any(), 'There are ages that exceede 95'
    assert tbl[tbl.yld_age_start > tbl.yld_age_end].empty, 'Start ages greater than End ages'

    all_ids = tbl.shape[0]
    unique_ids = set(all_ids)
    assert all_ids == unique_ids, 'There are duplicate icg ids'

    update_table(update_df, tbl_name)
    if update_version:
        update_map_version([tbl_name])



def update_map_version(dev=True, tables=None):
    '''
    Pulls the lastest version of desired table, updates the version id and then appends to
    the desired tables. Looks for row with map_value of -1 (a dummy value signalling
    either a new or updated row) updates to next map version, by a value of 1, then
    removes any instance of rows with map_vaule of -1. If there are no new / updated
    rows then the map version is incremented by 1 and appended to table(s).

    Args:
        table(list str): Identfies which table(s) should be updated. Keyword 'all'
        can be invoked to update all map related tables in the database.
        Defaults to None

    Raises:
        TypeError: When the tables is not in list format. This exludes the usage of
        the term 'all'
    '''
    engine = create_engine()

    tables = ['cause_code_icg', 'icg_bundle', 'icg_durations',
        'age_sex_restrictions']

    if table == 'all' and dev:
        tables = [e + '_dev' for e in tables]

    if type(tables) is not list:
        raise TypeError('tables must be in list format')

    for table in tables:
        tbl = cm.get_clinical_process_data(table)
        try:
            tbl_new = cm.get_clinical_process_data(table, map_version=-1)
        except AssertionError:
            tbl_new = None

        if type(tbl_new) == pd.core.frame.DataFrame:
            cols = list(tbl.columns)
            cols.remove('map_version')
            df = pd.concat([tbl, tbl_new])
            df.sort_values(by=['map_version'], inplace=True)
            tbl = df.drop_duplicates(subset=col, keep='first')
            q = '''
                DELETE FROM {}
                WHERE map_version = -1
                '''.format(table)
            engine.execute(q)

        tbl['map_version'] = max(tbl.map_version.unique()) + 1
        tbl = remove_nulls(tbl)
        tbl.to_sql(table, con=engine, if_exists='append', index=False)

def remove_nulls(df):
    df.dropna(inplace=True)
    return df

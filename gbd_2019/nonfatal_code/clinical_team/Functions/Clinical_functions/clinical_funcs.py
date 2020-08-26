"""
Set of functions to connect our team to our data now stored in a dB

!!!!======Current Issues======!!!!
problem, we have really strangely structured data, it's not square, so
we can't always assume data is present for a given location/year or age/location

Solutions?
    if you pass it a single thing, like location id for example then
    that's the only thing it will add to the where condition, I think
    I need to use *args to accomplish this
!!!!==========================!!!!


=========Main Function===========
get_clinical_data()
==================================


universal params
    if human_readable=True
        age <- pull in age start/end shared.age_group
        sex <- pull in sex name shared.sex
        location <- pull in location ascii name shared.location
        estimate_id <- pull in estimate_name clinical_data.estimate
        icg_id <- pull in icg_name clinical_data.icg_duration
        maybe pull in a list of bundles it maps to clinical_data.icg_bundle

    some option to pass and aggregate for a bundle at the intermediate level?
        rates can just be added, hard to aggregate uncertainty tho

    fix_table_name=False
        set to true if you want to infer the table name super simple str match


"""

import pandas as pd
import sys
import warnings
import os
import re
import sqlalchemy
import getpass
from db_tools import ezfuncs


user = getpass.getuser()
sys.path.append("FILEPATH".format(user))
import test_clinical_funcs
import query_funcs


def parse_odbc(odbc_filepath):
    """parse odbc.ini file into a dictionary.

        Args:
            odbc_filepath (string): path to odbc.ini file.

        Returns:
            dict: Dictionary of connection definition arguments.
            ::
                {
                    "conn_def_name1": {
                        "attr_name1": "attr_val1",
                        "attr_name2": "attr_val2",
                        ...
                    },
                    "conn_def_name2": {
                        ...
                    },
                    ...
                }
    """
    with open(os.path.expanduser(odbc_filepath)) as f:
        lines = f.readlines()
    conn_defs = {}
    def_name = ''
    for l in lines:


        name_match = re.search("\[.*\]", l)
        if name_match is not None:
            def_name = name_match.group()[1:-1]
            conn_defs[def_name] = {}
            continue


        if def_name == '':
            continue


        tokens = l.split("=")
        if len(tokens) < 2:
            continue
        k, v = tokens[0:2]
        k = k.strip().lower()
        v = v.strip()
        conn_defs[def_name][k] = v

    return conn_defs


def get_engine(epi_db_name='clinical',
               odbc_filepath="FILEPATH",
               db_name='clinical'):
    """
    Note: db_name is a misnomer, it should be called conn_def or something else

    Gets you a sqlalchemy engine; the Engine is the starting point for any
    SQLAlchemy application. It's "home base" for the actual database.

    Returns:
        sqlalchemy Engine instance
    """
    config = parse_odbc(odbc_filepath=odbc_filepath)
    hn = config[db_name]['server']
    pt = config[db_name]['port']
    usr = config[db_name]['user']
    psswrd = config[db_name]['password']

    conn_string = "SERVER".format(
        )


    engine = sqlalchemy.create_engine(conn_string, echo=False, pool_recycle=300)

    return engine


def get_named_data(col, clause):
    """
    Params:
        col (str): Which id column to pull the col name for. Currently must be one of
                   [location_id, age_group_id, estimate_id, bundle_id, icg_id]
        clause (str): A where clause to extract only the required ids.

    Returns:
        df (pd.DataFrame) with mappings from the integer ID columns to a human interpretable name

    """


    conn_dict = {'location_id': 'clinical_data',
                 'bundle_id': 'epi',
                 'icg_id': 'clinical_data',
                 'age_group_id': 'clinical_data',
                 'estimate_id': 'clinical_data'}

    q = query_funcs.named_query_gen(col, clause)

    try:
        conn_dict[col]
    except:
        assert False, 'Unable to determine which database to pull the column "{}" from'.format(col)
    if conn_dict[col] in ['clinical_data']:
        engine = get_engine()
        df = pd.read_sql("DB QUERY")
    elif conn_dict[col] in ['epi']:
        df = ezfuncs.query(query=q, conn_def=conn_dict[col])

    return df


def name_merger(df, name_df, on_col, exp_col):
    """
    Merges human readable names onto queried data using GBD ids.
    This function is called by merge_all_names

    Params:
        df: your data
        name_df: map from an id to a name
        on_col: column to merge on, an ID column
        exp_col: column(s) that must be merged on
    """
    pre = df.shape
    pre_col_names = df.columns

    df = df.merge(name_df, how='left', on=on_col)


    test_clinical_funcs.test_merge_name(df, pre, on_col=on_col, new_cols=exp_col)

    diff_cols = set(df.columns).symmetric_difference(pre_col_names)
    if not isinstance(exp_col, list):
        exp_col = [exp_col]
    assert diff_cols == set(exp_col),\
        "The only difference between the pre and most merge names should be {} not {}".\
        format(set(exp_col), diff_cols)
    assert pre[0] == df.shape[0],\
        "There were {} rows and now there are {}. This isn't correct".format(pre[0], df.shape[0])

    return df


def merge_all_names(df):
    """
    Our data will be stored in a format that is not easily human interpretable (lots of int ids)
    so we need to map from IDs to names

    df: a pd.DataFrame with standard id cols, acceptable values listed below in known_id_cols
    """

    known_id_cols = ['location_id', 'bundle_id', 'icg_id', 'age_group_id', 'estimate_id']

    df_cols = [c for c in df.columns if c in known_id_cols]


    id_col_d = {'location_id': 'location_name', 'bundle_id': 'bundle_name',
               'icg_id': 'icg_name', 'age_group_id': ['age_group_years_start', 'age_group_years_end'],
                'estimate_id': 'estimate_name'}

    pre = df.shape

    for col in df_cols:
        in_cond = tuple(df[col].unique())
        if len(in_cond) > 1:
            cond = "WHERE {} in {}".format(col, in_cond)
        elif len(in_cond) == 1:
            cond = "WHERE {} = {}".format(col, in_cond[0])
        else:
            warnings.warn("It's probably not great that this warning is happening")

        name_df = get_named_data(col=col, cond=cond)

        df = name_merger(df, name_df, on_col=col, exp_col=id_col_d[col])


    return df


def order_columns(df):
    """
    order the columns in a specific way
    """
    pre = df.columns
    ordered_cols = []

    df = df[ordered_cols]
    assert not set(df.columns).symmetric_difference(set(pre)), "dead"
    return df

def check_row_size(table, clause, engine, limit=10000000):
    """
    We don't want people to accidentally try to pull an entire table
    so set some arbitrary limit to how many rows you can get back from a query

    q: a partial mysql query, should just include table and where condition
    """
    table = query_funcs.table_name_fixer(table)
    q = "SQL".format(table, clause)
    df = pd.read_sql("DB QUERY")

    if df.rows.max() > limit:
        assert False, "Sorry, you've requested a df with over 10 million rows and hit our "\
                      "arbitrary size restriction. You'll have to use a different method "\
                      "to pull so much data"

    return


def get_clinical_data(data_type, run_id='best', test_size=False, gbd_round_id=6, human_readable=True, **kwargs):
    """
    This should be the actual forward facing function so add the most informative documentation here

    data_type: (str)
                should be one of these three ['intermediate', 'icg', 'bundle']
                to identify where in the process to pull data from
    run_id: (str or int)
                default is best, but latest is also an acceptable string. If input is an int it will pull that specific run_id
    test_size: (bool)
                if True then run an arbitrary and inefficient count(*) on the observations to return, default is 10 million
    gbd_round_id: (int)
                Same as other GBD functions, 5==2017, 6==2019, etc
    human_readable: (bool)
                if True merge on the names for all the incomprehensible id columns
    **kwargs: (keywords)
                the location/sex/year/age/cause/estimate_type you'd like to return. They get turned into part of a where clause

    """
    msg = """It's unclear which type of clinical estimates should be returned.
             The first argument to this function should be one of these three values -
             'intermediate', 'icg', 'bundle'
             where intermediate are post age-sex splitting clinical estimates in count space
             icg are final icg estimates stored in single years and
             bundle are final bundle estimates in 5 year groups
            """
    assert data_type in ['intermediate', 'icg', 'bundle'], msg
    test_clinical_funcs.test_kwargs(data_type, **kwargs)

    query, clause = query_funcs.agg_est_query(table=data_type, run_id=run_id, gbd_round_id=gbd_round_id, **kwargs)


    engine = get_engine()

    if test_size:

        check_row_size(table=data_type, clause=clause, engine=engine)


    print(query)
    df = pd.read_sql("DB QUERY")

    pre = df.shape[0]
    if human_readable:
        id_dict = query_funcs.agg_name_query(**kwargs)
        for key in id_dict.keys():
            if key in df.columns:

                tmp = get_named_data(key, clause="")
                df = df.merge(tmp, how='left', on=key)
                del tmp
    assert pre == df.shape[0], "Row counts changed after the merges. From {} to {}".format(pre, df.shape[0])

    return df

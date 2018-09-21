import numpy as np
import pandas as pd
import sqlalchemy as sql
import subprocess
import os
import sys
folder = "INSERT_PATH_HERE"
sys.path.append(folder)
import codem.db_connect as db_connect

LOCS_EXCLUDE_PATH = "FILEPATH"

def get_latest_covariate(model_version_id, db_connection):
    '''
    Given a covariate model version id find the best model version id for that
    covariate.

    :param model_version_id: int
        model version id to look up and create duplicate of
    :return: int
        new model version id
    '''
    call = '''
    SELECT model_version_id FROM covariate.model_version
    WHERE data_version_id IN (SELECT data_version_id FROM covariate.data_version
    WHERE covariate_id=(SELECT covariate_id FROM covariate.data_version
                        WHERE data_version_id =(SELECT data_version_id FROM
                                                covariate.model_version
                                                WHERE model_version_id={model})))
                                                AND is_best=1;
    '''.format(model=model_version_id)
    new_model = db_connect.query(call, db_connection)["model_version_id"][0]
    return new_model


def current_location_set_id(gbd_round_id, db_connection):
    """
    Looks up the latest 2015 location set id for codem

    :return: int
        latest 2015 location set id
    """
    call = '''
    SELECT location_set_version_id FROM shared.location_set_version_active
    WHERE location_set_id = 35 # this location set id refres specifically to the hierarchy that codem uses
    AND gbd_round_id = {};
    '''.format(gbd_round_id)
    location_set_version_id = \
        db_connect.query(call, db_connection)["location_set_version_id"][0]
    return location_set_version_id


def current_cause_set_id(gbd_round_id, db_connection):
    """
    Looks up the latest 2015 cause set id for codem

    :return: int
        latest 2015 cause set id
    """
    call = '''
    SELECT cause_set_version_id FROM shared.cause_set_version_active
    WHERE cause_set_id = 4 # referes to the set of causes specific to codem
    AND gbd_round_id = {};
    '''.format(gbd_round_id)
    cause_set_version_id = \
        db_connect.query(call, db_connection)["cause_set_version_id"][0]
    return cause_set_version_id


def get_current_commit(branch):
    '''
    (None) -> str

    Gets the hash of the most current commit from codem.
    '''
    os.chdir("FILEPATH")
    call = "ssh-agent bash -c 'ssh-add {0}; git ls-remote {1} refs/heads/{2} | cut -f 1'"
    key = "RSA_FILEPATH"
    repo = "REPO_LOCATION_PATH"
    call = call.format(key, repo, branch)
    process = subprocess.Popen(call, shell=True, stdout=subprocess.PIPE)
    out, err = process.communicate()
    return out.strip("\n")


def remove_covariates(df, covariates, db_connection):
    '''
    (df, list) -> df

    Removes the rows containing the input covariates from the input df. The
    covariates in the df are stored as covariate model version ids, and the
    input covariates should be in the covariate_name_short form
    '''
    call = '''SELECT mv.model_version_id
        FROM covariate.model_version mv
        INNER JOIN covariate.data_version dv
        ON mv.data_version_id = dv.data_version_id
        INNER JOIN shared.covariate sc
        ON dv.covariate_id = sc.covariate_id
        WHERE sc.covariate_name_short = "{}"'''
    for covariate in covariates:
        cov_mvids = db_connect.query(call.format(covariate), db_connection)
        cov_mvids = cov_mvids.model_version_id.values
        df = df[~df.covariate_model_version_id.isin(cov_mvids)]
    return df

def new_covariates(old_model_version_id, new_model_version_id, db_connection):
    '''
    (int, int) -> None

    Given two integers representing a previous model version ID used for CODEm
    and a new model, update the new model to use the newest version of the
    covariates by inputting those values in the production database.
    '''
    call = '''
    SELECT * FROM cod.model_covariate where model_version_id = {model}
    '''.format(model=old_model_version_id)
    df = db_connect.query(call, db_connection)

    df["model_version_id"] = new_model_version_id
    df["covariate_model_version_id"] = \
        df.covariate_model_version_id.map(lambda x: get_latest_covariate(x, db_connection))
    drop_cols = ["model_covariate_id", "date_inserted", "inserted_by",
                 "last_updated", "last_updated_by", "last_updated_action"]
    df.drop(drop_cols, axis=1, inplace=True)
    db_connect.write_df_to_sql(df, db="cod", table="model_covariate",
                               connection=db_connection, creds=None)


def set_new_models(gbd_round_id, description, db_connection, old_gbd_round_id=None):
    '''
    () -> list of ints

    Takes all the best models from 2013 and adapts them to run for their 2015
    run by placing their parameters and specifications in the production database.

    Returns the model version id numbers of the new models
    '''
    old_gbd_round_id = old_gbd_round_id if old_gbd_round_id is not None else gbd_round_id
    branch = "master"
    drop_cols = ["model_version_id", "date_inserted", "inserted_by", "last_updated",
                 "last_updated_by", "last_updated_action"]
    null_cols = ["pv_rmse_in", "pv_rmse_out", "pv_coverage_in", "pv_coverage_out",
                 "pv_trend_in", "pv_trend_out", "pv_psi", "best_start", "best_end", ]
    call = '''
    SELECT *
    FROM
    cod.model_version
    WHERE
    model_version_id IN (
        SELECT
            child_id # the children of hybrids
        FROM
            cod.model_version_relation
        WHERE parent_id IN (SELECT
                                model_version_id
                            FROM
                                cod.model_version
                            WHERE
                                model_version_type_id=3 # hybrid model
                                AND is_best=1 # is best
                                AND gbd_round_id = {}
                            )
        )
    '''.format(old_gbd_round_id)
    df = db_connect.query(call, db_connection)
    df["description"] = description
    commit = get_current_commit(branch)
    df["code_version"] = '{"branch": "%s", "commit": "%s"}' % (branch, commit)
    df[["status", "is_best"]] = 0
    df["previous_model_version_id"] = df["model_version_id"]
    df["location_set_version_id"] = \
        current_location_set_id(gbd_round_id, db_connection)
    df["cause_set_version_id"] = \
        current_cause_set_id(gbd_round_id, db_connection)
    df["gbd_round_id"] = gbd_round_id
    df.loc[df.age_end == 21, "age_end"] = 235
    df[null_cols] = np.NaN
    df.drop(drop_cols, axis=1, inplace=True)
    models = db_connect.write_df_to_sql(df, db="cod", table="model_version",
                                        connection=db_connection,
                                        creds=None, return_key=True)
    return models


def set_new_covariates(models, db_connection):
    """
    Sets the covariates for all the new models using their prior selected covariates

    :param models: list of int
        list of models to add covariates for
    :param db_connection: str
        db to connect to
    """
    model_str = ', '.join([str(x) for x in models])
    call = '''
    SELECT model_version_id, previous_model_version_id
    FROM cod.model_version
    WHERE model_version_id IN ({model_str});
    '''.format(model_str=model_str)
    df = db_connect.query(call, db_connection)
    for i in range(df.shape[0]):
        new_covariates(df["previous_model_version_id"][i],
                       df["model_version_id"][i], db_connection)


def run_codem(gbd_round_id, description, db_connection, old_gbd_round_id=None,
                                                            debug_mode=False):
    '''
    () -> None

    Taking all the current best hybrid models from CODEm, run a new model using
    updated data and covariates. Inputs all necessary parameters into the
    production database and submits a job for each model.
    '''
    print "setting new models \n"
    models = set_new_models(gbd_round_id, description,
                            db_connection, old_gbd_round_id)
    print "setting new covariates \n"
    set_new_covariates(models, db_connection)
    qsub = "SH_AND_QSUB_PATH "
    options = "-e ERROR_PATH -o OUTPUT_PATH "
    name = '-N cod_{model_version_id}_global -P proj_codem '
    slots = '-pe multi_slot 46 '
    bash_script = 'PATH_TO_CODEV2.SH '
    python_script = 'PATH_TO_CODEV2.PY {model_version_id} {db_c}'
    call = qsub + name + slots + options + bash_script + python_script
    print "Running models \n"
    for model in models:
        print model
        subprocess.call(call.format(model_version_id=model, db_c=db_connection, dbg=debug_mode),
                        shell=True)
    return None


def get_excluded_locations(df):

    """
    gets the locations to exclude based on the input model parameters. if
    the model_version_type_id is 2 (data rich), the locations are pulled from
    an external source according to their data completeness ranking (3 stars or
    fewer)

    :param df: pandas dataframe
        should contain at least: model_version_id, model_version_type_id,
        locations_exclude
    :return: pandas Series
        the list of locations to exclude
    """
    
    # check if reading in the locations_exclude list will be necessary
    type_ids = df.model_version_type_id.values
    if 2 in type_ids:
        exclude_df = pd.read_csv(LOCS_EXCLUDE_PATH)
        locs_to_exclude_list = exclude_df.location_id.values.tolist()
        locs_to_exclude = ' '.join([str(loc) for loc in locs_to_exclude_list])
    else:
        pass
    # change if model_version_type_id is 2 (data rich)
    for index, row in df.iterrows():
        if row['model_version_type_id'] == 2:
            df.set_value(index, 'locations_exclude', locs_to_exclude)
        else:
            pass
    return df.locations_exclude

def set_rerun_models(list_of_models, gbd_round_id, db_connection, desc=None):
    """
    set of codem models based on past model version id

    :param list_of_models: list like
        list like object of model version ids
    :param desc: str
        string to use for description name
    """
    models_string = "(" + ",".join([str(l) for l in set(list_of_models)]) + ")"
    branch = "master"
    drop_cols = ["model_version_id", "date_inserted", "inserted_by", "last_updated",
                 "last_updated_by", "last_updated_action"]
    null_cols = ["pv_rmse_in", "pv_rmse_out", "pv_coverage_in", "pv_coverage_out",
                 "pv_trend_in", "pv_trend_out", "pv_psi", "best_start", "best_end", ]
    call = '''
    SELECT *
    FROM
    cod.model_version
    WHERE
    model_version_id IN {ms}
    '''.format(ms=models_string)
    df = db_connect.query(call, db_connection)
    df["description"] = desc
    df["code_version"] = '{"branch": "%s", "commit": "%s"}' % (branch, get_current_commit(branch))
    df[["status", "is_best"]] = 0
    df["previous_model_version_id"] = df["model_version_id"]
    df["location_set_version_id"] = current_location_set_id(gbd_round_id, db_connection)
    df["cause_set_version_id"] = current_cause_set_id(gbd_round_id, db_connection)
    df["gbd_round_id"] = gbd_round_id
    df["locations_exclude"] = get_excluded_locations(df)
    df.loc[df.age_end == 21, "age_end"] = 235
    df[null_cols] = np.NaN
    df.drop(drop_cols, axis=1, inplace=True)
    models = db_connect.write_df_to_sql(df, db="cod", table="model_version",
                                        connection=db_connection,
                                        creds=None, return_key=True)
    return models


def rerun_models(list_of_models, gbd_round_id, db_connection, desc, **kwargs):
    """
    Rerun a set of codem models based on there past model version id

    :param list_of_models: list like
        list like object of model version ids
    :param desc: str
        string to use for description name
    :param kwargs:
        dbg and cov (debug mode and previous covariate model)
    """
    print "setting new models \n"
    models = set_rerun_models(list_of_models, gbd_round_id, db_connection, desc)
    print "setting new covariates \n"
    set_new_covariates(models, db_connection)

    qsub = "source SH_AND_QSUB_PATH "
    options = "-e ERROR_PATH -o OUTPUT_PATH "
    name = '-N cod_{model_version_id}_global -P proj_codem '
    slots = '-pe multi_slot 50 '
    python_script = 'PATH_TO_KICKOFF.PY '
    kick_off = 'PATH_TO_KICKOFF.SH '  
    call = 'sh {kick_off} {py_script} {user} {mvid} {db} {SITE}'
    if kwargs is not None:
        for key, item in kwargs.iteritems():
            call += ' -' + key + ' ' + str(item)
    else:
        pass


    print "Running models \n"
    for model in models:
        print model
        sub_call = call.format(kick_off=kick_off, py_script=python_script, 
                user='codem', mvid=model, db=db_connection, SITE='other')
        print sub_call
        subprocess.call(sub_call, shell=True)

    return models


if __name__ == "__main__":
    import argparse
    import time
    parser = argparse.ArgumentParser(description='Run CODEM centrally')
    parser.add_argument('-db', '--db_connection', help='the db server to connect to', type=str, required=True)
    parser.add_argument('-d', '--description', help='description of central model run', type=str, required=False)
    parser.add_argument('-g', '--gbd_round_id', help='gbd round id for this codem run', type=int, required=False)
    args = parser.parse_args()
    db_connection = args.db_connection
    if isinstance(args.description, type(None)):
        description = time.strftime("%m/%d/%Y") + " central codem run"
    else:
        description = args.description
    if isinstance(args.description, type(None)):
        gbd_round_id = 3
    else:
        gbd_round_id = args.gbd_round_id
    run_codem(gbd_round_id=gbd_round_id, description=description,
              db_connection=db_connection)

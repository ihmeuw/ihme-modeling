import numpy as np
import pandas as pd
import sqlalchemy as sql
import subprocess
import os
import sys

try:
    folder = "/".join(os.path.abspath(__file__).split("/")[:-2]) + "/FILEPATH"
except NameError:  # We are the main py2exe script, not a module
    import sys

    folder = "FILEPATH"

sys.path.append(folder)
import codem.db_connect as db_connect

LOCS_EXCLUDE_PATH = 'FILEPATH' + \
                    'FILEPATH'


def get_covariate_id(model_version_id):
    """Given a covariate model version ID find the covariate ID."""
    call = '''
    SELECT DISTINCT
        sc.covariate_id, sc.covariate_name
    FROM
        covariate.model_version cmv
            INNER JOIN
        covariate.data_version cdv ON cdv.data_version_id = cmv.data_version_id
            INNER JOIN
        shared.covariate sc ON sc.covariate_id = cdv.covariate_id
    WHERE
        cmv.model_version_id = {mvid}
    '''.format(mvid=model_version_id)
    covariate_id = db_connect.query(call, 'DATABASE')["covariate_id"][0]

    return covariate_id


def get_latest_covariate(model_version_id, acause, db_connection, gbd_round_id):
    '''
    Given a covariate model version id find the best model version id for that
    covariate.

    :param model_version_id: int
        model version id to look up and create duplicate of
    :param gbd_round_id: int
        gbd round id of the CURRENT model run
    :return: int
        new model version id
    '''
    covariate_id = get_covariate_id(model_version_id)

    # really sloppy stuff for covariate deprecation
    if covariate_id == 1:
        covariate_id = 2
    if covariate_id == 41:
        if "nutrition" not in acause:
            covariate_id = 911
        else:
            covariate_id = 914
    if covariate_id == 78:
        if "nutrition" not in acause:
            covariate_id = 917
        else:
            covariate_id = 920
    if covariate_id == 103:
        covariate_id = 105
    if covariate_id == 104:
        covariate_id = 105
    if covariate_id == 136:
        if "nutrition" not in acause:
            covariate_id = 938
        else:
            covariate_id = 941
    if covariate_id == 153:
        if "nutrition" not in acause:
            covariate_id = 965
        else:
            covariate_id = 968
    if covariate_id in [9, 52, 81, 95, 150, 971]:
        covariate_id = None

    if covariate_id is not None:
        call = '''
            SELECT model_version_id FROM covariate.model_version
            WHERE data_version_id IN (SELECT data_version_id FROM covariate.data_version WHERE covariate_id={cid})
            AND is_best=1 AND gbd_round_id = {gbd}
            '''.format(cid=covariate_id, gbd=gbd_round_id)
        new_model = db_connect.query(call, 'DATABASE')["model_version_id"][0]
    else:
        new_model = np.nan
    return new_model


def get_model_type(model_version_id, db):
    '''
    Given a model-version-id, pulls the type of model. Used for tracking and also for naming jobs.
    :param model_version_id: int
    :param db: str
        'modeling-cod-db'
    :return: str
        one of 'global' or 'datarich'
    '''
    call = '''
    SELECT model_version_type_id FROM cod.model_version
    WHERE model_version_id = {mvid}
    '''.format(mvid=model_version_id)

    df = db_connect.query(call, db).iloc[0, 0]

    if df == 1:
        type = "global"
    else:
        type = "datarich"

    return type


def get_model_metadata(model_version_ids, db):
    model_list = ', '.join([str(model) for model in model_version_ids])

    call = '''
    SELECT mv.model_version_id, mv.model_version_type_id, mv.sex_id, mv.cause_id, sc.cause_name, mv.age_start, mv.age_end
    FROM cod.model_version mv
    INNER JOIN shared.cause sc
    ON mv.cause_id = sc.cause_id
    WHERE mv.model_version_id IN ({mvids})
    '''.format(mvids=model_list)

    print call

    df = db_connect.query(call, db)

    return df


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
    key = "FILEPATH"
    repo = "REPO"
    call = call.format(key, repo, branch)
    process = subprocess.Popen(call, shell=True, stdout=subprocess.PIPE)
    out, err = process.communicate()
    return out.strip("\n")


def get_acause(model_version_id, db_connection):
    """
    Get acause from CODEm model version ID
    :param model_version_id:
    :return: str with acause
    """
    call = '''
    SELECT
        sc.acause AS acause
    FROM
        shared.cause sc
            INNER JOIN
        cod.model_version mv ON sc.cause_id = mv.cause_id
    WHERE
        mv.model_version_id = {mvid}
    '''.format(mvid=model_version_id)
    acause = db_connect.query(call, db_connection)["acause"][0]
    return acause


def new_covariates(old_model_version_id, new_model_version_id, db_connection, gbd_round_id):
    '''
    (int, int) -> None

    Given two integers representing a previous model version ID used for CODEm
    and a new model, update the new model to use the newest version of the
    covariates by inputting those values in the production database.
    '''

    acause = get_acause(old_model_version_id, db_connection)

    call = '''
    SELECT * FROM cod.model_covariate where model_version_id = {model}
    '''.format(model=old_model_version_id)
    df = db_connect.query(call, db_connection)
    df["model_version_id"] = new_model_version_id
    df["covariate_model_version_id"] = \
        df.covariate_model_version_id.map(
            lambda x: get_latest_covariate(x, acause, db_connection, gbd_round_id))
    drop_cols = ["model_covariate_id", "date_inserted", "inserted_by",
                 "last_updated", "last_updated_by", "last_updated_action"]
    df.drop(drop_cols, axis=1, inplace=True)
    df = df[np.isfinite(df['covariate_model_version_id'])]
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
    # for right now replace oldest age group 21 with new oldest age group
    # we probably want to change this in the future
    df.loc[df.age_end == 21, "age_end"] = 235
    df[null_cols] = np.NaN
    df.drop(drop_cols, axis=1, inplace=True)
    models = db_connect.write_df_to_sql(df, db="cod", table="model_version",
                                        connection=db_connection,
                                        creds=None, return_key=True)
    return models


def set_new_covariates(models, db_connection, gbd_round_id):
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
                       df["model_version_id"][i], db_connection, gbd_round_id)


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
    set_new_covariates(models, db_connection, gbd_round_id)
    qsub = "source FILEPATH "
    options = "-e FILEPATH -o FILEPATH "
    name = '-N cod_{model_version_id}_global -P proj_codem '
    slots = '-pe multi_slot 46 '
    bash_script = 'FILEPATH'
    python_script = 'FILEPATH {model_version_id} {gbd_round_id} {db_c}'
    call = qsub + name + slots + options + bash_script + python_script
    print "Running models \n"
    for model in models:
        print model
        subprocess.call(call.format(model_version_id=model, gbd_round_id=gbd_round_id, db_c=db_connection, dbg=debug_mode),
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
    df["code_version"] = '{"branch": "%s", "commit": "%s"}' % (
        branch, get_current_commit(branch))
    df[["status", "is_best"]] = 0
    df["previous_model_version_id"] = df["model_version_id"]
    df["location_set_version_id"] = current_location_set_id(
        gbd_round_id, db_connection)
    df["cause_set_version_id"] = current_cause_set_id(
        gbd_round_id, db_connection)
    df["gbd_round_id"] = gbd_round_id
    df["locations_exclude"] = get_excluded_locations(df)
    # for right now replace oldest age group 21 with new oldest age group
    # we probably want to change this in the future
    df.loc[df.age_end == 21, "age_end"] = 235
    df[null_cols] = np.NaN
    df.drop(drop_cols, axis=1, inplace=True)
    models = db_connect.write_df_to_sql(df, db="cod", table="model_version",
                                        connection=db_connection,
                                        creds=None, return_key=True)
    return models


def open_tracking_csv(tracking_file):
    '''
    Opens a tracking csv and returns the existing df, OR creates a new CSV if the tracking CSV does not already exist
    for this central run.
    :param tracking_file: str
    :return: df
    '''
    if os.path.exists(tracking_file):
        df = pd.read_csv(tracking_file)
    else:
        df = pd.DataFrame().reindex(columns={'model_version_id', 'model_version_type_id', 'cause_id', 'cause_name',
                                             'sex_id', 'age_start', 'age_end',
                                             'wave', 'run_iter', 'status'})

    return df


def rerun_models(list_of_models, gbd_round_id, db_connection, slots, desc, central_run=None, wave=None, testing=True, cov=None, **kwargs):
    """
    Rerun a set of codem models based on there past model version id

    :param list_of_models: list like
        list like object of model version ids
    :param central_run: int
        for the specified GBD round ID, which central run is it? Should start at 1
    :param wave: int
        for the specified central run, what wave is it? Should start at 1
    :param desc: str
        string to use for description name
    :param debug_mode: bool
        whether or not to store preds and set seeds for debugging
    """
    print "setting new models \n"
    models = set_rerun_models(
        list_of_models, gbd_round_id, db_connection, desc)
    print "setting new covariates \n"
    set_new_covariates(models, db_connection, gbd_round_id)
    qsub = "source FILEPATH "
    options = "-e FILEPATH -o FILEPATH "
    name = '-N cod_{model_version_id}_{type}_{acause} -P proj_codem '
    slots = '-pe multi_slot {} '.format(slots)

    if testing == False:
        repo = 'FILEPATH'
    else:
        repo = 'FILEPATH'

    bash_script = repo + 'FILEPATH '
    python_script = repo + \
        'FILEPATH {model_version_id} ' + db_connection + ' {gbd_round_id}'
    if cov is not None:
        python_script += ' -cov ' + str(cov)
    if kwargs is not None:
        for key, item in kwargs.iteritems():
            python_script += ' ' + str(item)
    else:
        pass

    call = qsub + name + slots + options + bash_script + python_script
    print "Running models \n"

    # TRACKING INFORMATION
    if central_run is not None:
        tracking_dir = "FILEPATH"
        filename = "tracking_GBDROUND-{}_CENTRALRUN-{}.csv".format(
            gbd_round_id, central_run)
        tracking_filepath = os.path.join(tracking_dir, filename)

        tracking_file = open_tracking_csv(tracking_filepath)

        # get new metadata
        model_metadata = get_model_metadata(models, db_connection)
        model_metadata["run_iter"] = None
        model_metadata["wave"] = wave

    for model in models:
        print model
        model_type = get_model_type(model, db_connection)
        acause = get_acause(model, db_connection)
        sub_call = call.format(model_version_id=model, gbd_round_id=gbd_round_id,
                               type=model_type, acause=acause)
        subprocess.call(sub_call, shell=True)

        if central_run is not None:
            # subset the metadata to just this model
            sub_metadata = model_metadata.loc[model_metadata.model_version_id == model]
            cause_id = sub_metadata.cause_id.iloc[0]
            sex_id = sub_metadata.sex_id.iloc[0]
            model_version_type_id = sub_metadata.model_version_type_id.iloc[0]
            age_start = sub_metadata.age_start.iloc[0]
            age_end = sub_metadata.age_end.iloc[0]

            # get list of run iterations
            existing_runs = tracking_file.loc[
                (tracking_file.cause_id == cause_id)
                & (tracking_file.sex_id == sex_id)
                & (tracking_file.model_version_type_id == model_version_type_id)
                & (tracking_file.age_start == age_start)
                & (tracking_file.age_end == age_end), 'run_iter'].tolist()

            if not existing_runs:
                run_iter = 1
            else:
                run_iter = max(existing_runs) + 1

            model_metadata.loc[model_metadata.model_version_id ==
                               model, 'run_iter'] = run_iter

    if central_run is not None:
        out_file = tracking_file.append(model_metadata)
        print "Writing tracking file."
        out_file.to_csv(tracking_filepath, index=False)

    return models


if __name__ == "__main__":
    import argparse
    import time

    parser = argparse.ArgumentParser(description='Rerun Model')
    parser.add_argument('model', help='models to rerun', type=int)
    parser.add_argument(
        'description', help='description of central model run', type=str)
    parser.add_argument('-g', '--gbd_round_id',
                        help='gbd round id for this codem run', type=int, required=False)
    parser.add_argument('-db', '--db_connection',
                        help='the db server to connect to', type=str, required=False)
    parser.add_argument(
        '-s', '--slots', help='number of slots for the job', type=int, required=False)

    parser.add_argument
    args = parser.parse_args()

    model = args.model
    description = args.description

    if isinstance(args.description, type(None)):
        description = 'testing the submodel confidence interval'
    else:
        description = args.description
    if isinstance(args.gbd_round_id, type(None)):
        gbd_round_id = 5
    else:
        gbd_round_id = args.gbd_round_id
    if isinstance(args.db_connection, type(None)):
        db_connection = 'DATABASE'
    else:
        db_connection = args.db_connection
    if isinstance(args.slots, type(None)):
        slots = 40
    else:
        slots = args.slots

    rerun_models([model], gbd_round_id, db_connection, slots,
                 description, central_run=None, wave=None, testing=True)

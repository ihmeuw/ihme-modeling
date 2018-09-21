import pandas as pd
import sqlalchemy as sql
import numpy as np
import os, sys
import json
import subprocess
from glob import glob
folder = "INSERT_PATH_HERE"
sys.path.append(folder)
from codem.db_connect import query


def get_broken_models(db_connection, description="relaunch new CoD data"):
    """
    () -> 1d-array

    Gets all the runs of codem that that are on the production database that
    failed the 2015 run and returns their model version id numbers in an array.
    """
    call = '''
    SELECT model_version_id FROM cod.model_version
        WHERE
        (description = "{description}" OR
        description LIKE "%%{description}:%%")
        AND pv_rmse_in IS NULL;
    '''.format(description=description)
    broken_models = np.array(query(call, db_connection))
    return sorted(broken_models[:, 0])


def see_log(model_version_id):
    """

    :param model_version_id: integer representing a codem model version id
    :return: None

    Prints a file log from codem that has been run centrally.
    """
    base = "INSERT_ERROR_PATH"
    error_log = [x for x in os.listdir(base) if
                 "cod_" + str(model_version_id) + "_global.e" in x][0]
    error_log = base + error_log
    f = open(error_log, "r")
    text = f.read()
    print text
    f.close()


def get_acause(model_version_id, db_connection):
    """
    :param model_version_id: integer representing a codem model version id
    :return: cause

    Given a model version id returns the corresponding cause.
    """
    call = '''
    SELECT cause_id FROM cod.model_version
        WHERE model_version_id = {model_version_id}
    '''.format(model_version_id=model_version_id)
    cause_id = np.array(query(call, db_connection))[0,0]
    call = '''
    SELECT acause FROM shared.cause WHERE cause_id = {cause_id}
    '''.format(cause_id = cause_id)
    acause = np.array(query(call, db_connection))[0,0]
    return acause


def see_r_log(model_version_id, lm_file=False):
    """
    :param model_version_id: integer representing a codem model version id
    :param lm_file: boolean on whether to check the linear regression file or not
    :return: None

    Prints the log file of a an R run from the corresponding model version ID.
    """
    f =["covariate_selection.Rout", "linear_model_build.Rout"] [int(lm_file)]
    path = "INSERT_PATH_TO_R_LOG"
    f = open(path, "r")
    text = f.read()
    print text
    f.close()


def submodel_covs(submodel_version_id, db_connection):
    """
    :param submodel_version_id: integer representing a codem submodel version id
    :return: Pandas data frame with information on submodel covariates

    Given a submodel version id returns the covariates that were used in the
    construction of that model.
    """
    call = '''
    SELECT covariate_name_short FROM shared.covariate
        WHERE covariate_id IN (SELECT covariate_id from covariate.data_version WHERE data_version_id IN
            (SELECT data_version_id FROM covariate.model_version
                WHERE model_version_id IN
                (SELECT covariate_model_version_id FROM cod.submodel_version_covariate
                    WHERE submodel_version_id={submodel_version_id})))
    '''.format(submodel_version_id=submodel_version_id)
    df = query(call, db_connection)
    df["submodel_version_id"] = submodel_version_id
    return df


def get_submodels(model_version_id, db_connection):
    """
    :param model_version_id: integer representing a codem model version id
    :return: Pandas Data frame with submodels and corresponding information
    """
    call = '''
    SELECT submodel_version_id, rank, weight, submodel_type_id, submodel_dep_id
    FROM cod.submodel_version
    WHERE model_version_id = {model_version_id}
    '''.format(model_version_id=model_version_id)
    df = query(call, db_connection)
    return df


def all_submodel_covs(model_version_id, db_connection):
    """
    :param model_version_id: integer representing a codem model version id
    :return: Pandas Data frame with submodels, covariates, and corresponding information
    """
    submodels = get_submodels(model_version_id, db_connection)
    covs = pd.concat([submodel_covs(x) for x in submodels.submodel_version_id],
                     axis=0).reset_index(drop=True)
    df = covs.merge(submodels, how="left")
    df = df.sort(["rank", "covariate_name_short"])
    call = '''
    SELECT submodel_type_id, submodel_type_name FROM cod.submodel_type;
    '''
    df2 = query(call, db_connection)
    df = df.merge(df2, how="left")
    call = '''
    SELECT submodel_dep_id, submodel_dep_name FROM cod.submodel_dep;
    '''
    df2 = query(call, db_connection)
    df = df.merge(df2, how="left")
    df.drop(["submodel_type_id", "submodel_dep_id"],inplace=True, axis=1)
    df = df.sort(["rank", "covariate_name_short"])
    df["approximate_draws"] = np.round(df.weight.values * 1000.)
    return df


def best_model_covs(acause, db_connection, year=2015):
    """

    :param acause: string representing a valid acause
    :return: Pandas Data frame with submodels, covariates, and corresponding information

    Given a string that represents a valid acause in the GBD heirarchy retruns the best
    models submodel covariates as well as metadata regarding those models.
    """
    call = '''
    SELECT model_version_id FROM cod.model_version
        WHERE cause_id=(SELECT cause_id FROM shared.cause WHERE acause="{acause}")
        AND date_inserted > '{year}-01-01' ORDER BY pv_rmse_out limit 1
    '''.format(acause=acause, year=year)
    model_version_id = np.array(query(call, db_connection))[0,0]
    return all_submodel_covs(model_version_id, db_connection)


def rerun_model(model_version_id, db_connection):
    qsub = "source INSERT_SGE_AND_QSUB_PATH "
    name = "-N cod_{0}_global -P proj_codem "
    slot_options = "-pe multi_slot 66 "
    options = "-e INSERT_ERROR_PATH -o INSERT_OUTPUT_PATH "
    bash_script = "PATH_TO_CODEV2.SH "
    python_script = "PATH_TO_CODEV2.PY {0} {1}"
    call = qsub + name + options + slot_options + bash_script + python_script
    subprocess.call(call.format(model_version_id, db_connection), shell=True)


def import_json(f):
    '''
    (str) -> list of lists

    Given a a string leading to a valid JSON file will return a list
    of lists with the terminal nodes being dictionaries that can later
    be converted to data frames or matrices.
    '''
    json_data = open(f)
    data = json.load(json_data)
    json_data.close()
    return data


def check_model_count(model_version_id, db_connection, broken=False):
    '''
    (int) -> int

    Returns the numne
    '''
    base = "PATH_TO_CV_SELECTED"
    if broken:
        number = len(import_json(base)["cf_vars"]) + len(import_json(base)["rate_vars"]) * 2
    else:
        call = '''
        SELECT submodel_version_id FROM cod.submodel_version WHERE model_version_id={model_version_id};
        '''.format(model_version_id=model_version_id)
        df = query(call, db_connection)
        number = df.shape[0]
    return number


def get_codviz_error(model_version_id):
    acause = get_acause(model_version_id)
    home = "HOME_PATH"
    assert len(glob(home)) > 0, "No logs found for model version id %s" % model_version_id
    path = glob(home)[-1]
    f = open(path, "r")
    text = f.read()
    print text
    f.close()


def get_model_covariates(model_version_id, db_connection):
    """
    :param model_version_id:
    :return:
    """
    call = '''
    SELECT covariate_model_version_id FROM cod.model_covariate
        WHERE model_version_id = "{model_version_id}"
    '''.format(model_version_id=model_version_id)
    covariate_ids = np.array(query(call, db_connection))[:, 0]
    return covariate_ids


def location_ids_for_covariate(covariate_model_version_id, db_connection):
    call = '''
    SELECT DISTINCT location_id FROM covariate.model
        WHERE model_version_id = {covariate_model_version_id}
        ORDER BY location_id
    '''.format(covariate_model_version_id=covariate_model_version_id)
    locations = np.array(query(call, db_connection))[:, 0]
    return locations


def covariate_id_to_short_name(covariate_model_version_id, db_connection):
    call = '''
    SELECT covariate_name_short FROM shared.covariate
        WHERE covariate_id IN (SELECT covariate_id from covariate.data_version WHERE data_version_id IN
            (SELECT data_version_id FROM covariate.model_version
                WHERE model_version_id = {covariate_model_version_id}))
    '''.format(covariate_model_version_id=covariate_model_version_id)
    covariate_short_name = np.array(query(call, db_connection))[0, 0]
    return covariate_short_name


def find_failed_jobs(usr):
    """
    Find failed jobs that are on qacct based on a usr and project

    :param usr: str
        string corresponding to a usr name
    :return: list
        list of failed jobs
    """
    find_jobs = 'qacct -o {usr} -j'.format(usr=usr)
    isolate_failures = 'egrep "submit_cmd|exit" | grep "exit_status  1" -B1 | grep " proj_codem " -B1'
    find_model_ids = 'grep submit_cmd | rev | cut -d " "  -f 1 | rev'
    call = ' | '.join([find_jobs, isolate_failures, find_model_ids])
    process = subprocess.Popen(call, shell=True, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    out_list = out.split("\n")[:-1]
    out_list_numeric = [int(x) for x in out_list]
    return out_list_numeric

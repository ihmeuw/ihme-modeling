import sqlalchemy as sql
import numpy as np
import subprocess, sys
import sys, os
folder = "INSERT_PATH_HERE"
sys.path.append(folder)
from codem.db_connect import query

def get_broken_models(db_connection):
    '''
    () -> 1d-array

    Gets all the runs of codem that that are on the production database that
    failed the run and returns their model version id numbers in an array.
    '''
    call = '''
    SELECT model_version_id FROM cod.model_version
        WHERE description = "2015 full run test 6/10"
        AND pv_rmse_in IS NULL;
    '''
    broken_models = np.array(query(call, db_connection))
    return sorted(broken_models[:, 0])


def run_broken_model(model, db_connection):
    '''
    (int) -> None

    Re-runs a model of codem using an updated version of the code located on
    home drive. This is a one off function for debugging and the sorts.
    '''
    qsub = "source INSERT_PATH_TO_SGE_AND_QSUB "
    name = "-N cs_{0}_2 -P proj_codem "
    slot_options = "-pe multi_slot 80 "
    options = "-e INSERT_ERROR_PATH -o INSERT_OUTPUT_PATH "
    bash_script = "INSERT_PATH_TO_CODEV2.SH "
    python_script = "INSERT_PATH_TO_CODEV2.PY {0} {1}"
    call = qsub + name + options + slot_options + bash_script + python_script
    subprocess.call(call.format(model, db_connection), shell=True)
    return None


def main():
    '''
    () -> None

    Gets all the models from the production database that failed the run
    and re-runs them using the version of codem located in home drive.
    Requires there to be an integer input value with the script.
    '''
    N = int(sys.argv[1])
    db_connection = sys.argv[2]
    broken_models = get_broken_models(db_connection)
    for i in range(N):
        run_broken_model(broken_models[i], db_connection)
    return None


if __name__ == "__main__":
    main()

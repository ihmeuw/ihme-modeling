import pytest

from codem.scripts.central_run import collect_tasks
from codem.joblaunch.run_utils import list_check, change_model_status
from codem.joblaunch.run_utils import log_branch, get_logged_branch, get_current_branch


def test_get_tasks(central_run_filepath,
                   db_connection, description, gbd_round_id):
    tasks = collect_tasks(1, "main", central_run_filepath, db_connection=db_connection,
                          description='collect tasks ' +
                                      description, gbd_round_id=gbd_round_id,
                          decomp_step_id=4)
    tasks = [t for t in tasks if not t.hash_name.startswith("sleep")]
    try:
        assert len(tasks) == 3 * 2 * 3
        log_branch(tasks[0].model_version_id, db_connection)
        assert get_logged_branch(tasks[0].model_version_id, db_connection) == get_current_branch()
        with pytest.raises(RuntimeError):
            log_branch(tasks[0].model_version_id, db_connection, branch_name=get_current_branch() + 'wrong_branch')
    finally:
        for t in tasks:
            change_model_status(t.model_version_id,
                                status=3,
                                db_connection=db_connection)


def test_get_tasks_addmodels(central_run_filepath,
                             addmodels_filepath,
                             db_connection,
                             description, gbd_round_id):
    tasks = collect_tasks(1, "main", central_run_filepath, addmodels_filepath=addmodels_filepath,
                          db_connection=db_connection,
                          description='collect tasks addmodels ' +
                                      description, gbd_round_id=gbd_round_id,
                          decomp_step_id=4)
    tasks = [t for t in tasks if not t.hash_name.startswith("sleep")]
    try:
        assert len(tasks) == 3 * 2 * 3 + 2
    finally:
        for t in tasks:
            change_model_status(t.model_version_id,
                                status=3,
                                db_connection=db_connection)

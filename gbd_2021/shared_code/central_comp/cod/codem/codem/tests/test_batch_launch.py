import pytest

from gbd import constants as gbd
from hybridizer.joblaunch.HybridTask import HybridTask

from codem.joblaunch.CODEmTask import CODEmTask
from codem.joblaunch.batch_launch import demographic_launch
from codem.joblaunch.batch_launch import version_launch
from codem.joblaunch.batch_launch import individual_launch
from codem.joblaunch.batch_launch import check_model_attribute
from codem.joblaunch.profiling import get_all_parameters
from codem.joblaunch.run_utils import list_check
from codem.joblaunch.run_utils import change_model_status


def cleanup_tasks(tasks, db_connection):
    tasks = list_check(tasks)
    for t in tasks:
        assert type(t.model_version_id) is int
        change_model_status(t.model_version_id,
                            status=3,
                            db_connection=db_connection)


@pytest.fixture(scope='module')
def global_model_version_id():
    return 620906


@pytest.fixture(scope='module')
def datarich_model_version_id():
    return 619700


@pytest.fixture(scope='module')
def db_connection():
    return 'ADDRESS'


@pytest.fixture(scope='module')
def cause_id():
    return 573


def test_demographic_launch(db_connection, description, cause_id):
    tasks = demographic_launch(acause='mental_eating_anorexia', age_start=6,
                               age_end=14, sex_id=gbd.sex.FEMALE,
                               db_connection=db_connection,
                               decomp_step_id=4,
                               gbd_round_id=6,
                               description='demographic launch ' + description)
    codem_params = get_all_parameters(cause_ids=cause_id, hybridizer=False)
    hybrid_params = get_all_parameters(cause_ids=cause_id, hybridizer=True)
    try:
        assert len(tasks) == 3
        for t in tasks[0:2]:
            assert type(t) == CODEmTask
            assert t.gb == 10
            assert t.minutes == 14400
            assert t.cores == 3
            assert t.max_attempts == 5
        h = tasks[2]
        assert type(h) == HybridTask
        assert h.gb == 20
        assert h.minutes == 120
        assert h.cores == 1
        assert h.max_attempts == 3

        assert tasks[0].model_version_type_id == 2
        assert tasks[1].model_version_type_id == 1
        assert tasks[2].model_version_type_id == 3

        assert type(tasks[2]) == HybridTask
        for t in tasks:
            check_model_attribute(t.model_version_id,
                                  'sex_id', gbd.sex.FEMALE,
                                  db_connection=db_connection)
            check_model_attribute(t.model_version_id,
                                  'age_start', 6, db_connection=db_connection)
            check_model_attribute(t.model_version_id,
                                  'age_end', 14, db_connection=db_connection)
    finally:
        cleanup_tasks(tasks, db_connection)


def test_version_launch(global_model_version_id, datarich_model_version_id,
                        db_connection, description,
                        gbd_round_id):
    tasks = version_launch(global_model_version_id=global_model_version_id,
                           datarich_model_version_id=datarich_model_version_id,
                           db_connection=db_connection,
                           gbd_round_id=gbd_round_id,
                           decomp_step_id=4,
                           description='version launch ' + description)
    try:
        assert len(tasks) == 3
        for t in tasks[0:2]:
            assert type(t) == CODEmTask
        assert type(tasks[2]) == HybridTask
        for t in tasks:
            check_model_attribute(t.model_version_id,
                                  'sex_id', gbd.sex.FEMALE,
                                  db_connection=db_connection)
            check_model_attribute(t.model_version_id,
                                  'age_start', 6, db_connection=db_connection)
            check_model_attribute(t.model_version_id,
                                  'age_end', 14, db_connection=db_connection)
    finally:
        cleanup_tasks(tasks, db_connection)


def test_individual_launch(global_model_version_id, db_connection,
                           description, gbd_round_id):
    t = individual_launch(model_version_id=global_model_version_id,
                          description='individual launch ' + description,
                          gbd_round_id=gbd_round_id,
                          decomp_step_id=4,
                          db_connection=db_connection)[0]
    try:
        assert type(t) == CODEmTask
    finally:
        cleanup_tasks(t, db_connection)



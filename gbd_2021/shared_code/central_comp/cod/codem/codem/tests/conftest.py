import pytest
import datetime


@pytest.fixture(scope='session')
def model_version_id():
    """
    Test model version ID to run on.
    SIDS, Female, data-rich, GBD 2019 step 4
    """
    return 632867


@pytest.fixture(scope='session')
def db_connection():
    """
    Database connection to pull from -- use test!
    """
    return 'ADDRESS'


@pytest.fixture(scope='session')
def gbd_round_id():
    """
    Round of GBD.
    """
    return 6


@pytest.fixture(scope='session')
def description():
    return 'codem pytest on {}'.format(str(datetime.datetime.now()))


@pytest.fixture(scope='session')
def central_run_filepath():
    return 'FILEPATH'


@pytest.fixture(scope='session')
def addmodels_filepath():
    return 'FILEPATH'


@pytest.fixture(scope='session')
def codem_command():
    return """MKL_NUM_THREADS=1 OMP_NUM_THREADS=1 run {model_version_id}
              {db_connection} {gbd_round_id} 0"""


@pytest.fixture(scope='session')
def hybrid_command():
    return """MKL_NUM_THREADS=1 OMP_NUM_THREADS=1 run {model_version_id}
              {db_connection} {gbd_round_id} 0"""
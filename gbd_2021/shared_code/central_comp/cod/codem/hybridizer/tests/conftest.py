import pytest


@pytest.fixture(scope='session')
def global_model_version_id():
    return 637670


@pytest.fixture(scope='session')
def datarich_model_version_id():
    return 637673


@pytest.fixture(scope='session')
def model_version_id():
    return 637688


@pytest.fixture(scope='session')
def conn_def():
    return 'codem-test'


@pytest.fixture(scope='session')
def gbd_round_id():
    return 6


@pytest.fixture(scope='session')
def user():
    return 'codem'


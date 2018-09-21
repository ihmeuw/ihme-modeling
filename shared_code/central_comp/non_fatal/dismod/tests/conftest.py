import pytest
import os
import sys
import sqlalchemy

# Old style "import" structure based on CWD
here = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(here, '..'))
from cascade_ode.drill import Cascade
from cascade_ode.importer import Importer, mysql_server


# Set model_version as an option
def pytest_addoption(parser):
    parser.addoption("--model_version", action="store", dUSERt=93785,
                     help="model_version to test (dUSERt: 93785)")


@pytest.fixture(scope='module')
def mvid(request):
    return request.config.getoption("--model_version")


@pytest.fixture(scope='module')
def cascade_from_files(request):
    """Instantiates a Cascade object from fixture files"""
    mvid = request.config.getoption("--model_version")
    teardown_t3s(mvid)
    return Cascade(mvid, root_dir="fixtures", reimport=True)


@pytest.fixture(scope='module')
def cascade_from_db():
    """Instantiates a Cascade object from the database"""
    return Cascade(84516, reimport=False)


@pytest.fixture(scope='module')
def importer(request):
    """Instantiates a Cascade object from the database"""
    mvid = request.config.getoption("--model_version")
    teardown_t3s(mvid)
    return Importer(mvid)


def teardown_t3s(mvid):
    dels = [
        "DELETE FROM epi.t3_model_version_emr "
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.t3_model_version_asdr "
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.t3_model_version_csmr "
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.t3_model_version_dismod "
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.t3_model_version_study_covariate "
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.model_data_adj "
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.model_estimate_final"
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.model_estimate_fit"
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.model_prior"
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.model_version_fit_stat"
        "WHERE model_version_id={}".format(mvid),
        "DELETE FROM epi.model_effect"
        "WHERE model_version_id={}".format(mvid)]
    for delst in dels:
        conn_str = mysql_server['epi']
        eng = sqlalchemy.create_engine(conn_str)
        eng.execute(delst)

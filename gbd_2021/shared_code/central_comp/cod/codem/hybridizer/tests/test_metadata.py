import pytest

from hybridizer.database import invalid_attribute, read_input_model_data
from hybridizer.utilities import get_params


@pytest.fixture(scope='module')
def wrong_datarich_model_version_id():
    return 410912


def test_input_model_data(global_model_version_id,
                          datarich_model_version_id,
                          conn_def):
    df = read_input_model_data(global_model_version_id,
                               datarich_model_version_id,
                               conn_def)
    assert len(df) == 2


def test_params(global_model_version_id, datarich_model_version_id,
                conn_def, user):
    params = get_params(global_model_version_id, datarich_model_version_id,
                        conn_def, user)
    assert len(params) == 9


def test_params_fail(global_model_version_id, wrong_datarich_model_version_id,
                     conn_def, user):
    with pytest.raises(RuntimeError):
        get_params(global_model_version_id, wrong_datarich_model_version_id,
                   conn_def, user)


def test_refresh_exceptions():
    assert not invalid_attribute(28, 'refresh_id', 29)
    assert not invalid_attribute(29, 'refresh_id', 29)
    assert not invalid_attribute(30, 'refresh_id', 30)
    assert invalid_attribute(27, 'refresh_id', 29)
    assert invalid_attribute(27, 'refresh_id', 28)
    assert invalid_attribute(29, 'refresh_id', 30)
    assert invalid_attribute(686, 'cause_id', 289)
    assert not invalid_attribute(686, 'cause_id', 686)

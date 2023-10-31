import pandas as pd
import pytest
from codem.stgpr.gpr_smooth import find_variance_type


@pytest.fixture
def df():
    df = pd.DataFrame({
        'age':          [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
        'location_id':  [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 3, 4],
        'national':     [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 0],
        'source_type':  ['VR', 'VR', 'VR', 'VR', 'VR', 'VR', 'VR-S', 'VR-S', 'VR-S', 'VR-S', 'VR-S',
                         'VA', 'VR',
                         'VA', 'VR',
                         'VA']
    })
    return df


@pytest.fixture
def ko():
    return pd.DataFrame({'ko21_train': [True] * 16})


def test_variance_old(df, ko):
    variance_type = find_variance_type(df, ko, decomp_step_id=2, gbd_round_id=6)
    assert variance_type.tolist() == ['intermediate'] * 11 + ['sparse', 'intermediate', 'intermediate', 'sparse', 'intermediate']


def test_variance_new(df, ko):
    variance_type = find_variance_type(df, ko, decomp_step_id=3, gbd_round_id=6)
    assert variance_type.tolist() == ['rich'] * 11 + ['sparse', 'intermediate', 'intermediate', 'sparse', 'intermediate']

"""
Test data module
"""
import numpy as np
import pandas as pd
import pytest
from regmod.data import Data


# pylint:disable=redefined-outer-name


NUM_OBS = 10
COL_OBS = 'obs'
COL_COVS = ['cov1', 'cov2']
COL_WEIGHTS = 'weights'
COL_OFFSET = 'offset'


@pytest.fixture
def df():
    obs = np.random.randn(NUM_OBS)
    covs = {
        cov: np.random.randn(NUM_OBS)
        for cov in COL_COVS
    }
    weights = np.ones(NUM_OBS)
    offset = np.zeros(NUM_OBS)
    df = pd.DataFrame({
        COL_OBS: obs,
        COL_WEIGHTS: weights,
        COL_OFFSET: offset
    })
    for cov, val in covs.items():
        df[cov] = val
    return df


@pytest.fixture
def df_simple():
    obs = np.random.randn(NUM_OBS)
    return pd.DataFrame({COL_OBS: obs})


@pytest.fixture
def data(df):
    return Data(COL_OBS, COL_COVS, COL_WEIGHTS, COL_WEIGHTS, df)


def test_init(df):
    data = Data(COL_OBS, COL_COVS, COL_WEIGHTS, COL_WEIGHTS, df)
    assert data.num_obs == NUM_OBS


def test_post_init_empty():
    data = Data(COL_OBS)
    assert data.is_empty()


def test_post_init_fill_df(df_simple):
    data = Data(COL_OBS, df=df_simple)
    assert data.num_obs == NUM_OBS
    assert all(data.weights == 1.0)
    assert all(data.offset == 0.0)
    assert all(data.get_cols('intercept') == 1.0)


def test_detach_df(data):
    data.detach_df()
    assert data.is_empty()


def test_attach_df(df):
    data = Data(COL_OBS, COL_COVS, COL_WEIGHTS, COL_OFFSET)
    assert data.is_empty()
    data.attach_df(df)
    assert data.num_obs == NUM_OBS


def test_copy(data):
    data_copy = data.copy()
    assert set(data_copy.cols) == set(data.cols)
    data_copy = data.copy(with_df=True)
    assert set(data_copy.cols) == set(data.cols)
    assert data_copy.num_obs == NUM_OBS


def test_covs(data):
    assert len(data.covs) == len(COL_COVS) + 1


def test_no_obs(df):
    data = Data(col_covs=COL_COVS, df=df)
    with pytest.raises(ValueError):
        data.obs


def test_mult_obs(df):
    data = Data(col_obs=[COL_OBS, COL_COVS[0]], df=df)
    assert len(data.col_obs) == 2
    obs = data.obs
    assert obs.shape == (data.num_obs, 2)


def test_no_match_col_obs(df):
    df = df.drop(COL_OBS, axis=1)
    data = Data(col_obs=COL_OBS, df=df)
    assert all(np.isnan(data.obs))


def test_default_trim_weights(data):
    assert all(data.trim_weights == 1.0)


def test_trim_weights_setter_value_error(data):
    with pytest.raises(ValueError):
        data.trim_weights = 2.0


@pytest.mark.parametrize("weights", [0.0, np.zeros(NUM_OBS)])
def test_trim_weights_setter(data, weights):
    data.trim_weights = weights
    assert all(data.trim_weights == 0)

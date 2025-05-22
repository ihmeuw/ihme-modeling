"""
Test variable module
"""
import numpy as np
import pandas as pd
import pytest
from regmod.data import Data
from regmod.variable import Variable, SplineVariable
from regmod.prior import GaussianPrior, UniformPrior, SplineGaussianPrior, SplineUniformPrior
from regmod.utils import SplineSpecs


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
def data(df):
    return Data(COL_OBS, COL_COVS, COL_WEIGHTS, COL_WEIGHTS, df)


@pytest.fixture
def variable():
    return Variable(name=COL_COVS[0])


@pytest.fixture
def spline_variable():
    return SplineVariable(
        name=COL_COVS[0],
        spline_specs=SplineSpecs(
            knots=np.linspace(0.0, 1.0, 5),
            degree=3
        )
    )


@pytest.fixture
def gprior():
    return GaussianPrior(mean=0.0, sd=1.0)


@pytest.fixture
def uprior():
    return UniformPrior(lb=0.0, ub=1.0)


def test_add_priors(variable, gprior):
    variable.add_priors(gprior)
    assert len(variable.priors) == 1
    assert variable.gprior is not None


@pytest.mark.parametrize("indices", [0, [0], [True, False]])
def test_rm_priors(variable, gprior, uprior, indices):
    variable.add_priors([gprior, uprior])
    assert len(variable.priors) == 2
    assert variable.gprior is not None
    assert variable.uprior is not None
    variable.rm_priors(indices)
    assert len(variable.priors) == 1
    assert variable.gprior is None
    assert variable.uprior is not None


def test_get_mat(variable, data):
    mat = variable.get_mat(data)
    assert np.allclose(mat, data.get_covs(variable.name))


def test_get_gvec(variable, gprior):
    gvec = variable.get_gvec()
    assert all(gvec[0] == 0.0)
    assert all(np.isinf(gvec[1]))
    variable.add_priors(gprior)
    gvec = variable.get_gvec()
    assert np.allclose(gvec[0], gprior.mean)
    assert np.allclose(gvec[1], gprior.sd)


def test_get_uvec(variable, uprior):
    uvec = variable.get_uvec()
    assert all(np.isneginf(uvec[0]))
    assert all(np.isposinf(uvec[1]))
    variable.add_priors(uprior)
    uvec = variable.get_uvec()
    assert np.allclose(uvec[0], uprior.lb)
    assert np.allclose(uvec[1], uprior.ub)


def test_copy(variable, gprior, uprior):
    variable.add_priors([gprior, uprior])
    variable_copy = variable.copy()
    assert variable_copy == variable
    assert variable_copy is not variable


def test_linear_variable_check_data(spline_variable, data):
    spline_variable.check_data(data)
    assert spline_variable.spline is not None


def test_linear_variable_get_mat(spline_variable, data):
    mat = spline_variable.get_mat(data)
    assert mat.shape == (data.num_obs, spline_variable.size)


def test_linear_variable_get_vec(spline_variable):
    uprior = UniformPrior(lb=0.0, ub=1.0, size=spline_variable.size)
    gprior = GaussianPrior(mean=0.0, sd=1.0, size=spline_variable.size)
    spline_variable.add_priors([uprior, gprior])

    uvec = spline_variable.get_uvec()
    gvec = spline_variable.get_gvec()

    assert uvec.shape == (2, spline_variable.size)
    assert gvec.shape == (2, spline_variable.size)


def test_linear_variable_get_linear_vec(spline_variable):
    spline_uprior = SplineUniformPrior(lb=0.0, ub=np.inf, order=1)
    spline_gprior = SplineGaussianPrior(mean=0.0, sd=1.0, order=1)
    spline_variable.add_priors([spline_uprior, spline_gprior])

    uvec = spline_variable.get_linear_uvec()
    gvec = spline_variable.get_linear_gvec()

    assert uvec.shape == (2, spline_uprior.size)
    assert gvec.shape == (2, spline_gprior.size)


def test_linear_variable_get_linear_mat(spline_variable, data):
    spline_uprior = SplineUniformPrior(lb=0.0, ub=np.inf, order=1)
    spline_gprior = SplineGaussianPrior(mean=0.0, sd=1.0, order=1)
    spline_variable.add_priors([spline_uprior, spline_gprior])
    spline_variable.check_data(data)

    umat = spline_variable.get_linear_umat()
    gmat = spline_variable.get_linear_gmat()

    assert umat.shape == (spline_uprior.size, spline_variable.size)
    assert gmat.shape == (spline_gprior.size, spline_variable.size)

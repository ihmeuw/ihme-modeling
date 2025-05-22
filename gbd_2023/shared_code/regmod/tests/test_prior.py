"""
Test prior module
"""
import numpy as np
import pytest
from xspline import XSpline
from regmod.prior import *


@pytest.fixture
def spline():
    return XSpline(knots=np.linspace(0.0, 1.0, 5), degree=3)


@pytest.mark.parametrize(('mean', 'sd', 'size'),
                         [(np.zeros(5), 1.0, None),
                          (0.0, np.ones(5), None),
                          (0.0, 1.0, 5)])
def test_gaussian(mean, sd, size):
    gaussian = GaussianPrior(mean=mean, sd=sd, size=size)
    assert gaussian.size == 5


@pytest.mark.parametrize(('lb', 'ub', 'size'),
                         [(np.zeros(5), 1.0, None),
                          (0.0, np.ones(5), None),
                          (0.0, 1.0, 5)])
def test_uniform(lb, ub, size):
    uniform = UniformPrior(lb=lb, ub=ub, size=size)
    assert uniform.size == 5


@pytest.mark.parametrize("order", [0, 1, 2])
@pytest.mark.parametrize("size", [10])
@pytest.mark.parametrize("domain_type", ["rel", "abs"])
def test_spline_gaussian(order, domain_type, size, spline):
    prior = SplineGaussianPrior(
        mean=0.0,
        sd=1.0,
        order=order,
        size=size,
        domain_type=domain_type
    )

    prior.attach_spline(spline)
    assert prior.mat.shape == (prior.size, spline.num_spline_bases)


@pytest.mark.parametrize("order", [0, 1, 2])
@pytest.mark.parametrize("size", [10])
@pytest.mark.parametrize("domain_type", ["rel", "abs"])
def test_spline_uniform(order, domain_type, size, spline):
    prior = SplineUniformPrior(
        lb=0.0,
        ub=1.0,
        order=order,
        size=size,
        domain_type=domain_type
    )

    prior.attach_spline(spline)
    assert prior.mat.shape == (prior.size, spline.num_spline_bases)


@pytest.mark.parametrize("mat", [None, np.ones((2, 3))])
def test_linear_prior(mat):
    if mat is not None:
        prior = LinearPrior(mat=mat)
        assert prior.mat.shape[0] == prior.size
    else:
        prior = LinearPrior()
        assert prior.is_empty()


@pytest.mark.parametrize("mat", [np.ones((2, 3)), np.zeros((3, 4))])
def test_linear_gaussian_prior(mat):
    prior = LinearGaussianPrior(mat=mat, mean=0.0, sd=1.0)
    assert prior.mat.shape[0] == prior.size
    assert prior.mean.size == prior.size
    assert prior.sd.size == prior.size


@pytest.mark.parametrize("mat", [np.ones((2, 3)), np.zeros((3, 4))])
def test_linear_uniform_prior(mat):
    prior = LinearUniformPrior(mat=mat, lb=0.0, ub=1.0)
    assert prior.mat.shape[0] == prior.size
    assert prior.lb.size == prior.size
    assert prior.ub.size == prior.size

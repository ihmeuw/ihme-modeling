# -*- coding: utf-8 -*-
"""
    Test covariate model
"""
import numpy as np
import pandas as pd
import pytest
from mrtool import MRData, CovModel, LinearCovModel, LogCovModel
from xspline import XSpline

DUMMY_GPRIOR = np.array([[0.0], [np.inf]])
DUMMY_LPRIOR = np.array([[0.0], [np.inf]])
DUMMY_UPRIOR = np.array([[-np.inf], [np.inf]])

@pytest.fixture
def mrdata(seed=123):
    np.random.seed(seed)
    data = pd.DataFrame({
        'obs': np.random.randn(10),
        'obs_se': np.full(10, 0.1),
        'cov0': np.ones(10),
        'cov1': np.random.randn(10),
        'study_id': np.random.choice(range(3), 10)
    })
    mrdata = MRData()
    mrdata.load_df(data,
                   col_obs='obs',
                   col_obs_se='obs_se',
                   col_covs=['cov0', 'cov1'],
                   col_study_id='study_id')
    return mrdata


def test_covmodel_default():
    covmodel = CovModel('cov0')
    # direct priors
    assert all(covmodel.prior_beta_gaussian == DUMMY_GPRIOR)
    assert all(covmodel.prior_beta_laplace == DUMMY_LPRIOR)
    assert all(covmodel.prior_beta_uniform == DUMMY_UPRIOR)
    assert covmodel.prior_gamma_gaussian.size == 0
    assert covmodel.prior_gamma_laplace.size == 0
    assert covmodel.prior_gamma_uniform.size == 0

    # spline priors
    assert covmodel.prior_spline_monotonicity is None
    assert covmodel.prior_spline_convexity is None
    assert covmodel.prior_spline_num_constraint_points == 20
    assert covmodel.prior_spline_maxder_gaussian is None
    assert covmodel.prior_spline_maxder_uniform is None

    # spline settings
    assert np.all(covmodel.spline_knots_template == np.linspace(0.0, 1.0, 4))
    assert covmodel.spline_degree == 3
    assert not covmodel.spline_l_linear
    assert not covmodel.spline_r_linear

    # check dimensions
    assert covmodel.num_x_vars == 1
    assert covmodel.num_z_vars == 0
    assert covmodel.num_constraints == 0
    assert covmodel.num_regularizations == 0


@pytest.mark.parametrize('use_re', [True, False])
def test_covmodel_re(use_re):
    covmodel = CovModel('cov0', use_re=use_re)
    assert covmodel.num_x_vars == 1
    if use_re:
        assert covmodel.num_z_vars == 1
        assert all(covmodel.prior_gamma_gaussian == DUMMY_GPRIOR)
        assert all(covmodel.prior_gamma_laplace == DUMMY_LPRIOR)
        assert all(covmodel.prior_gamma_uniform == np.array([[0.0], [np.inf]]))
    else:
        assert covmodel.num_z_vars == 0
        assert covmodel.prior_gamma_gaussian.size == 0
        assert covmodel.prior_gamma_laplace.size == 0
        assert covmodel.prior_gamma_uniform.size == 0


@pytest.mark.parametrize('spline_knots', [np.array([0.0, 0.25, 0.75, 1.0]),
                                          np.array([0.0, 0.25, 0.75]),
                                          np.array([0.25, 0.75, 1.0]),
                                          np.array([0.25, 0.75])])
@pytest.mark.parametrize('spline_degree', [2, 3])
@pytest.mark.parametrize('spline_l_linear', [True, False])
@pytest.mark.parametrize('spline_r_linear', [True, False])
@pytest.mark.parametrize('use_re', [True])
@pytest.mark.parametrize('use_re_mid_point', [True, False])
def test_covmodel_spline(spline_knots, spline_degree, spline_l_linear, spline_r_linear,
                         use_re, use_re_mid_point, mrdata):
    covmodel = CovModel('cov1',
                        use_re=use_re,
                        use_re_mid_point=use_re_mid_point,
                        use_spline=True,
                        spline_knots=spline_knots,
                        spline_degree=spline_degree,
                        spline_l_linear=spline_l_linear,
                        spline_r_linear=spline_r_linear)

    assert all(covmodel.spline_knots_template == np.array([0.0, 0.25, 0.75, 1.0]))
    assert covmodel.spline_degree == spline_degree
    assert covmodel.spline_l_linear == spline_l_linear
    assert covmodel.spline_r_linear == spline_r_linear
    covmodel.attach_data(mrdata)

    assert covmodel.num_x_vars == covmodel.spline.num_spline_bases - 1
    if use_re and use_re_mid_point:
        assert covmodel.num_z_vars == 1
    elif use_re and not use_re_mid_point:
        assert covmodel.num_z_vars == covmodel.num_x_vars
    elif not use_re:
        assert covmodel.num_z_vars == 0


@pytest.mark.parametrize('prior_beta_gaussian', [np.array([0.0, 1.0])])
@pytest.mark.parametrize('use_spline', [True, False])
def test_covmodel_beta_gprior(prior_beta_gaussian, use_spline, mrdata):
    covmodel = CovModel('cov1', use_spline=use_spline,
                        prior_beta_gaussian=prior_beta_gaussian)
    covmodel.attach_data(mrdata)
    assert covmodel.prior_beta_gaussian.ndim == 2
    assert np.all(covmodel.prior_beta_gaussian[0] == 0.0)
    assert np.all(covmodel.prior_beta_gaussian[1] == 1.0)


@pytest.mark.parametrize('prior_beta_laplace', [np.array([0.0, 1.0])])
@pytest.mark.parametrize('use_spline', [True, False])
def test_covmodel_beta_lprior(prior_beta_laplace, use_spline, mrdata):
    covmodel = CovModel('cov1', use_spline=use_spline,
                        prior_beta_laplace=prior_beta_laplace)
    covmodel.attach_data(mrdata)
    assert covmodel.prior_beta_laplace.ndim == 2
    assert np.all(covmodel.prior_beta_laplace[0] == 0.0)
    assert np.all(covmodel.prior_beta_laplace[1] == 1.0)


@pytest.mark.parametrize('prior_beta_uniform', [np.array([0.0, 1.0])])
@pytest.mark.parametrize('use_spline', [True, False])
def test_covmodel_beta_uprior(prior_beta_uniform, use_spline, mrdata):
    covmodel = CovModel('cov1', use_spline=use_spline,
                        prior_beta_uniform=prior_beta_uniform)
    covmodel.attach_data(mrdata)
    assert covmodel.prior_beta_uniform.ndim == 2
    assert np.all(covmodel.prior_beta_uniform[0] == 0.0)
    assert np.all(covmodel.prior_beta_uniform[1] == 1.0)


@pytest.mark.parametrize('prior_gamma_gaussian', [np.array([0.0, 1.0])])
@pytest.mark.parametrize('use_re', [True])
@pytest.mark.parametrize('use_spline', [True, False])
@pytest.mark.parametrize('use_re_mid_point', [True, False])
def test_covmodel_gamma_gprior(prior_gamma_gaussian, use_spline, use_re, use_re_mid_point, mrdata):
    covmodel = CovModel('cov1', use_spline=True,
                        prior_gamma_gaussian=prior_gamma_gaussian,
                        use_re=use_re, use_re_mid_point=use_re_mid_point)
    covmodel.attach_data(mrdata)
    assert covmodel.prior_gamma_gaussian.ndim == 2
    assert np.all(covmodel.prior_gamma_gaussian[0] == 0.0)
    assert np.all(covmodel.prior_gamma_gaussian[1] == 1.0)


@pytest.mark.parametrize('prior_gamma_laplace', [np.array([0.0, 1.0])])
@pytest.mark.parametrize('use_re', [True])
@pytest.mark.parametrize('use_spline', [True, False])
@pytest.mark.parametrize('use_re_mid_point', [True, False])
def test_covmodel_gamma_lprior(prior_gamma_laplace, use_spline, use_re, use_re_mid_point, mrdata):
    covmodel = CovModel('cov1', use_spline=use_spline,
                        prior_gamma_laplace=prior_gamma_laplace,
                        use_re=use_re, use_re_mid_point=use_re_mid_point)
    covmodel.attach_data(mrdata)
    assert covmodel.prior_gamma_laplace.ndim == 2
    assert np.all(covmodel.prior_gamma_laplace[0] == 0.0)
    assert np.all(covmodel.prior_gamma_laplace[1] == 1.0)


@pytest.mark.parametrize('prior_gamma_uniform', [np.array([0.0, 1.0])])
@pytest.mark.parametrize('use_re', [True])
@pytest.mark.parametrize('use_spline', [True, False])
@pytest.mark.parametrize('use_re_mid_point', [True, False])
def test_covmodel_gamma_uprior(prior_gamma_uniform, use_spline, use_re, use_re_mid_point, mrdata):
    covmodel = CovModel('cov1', use_spline=use_spline,
                        prior_gamma_uniform=prior_gamma_uniform,
                        use_re=use_re, use_re_mid_point=use_re_mid_point)
    covmodel.attach_data(mrdata)
    assert covmodel.prior_gamma_uniform.ndim == 2
    assert np.all(covmodel.prior_gamma_uniform[0] == 0.0)
    assert np.all(covmodel.prior_gamma_uniform[1] == 1.0)


@pytest.mark.parametrize('use_spline', [True])
@pytest.mark.parametrize('prior_spline_monotonicity', ['increasing', 'decreasing', None])
@pytest.mark.parametrize('prior_spline_convexity', ['convex', 'concave', None])
@pytest.mark.parametrize('prior_spline_num_constraint_points', [20, 40])
@pytest.mark.parametrize('prior_spline_maxder_gaussian', [np.array([0.0, 1.0]), None])
@pytest.mark.parametrize('prior_spline_maxder_uniform', [np.array([0.0, 1.0]), None])
def test_covmodel_spline_prior(use_spline,
                               prior_spline_monotonicity,
                               prior_spline_convexity,
                               prior_spline_num_constraint_points,
                               prior_spline_maxder_gaussian,
                               prior_spline_maxder_uniform,
                               mrdata):
    covmodel = CovModel('cov1',
                        use_spline=use_spline,
                        prior_spline_monotonicity=prior_spline_monotonicity,
                        prior_spline_convexity=prior_spline_convexity,
                        prior_spline_num_constraint_points=prior_spline_num_constraint_points,
                        prior_spline_maxder_gaussian=prior_spline_maxder_gaussian,
                        prior_spline_maxder_uniform=prior_spline_maxder_uniform)
    covmodel.attach_data(mrdata)

    num_constraints = ((prior_spline_monotonicity is not None) +
                       (prior_spline_convexity is not None))*prior_spline_num_constraint_points
    num_constraints += (prior_spline_maxder_uniform is not None)*covmodel.spline.num_intervals
    num_regularizers = (prior_spline_maxder_gaussian is not None)*covmodel.spline.num_intervals

    assert covmodel.num_constraints == num_constraints
    assert covmodel.num_regularizations == num_regularizers


def test_use_spline_intercept(mrdata):
    linear_cov_model = LinearCovModel('cov1',
                                      use_spline=True,
                                      use_spline_intercept=True,
                                      use_re=True,
                                      use_re_mid_point=True,
                                      prior_spline_monotonicity='increasing')
    linear_cov_model.attach_data(mrdata)

    assert linear_cov_model.num_x_vars == linear_cov_model.spline.num_spline_bases
    assert linear_cov_model.num_z_vars == 2
    alt_mat, ref_mat = linear_cov_model.create_design_mat(mrdata)
    c_mat, c_val = linear_cov_model.create_constraint_mat()
    assert alt_mat.shape == (mrdata.num_obs, linear_cov_model.num_x_vars)
    assert c_mat.shape == (linear_cov_model.prior_spline_num_constraint_points, linear_cov_model.num_x_vars)

    # with pytest.raises(ValueError):
    #     LogCovModel('cov1', use_spline=True, use_spline_intercept=True, use_re=True,
    #                 use_re_mid_point=True)

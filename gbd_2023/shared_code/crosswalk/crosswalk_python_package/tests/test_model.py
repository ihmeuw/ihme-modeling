# -*- coding: utf-8 -*-
"""
    test_model
    ~~~~~~~~~~

    Test `model` module for the `crosswalk` package.
"""
import numpy as np
import pandas as pd
import pytest
import crosswalk
import crosswalk.model as model
from xspline import XSpline


@pytest.fixture
def cwdata():
    np.random.seed(123)
    num_obs = 10
    num_covs = 3
    df = pd.DataFrame({
        'obs': np.random.randn(num_obs),
        'obs_se': 0.1 + np.random.rand(num_obs)*0.1,
        'alt_dorms': np.random.choice(4, num_obs),
        'study_id': np.array([1, 1, 2, 2, 2, 2, 3, 3, 3, 3])
    })
    df['ref_dorms'] = np.array([
        3 if alt_dorm != 3 else 0
        for alt_dorm in df['alt_dorms']
    ])
    for i in range(num_covs):
        df['cov%i' % i] = np.random.randn(num_obs)

    return crosswalk.data.CWData(df,
                                 'obs',
                                 'obs_se',
                                 'alt_dorms',
                                 'ref_dorms',
                                 covs=['cov%i' % i for i in range(num_covs)],
                                 study_id='study_id')


@pytest.fixture
def cov_models():
    return [model.CovModel('intercept'),
            model.CovModel('cov0')]


@pytest.mark.parametrize('obs_type', ['diff_log', 'diff_logit'])
def test_input(cwdata, obs_type, cov_models):
    cwmodel = model.CWModel(cwdata, obs_type,
                            cov_models=cov_models)
    cwmodel.check()


def test_design_mat(cwdata, cov_models):
    obs_type = 'diff_log'
    cwmodel = model.CWModel(cwdata, obs_type,
                            cov_models=cov_models)
    design_mat = cwmodel.design_mat
    assert np.allclose(cwmodel.relation_mat.sum(axis=1), 0.0)
    assert np.allclose(design_mat.sum(axis=1), 0.0)


@pytest.mark.parametrize('order_prior', [[['1', '2'], ['2', '3']]])
def test_order_prior(cwdata, cov_models, order_prior):
    obs_type = 'diff_log'
    cwmodel = model.CWModel(cwdata, obs_type,
                            cov_models=cov_models,
                            order_prior=order_prior)

    constraints_mat = cwmodel.constraint_mat
    assert isinstance(constraints_mat, np.ndarray)
    assert constraints_mat.shape == (4, cwmodel.num_vars)
    assert (constraints_mat.sum(axis=1) == 0.0).all()


@pytest.mark.parametrize('alt_dorm', ['2', '3'])
@pytest.mark.parametrize('ref_dorm', ['3'])
def test_adjust_orig_vals(cwdata, cov_models, alt_dorm, ref_dorm):
    obs_type = 'diff_log'
    gold_dorm = ref_dorm
    cwmodel = model.CWModel(cwdata, obs_type,
                            cov_models=cov_models,
                            gold_dorm=gold_dorm)
    cwmodel.fit()
    new_df = pd.DataFrame({
        'dorms': np.array([alt_dorm]*cwdata.num_obs),
        'vals': np.ones(cwdata.num_obs),
        'se': np.zeros(cwdata.num_obs)
    })
    for cov in cwdata.covs.columns:
        new_df[cov] = np.ones(cwdata.num_obs)

    pred_df = cwmodel.adjust_orig_vals(
        new_df,
        orig_dorms='dorms',
        orig_vals_mean='vals',
        orig_vals_se='se'
    )
    assert np.allclose(pred_df['ref_vals_mean'], np.exp(np.sum(
        cwmodel.beta[cwmodel.var_idx[ref_dorm]] -
        cwmodel.beta[cwmodel.var_idx[alt_dorm]]
    )))
    assert np.allclose(pred_df['data_id'], np.arange(pred_df.shape[0]))


@pytest.mark.parametrize('cov_name', ['cov0', 'cov1'])
@pytest.mark.parametrize('spline', [None,
                                    XSpline(np.linspace(-2.0, 2.0, 3), 3)])
@pytest.mark.parametrize('soln_name', [None, 'cov'])
def test_cov_model(cwdata, cov_name, spline, soln_name):
    cov_model = model.CovModel(cov_name, spline=spline, soln_name=soln_name)

    if spline is None:
        assert cov_model.num_vars == 1
        assert (cov_model.create_design_mat(cwdata) ==
                cwdata.covs[cov_name][:, None]).all()
    else:
        assert cov_model.num_vars == spline.num_spline_bases - 1
        assert (cov_model.create_design_mat(cwdata) ==
                spline.design_mat(cwdata.covs[cov_name])[:, 1:]).all()

    if soln_name is None:
        assert cov_model.soln_name == cov_name
    else:
        assert cov_model.soln_name == soln_name


@pytest.mark.parametrize('cov_name', ['cov0', 'cov1'])
@pytest.mark.parametrize('spline', [None,
                                    XSpline(np.linspace(-2.0, 2.0, 3), 3)])
@pytest.mark.parametrize('soln_name', [None, 'cov'])
@pytest.mark.parametrize('spline_monotonicity', ['increasing',
                                                 'decreasing',
                                                 None])
@pytest.mark.parametrize('spline_convexity', ['convex', 'concave', None])
def test_spline_prior(cov_name, spline, soln_name,
                      spline_monotonicity, spline_convexity):
    cov_model = model.CovModel(cov_name,
                               spline=spline,
                               spline_monotonicity=spline_monotonicity,
                               spline_convexity=spline_convexity,
                               soln_name=soln_name)

    constraints_mat = cov_model.create_constraint_mat()

    if not cov_model.use_constraints:
        assert constraints_mat.size == 0
    else:
        num_constraints = np.sum([spline_monotonicity is not None,
                                  spline_convexity is not None])*20
        assert constraints_mat.shape == (num_constraints, cov_model.num_vars)

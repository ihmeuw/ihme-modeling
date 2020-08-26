import sys
import os

import numpy as np
import pandas as pd

from xspline import xspline
from limetr import LimeTr

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from mrbrt.__init__ import MR_BRT, MR_BeRT
from mrbrt.utils import sampleKnots, ratioInit

sys.path.append(os.path.dirname(__file__))
from globals import *


def get_cov_lists(df=None, model_cols=None, measure=None, add_age=False, linear=False,
                  pred=False, pred_ref=True, exp_pred_array=None, age_pred_array=None):
    x_cov_list = []
    z_cov_list = []

    if pred:
        mat_len = len(exp_pred_array)
    else:
        mat_len = len(df)

    # exposure
    if pred:
        x_cov_list += [{
            'cov_type': 'spline',
            'mat': exp_pred_array,
            'spline_id':0,
            'name': 'exposure_spline',
            'x_cov_id':0
        }]
        if add_age:
            x_cov_list += [{
                'cov_type': 'linear',
                'mat': age_pred_array,
                'name': 'age',
                'x_cov_id':1
            }]
    elif linear:
        # x-intercept
        x_cov_list += [{
            'cov_type': 'linear',
            'mat': np.ones(mat_len),
            'name': 'intercept',
            'x_cov_id': 0
        }]
        # x_cov_list += [{
        #     'cov_type': 'linear',
        #     'mat': (df['conc'] - df['conc_den']).values,
        #     'name': 'exposure_linear',
        #     'x_cov_id':0
        # }]
    elif measure == 'diff':
        x_cov_list += [{
            'cov_type': 'diff_spline',
            'mat': df[['conc', 'conc_den']].values.T,
            'spline_id':0,
            'name': 'exposure_diff_spline',
            'x_cov_id':0
        }]
    elif measure == 'log_ratio':
        x_cov_list += [{
            'cov_type': 'log_ratio_spline',
            'mat': df[['conc', 'conc_den']].values.T,
            'spline_id':0,
            'name': 'exposure_log_ratio_spline',
            'x_cov_id':0
        }]

    # age
    if add_age and not pred:
        x_cov_list += [{
            'cov_type': 'linear',
            'mat': df['median_age_fup'].values,
            'name': 'age',
            'x_cov_id':1
        }]

    # study-level covariates
    i = 1 + add_age
    for cov in model_cols:
        if pred and (pred_ref or cov == 'other_ap'):
            x_cov_list += [{
                'cov_type': 'linear',
                'mat': np.zeros(mat_len),
                'name': cov,
                'x_cov_id': i
            }]
        elif pred and not pred_ref:
            x_cov_list += [{
                'cov_type': 'linear',
                'mat': np.ones(mat_len),
                'name': cov,
                'x_cov_id': i
            }]
        else:
            x_cov_list += [{
                'cov_type': 'linear',
                'mat': df[cov].values,
                'name': cov,
                'x_cov_id': i
            }]
        i += 1

    # z-intercept (gamma0)
    z_cov_list += [{
        'cov_type': 'linear',
        'mat': np.ones(mat_len),
        'name': 'intercept',
        'z_cov_id': 0
    }]

    return x_cov_list, z_cov_list


def create_spline_list(spline_mat, degree=3, n_knots=5, l_linear=False, r_linear=False,
                       n_splines=10, width_pct=0.1, l_zero=True):
    dose_max = spline_mat.max()
    if l_zero:
        dose_min = 0
    else:
        dose_min = spline_mat.min()
    if np.percentile(spline_mat, 5) > dose_min:
        start = (np.percentile(spline_mat, 5) - dose_min) / \
                (dose_max - dose_min)
    else:
        start = 0
    end = (np.percentile(spline_mat, 95) - dose_min) / \
          (dose_max - dose_min)
    print(f'Knot range: {dose_min + start * (dose_max - dose_min)} to {dose_min + end * (dose_max - dose_min)}')
    b = np.array([[start] * (n_knots - 2), [end] * (n_knots - 2)]).T
    min_dist = (end - start) * width_pct
    min_dist_val = min_dist * (dose_max - dose_min)
    print(f'Minimum interval width: {min_dist_val}')
    d = np.array([[min_dist] * (n_knots - 1), [1.] * (n_knots - 1)]).T
    knots_samples = sampleKnots(dose_min, dose_max,
                                n_knots-1,
                                b=b, d=d,
                                N=n_splines)
    spline_list = [xspline(knots, degree, l_linear=l_linear, r_linear=r_linear) for knots in knots_samples]

    return spline_list


def get_priors(outcome, measure, n_ns_knots, exp_spline=None, age_decreasing=None, linear=False, n_x_covs=None, mono=False, cvcv=False):
    prior_list = []

    if linear:
        # prior_list += [{
        #     'prior_type': 'x_cov_lprior',
        #     'x_cov_id': 0,
        #     'prior': np.array([[0], [np.inf]])
        # }]
        for cov_id in range(1, n_x_covs):
            prior_list += [{
                'prior_type': 'x_cov_lprior',
                'x_cov_id': cov_id,
                'prior': np.array([[0], np.sqrt([1e-4])])
            }]
    else:
        if measure == 'diff':
            # set spline intercept at 0
            prior_list += [{
                'prior_type': 'x_cov_uprior',
                'x_cov_id': 0,
                'prior': np.array([[0] + [-np.inf] * (exp_spline.num_spline_bases - 1),
                                   [0] + [np.inf] * (exp_spline.num_spline_bases - 1)])
            }]

            # monotonically decreasing
            if mono:
                prior_list += [{
                    'prior_type': 'spline_shape_monotonicity',
                    'x_cov_id': 0,
                    'interval': [exp_spline.knots[0], exp_spline.knots[-1]],
                    'indicator': 'decreasing',
                    'num_points': 30
                }]

            # convex
            if cvcv:
                prior_list += [{
                    'prior_type': 'spline_shape_convexity',
                    'x_cov_id': 0,
                    'interval': [exp_spline.knots[0], exp_spline.knots[-1]],
                    'indicator': 'convex',
                    'num_points': 30
                }]
        elif measure == 'log_ratio':
            # # constrain the slope
            # prior_list += [{
            #     'prior_type': 'spline_shape_derivative_uprior',
            #     'x_cov_id': 0,
            #     'interval': [exp_spline.knots[0], exp_spline.knots[n_ns_knots-1]],
            #     'indicator': [-0.05, 0.05],
            #     'num_points': 50
            # }]
            # if len(exp_spline.knots) > n_ns_knots:
            #     prior_list += [{
            #         'prior_type': 'spline_shape_derivative_uprior',
            #         'x_cov_id': 0,
            #         'interval': [exp_spline.knots[n_ns_knots-1], exp_spline.knots[-1]],
            #         'indicator': [-0.05, 0.05],
            #         'num_points': 50
            #     }]

            # set intercept at 1
            prior_list += [{
                'prior_type': 'x_cov_uprior',
                'x_cov_id': 0,
                'prior': np.array([[1] + [-np.inf] * (exp_spline.num_spline_bases - 1),
                                   [1] + [np.inf] * (exp_spline.num_spline_bases - 1)])
            }]

            # RR needs to be positive
            prior_list += [{
                'prior_type': 'spline_shape_function_uprior',
                'x_cov_id': 0,
                'interval': [exp_spline.knots[0], exp_spline.knots[-1]],
                'indicator': [0.01, 30.],
                'num_points': 100
            }]

            # monotonically increasing
            if mono:
                prior_list += [{
                    'prior_type': 'spline_shape_monotonicity',
                    'x_cov_id': 0,
                    'interval': [exp_spline.knots[0], exp_spline.knots[-1]],
                    'indicator': 'increasing',
                    'num_points': 100
                }]

            # concave
            if cvcv:
                prior_list += [{
                    'prior_type': 'spline_shape_convexity',
                    'x_cov_id': 0,
                    'interval': [exp_spline.knots[0], exp_spline.knots[-1]],
                    'indicator': 'concave',
                    'num_points': 100
                }]

        # spline highest-order derivative priors
        if outcome == 'resp_copd':
            prior_list += [{
                'prior_type': 'spline_shape_max_derivative_gprior',
                'x_cov_id': 0,
                'prior': np.array([
                    [0] * (exp_spline.num_invls),
                    np.sqrt([0.01] * (exp_spline.num_invls - 1) + [1e-4])
                ])
            }]
        else:
            prior_list += [{
                'prior_type': 'spline_shape_max_derivative_gprior',
                'x_cov_id': 0,
                'prior': np.array([
                    [0] * (exp_spline.num_invls),
                    np.sqrt([0.01] * (exp_spline.num_invls - 1) + [1e-6])
                ])
            }]

    if age_decreasing:
        # set so that age must be negative
        prior_list += [{
            'prior_type': 'x_cov_uprior',
            'x_cov_id': 1,
            'prior': np.array([[-np.inf], [0]])
        }]

    # z-cov positive
    prior_list += [{
        'prior_type': 'z_cov_uprior',
        'z_cov_id': 0,
        'prior': np.array([[1e-7], [1]])
    }]

    return prior_list


def get_parameter_samples(mr, n_samples=1000):
    # sample for each submodel
    sample_size_list = mr.compute_sample_sizes(n_samples)
    param_samples = [
        LimeTr.sampleSoln(sub_mr.lt, sample_size=ss) for sub_mr, ss in zip(mr.mr_list, sample_size_list)
    ]
    given_samples = {
        'given_beta_samples_list': [i[0] for i in param_samples],
        'given_gamma_samples_list': [i[1] for i in param_samples]
    }

    return given_samples

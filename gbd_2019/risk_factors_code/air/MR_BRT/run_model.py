import sys
import os

import itertools
import dill as pickle

import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from mrbrt.__init__ import MR_BeRT
from mrbrt.utils import ratioInit

sys.path.append(os.path.dirname(__file__))
from globals import *
from data_prep import load_data
from model import get_cov_lists, create_spline_list, get_priors, get_parameter_samples
from plotting import ratio_plot, diff_plot


def create_cartesian(x, z):
    n = np.ones(z.shape[0])
    xz = np.array([i for i in itertools.product(z, x)])
    z = np.vstack(xz[:,0])
    x = np.vstack(xz[:,1])

    return x, z


def find_nearest(point, ier_exp):
    idx = np.abs(point - ier_exp).argmin()
    return ier_exp[idx]


def add_ier_prior(prior_list, outcome, spline_list, n_ns_knots):
    if outcome[-2:].isdigit():
        ier_outcome = outcome.upper()
    elif outcome in ['bw', 'ga']:
        ier_outcome = outcome.upper() + '_0'
    else:
        ier_outcome = outcome.upper() + '_99'
    ier_df = pd.read_csv(os.path.join(IER_DIR, f'preds_{ier_outcome}.csv'))

    # subset to points we want based on knots
    avg_knots = np.mean([spline.knots for spline in spline_list], axis=0)
    low_points = np.linspace(avg_knots[0], avg_knots[1], 4)
    mid_points = np.linspace(avg_knots[1], avg_knots[n_ns_knots-1], 4)
    if len(avg_knots) > n_ns_knots:
        high_points = np.linspace(avg_knots[n_ns_knots-1], avg_knots[-1], 6)
        points = np.hstack([low_points, mid_points, high_points])
    else:
        points = np.hstack([low_points, mid_points])
    points = points[points <= ier_df.exposure.max()]
    rounded_points = np.unique([find_nearest(point, ier_df.exposure.values) for point in points])
    ier_df = ier_df.loc[ier_df.exposure.isin(rounded_points)].reset_index(drop=True)

    # set SD floor
    ier_df.loc[ier_df.sd <= 1e-5, 'sd'] = 1e-5

    # load our values
    exposures = ier_df['exposure'].values
    priors = ier_df.apply(lambda i: [i['mn'], i['sd']], axis=1).tolist()

    # set prior
    for exposure, prior in zip(exposures, priors):
        prior_list += [{
            'prior_type': 'spline_shape_function_gprior',
            'x_cov_id': 0,
            'interval': [exposure, exposure],
            'indicator': prior,
            'num_points': 2
        }]

    return prior_list


def run_ap_model(outcome, ier_prior=False, measure='log_ratio', include_smoking=False, include_shs=True, mono=False, cvcv=False, oap_gold_standard=False, n_splines=100, n_ns_knots=4, n_s_knots=4, n_bins=200):
    # load data
    if outcome.startswith('cvd'):
        age_adjust=True
    else:
        age_adjust=False
    df, model_cols, obs_mean, obs_std, study_sizes, N = load_data(
        outcome, measure=measure, age_adjust=age_adjust,
        include_smoking=include_smoking, include_shs=include_shs
    )
    # if outcome == 'lri':
    #     model_cols = ['child']
    #     add_age = False
    # elif outcome.startswith('cvd'):
    #     model_cols = ['incidence']
    #     add_age = False
    # else:
    #     model_cols = []
    #     add_age = False
    model_cols = []
    add_age = False

    if oap_gold_standard:
        model_cols = model_cols + ['other_ap']

    # check for NAs
    for model_col in model_cols:
        # df.loc[df[model_col].isnull(), model_col] = 0
        if len(df.loc[df[model_col].isnull()]) > 0:
            problem_nid_list = df.loc[df[model_col].isnull(), 'nid'].tolist()
            problem_nid = ', '.join(problem_nid_list)
            raise ValueError(f'Missing value for {model_col} in NID(s) {problem_nid}')

    if add_age:
        assert df['median_age_fup'].max() > 0, 'Age included model with no age data.'

    # create spline
    ns_spline_mat = df.loc[df.ier_source != 'AS', ['conc_den', 'conc']].values.flatten()
    spline_list = create_spline_list(ns_spline_mat, degree=3, n_knots=n_ns_knots, l_linear=False, r_linear=True,
                                     n_splines=n_splines, width_pct=0.2, l_zero=True)
    if include_smoking:
        s_spline_mat = df.loc[df.ier_source == 'AS', 'conc'].values  # just use tail end for smoking
        s_spline_list = create_spline_list(s_spline_mat, degree=3, n_knots=n_s_knots,
                                           n_splines=n_splines, width_pct=0.2, l_zero=False)
        for i in range(n_splines):
            spline_list[i].knots = np.hstack([spline_list[i].knots, s_spline_list[i].knots])


    # covs and priors
    x_cov_list, z_cov_list = get_cov_lists(df, model_cols, measure=measure, add_age=add_age)
    prior_list = get_priors(outcome=outcome, measure=measure, n_ns_knots=n_ns_knots, exp_spline=spline_list[0], age_decreasing=False,
                            cvcv=cvcv, mono=mono)

    if ier_prior:
        prior_list = add_ier_prior(prior_list, outcome, spline_list, n_ns_knots)

    # run meta-regression
    mr = MR_BeRT(obs_mean=obs_mean,
                 obs_std=obs_std,
                 study_sizes=study_sizes,
                 x_cov_list=x_cov_list, z_cov_list=z_cov_list,
                 spline_list=spline_list,
                 inlier_percentage=0.9)
    mr.addPriors(prior_list)
    if measure == 'log_ratio':
        x0 = ratioInit(mr, 0)
    else:
        x0 = None
    mr.fitModel(x0=x0)
    mr.scoreModel(np.array([0.4, 0.6]))

    given_samples = get_parameter_samples(mr, len(mr.spline_list)*10)

    # if include_smoking:
    #     exp_pred_array = np.linspace(spline_list[0].knots[0], spline_list[0].knots[n_ns_knots-1], int(n_bins / 2) + 1)
    #     s_exp_pred_array = np.linspace(spline_list[0].knots[n_ns_knots-1], spline_list[0].knots[-1], int(n_bins / 2))
    #     exp_pred_array = np.hstack([exp_pred_array[:-1], s_exp_pred_array])
    #     exp_pred_array = np.unique(exp_pred_array)
    # else:
    #     exp_pred_array = np.linspace(spline_list[0].knots[0], spline_list[0].knots[-1], n_bins)
    exp_pred_array = np.hstack([
        np.arange(0, 10, 0.01), np.arange(10, 100, 0.1),
        np.arange(100, 1000), np.arange(1000, 10010, 10)
    ])

    if add_age:
        age_pred_array = np.percentile(df['median_age_fup'], 50)
        age_pred_array = np.repeat(age_pred_array, n_bins)
    else:
        age_pred_array = None
    pred_x_cov_list, pred_z_cov_list = get_cov_lists(
        model_cols=model_cols, measure=measure, add_age=add_age, linear=False,
        pred=True, pred_ref=True, exp_pred_array=exp_pred_array, age_pred_array=age_pred_array
    )
    if len(x_cov_list) > 1:
        pred_x_cov_list_alt, pred_z_cov_list_alt = get_cov_lists(
            model_cols=model_cols, measure=measure, add_age=add_age, linear=False,
            pred=True, pred_ref=False, exp_pred_array=exp_pred_array, age_pred_array=age_pred_array
        )
        for i in range(len(pred_x_cov_list)):
            pred_x_cov_list[i]['mat'] = np.hstack([pred_x_cov_list[i]['mat'], pred_x_cov_list_alt[i]['mat']])
        for i in range(len(pred_z_cov_list)):
            pred_z_cov_list[i]['mat'] = np.hstack([pred_z_cov_list[i]['mat'], pred_z_cov_list_alt[i]['mat']])
    pred_x_cov_list_data_l, pred_z_cov_list_data_l = get_cov_lists(
        model_cols=model_cols, measure=measure, add_age=add_age, linear=False,
        pred=True, pred_ref=True, exp_pred_array=df['conc_den'].values
    )

    if measure == 'log_ratio':
        ref_point = spline_list[0].knots[0]
    elif measure == 'diff':
        ref_point = None
    y_samples = mr.predictData(
        pred_x_cov_list, pred_z_cov_list,
        sample_size=n_splines*10,
        pred_study_sizes=[len(pred_x_cov_list[0]['mat'])],
        include_random_effect=True,
        ref_point=ref_point,
        **given_samples
    )[0]
    y_samples = np.vstack(y_samples)

    y_samples_fe = mr.predictData(
        pred_x_cov_list, pred_z_cov_list,
        sample_size=n_splines*10,
        pred_study_sizes=[len(pred_x_cov_list[0]['mat'])],
        include_random_effect=False,
        ref_point=ref_point,
        **given_samples
    )[0]
    y_samples_fe = np.vstack(y_samples_fe)

    y_samples_fe_data_l = mr.predictData(
        pred_x_cov_list_data_l, pred_z_cov_list_data_l,
        sample_size=n_splines*10,
        pred_study_sizes=mr.study_sizes,
        include_random_effect=False,
        ref_point=ref_point,
        **given_samples
    )[0]
    y_samples_fe_data_l = np.vstack(y_samples_fe_data_l)

    return df, mr, given_samples, pred_x_cov_list, y_samples, y_samples_fe, y_samples_fe_data_l


if __name__ == '__main__':
    outcome = sys.argv[1]
    label = sys.argv[2]
    n_bins = 200

    if not os.path.exists(os.path.join(OUT_DIR, f'{outcome}_{label}')):
        os.mkdir(os.path.join(OUT_DIR, f'{outcome}_{label}'))

    # run model
    if 'ier_prior' in label:
        ier_prior = True
    else:
        ier_prior = False
    if 'w_smoking' in label:
        include_smoking = True
    else:
        include_smoking = False
    if 'w_shs' in label:
        include_shs = True
    else:
        include_shs = False
    if outcome in ['bw', 'ga']:
        measure = 'diff'
    else:
        measure = 'log_ratio'
    if 'oap' in label:
        oap_gold_standard = True
    else:
        oap_gold_standard = False
    if 'cvcv' in label:
        cvcv = True
    else:
        cvcv = False
    if 'mono' in label:
        mono = True
    else:
        mono = False
    df, mr, given_samples, pred_x_cov_list, y_samples, y_samples_fe, y_samples_fe_data_l = run_ap_model(
        outcome, measure=measure, ier_prior=ier_prior, include_smoking=include_smoking, include_shs=include_shs,
        oap_gold_standard=oap_gold_standard,
        n_bins=n_bins, cvcv=cvcv, mono=mono
    )

    # store datasets, model, and samples
    df.to_csv(os.path.join(OUT_DIR, f'{outcome}_{label}', f'{outcome}_data_frame.csv'), index=False)
    with open(os.path.join(OUT_DIR, f'{outcome}_{label}', f'{outcome}_mr_obj.pkl'), 'wb') as fwrite:
        pickle.dump(mr, fwrite, -1)
    with open(os.path.join(OUT_DIR, f'{outcome}_{label}', f'{outcome}_param_samples.pkl'), 'wb') as fwrite:
        pickle.dump(given_samples, fwrite, -1)
    beta_summary_df = []
    for i in range(len(mr.x_cov_list)):
        beta_summary_df += [pd.DataFrame({
            'cov_name':mr.x_cov_list[i]['name'],
            'beta_mean':np.vstack(given_samples['given_beta_samples_list'])[:, mr.id_beta_list[i]].mean(axis=0),
            'beta_std':np.vstack(given_samples['given_beta_samples_list'])[:, mr.id_beta_list[i]].std(axis=0)
        })]
    beta_summary_df = pd.concat(beta_summary_df).reset_index(drop=True)
    beta_summary_df.to_csv(os.path.join(OUT_DIR, f'{outcome}_{label}', f'{outcome}_betas.csv'), index=False)
    pred_df = pd.DataFrame(
        dict(zip(
            [f'draw_{i}' for i in range(y_samples.shape[0])], y_samples
        ))
    )
    for i in range(len(pred_x_cov_list)):
        pred_df[pred_x_cov_list[i]['name']] = pred_x_cov_list[i]['mat']
    pred_df = pred_df[
        [i['name'] for i in pred_x_cov_list] + [f'draw_{i}' for i in range(y_samples.shape[0])]
    ]
    pred_fe_df = pd.DataFrame(
        dict(zip(
            [f'draw_{i}' for i in range(y_samples_fe.shape[0])], y_samples_fe
        ))
    )
    for i in range(len(pred_x_cov_list)):
        pred_fe_df[pred_x_cov_list[i]['name']] = pred_x_cov_list[i]['mat']
    pred_fe_df = pred_fe_df[
        [i['name'] for i in pred_x_cov_list] + [f'draw_{i}' for i in range(y_samples_fe.shape[0])]
    ]
    pred_df.to_csv(os.path.join(OUT_DIR, f'{outcome}_{label}', f'{outcome}_y_samples.csv'), index=False)
    pred_fe_df.to_csv(os.path.join(OUT_DIR, f'{outcome}_{label}', f'{outcome}_y_samples_fe.csv'), index=False)

    # store plot
    low_num = (pred_x_cov_list[0]['mat'] <= 400).sum()
    if measure == 'log_ratio':
        ratio_plot(pred_x_cov_list, y_samples_fe, mr,
                   n_bins=low_num, file_name=f'{outcome}_{label}')
        if include_smoking:
            ratio_plot(pred_x_cov_list, y_samples_fe, mr,
                       n_bins=low_num, file_name=f'{outcome}_{label}',
                       data_index=(df.ier_source != 'AS').values)
    elif measure == 'diff':
        diff_plot(pred_x_cov_list, y_samples_fe, y_samples_fe_data_l, df, mr,
                  n_bins=low_num, file_name=f'{outcome}_{label}')

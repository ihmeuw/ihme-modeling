import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mrtool import MRData
import dill as pickle


def plot_risk_function(mrbrt, pair, beta_samples, gamma_samples, alt_cov_names=None, 
    ref_cov_names=None, continuous_variables=[], plot_note=None, plots_dir=None, 
    write_file=False):
    """Plot predicted relative risk.
    Args:
        mrbrt (mrtool.MRBRT):
            MRBeRT object.
        pair (str):
            risk_outcome pair. eg. 'redmeat_colorectal'
        beta_samples (np.ndarray):
            Beta samples generated using `sample_soln` function in MRBRT
        gamma_samples (np.ndarray):
            Gamma samples generated using `sample_soln` function in MRBRT
        alt_cov_names (List[str], optional):
            Name of the alternative exposures, if `None` use `['b_0', 'b_1']`.
            Default to `None`.
        ref_cov_names (List[str], optional):
            Name of the reference exposures, if `None` use `['a_0', 'a_1']`.
            Default to `None`.
        continuous_variables (list):
            List of continuous covariate names.
        plot_note (str):
            The notes intended to be written on the title.
        plots_dir (str):
            Directory where to save the plot.
        write_file (bool):
            Specify `True` if the plot is expected to be saved on disk.
            If True, `plots_dir` should be specified too.
    """
    data_df = mrbrt.data.to_df()
    sub = mrbrt.sub_models[0]
    knots = sub.get_cov_model(mrbrt.ensemble_cov_model_name).spline.knots
    min_cov = knots[0]
    max_cov = knots[-1]
    dose_grid = np.linspace(min_cov, max_cov)
    col_covs = sub.cov_names
    pred_df = pd.DataFrame(dict(zip(col_covs, np.zeros(len(col_covs)))), 
        index=np.arange(len(dose_grid)))

    alt_cov_names = ['b_0', 'b_1'] if alt_cov_names is None else alt_cov_names
    ref_cov_names = ['a_0', 'a_1'] if ref_cov_names is None else ref_cov_names
    pred_df['intercept'] = 1
    pred_df[alt_cov_names[0]] = dose_grid
    pred_df[alt_cov_names[1]] = dose_grid
    pred_df[ref_cov_names[0]] = knots[0]
    pred_df[ref_cov_names[1]] = knots[0]
    
    # if it's continuous variables, take median 
    for var in continuous_variables:
        pred_df[var] = np.median(data_df[var])

    pred_data = MRData()
    pred_data.load_df(pred_df, col_covs=col_covs)

    y_draws = mrbrt.create_draws(pred_data, beta_samples, gamma_samples, random_study=True)
    y_draws_fe = mrbrt.create_draws(pred_data, beta_samples, gamma_samples, random_study=False)

    num_samples = y_draws_fe.shape[1]
    sort_index = np.argsort(y_draws_fe[-1])
    trimmed_draws = y_draws_fe[:, sort_index[int(num_samples*0.01): -int(num_samples*0.01)]]
    patch_index = np.random.choice(trimmed_draws.shape[1], 
        y_draws_fe.shape[1] - trimmed_draws.shape[1], replace=True)
    y_draws_fe = np.hstack((trimmed_draws, trimmed_draws[:, patch_index]))
    
    y_mean_fe = np.mean(y_draws_fe, axis=1)
    y_lower_fe = np.percentile(y_draws_fe, 2.5, axis=1)
    y_upper_fe = np.percentile(y_draws_fe, 97.5, axis=1)
    
    plt.rcParams['axes.edgecolor'] = '0.15'
    plt.rcParams['axes.linewidth'] = 0.5

    plt.plot(dose_grid, np.exp(y_lower_fe), c='gray')
    plt.plot(dose_grid, np.exp(y_upper_fe), c='gray')
    plt.plot(dose_grid, np.exp(y_mean_fe), c='red')
    plt.ylim([np.exp(y_lower_fe).min() - np.exp(y_mean_fe).ptp()*0.1,
              np.exp(y_upper_fe).max() + np.exp(y_mean_fe).ptp()*0.1])
    plt.ylabel('RR', fontsize=10)
    plt.xlabel("Exposure", fontsize=10)
    
    if plot_note is not None:
        plt.title(plot_note)

    # save plot    
    if write_file:
        assert plots_dir is not None, "plots_dir is not specified!"
        outfile = os.path.join(plots_dir, f'{pair}_risk_function.pdf')
        plt.savefig(outfile, bbox_inches='tight')
        print(f"Risk function plot saved at {outfile}")
    else:
        plt.show()
    plt.close()


def plot_derivative_fit(mrbrt, pair, alt_cov_names=None, ref_cov_names=None,
    plot_note=None, plots_dir=None, write_file=False):
    """Plot fitted derivative.
    Args:
        mrbrt (mrtool.MRBRT):
            MRBRT object.
        pair (str):
            risk_outcome pair. eg. 'redmeat_colorectal'
        alt_cov_names (List[str], optional):
            Name of the alternative exposures, if `None` use `['b_0', 'b_1']`.
            Default to `None`.
        ref_cov_names (List[str], optional):
            Name of the reference exposures, if `None` use `['a_0', 'a_1']`.
            Default to `None`.
        plot_note (str, optional):
            The notes intended to be written on the title.
        plots_dir (str):
            Directory where to save the plot.
        write_file (bool):
            Specify `True` if the plot is expected to be saved on disk.
            If True, `plots_dir` should be specified too.
    """
    dose_variable = alt_cov_names[1]
    data_df = mrbrt.data.to_df()
    # construct dataframe 
    min_cov = 0
    max_cov = np.max(data_df[dose_variable])
    dose_grid = np.linspace(min_cov, max_cov)
    sub = mrbrt.sub_models[0]
    col_covs = sub.cov_models[0].covs
    # construct dataframe for plotting and derivation of derivative.
    pred_df = pd.DataFrame(dict(zip(col_covs, np.zeros(len(col_covs)))), 
        index=np.arange(len(dose_grid)))
    pred_df['intercept'] = 1
    pred_df[alt_cov_names[0]] = dose_grid
    pred_df[alt_cov_names[1]] = dose_grid
    
    # spline of submodel
    spline_list = [sub_model.get_cov_model(mrbrt.ensemble_cov_model_name).spline \
        for sub_model in mrbrt.sub_models]

    # beta solution of each submodel; excluding beta of covariates other than [b0, b1, a0, a1]
    beta_soln_list = []
    for sub_model in mrbrt.sub_models:
        beta_soln_list.append(
            sub_model.beta_soln[\
            sub_model.x_vars_indices[\
            sub_model.get_cov_model_index(\
            mrbrt.ensemble_cov_model_name)]])
 
    # get RR from model
    d_log_rr = [get_rr_data(pred_df[alt_cov_names[1]], spline_obj, beta_soln) \
                for spline_obj, beta_soln in zip(spline_list, beta_soln_list)]
    
    alt_covs = mrbrt.data.get_covs(alt_cov_names).T
    ref_covs = mrbrt.data.get_covs(ref_cov_names).T
    
    alt_mid = alt_covs.mean(axis=0)
    ref_mid = ref_covs.mean(axis=0)

    # exposure middle point 
    effect_mid = (ref_mid + alt_mid) / 2
    # estimate slope 
    ln_effect_unit = mrbrt.data.obs / (alt_mid - ref_mid)
    
    # weight of each submodel multiplied by inlier/outlier weight of submodel 
    w = np.sum([sub_mr.lt.w * weight for sub_mr, weight 
        in zip(mrbrt.sub_models, mrbrt.weights)], axis=0)
    inliers = w > 0.6
    
    # size
    pt_size = 1 / mrbrt.data.obs_se**2
    pt_size = pt_size * (300 / pt_size.max())

    plt.figure(figsize=(12, 10))
    plt.plot(ref_covs, np.array([ln_effect_unit, ln_effect_unit]), color='red', alpha=0.15)
    plt.plot(alt_covs, np.array([ln_effect_unit, ln_effect_unit]), color='blue', alpha=0.15)
    plt.plot(np.array([ref_covs[1,:], alt_covs[1,:]]),
                 np.array([ln_effect_unit, ln_effect_unit]), color='grey', alpha=0.15)
    # plot slope for each submodel
    for slope in d_log_rr:
        plt.plot(dose_grid, slope, color='green', alpha=0.15)
    
    plt.ylabel('Slope of ln(RR)', fontsize=10)
    plt.xlabel('Exposure', fontsize=10)
    plt.tick_params(labelsize=8)
    # scatterplot of empirical slope of each point; inlier/outlier specified;
    # point side is proportional to obs_se squared.
    plt.scatter(effect_mid[inliers], ln_effect_unit[inliers], s=pt_size[inliers],
                    c='grey', edgecolors='black', marker='o', alpha=0.75)
    plt.scatter(effect_mid[~inliers], ln_effect_unit[~inliers], s=pt_size[~inliers],
                    c='black', marker='x', alpha=0.75)
    plt.axhline(0, xmin=min_cov, xmax=max_cov, linestyle='--', c='grey')

    if plot_note is not None:
        plt.title(plot_note)
    # save plot
    if write_file:
        assert plots_dir is not None, "plots_dir is not specified!"
        outfile = os.path.join(plots_dir, f'{pair}_derivative_fit.pdf')
        plt.savefig(outfile, bbox_inches='tight')
        print(f"Derivative fit plot saved at {outfile}")
    else:
        plt.show()
    plt.close()


def get_rr_data(x, spline, beta):
    """get RR from model
    x: exposure on the x-axis
    spline: spline of sub model
    beta: beta solution of sub model
    """
    # Index of design matrix from 1 to drop intercept.
    rr = 1.0 + spline.design_mat(x)[:, 1:].dot(beta)
    # get derivative of RR
    d_rr = spline.design_dmat(x, 1)[:, 1:].dot(beta)
    # get the derivative of log RR
    d_log_rr = d_rr / rr
    return d_log_rr
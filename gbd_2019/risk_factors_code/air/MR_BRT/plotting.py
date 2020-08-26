import sys
import os

import numpy as np
import matplotlib.pyplot as plt

sys.path.append(os.path.dirname(__file__))
from globals import *


def diff_plot(pred_x_cov_list, y_samples, y_samples_data_l, df, mr, n_bins, file_name=None):
    # pred x values
    x = pred_x_cov_list[0]['mat'][:n_bins]
    
    # subset y
    y_samples = y_samples[:, :n_bins]
    
    # get summaries
    y_mean = y_samples.mean(axis=0)
    y_lower = np.percentile(y_samples, 2.5, axis=0)
    y_upper = np.percentile(y_samples, 97.5, axis=0)
    y_mean_data_l = y_samples_data_l.mean(axis=0)
    y_mean_data_r = y_mean_data_l + mr.obs_mean
    pmhap_mat = df[['conc_den', 'conc']].values.T

    w_array = np.sum([sub_mr.lt.w * weight for sub_mr, weight in zip(mr.mr_list, mr.weights)], axis=0)
    # w_array = mr.lt.w

    pt_size = 1 / mr.obs_std**2
    pt_size = pt_size * (500 / pt_size.max())

    fig, ax = plt.subplots(1, 2, figsize=(22, 8.5))
    plt.rcParams['axes.edgecolor'] = '0.15'
    plt.rcParams['axes.linewidth'] = 0.5

    ax[0].plot(x, y_samples.T, color='dodgerblue', alpha=0.25)
    ax[0].plot(x, y_mean, color='firebrick')
    ax[0].plot(pmhap_mat, np.array([y_mean_data_l, y_mean_data_r]), 
               color='grey', alpha=0.1)
    ax[0].scatter(pmhap_mat[1], y_mean_data_r, s=pt_size,
                c='grey', edgecolors='black', marker='o', alpha=0.75)


    ax[1].plot(x, y_mean, color='firebrick')
    ax[1].fill_between(x, y_lower, y_upper, color='firebrick', alpha=0.1)
    ax[1].plot(pmhap_mat, np.array([y_mean_data_l, y_mean_data_r]), 
               color='grey', alpha=0.1)
    ax[1].scatter(pmhap_mat[1], y_mean_data_r, s=pt_size,
                c='grey', edgecolors='black', marker='o', alpha=0.75)
    ax[1].set_ylim(np.percentile(mr.obs_mean, [10, 90]))
    # ax[1].set_ylim([y_mean.min() - y_mean.ptp() * 0.1, 
    #                 y_mean.max() + y_mean.ptp() * 0.1])
    ax[1].set_xlim([0, np.percentile(pmhap_mat[1], 90)])
    
    if file_name is not None:
        plt.savefig(os.path.join(OUT_DIR, file_name, f'{file_name}_diff.pdf'), bbox_inches='tight')
    else:
        plt.show()


def get_rr_data(x, spline_obj, beta_soln):
    mat = spline_obj.designMat(x)
    dmat = spline_obj.designDMat(x, 1)
    
    rr = mat.dot(beta_soln[:spline_obj.num_spline_bases])
    log_rr = np.log(rr)
    #
    d_rr = dmat.dot(beta_soln[:spline_obj.num_spline_bases])
    #
    d_log_rr = d_rr / rr

    return d_log_rr


def ratio_plot(pred_x_cov_list, y_samples, mr, n_bins, file_name=None, data_index=None):
    # pred x values
    x = pred_x_cov_list[0]['mat'][:n_bins]
    
    # subset y
    y_samples = y_samples[:, :n_bins]

    # data points (slopes)
    ln_effect_unit = mr.obs_mean / (mr.x_cov_list[0]['mat'][0] - mr.x_cov_list[0]['mat'][1])
    effect_mid = mr.x_cov_list[0]['mat'].mean(axis=0)
    pt_size = 1 / mr.obs_std**2
    pt_size = pt_size * (500 / pt_size.max())
    data_mat = mr.x_cov_list[0]['mat']

    # limit values if index is provided
    if data_index is not None:
        data_mat = data_mat[:, data_index]
        ln_effect_unit = ln_effect_unit[data_index]
        effect_mid = effect_mid[data_index]
        pt_size = pt_size[data_index]
        y_samples = y_samples[:, x < data_mat.max()]
        x = x[x < data_mat.max()]
        
    # get RR from model
    d_log_rr = [get_rr_data(x, spline_obj, beta_soln) \
                for spline_obj, beta_soln in zip(mr.spline_list, mr.beta_soln)]
      
    # get means
    sample_mean = y_samples.mean(axis=0)
    sample_upper = np.percentile(y_samples, 97.5, axis=0)
    sample_lower = np.percentile(y_samples, 2.5, axis=0)

    # set up plot
    fig, ax = plt.subplots(1, 2, figsize=(22, 8.5))
    plt.rcParams['axes.edgecolor'] = '0.15'
    plt.rcParams['axes.linewidth'] = 0.5

    # curve
    ax[0].plot(x, y_samples.T, c='dodgerblue', alpha=0.1)
    ax[0].fill_between(x, sample_lower, sample_upper, color='grey', alpha=0.4)
    ax[0].plot(x, sample_mean, c='black')
    ax[0].set_ylim([sample_lower.min() - sample_mean.ptp()*0.1,
                    sample_upper.max() + sample_mean.ptp()*0.1])
    ax[0].set_ylabel('RR', fontsize=8)
    ax[0].set_xlabel('Exposure', fontsize=8)
    
    # derivative fit
    ax[1].plot(data_mat,
               np.array([ln_effect_unit, ln_effect_unit]), color='grey', alpha=0.15)
    for slope in d_log_rr:
        ax[1].plot(x, slope, color='green', alpha=0.15)
    ax[1].scatter(effect_mid, ln_effect_unit, s=pt_size,
                    c='grey', edgecolors='black', marker='o', alpha=0.75)
    ax[1].plot(x, np.mean(d_log_rr, axis=0), color='darkgreen')
    ax[1].set_ylabel('Slope of ln(RR)', fontsize=8)
    ax[1].yaxis.set_tick_params(labelsize=8)
    ax[1].set_xlabel('Exposure', fontsize=8)
    ax[1].xaxis.set_tick_params(labelsize=8)
    
    if file_name is not None:
        if data_index is not None:
            pdf_name = f'{file_name}_subset_log_ratio.pdf'
        else:
            pdf_name = f'{file_name}_log_ratio.pdf'
        plt.savefig(os.path.join(OUT_DIR, file_name, pdf_name), bbox_inches='tight')
    else:
        plt.show()

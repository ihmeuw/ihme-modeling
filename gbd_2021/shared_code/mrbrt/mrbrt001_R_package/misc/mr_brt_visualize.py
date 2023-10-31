import os
import argparse
import pickle

import numpy as np
import pandas as pd

from matplotlib import pyplot as plt
import seaborn as sns

# sys.path.append('./')
from mr_brt import predict_mr_brt


def funnel_plot(mr_dir=None, model_data=None, mr=None, continuous_variables=[],
                file_name='funnel_plot', plot_note=None, include_bias=False,
                write_file=False):
    # make sure we have enough information
    assert np.all([i is not None for i in [mr, model_data]]) or mr_dir is not None, \
    'Need to provide both data and model object or directory where they are stored.'
    assert not write_file or mr_dir is not None, \
    'Need to provide model directory if writing plots.'

    # load data and model objects
    if model_data is None:
        with open(f'{mr_dir}/data_obj.pkl', 'rb') as data_input:
            model_data = pickle.load(data_input)
    if mr is None:
        with open(f'{mr_dir}/mr_obj.pkl', 'rb') as mr_input:
            mr = pickle.load(mr_input)
    if mr.use_trimming:
        w = mr.w
    else:
        w = 1

    # get inlier/outlier datasets
    data_df = pd.DataFrame({'y':model_data.y, 'se':model_data.s, 'w':w})
    il_data_df = data_df.loc[data_df.w >= 0.6]
    ol_data_df = data_df.loc[data_df.w < 0.6]

    # get column names
    X_columns = model_data.x_covariates.feature('name')
    Z_columns = model_data.z_covariates.feature('name')

    # predict effect
    if include_bias:
        pred_x_df = pd.DataFrame(dict(zip(X_columns, np.ones(len(X_columns)))), index=[0])
    else:
        pred_x_df = pd.DataFrame(dict(zip(X_columns, np.zeros(len(X_columns)))), index=[0])
        pred_x_df['intercept'] = 1
    for continuous_variable in continuous_variables:
        pred_x_df[continuous_variable] = np.median(model_data.x_covariates.covariates[continuous_variable]['data'])
    pred_z_df = pd.DataFrame(dict(zip(Z_columns, np.zeros(len(Z_columns)))), index=[0])
    pred_z_df['intercept'] = 1
    draw_df, pred_df = predict_mr_brt(model_data, mr,
                                      pred_x_df=pred_x_df, pred_z_df=pred_z_df,
                                      write_summaries=False, write_draws=False)
    del draw_df
    y_mean = round(pred_df['Y_mean'].values.item(), 3)
    y_lower = round(pred_df['Y_mean_lo'].values.item(), 3)
    y_upper = round(pred_df['Y_mean_hi'].values.item(), 3)

    # get results
    p_val = round(pred_df['Y_negp'].values.item(), 3)
    content_string = f'Mean effect: {y_mean} (95% CI: {y_lower} to {y_upper}); p-value: {p_val}'

    # do coefficient writing
    x_covs = [a for a in model_data.x_covariates.feature('name') if a in ['intercept'] + continuous_variables]
    chi_covs = [chi for chi in model_data.x_covariates.feature('name') if chi not in ['intercept'] + continuous_variables]
    z_covs = model_data.z_covariates.feature('name')
    if len(x_covs) + len(chi_covs) == len(mr.beta_soln):
        x_covs_i = [i for i, a in enumerate(model_data.x_covariates.feature('name')) if a in x_covs]
        chi_covs_i = [i for i, chi in enumerate(model_data.x_covariates.feature('name')) if chi in chi_covs]
        alphas = mr.beta_soln[x_covs_i]
        betas = mr.beta_soln[chi_covs_i]
        gammas = mr.gama_soln

        alpha_string = ', '.join([r'$\alpha_{}$={}'.format('{' + x_cov.replace('_',' ') + '}', round(alpha, 3))
                           for x_cov, alpha in zip(x_covs, alphas)])
        beta_string = ', '.join([r'$\beta_{}$={}'.format('{' + chi_cov.replace('_',' ') + '}', round(beta, 3))
                           for chi_cov, beta in zip(chi_covs, betas)])
        if alpha_string: beta_string = '\n'.join([alpha_string, beta_string])
        gamma_string = ', '.join([r'$\gamma_{}$={}'.format('{' + z_cov.replace('_',' ') + '}', round(gamma, 3))
                            for z_cov, gamma in zip(z_covs, gammas)])
        content_string = content_string + '\n' + beta_string + '\n' + gamma_string

    # make funnel lines
    max_se = data_df.se.max()
    se_domain = np.arange(0, max_se*1.1, max_se / 100)
    se_lower = y_mean - (se_domain*1.96)
    se_upper = y_mean + (se_domain*1.96)

    # plot
    sns.set_style('darkgrid')
    plt.rcParams['axes.edgecolor'] = '0.15'
    plt.rcParams['axes.linewidth'] = 0.5
    plt.fill_betweenx(se_domain, se_lower, se_upper, color='white', alpha=0.75)
    plt.axvline(y_mean, 0, 1 - (0.025*max(se_domain) / (max(se_domain)*1.025)),
                color='black', alpha=0.75, linewidth=0.75)
    plt.axvline(0, color='mediumseagreen', alpha=0.75, linewidth=0.75)
    plt.plot(se_lower, se_domain, color='black', linestyle='--', linewidth=0.75)
    plt.plot(se_upper, se_domain, color='black', linestyle='--', linewidth=0.75)
    plt.plot(il_data_df.y, il_data_df.se, 'o',
             markersize=5, markerfacecolor='royalblue', markeredgecolor='navy', markeredgewidth=0.6,
             alpha=.6)
    plt.plot(ol_data_df.y, ol_data_df.se, 'o',
             markersize=5, markerfacecolor='indianred', markeredgecolor='maroon',  markeredgewidth=0.6,
             alpha=.6)
    plt.ylim([-0.025*max(se_domain), max(se_domain)])
    plt.xlabel('Effect size', fontsize=6)
    plt.xticks(fontsize=6)
    plt.ylabel('Standard error', fontsize=6)
    plt.yticks(fontsize=6)
    plt.gca().invert_yaxis()
    if plot_note is not None:
        plt.title(content_string, fontsize=6)
        plt.suptitle(plot_note, y=1.01, fontsize=8)
    else:
        plt.title(content_string, fontsize=8)
    # plt.tight_layout()
    if write_file:
        plt.savefig(os.path.join(mr_dir, file_name + '.pdf'), orientation='landscape', bbox_inches='tight')
    else:
        plt.show()
    plt.clf()


def dose_response_curve(dose_variable, continuous_variables=[],
                        mr_dir=None, model_data=None, mr=None,
                        file_name='dose_response_plot',
                        from_zero=False, include_bias=False,
                        ylim=None, y_transform=None, x_transform=None,
                        plot_note=None,
                        write_file=False):
    # make sure we have enough information
    assert np.all([i is not None for i in [mr, model_data]]) or mr_dir is not None, \
        'Need to provide both data and model object or directory where they are stored.'
    assert not write_file or mr_dir is not None, \
        'Need to provide model directory if writing plots.'

    # load data and model objects
    if model_data is None:
        with open(f'{mr_dir}/data_obj.pkl', 'rb') as data_input:
            model_data = pickle.load(data_input)
    if mr is None:
        with open(f'{mr_dir}/mr_obj.pkl', 'rb') as mr_input:
            mr = pickle.load(mr_input)
    if mr.use_trimming:
        w = mr.w
    else:
        w = 1

    # check for knots
    if dose_variable in model_data.x_covariates.spline_specs.keys():
        knots = model_data.x_covariates.spline_specs[dose_variable]['knots']
    else:
        knots = np.array([])

    # get prediction dose
    data_doses = np.squeeze(model_data.x_covariates.covariates[dose_variable]['data'])
    if knots.any():
        dose_min = knots[0]
        dose_max = knots[-1]
    else:
        dose_min = data_doses.min()  # - (data_doses.max() - data_doses.min()) * 0.01
        dose_max = data_doses.max()  # + (data_doses.max() - data_doses.min()) * 0.01
    if from_zero:
        dose_min = 0
    dose_range = (dose_max - dose_min)
    plot_doses = np.arange(dose_min, dose_max + dose_range * 0.01, dose_range / 100)

    # set up dataframe
    data_df = pd.DataFrame({'y':model_data.y, 'se':model_data.s, 'w':w,
                            'dose':data_doses})

    # lose dose from continuous variables
    continuous_variables = [i for i in continuous_variables if i != dose_variable]

    # set up prediction z
    x_cov = model_data.x_covariates.feature('name')
    z_cov = model_data.z_covariates.feature('name')
    pred_x_df = pd.DataFrame({dose_variable:plot_doses})
    for c in x_cov:
        if c in continuous_variables:
            pred_x_df[c] = np.median(model_data.x_covariates.covariates[c]['data'])
        elif (include_bias or c == 'intercept') and c != dose_variable:
            pred_x_df[c] = 1
        elif c != dose_variable:
            pred_x_df[c] = 0
    pred_z_df = pd.DataFrame(dict(zip(z_cov, np.zeros(len(z_cov)))), index=[0])
    pred_z_df['intercept'] = 1

    # make prediction for data (define in/out of funnel) and line/UI
    d_draw_df, d_summ_df = predict_mr_brt(model_data, mr,
                                          write_summaries=False, write_draws=False, n_sample=10000)
    del d_draw_df

    # make prediction for line
    pl_draw_df, pl_summ_df = predict_mr_brt(model_data, mr,
                                            pred_x_df=pred_x_df, pred_z_df=pred_z_df,
                                            write_summaries=False, write_draws=False, n_sample=10000)
    del pl_draw_df

    # determine points outside funnel
    data_df['position'] = 'inside funnel'
    data_df.loc[data_df.y < d_summ_df.Y_mean - (data_df.se * 1.96),
                'position'] = 'outside funnel'
    data_df.loc[data_df.y > d_summ_df.Y_mean + (data_df.se * 1.96),
                'position'] = 'outside funnel'

    # get inlier/outlier datasets
    data_df.loc[data_df.w >= 0.6, 'trim'] = 'inlier'
    data_df.loc[data_df.w < 0.6, 'trim'] = 'outlier'

    # get plot guide
    data_df['plot_guide'] = data_df['trim'] + ', ' + data_df['position']
    plot_key = {
        'inlier, inside funnel':('o', 'seagreen', 'darkgreen'),
        'inlier, outside funnel':('o', 'coral', 'firebrick'),
        'outlier, inside funnel':('x', 'darkgreen', 'darkgreen'),
        'outlier, outside funnel':('x', 'firebrick', 'firebrick')
    }

    # get scaled marker size
    data_df['size_var'] = 1 / data_df.se
    data_df['size_var'] = data_df['size_var'] * (300 / data_df['size_var'].max())

    # apply any transformations
    if y_transform is not None:
        pl_summ_df['Y_mean'] = y_transform(pl_summ_df['Y_mean'])
        pl_summ_df['Y_mean_lo'] = y_transform(pl_summ_df['Y_mean_lo'])
        pl_summ_df['Y_mean_hi'] = y_transform(pl_summ_df['Y_mean_hi'])
        pl_summ_df['Y_mean_lo_fe'] = y_transform(pl_summ_df['Y_mean_lo_fe'])
        pl_summ_df['Y_mean_hi_fe'] = y_transform(pl_summ_df['Y_mean_hi_fe'])
        data_df.y = y_transform(data_df.y)
    if x_transform is not None:
        pl_summ_df['X_' + dose_variable] = x_transform(pl_summ_df['X_' + dose_variable])
        data_df['dose'] = x_transform(data_df['dose'])

    # plot
    sns.set_style('whitegrid')
    plt.rcParams['axes.edgecolor'] = '0.15'
    plt.rcParams['axes.linewidth'] = 0.5
    plt.fill_between(pl_summ_df['X_' + dose_variable], pl_summ_df.Y_mean_lo_fe, pl_summ_df.Y_mean_hi_fe, color='darkgrey', alpha=0.75)
    plt.fill_between(pl_summ_df['X_' + dose_variable], pl_summ_df.Y_mean_lo, pl_summ_df.Y_mean_hi, color='lightgrey', alpha=0.5)
    plt.plot(pl_summ_df['X_' + dose_variable], pl_summ_df.Y_mean, color='black', linewidth=0.75)
    for key, value in plot_key.items():
        plt.scatter(
            data_df.loc[data_df.plot_guide == key, 'dose'],
            data_df.loc[data_df.plot_guide == key, 'y'],
            s=data_df.loc[data_df.plot_guide == key, 'size_var'],
            marker=value[0],
            facecolors=value[1], edgecolors=value[2], linewidth=0.6,
            alpha=.6
        )
    for knot in knots:
        plt.axvline(knot, color='navy', linestyle='--', alpha=0.5, linewidth=0.75)
    if ylim is not None:
        plt.ylim(ylim)
    plt.xlim([min(pl_summ_df['X_' + dose_variable]), max(pl_summ_df['X_' + dose_variable])])
    plt.xlabel('Exposure', fontsize=6)
    plt.xticks(fontsize=6)
    plt.ylabel('Effect size', fontsize=6)
    plt.yticks(fontsize=6)
    if plot_note is not None:
        plt.title(plot_note, fontsize=8)
    if write_file:
        plt.savefig(os.path.join(mr_dir, f'{file_name}.pdf'), orientation='landscape', bbox_inches='tight')
    else:
        plt.show()
    plt.clf()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--mr_dir', help='Directory where data will read and written.', type=str
    )
    parser.add_argument(
        '--continuous_variables',
        help='Continuous variables, use median for plots (except dose for dose plot).', type=str, nargs='+'
    )
    parser.add_argument(
        '--dose_variable', help='Variable corresponding to dose (for plots).', type=str
    )
    parser.add_argument(
        '--plot_note', help='Variable corresponding to dose (for plots).', type=str, nargs='+'
    )
    args = parser.parse_args()

    if args.continuous_variables is None:
        continuous_variables = []
    else:
        continuous_variables = args.continuous_variables
    if args.dose_variable is not None and args.dose_variable not in continuous_variables:
        continuous_variables = continuous_variables + [args.dose_variable]
        
    if args.plot_note is not None:
        plot_note = ' '.join(args.plot_note)
    else:
        plot_note = None
    funnel_plot(mr_dir=args.mr_dir,
                continuous_variables=continuous_variables,
                file_name='funnel_plot',
                plot_note=plot_note,
                write_file=True)
    funnel_plot(mr_dir=args.mr_dir,
                continuous_variables=continuous_variables,
                include_bias=True,
                file_name='funnel_plot_w_bias',
                plot_note=plot_note,
                write_file=True)
    if args.dose_variable is not None:
        dose_response_curve(mr_dir=args.mr_dir,
                            continuous_variables=continuous_variables,
                            dose_variable=args.dose_variable,
                            file_name='dose_response_plot',
                            plot_note=plot_note,
                            write_file=True)
        dose_response_curve(mr_dir=args.mr_dir,
                            continuous_variables=continuous_variables,
                            dose_variable=args.dose_variable,
                            include_bias=True,
                            file_name='dose_response_plot_w_bias',
                            plot_note=plot_note,
                            write_file=True)

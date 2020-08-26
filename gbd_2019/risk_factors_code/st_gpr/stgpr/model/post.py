
import math
import os
import sys

import numpy as np
import pandas as pd

from orm_stgpr.lib.constants import parameters
from orm_stgpr.lib.util import transform as transform_helpers

from stgpr.model import paths
from stgpr.model.config import *
from stgpr.st_gpr import helpers as hlp


def save_gpr_summaries(run_id, output_path, holdout_num, params):
    for p in params:

        inpath = '{}/gpr_temp_{}/{}/'.format(output_path, holdout_num, p)
        dt = pd.concat([pd.read_csv('{}/{}'.format(inpath, f))
                        for f in os.listdir(inpath)])
        dt.drop_duplicates(inplace=True)

        hlp.model_save(dt, run_id, 'gpr', holdout=holdout_num,
                       param_set=p, output_path=output_path)


def get_data(run_id, output_path, holdout_num=0, param_set=0):
    """Returns some useful data columns, stage1, st, and gpr.
    The modeling columns are in NORMAL space - ie backtransformed out of
    any log/logit space if the model ran in that space. Data and variance
    are provided in both transformed and non-transformed space."""

    # pull and subset prepped data to necessities
    data = hlp.model_load(run_id, 'prepped', output_path=output_path)
    data_transform = hlp.model_load(run_id, 'parameters', param_set=None, output_path=output_path)[
        'data_transform'].iat[0]

    ko_col = ['ko_{}'.format(holdout_num)]
    data_cols = ['data', 'variance', 'original_data', 'original_variance']
    data = data[IDS + data_cols + ko_col]

    # pull linear estimates outputs
    stage1 = hlp.model_load(
        run_id, 'stage1', holdout=holdout_num, output_path=output_path)
    if 'location_id_count' in stage1.columns:
        stage1.drop(columns='location_id_count', inplace=True)

    # pull st and gpr outputs
    st = hlp.model_load(run_id, 'st', holdout=holdout_num,
                        param_set=param_set, output_path=output_path)
    st.drop(columns='scale', inplace=True)
    gpr = hlp.model_load(
        run_id, 'gpr', holdout=holdout_num, param_set=param_set, output_path=output_path)

    # merge
    df = stage1.merge(data, on=IDS, how='outer')
    df = df.merge(st, on=IDS, how='left')
    df = df.merge(gpr, on=IDS, how='left')

    # transform modeling columns
    df['stage1'] = transform_helpers.transform_data(
        df['stage1'], data_transform, reverse=True
    )
    df['st'] = transform_helpers.transform_data(
        df['st'], data_transform, reverse=True
    )

    return df


def rmse(error):
    return(math.sqrt(np.mean([(x**2.0) for x in error])))


def calculate_rmse(df, holdout_num, var='gpr_mean', inv_variance_weight=False):

    tmp = df.copy()
    errs = pd.DataFrame(
        {'ko': holdout_num, 'in_sample_rmse': np.nan, 'oos_rmse': np.nan}, index=[0])

    # calculate error (in-sample and out-of-sample) for each holdout requested
    data_is = tmp.loc[tmp['ko_{}'.format(holdout_num)] == 1, 'original_data']
    data_oos = tmp.loc[tmp['ko_{}'.format(holdout_num)] != 1, 'original_data']

    outvar_is = tmp.loc[tmp['ko_{}'.format(holdout_num)] == 1, var]
    outvar_oos = tmp.loc[tmp['ko_{}'.format(holdout_num)] != 1, var]

    if inv_variance_weight:
        variances_is = tmp.loc[tmp['ko_{}'.format(
            holdout_num)] == 1, 'original_variance']
        variances_oos = tmp.loc[tmp['ko_{}'.format(
            holdout_num)] != 1, 'original_variance']
        wt_is = variances_is / np.sum(variances_is)
        wt_oos = variances_oos / np.sum(variances_oos)
    else:
        wt_is = wt_oos = 1

    tmp['is_error'] = wt_is * (data_is - outvar_is)
    tmp['oos_error'] = wt_oos * (data_oos - outvar_oos)

    # calculate in-sample and out-of-sample RMSE (will automatically
    # recognize oos rmse is unavailable for runs with no kos)
    errs['in_sample_rmse'] = rmse(
        tmp.loc[tmp.is_error.notnull(), 'is_error'].tolist())
    errs['oos_rmse'] = rmse(
        tmp.loc[tmp.oos_error.notnull(), 'oos_error'].tolist())
    errs['var'] = var

    errs = errs[['var', 'ko', 'in_sample_rmse', 'oos_rmse']]

    return(errs)


def calculate_fit_stats(run_id, output_path, run_type, holdout_num, holdouts,
                        param_groups, csv=True, inv_variance_weight=False):

    rmse_list = []

    # pull data
    for param in param_groups:

        # pull data
        df = get_data(run_id, output_path, holdout_num, param)

        for var in ['stage1', 'st', 'gpr_mean']:

            # calculate in-sample and (where relevant) out-of-sample RMSE
            rmse_table = calculate_rmse(
                df, holdout_num, var, inv_variance_weight)
            rmse_table['parameter_set'] = param

            # selection runs have multiple hyperparameter sets
            # out-of-sample evaluation runs use just need oos-rmse for one set of parameters
            if run_type in ['in_sample_selection', 'oos_selection']:

                hyperparams = hlp.model_load(
                    run_id, 'parameters', holdout=holdout_num, param_set=param, output_path=output_path)
                rmse_table['zeta'] = hyperparams[parameters.ST_ZETA].iat[0]
                rmse_table['lambdaa'] = hyperparams[parameters.ST_LAMBDA].iat[0]
                rmse_table['omega'] = hyperparams[parameters.ST_OMEGA].iat[0]
                rmse_table['scale'] = hyperparams[parameters.GPR_SCALE].iat[0]

            else:
                hyperparams = hlp.model_load(
                    run_id, 'parameters', param_set=None, output_path=output_path)
                rmse_table['zeta'] = hyperparams[parameters.ST_ZETA].iat[0]
                rmse_table['lambdaa'] = hyperparams[parameters.ST_LAMBDA].iat[0]
                rmse_table['omega'] = hyperparams[parameters.ST_OMEGA].iat[0]
                rmse_table['scale'] = hyperparams[parameters.GPR_SCALE].iat[0]
                rmse_table['density_cutoffs'] = hyperparams.density_cutoffs.iat[0]

            rmse_list.append(rmse_table)

    rmses = pd.concat(rmse_list)
    rmses = rmses[['var', 'ko', 'parameter_set', 'zeta', 'lambdaa', 'omega',
                   'scale', 'in_sample_rmse', 'oos_rmse']]

    # set in order of 'best' ie lowest oos rmse if ko run
    if run_type == 'oos_selection':
        rmses.sort_values(by=['oos_rmse'], ascending=True)

    if ((run_type in ['in_sample_selection', 'oos_selection']) | (holdouts > 0)):
        outpath = '{}/fit_stats_{}_{}.csv'.format(
            output_path, holdout_num, param)
    else:
        outpath = '{}/fit_stats.csv'.format(output_path)

    if(csv):
        rmses.to_csv(outpath, index=False)
        print('Saved fit statistics for holdout {} to {}'.format(
            holdout_num, outpath))

    return(rmses)


if __name__ == '__main__':
    run_id = int(sys.argv[1])
    output_path = sys.argv[2]
    holdout_num = int(sys.argv[3])
    run_type = sys.argv[4]
    holdouts = int(sys.argv[5])
    param_sets = sys.argv[6]

    for i in ['run_id', 'output_path', 'holdout_num', 'run_type', 'holdouts', 'param_sets']:
        print('{} : {}'.format(i, eval(i)))

    # split out param sets to list
    param_groups = hlp.separate_string_to_list(str(param_sets), typ=int)

    # Combine and save summaries of gpr_temp files
    print('Saving GP summaries.')
    save_gpr_summaries(run_id, output_path, holdout_num, param_groups)

    # calculate fit stats
    print('Calculating fit stats')
    calculate_fit_stats(run_id, output_path, run_type,
                        holdout_num, holdouts, param_groups)

    print('Post-modeling calculations complete.')

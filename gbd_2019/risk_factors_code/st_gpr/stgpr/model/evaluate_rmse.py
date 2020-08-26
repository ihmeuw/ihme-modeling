import os
import sys

import pandas as pd

from stgpr.model.config import *
from stgpr.st_gpr.helpers import equal_sets, antijoin

if __name__ == '__main__':
    run_id = int(sys.argv[1])
    output_path = sys.argv[2]
    run_type = sys.argv[3]
    holdouts = int(sys.argv[4])
    n_params = int(sys.argv[5])

    for i in ['run_id', 'output_path', 'run_type', 'holdouts', 'n_params']:
        print('{} : {}'.format(i, eval(i)))

    # pull all fit_stat files and make sure no parameter sets are missing stats
    rmse_files = [x for x in os.listdir(output_path) if 'fit_stats' in x]
    rmse = pd.concat([pd.read_csv('{}/{}'.format(output_path, f))
                      for f in rmse_files])

    # make sure that fit statistics are present for all parameter sets
    rmse_param_sets = rmse.parameter_set.unique()
    mi = antijoin(rmse_param_sets, list(range(0, n_params)))
    msg = 'You are missing fit statistics for the following parameter sets: {}'.format(
        mi)
    assert equal_sets(rmse_param_sets, list(range(0, n_params))), msg

    # take means - since each holdout contributes equivalent weight, should be
    df = rmse.groupby(['parameter_set', 'var'])[
        'in_sample_rmse', 'oos_rmse'].mean().reset_index()

    if run_type == 'in_sample_selection':
        min_gp_rmse = df.loc[df['var'] == 'gpr_mean']['in_sample_rmse'].min()
        df['best'] = (df['in_sample_rmse'] == min_rmse).astype(int)
    elif run_type == 'oos_selection':
        min_rmse = df.loc[df['var'] == 'gpr_mean']['oos_rmse'].min()
        df['best'] = (df['oos_rmse'] == min_rmse).astype(int)
    else:
        df['best'] = 1

    # also save RMSE values
    # df = df.reset_index()
    df = df.rename(
        columns={'in_sample_rmse': 'is_mean', 'oos_rmse': 'oos_mean'})
    out = rmse.merge(df, on=['parameter_set', 'var'])
    out.to_csv('{}/fit_stats.csv'.format(output_path), index=False)

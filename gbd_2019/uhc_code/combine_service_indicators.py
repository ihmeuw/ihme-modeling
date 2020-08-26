import argparse
import getpass
import functools
import numpy as np
import pandas as pd

from uhc_estimation import specs, misc, uhc_io


def get_uhc_ids():
    '''
    Get the uhc_ids that are in use.
    '''
    uhc_df = pd.read_excel(FILEPATH)
    uhc_df = uhc_df.loc[~uhc_df.service_proxy.isin(specs.NOT_USED)]
    uhc_ids = uhc_df.uhc_id.tolist()

    return uhc_ids


def draw_divide(num_df, denom_df):
    '''
    Get the weight value for each uhc_id.
    '''
    # print uhc_id for ease of debugging if loop breaks
    print(pd.unique(num_df['uhc_id']))
    df = misc.draw_math(
        [num_df, denom_df], specs.ID_COLS, specs.DRAW_COLS, '/'
    )
    df['uhc_id'] = num_df['uhc_id'].tolist()

    return df


def produce_uhc():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--uhc_id', help='0 for UHC service coverage aggregate.', type=int
    )
    parser.add_argument(
        '--uhc_version', help='Version number for run.', type=int
    )
    parser.add_argument(
        '--value_type', help='What are we storing,', type=str
    )
    args = parser.parse_args()

    # get service_proxys and service pop
    uhc_version_dir = FILEPATH

    # retrieve the IDs we need
    uhc_ids = get_uhc_ids()

    # calculate health gain weight fraction (health gain / sum of health gains)
    count_dfs = uhc_io.compile_dfs('counterfactual_burden', uhc_ids, uhc_version_dir)
    total_df = pd.concat(count_dfs)
    total_df = total_df.groupby(specs.ID_COLS, as_index=False)[specs.DRAW_COLS].sum()
    weight_dfs = [draw_divide(count_df, total_df) for count_df in count_dfs]
    weight_df = pd.concat(weight_dfs)
    weight_df['mean_weight'] = weight_df[specs.DRAW_COLS].mean(axis=1)
    # output the unadjusted weights
    summary_unadjusted_weights = misc.summarize(weight_df.copy(), specs.DRAW_COLS)
    summary_unadjusted_weights.to_csv(FILEPATH)
    weight_df.to_csv(FILEPATH)

    # adjust the weights -- take mean weight of indicator in a specified number of bands within a country and year
    weight_df['mean_weight'] = weight_df['mean_weight'].replace(0, np.nan)
    num_bands = 3
    weight_df['weight_band'] = weight_df.groupby(['location_id', 'year_id']).mean_weight.transform(
        lambda x: pd.qcut(x, num_bands, labels=range(1, num_bands+1))
    )
    weight_df['weight_band'] = weight_df['weight_band'].replace(np.nan, 0)
    weight_df[specs.DRAW_COLS] = weight_df.groupby(['location_id', 'year_id', 'weight_band'])[specs.DRAW_COLS].transform('mean')
    weight_df = weight_df.drop(['weight_band', 'mean_weight'], axis=1)

    # apply weight
    cov_dfs = uhc_io.compile_dfs('coverage', uhc_ids, uhc_version_dir)
    cov_df = pd.concat(cov_dfs)
    # weight_df = pd.concat(weight_dfs)
    uhcw_df = misc.draw_math(
        [cov_df, weight_df], specs.ID_COLS + ['uhc_id'], specs.DRAW_COLS, '*'
    )
    uhcw_df = uhcw_df.groupby(specs.ID_COLS, as_index=False)[specs.DRAW_COLS].sum()
    uhca_df = cov_df.groupby(specs.ID_COLS, as_index=False)[specs.DRAW_COLS].mean()

    # summarize and store...
    ## WEIGHTS
    summaryw_df = misc.summarize(weight_df, specs.DRAW_COLS)
    weight_df.to_hdf(FILEPATH)
    summaryw_df.to_csv(FILEPATH)

    ## WEIGHTED VALUES
    summaryuhcw_df = misc.summarize(uhcw_df, specs.DRAW_COLS)
    uhcw_df.to_hdf(FILEPATH)
    summaryuhcw_df.to_csv(FILEPATH)

    ## AVERAGE VALUES
    summaryuhca_df = misc.summarize(uhca_df, specs.DRAW_COLS)
    uhca_df.to_hdf(FILEPATH)
    summaryuhca_df.to_csv(FILEPATH)


if __name__ == '__main__':
    produce_uhc()

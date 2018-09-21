from __future__ import division

import argparse
import pandas as pd
from HALE_summary import calc_summary, long_to_wide
from ihme_dimensions.dfutils import resample

def resample_ar(df, draw_prefixes, n_draws):
    index_cols = ['sex_id', 'age_group_id', 'year_id', 'location_id']
    down_dict = long_to_wide(df, draw_prefixes, index_cols)
    dfs = []
    for pref in draw_prefixes:
        down_df = resample(down_dict[pref], n_draws, pref)
        re_draws = [col for col in down_df.columns if pref in col]
        num_draws = [i for i in range(len(re_draws))]
        renames = dict(zip(re_draws, num_draws))
        down_df.rename(columns=renames, inplace=True)
        down_df = pd.melt(down_df, id_vars=index_cols,
                          value_vars=num_draws, var_name='draw',
                          value_name=pref)
        down_df.set_index(index_cols + ['draw'], inplace=True)
        dfs.append(down_df)
    n_df = pd.concat(dfs, axis=1)
    n_df.reset_index(inplace=True)
    return n_df

def run_lt_compile(lt_in, lt_tmp, lt_dir, location, ar, n_draws):
    try:
        lt_draws = pd.read_csv('{lt_in}/lt_{location}.csv'.format(lt_in=lt_in,
                               location=location))
    except:
        lt_draws = pd.read_csv(
                '{lt_in}/PATH/lt_{location}.csv'.format(
                        lt_in=lt_in, location=location))
    if ar:
        years = range(1990, 2017)
    else:
        years = [1990, 1995, 2000, 2005, 2010, 2016]
    lt_draws = lt_draws.loc[lt_draws['year_id'].isin(years)]

    lt_draws = lt_draws.rename(columns={'ex':'Ex', 'nlx':'nLx', 'tx':'Tx'})
    summ_cols = ['mx', 'ax', 'qx', 'dx', 'Tx', 'nLx', 'lx', 'Ex']
    if n_draws != 1000:
        lt_draws = resample_ar(lt_draws, summ_cols, n_draws)

    lt_draws.loc[lt_draws['age_group_id'] == 33, 'age_group_id'] = 235
    ages = range(5, 21) + range(30, 33) + [28, 235]
    lt_draws = lt_draws.loc[lt_draws['age_group_id'].isin(ages)]
    lt_draws[['lx', 'dx', 'nLx', 'Tx']] = lt_draws[['lx', 'dx', 'nLx',
                                                    'Tx']].divide(100000)
    csv_draws = lt_draws.set_index('location_id')
    csv_draws.to_csv('{lt_tmp}/{location}_draws.csv'.format(lt_tmp=lt_tmp,
                    location=location))

    calc_summary(lt_draws, summ_cols, lt_dir, location)

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--lt_in",
            help="lt source directory",
            dUSERt=("/PATH"),
            type=str)
    parser.add_argument(
            "--lt_tmp",
            help="lt tmp directory",
            dUSERt="/PATH",
            type=str)
    parser.add_argument(
            "--lt_dir",
            help="lt summary directory",
            dUSERt="/PATH",
            type=str)
    parser.add_argument(
            "--location",
            help="location",
            dUSERt=62,
            type=int)
    parser.add_argument(
            "--ar",
            help="annual results",
            dUSERt=False,
            type=bool)
    parser.add_argument(
            "--n_draws",
            help="number of draws",
            dUSERt=1000,
            type=int)
    args = parser.parse_args()
    lt_in = args.lt_in
    lt_tmp = args.lt_tmp
    lt_dir = args.lt_dir
    location = args.location
    ar = args.ar
    n_draws = args.n_draws

    run_lt_compile(lt_in, lt_tmp, lt_dir, location, ar, n_draws)

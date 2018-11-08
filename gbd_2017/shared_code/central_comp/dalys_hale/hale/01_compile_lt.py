from __future__ import division

import argparse
import pandas as pd
from gbd.constants import GBD_ROUND_ID
from HALE_summary import long_to_dict
from ihme_dimensions.dfutils import resample
from parameter_csv import pull_mort_vers


def resample_ar(df, draw_prefixes, n_draws):
    ###############################################
    #If running annual results, we need to resample
    #life table draws down. This function resamples
    #long draws to a given number (n_draws) given
    #a df and the draw prefixes
    ###############################################
    index_cols = ['sex_id', 'age_group_id', 'year_id', 'location_id']
    down_dict = long_to_dict(df, draw_prefixes, index_cols)
    dfs = []
    for pref in draw_prefixes:
        down_df = resample(down_dict[pref], n_draws, pref)
        re_draws = [col for col in down_df.columns if pref in col]
        num_draws = [i for i in list(range(len(re_draws)))]
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


def run_lt_compile(lt_in, lt_tmp, location, year, n_draws):
    ###############################################
    #Reads in life table draws and formats them
    #for use in calculating HALE. Resamples draws
    #if running annual results. Subsets on correct
    #ages and converts from 100000 to 1. Saves
    #formatted draws and summarizes
    ###############################################
    lt_draws = pd.read_csv('{lt_in}/lt_{location}.csv'.format(lt_in=lt_in,
                           location=location))
    lt_draws = lt_draws.loc[lt_draws['year_id'] == year]
    if len(lt_draws) == 0:
        raise RuntimeError("Year {} is missing from lt draws".format(year))
    
    lt_draws = lt_draws.rename(columns={'ex':'Ex', 'nlx':'nLx', 'tx':'Tx'})
    summ_cols = ['mx', 'ax', 'qx', 'dx', 'Tx', 'nLx', 'lx', 'Ex']
    
    if n_draws not in [1000, 'max']:
        lt_draws = resample_ar(lt_draws, summ_cols, n_draws)

    lt_draws.loc[lt_draws['age_group_id'] == 33, 'age_group_id'] = 235
    ages = list(range(5, 21)) + list(range(30, 33)) + [28, 235]
    lt_draws = lt_draws.loc[lt_draws['age_group_id'].isin(ages)]
    lt_draws[['lx', 'dx', 'nLx', 'Tx']] = lt_draws[['lx', 'dx', 'nLx',
                                                    'Tx']].divide(100000)
    csv_draws = lt_draws.set_index('location_id')
    csv_draws.to_csv('{lt_tmp}/{location}_{year}_draws.csv'.format(
            lt_tmp=lt_tmp, location=location, year=year))


if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    lt_vers = pull_mort_vers(GBD_ROUND_ID)
    d_dir = "DIRECTORY"
    parser.add_argument(
            "--lt_in",
            help="lt source directory",
            default=d_dir,
            type=str)
    parser.add_argument(
            "--lt_tmp",
            help="lt tmp directory",
            default="DIRECTORY",
            type=str)
    parser.add_argument(
            "--location",
            help="location",
            default=7,
            type=int)
    parser.add_argument(
            "--year",
            help="year",
            default=1990,
            type=int)
    parser.add_argument(
            "--n_draws",
            help="n_draws",
            default=1000,
            type=int)

    args = parser.parse_args()
    lt_in = args.lt_in
    lt_tmp = args.lt_tmp
    location = args.location
    year = args.year
    n_draws = args.n_draws

    run_lt_compile(lt_in, lt_tmp, location, year, n_draws)
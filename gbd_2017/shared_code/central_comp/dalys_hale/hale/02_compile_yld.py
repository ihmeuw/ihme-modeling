from __future__ import division

import argparse
import pandas as pd
from get_draws.api import get_draws


def run_yld_compile(yld_tmp, yld_version, root_dir, location, year, n_draws):
    ###############################################
    #Uses get_draws to read in YLD draws. Also
    #reads in cahced pop from 00_master. Subsets on
    #correct ages and aggregates to create
    #age_group_id 28. Converts from wide to long.
    #Saves formatted draws and summarizes
    ###############################################

    pops = pd.read_csv('{root_dir}/inputs/pop.csv'.format(root_dir=root_dir))
    index_cols = ['location_id', 'age_group_id', 'sex_id', 'year_id']

    if yld_version == 0:
        yld_draws = get_draws("cause_id", source="como", gbd_id=294,
                              measure_id=3, location_id=location,
                              year_id=year, sex_id=[1,2])
    else:
        yld_draws = get_draws("cause_id", source="como", gbd_id=294,
                              measure_id=3, location_id=location,
                              year_id=year, sex_id=[1,2],
                              version_id=yld_version)
    draw_cols = [col for col in yld_draws.columns if 'draw' in col]
    if len(draw_cols) != n_draws:
        raise RuntimeError("expected {} draws, got {} instead".format(n_draws,
                           len(draw_cols)))

    yld_draws = yld_draws[index_cols + draw_cols]
    yld_draws = yld_draws.merge(pops, on=index_cols)
    yld_draws = yld_draws.set_index(index_cols)
    yld_draws[draw_cols] = yld_draws[draw_cols].multiply(
            yld_draws['population'], axis='index')
    yld_draws.drop('population', axis=1, inplace=True)
    yld_draws = yld_draws.reset_index()
    
    yld_draws.loc[yld_draws['age_group_id'].isin(list(range(2,5)) + [164]),
                  'age_group_id'] = 28
    ages = list(range(5, 21)) + list(range(30, 33)) + [28, 235]
    yld_draws = yld_draws.loc[yld_draws['age_group_id'].isin(ages)]
    yld_draws = yld_draws.groupby(index_cols).sum().reset_index()
    sex_agg = yld_draws.groupby(['age_group_id', 'location_id',
                                 'year_id']).sum().reset_index()
    sex_agg['sex_id'] = 3
    yld_draws = yld_draws.append(sex_agg)
    
    yld_draws = yld_draws.merge(pops, on=index_cols)
    yld_draws = yld_draws.set_index(index_cols)
    yld_draws[draw_cols] = yld_draws[draw_cols].divide(
            yld_draws['population'], axis='index')
    yld_draws.drop('population', axis=1, inplace=True)
    yld_draws = yld_draws.reset_index()
    
    yld_draws.rename(columns=(lambda x: x.replace('draw_', '') if x in
                              draw_cols else x), inplace=True)
    new_draws = [col for col in yld_draws.columns if col.isdigit()]
    yld_draws = pd.melt(yld_draws, id_vars=index_cols,
                        value_vars=new_draws, var_name='draw',
                        value_name='yld_rate')
    csv_draws = yld_draws.set_index('location_id')
    csv_draws.to_csv('{yld_tmp}/{location}_{year}_draws.csv'.format(
            yld_tmp=yld_tmp, location=location, year=year))


if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--yld_tmp",
            help="yld tmp directory",
            default="DIRECTORY",
            type=str)
    parser.add_argument(
            "--yld_version",
            help="yld version",
            default=0,
            type=int)
    parser.add_argument(
            "--root_dir",
            help="root directory",
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
    yld_tmp = args.yld_tmp
    yld_version = args.yld_version
    root_dir = args.root_dir
    location = args.location
    year = args.year
    n_draws = args.n_draws

    run_yld_compile(yld_tmp, yld_version, root_dir, location, year, n_draws)
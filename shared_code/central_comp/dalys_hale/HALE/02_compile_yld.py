from __future__ import division

import argparse
import pandas as pd
from HALE_summary import calc_summary
from transmogrifier.draw_ops import get_draws

def run_yld_compile(yld_tmp, yld_dir, yld_version, root_dir, location, ar,
                    n_draws):
    if ar:
        years = range(1990, 2017)
    else:
        years = [1990, 1995, 2000, 2005, 2010, 2016]
    pops = pd.read_csv('{root_dir}/PATH/pop.csv'.format(root_dir=root_dir))
    index_cols = ['location_id', 'age_group_id', 'sex_id', 'year_id']

    if yld_version == 0:
        yld_draws = get_draws("cause_id", source="como", gbd_id=294,
                              measure_ids=3, location_ids=location,
                              year_ids=years, sex_ids=[1,2], n_draws=n_draws,
                              resample=True)
    else:
        yld_draws = get_draws("cause_id", source="como", gbd_id=294,
                              measure_ids=3, location_ids=location,
                              year_ids=years, sex_ids=[1,2],
                              version=yld_version, n_draws=n_draws,
                              resample=True)
    draw_cols = [col for col in yld_draws.columns if 'draw' in col]
    yld_draws = yld_draws[index_cols + draw_cols]

    yld_draws = yld_draws.merge(pops, on=index_cols)
    yld_draws = yld_draws.set_index(index_cols)
    yld_draws[draw_cols] = yld_draws[draw_cols].multiply(
            yld_draws['population'], axis='index')
    yld_draws.drop('population', axis=1, inplace=True)
    yld_draws = yld_draws.reset_index()

    yld_draws.loc[yld_draws['age_group_id'].isin(range(2,5) + [164]),
                  'age_group_id'] = 28
    ages = range(5, 21) + range(30, 33) + [28, 235]
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
    csv_draws.to_csv('{yld_tmp}/{location}_draws.csv'.format(yld_tmp=yld_tmp,
                     location=location))

    summ_cols = ['yld_rate']
    calc_summary(yld_draws, summ_cols, yld_dir, location)


if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--yld_tmp",
            help="yld tmp directory",
            dUSERt="/PATH",
            type=str)
    parser.add_argument(
            "--yld_dir",
            help="yld summary directory",
            dUSERt="/PATH",
            type=str)
    parser.add_argument(
            "--yld_version",
            help="yld version",
            dUSERt=146,
            type=int)
    parser.add_argument(
            "--root_dir",
            help="root directory",
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
    yld_tmp = args.yld_tmp
    yld_dir = args.yld_dir
    yld_version = args.yld_version
    root_dir = args.root_dir
    location = args.location
    ar = args.ar
    n_draws = args.n_draws

    run_yld_compile(yld_tmp, yld_dir, yld_version, root_dir, location, ar,
                    n_draws)

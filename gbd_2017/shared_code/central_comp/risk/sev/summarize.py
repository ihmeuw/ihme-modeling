import argparse
import os
import re
from glob import glob
import numpy as np
import pandas as pd
import itertools
from multiprocessing import Pool

from adding_machine.summarizers import (Globals,
                                        combine_sexes_indf,
                                        combine_ages)
from core_maths.summarize import (get_summary, pct_change)
from db_queries import (get_demographics,
                        get_location_metadata,
                        get_ids)
from db_queries.get_age_metadata import get_age_weights
from draw_sources.draw_sources import DrawSink, DrawSource
from ihme_dimensions import dimensionality, gbdize


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sev_version_id', type=int)
    parser.add_argument('--location_id', type=int)
    parser.add_argument('--year_id', type=int, nargs='+')
    parser.add_argument('--change', action='store_true', default=False)
    parser.add_argument('--gbd_round_id', type=int)

    args = parser.parse_args()
    sev_version_id = args.sev_version_id
    location_id = args.location_id
    year_id = args.year_id
    change = args.change
    gbd_round_id = args.gbd_round_id

    if change:
        change_intervals = [(1990, 2017), (1990, 2007), (2007, 2017)]
    else:
        change_intervals = None

    return (sev_version_id, location_id, gbd_round_id, year_id, change_intervals)


def fill_square(df, col, gbd_round_id):
    '''make data square across a column for a set of index columns'''
    demo = get_demographics(gbd_team='epi', gbd_round_id=gbd_round_id)
    draw_cols = list(df.filter(like='draw_').columns)
    index_cols = list(set(df.columns) - set(draw_cols))
    index_cols.remove(col)
    index_dct = {
        tuple(index_cols): list(set(
            tuple(x) for x in df[index_cols].values)),
        col: demo[col]
    }
    data_dct = {'draw_cols': draw_cols}
    gbdizer = gbdize.GBDizeDataFrame(
        dimensionality.DataFrameDimensions(index_dct, data_dct))
    return gbdizer.fill_empty_indices(df, 0)


def summarize_loc_rei(source,
                      location_id,
                      rei_id,
                      year_id,
                      change_intervals,
                      gbd_round_id):
    '''aggregate age and sex then calc mean ui for single and multi year
    for one location risk pair'''
    if change_intervals:
        change_years = [i for i in itertools.chain(*change_intervals)]
    else:
        change_years = []

    multi_yrs = []
    single = []
    for year in year_id:
        df = source.content(filters={'location_id': location_id,
                                     'year_id': year,
                                     'rei_id': rei_id})
        both_sex = combine_sexes_indf(fill_square(df, 'sex_id', gbd_round_id))
        df = df.append(both_sex)
        age_agg = combine_ages(fill_square(df, 'age_group_id', gbd_round_id),
                               gbd_compare_ags=True)
        df = df.append(age_agg)
        draw_cols = [c for c in df if c.startswith('draw_')]
        single.append(get_summary(df, draw_cols))
        if year in change_years:
            multi_yrs.append(df)

    single = pd.concat(single)
    single = single[[
       'location_id', 'year_id', 'age_group_id', 'sex_id',
       'measure_id', 'metric_id', 'rei_id', 'mean', 'lower',
       'upper']]
    single.rename(columns={'mean': 'val'}, inplace=True)

    multi_yrs = pd.concat(multi_yrs)
    multi = []
    for ci in change_intervals:
        draw_cols = [c for c in multi_yrs if c.startswith('draw_')]
        chg_df = pct_change(multi_yrs, ci[0], ci[1], 'year_id', draw_cols)
        draw_cols = [c for c in chg_df if c.startswith('draw_')]
        multi.append(get_summary(chg_df, draw_cols))
    multi = pd.concat(multi)
    multi = multi[[
       'location_id', 'year_start_id', 'year_end_id',
       'age_group_id', 'sex_id', 'measure_id', 'rei_id',
       'metric_id', 'pct_change_means', 'lower', 'upper']]
    multi.rename(columns={'pct_change_means': 'val'}, inplace=True)

    return single, multi


def summ_loc(args):
    try:
        summ, change_summ = summarize_loc_rei(*args[0])
        return summ, change_summ
    except Exception as e:
        print(args)
        print(e)
        return None


def summarize_loc(source,
                  drawdir,
                  outdir,
                  location_id,
                  year_id,
                  rei_ids,
                  change_intervals=None,
                  gbd_round_id=5):
    '''summarize every rei for a single location'''
    # Set global age weights
    gbd_round_map = get_ids('gbd_round')
    gbd_round = gbd_round_map.loc[
        gbd_round_map.gbd_round_id == gbd_round_id].gbd_round.item()
    Globals.aw = get_age_weights(gbd_round_id=int(gbd_round_id))
    # Set global population
    pops = []
    popfiles = glob(os.path.join(drawdir, 'population_*.csv'))
    for popfile in popfiles:
        pops.append(pd.read_csv(popfile))
    pops = pd.concat(pops).drop_duplicates(
        subset=['location_id', 'age_group_id', 'year_id', 'sex_id'])
    Globals.pop = pops.rename(columns={'population': 'pop_scaled'})

    pool = Pool(10)
    results = pool.map(summ_loc, [(
        (source, location_id, rei, year_id, change_intervals, gbd_round_id), {})
        for rei in rei_ids])
    pool.close()
    pool.join()
    results = [res for res in results if isinstance(res, tuple)]
    results = list(zip(*results))

    single_year = pd.concat([res for res in results[0] if res is not None])
    single_year = single_year[
        ['rei_id', 'location_id', 'year_id', 'age_group_id', 'sex_id',
         'measure_id', 'metric_id', 'val', 'lower', 'upper']]
    single_file = os.path.join(outdir, 'single_year_{}.csv'.format(location_id))
    single_year.to_csv(single_file, index=False)
    os.chmod(single_file, 0o775)

    multi_year = pd.concat(results[1])
    if len(multi_year) > 0:
        multi_year = multi_year[
            ['rei_id', 'location_id', 'year_start_id', 'year_end_id',
             'age_group_id', 'sex_id', 'measure_id', 'metric_id', 'val',
             'lower', 'upper']]
        multi_year.replace([np.inf, -np.inf], np.nan)
        multi_year.dropna(inplace=True)
        multi_file = os.path.join(outdir, 'multi_year_{}.csv'.format(location_id))
        multi_year.to_csv(multi_file, index=False)
        os.chmod(multi_file, 0o775)


if __name__ == '__main__':
    (sev_version_id, location_id, gbd_round_id, year_id,
     change_intervals) = parse_arguments()

    drawdir = 'FILEPATH/{}/draws/'.format(sev_version_id)
    outdir = 'FILEPATH/{}/summaries/'.format(sev_version_id)

    # identify rei_ids from the csvs in the draw_dir
    files = glob(os.path.join(drawdir, '*'))
    rei_ids = [int(os.path.basename(file)) for file in files if 'population' not in file]

    # Instantiate draw source
    source = DrawSource(
        params={'draw_dir': drawdir,
                'file_pattern': '{rei_id}/{location_id}.csv'})

    summarize_loc(source,
                  drawdir,
                  outdir,
                  location_id,
                  year_id,
                  rei_ids,
                  change_intervals=change_intervals,
                  gbd_round_id=gbd_round_id)

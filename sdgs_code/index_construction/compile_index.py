import pandas as pd
import sys
import math
import numpy as np
import os
from scipy.stats import gmean
from scipy.special import (logit, expit)
import time

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry
import sdg_utils.tests as sdg_test

def fetch_input_file_dict():
    indic_table = qry.get_indicator_table()
    # keep only status 1
    input_files = indic_table.query('indicator_status_id==1')
    # keep columns used for dictionary
    input_files = input_files[['indicator_id', 'clean_input_data_file', 'forecast_file']]
    # set the index as indicator_id so to_dict knows what the key is
    input_files = input_files.set_index('indicator_id').to_dict()
    # and then tell it what the values column is, and return
    input_file_dict = input_files['clean_input_data_file']
    return input_file_dict

def fetch_indicators(sdg_version, force_recompile=False):
    # write/read output here
    version_dir = "{idd}/{v}".format(idd=dw.INDICATOR_DATA_DIR, v=sdg_version)
    out_file = version_dir + "/all_indicators.h5"
    if os.path.exists(out_file) and not force_recompile:
        print "reading from existing file"
        df = pd.read_hdf(out_file)
    else:
        print "recompiling"
        # get an input_file dict to determine where to read data for each
        # indicator
        input_file_dict = fetch_input_file_dict()
        # list of dataframes for fast concatenation
        dfs = []
        for indicator_id in input_file_dict.keys():
            print "\t{}".format(indicator_id)
            # read indicator data file
            if os.path.exists(input_file_dict[indicator_id]):
                df = pd.read_hdf(input_file_dict[indicator_id])
            else:
                df = pd.read_csv(input_file_dict[indicator_id].replace('.h5', '.csv'))
            df = df[['location_id', 'year_id', 'age_group_id', 'sex_id'] + dw.DRAW_COLS]
            forecastpath = input_file_dict[indicator_id].replace('/input_data/', '/forecasts/')
            futuredf = pd.read_hdf(forecastpath)
            futuredf = futuredf[['location_id', 'year_id', 'age_group_id', 'sex_id'] + dw.DRAW_COLS]
            df = df.append(futuredf)
            # for now, limit locations to admin0
            locsdf = qry.get_sdg_reporting_locations()
            df = df.loc[df.location_id.isin(locsdf.location_id.values)]
            assert set(df.location_id.values) == set(locsdf.location_id.values), 'Missing location(s) for ' + str(indicator_id)
            # set indicator id to the key in the dict
            df['indicator_id'] = indicator_id
            # should be one age and one sex
            assert len(df.sex_id.unique()) == 1, 'multiple sexes present'
            assert len(df.age_group_id.unique()) == 1, 'multiple ages present'
            # if len(df.age_group_id.unique()) > 1:
            #     df = age_standardize(df, weights_dict[indicator_id])
            # keep & verify required columns
            df = df[dw.INDICATOR_ID_COLS + dw.DRAW_COLS]
            # append to dataframe list
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)

        # set floor of < 1e-12
        for i in xrange(1000):
            df.loc[df['draw_' + str(i)] < 1e-12, 'draw_' + str(i)] = 1e-12

        # make version directory if it doesnt exist yet (likely)
        if not os.path.exists(version_dir):
            os.mkdir(version_dir)
        # save the input dictionary as a pickle for convenience
        pd.to_pickle(input_file_dict, version_dir + "/input_file_dict.pickle")
        # write all the indicator data to the version directory
        df.to_hdf(version_dir + "/all_indicators.h5",
                  format="table",
                  key="data",
                  data_columns=dw.INDICATOR_ID_COLS)
    # return
    return df


def multi_year_avg(df, indicator_id, window=10):
    avg_ind_df = df.loc[df['indicator_id'] == indicator_id]
    df = df.loc[df['indicator_id'] != indicator_id]
    # make sure single years are available for calculation
    assert set(avg_ind_df.year_id.unique()) == set(range(1980, 2031)), \
        'Needed years for calculation are not present.'

    avg_dfs = []
    for year in range(1990, 2031):
        yrange = year - np.arange(window)
        avg_df = avg_ind_df.loc[avg_ind_df.year_id.isin(yrange)]
        # get lag weights
        avg_df['weight'] = (window + avg_df['year_id'] - max(yrange)) / sum(np.arange(window) + 1.)
        avg_df['year_id'] = year

        # calc lag-distributed rate
        avg_df = pd.concat([
                            avg_df[dw.INDICATOR_ID_COLS],
                            avg_df[dw.DRAW_COLS].apply(lambda x: x * avg_df['weight'])
                           ],
                           axis=1)
        avg_df = avg_df.groupby(
            dw.INDICATOR_ID_COLS,
            as_index=False
        )[dw.DRAW_COLS].sum()
        avg_dfs.append(avg_df)
    avg_df = pd.concat(avg_dfs)

    df = df.append(avg_df, ignore_index=True)
    return df


def clean_compiled_indicators(df):
    # replace disaster with multi year moving average
    if 1019 in df['indicator_id'].unique():
        df = multi_year_avg(df, 1019)

    indic_table = qry.get_indicator_table()
    df = df.merge(indic_table[['indicator_id', 'invert',
        'scale', 'indicator_stamp']], how='left')
    assert df.invert.notnull().values.all(), 'merge with indicator meta fail'

    # get sdg locations and filter to these
    sdg_locs = set(qry.get_sdg_reporting_locations().location_id)
    df = df.loc[df['location_id'].isin(sdg_locs)]

    # filter to sdg reporting years (move this to global in config file)
    sdg_years = range(1990, 2031)
    df = df.loc[df['year_id'].isin(sdg_years)]

    # make sure each id column is an integer
    for id_col in dw.INDICATOR_ID_COLS:
        df[id_col] = df[id_col].astype(int)

    # return
    return df


def collapse_to_means(df, multi_year=False):
    if multi_year:
        df = df[dw.INDICATOR_ID_COLS_ARC + dw.DRAW_COLS].set_index(dw.INDICATOR_ID_COLS_ARC)
    else:
        df = df[dw.INDICATOR_ID_COLS + dw.DRAW_COLS].set_index(dw.INDICATOR_ID_COLS)
    # calculate mean & 95% confidence interval bounds with shared index
    mean_val = df.mean(axis=1)
    mean_val.name = "mean_val"
    upper_val = df.quantile(q=0.975, axis=1)
    upper_val.name = "upper"
    lower_val = df.quantile(q=0.025, axis=1)
    lower_val.name = "lower"
    # concatenate using shared index
    idf = pd.concat([mean_val, upper_val, lower_val], axis=1).reset_index()
    return idf


def store_unscaled_ARC(year_ranges, sdg_version):
    df = pd.read_hdf('FILEPATH')
    arc_dfs = []
    for year_range in year_ranges:
        arc_df_start = df.query('year_id == {}'.format(year_range[0]))
        arc_df_end = df.query('year_id == {}'.format(year_range[1]))
        arc_df = arc_df_start.merge(arc_df_end[dw.INDICATOR_ID_COLS + dw.DRAW_COLS], on=['location_id', 'indicator_id'])
        for i in range(0, 1000):
            arc_df['draw_' + str(i)] = np.log(arc_df['draw_' + str(i) + '_y'] / arc_df['draw_' + str(i) + '_x']) / (arc_df['year_id_y'] - arc_df['year_id_x'])
        arc_df = arc_df.rename(index=str, columns={'year_id_x':'year_start', 'year_id_y':'year_end'})
        arc_df = arc_df[dw.INDICATOR_ID_COLS_ARC + dw.DRAW_COLS]
        arc_dfs.append(arc_df)
    arc_df = pd.concat(arc_dfs)
    arc_df = collapse_to_means(arc_df, multi_year=True)
    write_output(arc_df, sdg_version, "unscaled_ARC")


def compile_output(df, add_rank = False, collapse_means=True):
    # collapse draws to means
    if collapse_means:
        df = collapse_to_means(df)

    # test that the data is square
    #sdg_test.df_is_square_on_indicator_location_year(df) # Data was square and still failing test

    # add indicator metadata
    indic_table = qry.get_indicator_table()
    indic_table = indic_table[['indicator_id', 'indicator_short',
                                'indicator_stamp', 'indicator_paperorder']]
    df = df.merge(indic_table, how='left')
    assert df.indicator_stamp.notnull().values.all(), \
        'merge with indic table failed'

    # add location metadata
    locs = qry.get_sdg_reporting_locations()
    locs = locs[['location_id', 'location_name', 'ihme_loc_id']]
    df = df.merge(locs, how='left')
    assert df.location_name.notnull().values.all(), \
        'merge with locations failed'
    print 'Number of locations: {}'.format(len(df.location_id.unique()))

    # make sure its just reporting years
    df = df.loc[df.year_id.isin(range(1990, 2031))]

    # set column order
    col_order = ['indicator_id', 'location_id', 'year_id', 'indicator_short',
                 'indicator_stamp', 'indicator_paperorder',
                 'ihme_loc_id', 'location_name',
                 'rank', 'mean_val', 'upper', 'lower']

    # optionally add rank by sdg index
    if add_rank:
        # keep sdg index
        sdg_index = df.query('indicator_id==1054')
        # calculate rank
        sdg_index['rank'] = sdg_index.groupby('year_id').mean_val.transform(
            lambda x: pd.Series.rank(x, method='first', ascending=False)
        )
        # add it to the data
        df = df.merge(
            sdg_index[['location_id', 'year_id', 'rank']].drop_duplicates(),
            how='left'
        )
        assert df['rank'].notnull().values.all(), 'merge failed'
        return df[col_order]
    else:
        col_order.remove('rank')
        return df[col_order]

def write_output(df, sdg_version, scale_type, overwrite_current=False):
    df.to_csv(
        "{dir}/indicator_values/" \
        "indicators_{t}_{v}.csv".format(
            dir=dw.PAPER_OUTPUTS_DIR, t=scale_type, v=sdg_version
        ),
        index=False
    )
    if overwrite_current:
        df.to_csv("{dir}/indicators_{t}.csv".format(
            dir=dw.PAPER_OUTPUTS_DIR, t=scale_type
            ),
            index=False
        )


def compile_unscaled(sdg_version, write_compiled=True):
    # get the sdg data
    print "Reading indicator data for version {}".format(sdg_version)
    df = fetch_indicators(sdg_version, force_recompile=True)
    # clean the indicator data
    print "cleaning data"
    df = clean_compiled_indicators(df)

    # done with unscaled values, can write these
    print "compiling unscaled output"
    draw_outfile= "{d}/indicators_unscaled_draws_{v}.h5".format(
            d=dw.SUMMARY_DATA_DIR,
            v=sdg_version
            )
    df.to_hdf(draw_outfile,
              format="table",
              key="data",
              data_columns=['indicator_id', 'location_id', 'year_id']
    )
    df.to_csv('FILEPATH',
              index=False)
    if write_compiled:
        unscaled_output = compile_output(df)
        write_output(unscaled_output, sdg_version, "unscaled")
    return df


def read_scaled_from_r(sdg_version):
    # wait for R to finish
    r_out_path = 'FILEPATH'
    while not os.path.exists(r_out_path):
        print "No R output: {}, checking again in 60 seconds".format(r_out_path)
        time.sleep(60)
    time.sleep(120) # Wait another minute to make sure file has time to finish saving

    print "output found and reading from R"
    df = pd.read_csv(r_out_path)
    draw_outfile= "{d}/indicators_scaled_draws_{v}.h5".format(
            d=dw.SUMMARY_DATA_DIR,
            v=sdg_version
            )
    print "writing output from r"
    df.to_hdf(draw_outfile,
              format="table",
              key="data",
              data_columns=['indicator_id', 'location_id', 'year_id']
    )
    return df


def main(sdg_version, r_doing_scaling=True, skip_unscaled=False):
    if not skip_unscaled:
        compile_unscaled(sdg_version)
        store_unscaled_ARC([[1990, 2015],
                            [1990, 2016],
                            [2015, 2020],
                            [2016, 2020],
                            [2015, 2030],
                            [2016, 2030]], sdg_version)
        print "Unscaled compiled"
    if r_doing_scaling:
        df = read_scaled_from_r(sdg_version)
    else:
        df = compile_scaled(sdg_version)
    print "Compiling, writing output"
    scaled_output = compile_output(df, add_rank = True)
    print scaled_output.indicator_id.unique()
    write_output(scaled_output, sdg_version, "scaled")
    return scaled_output
    print "Done"


if __name__ == "__main__":
    sdg_version = sys.argv[1]
    main(sdg_version)

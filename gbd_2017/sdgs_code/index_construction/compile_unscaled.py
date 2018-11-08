import pandas as pd
import sys
import math
import numpy as np
import os

from getpass import getuser

sys.path.append(SDG_REPO)
import sdg_utils.draw_files as dw
import sdg_utils.queries as qry
import sdg_utils.tests as sdg_test


def fetch_input_file_dict():
    """Create a dictionary from indicator_id to a clean data file."""
    df = pd.read_csv('FILEPATH')
    input_file_dict = dict(zip(df['indicator_id'].tolist(), df['past'].tolist()))
    input_file_dict[1096] = input_file_dict[1096].replace('1096', '1096_unscaled')
    return input_file_dict


def fetch_forecast_file_dict():
    """Create a dictionary from indicator_id to a forecast file."""
    df = pd.read_csv('FILEPATH')
    forecast_file_dict = dict(zip(df['indicator_id'].tolist(), df['future'].tolist()))
    forecast_file_dict[1096] = forecast_file_dict[1096].replace('1096', '1096_unscaled')
    return forecast_file_dict


def fetch_indicators(sdg_version=dw.SDG_VERS, force_recompile=False):
    """Fetch all indicator data and save in shared scratch space."""
    # write/read output here
    path = "{idd}/gbd2017/".format(idd=dw.INDICATOR_DATA_DIR)
    out_file = path + "all_indicators_unscaled_v{}.feather".format(sdg_version)
    if os.path.exists(out_file) and not force_recompile:
        print "reading from existing file"
        df = pd.read_feather(out_file)
    else:
        print "compiling {}".format(sdg_version)
        print out_file
        # get an input_file dict to determine where to read data for each
        input_file_dict = fetch_input_file_dict()
        forecast_file_dict = fetch_forecast_file_dict()

        # list of dataframes for fast concatenation
        dfs = []

        for indicator_id in input_file_dict.keys():

            # read indicator data file if both past and future exist
            if os.path.exists(input_file_dict[indicator_id]) and os.path.exists(forecast_file_dict[indicator_id]):
                print "\t{}".format(indicator_id)

                df = pd.read_feather(input_file_dict[indicator_id])
                if len(df.sex_id.unique()) > 1:
                	df = df.loc[df.sex_id == 3,:]
                df = df[['location_id', 'year_id', 'age_group_id', 'sex_id'] + dw.DRAW_COLS]

            	futuredf = pd.read_feather(forecast_file_dict[indicator_id])

                if indicator_id == 1037:
                    futuredf['sex_id'] = 3
                    futuredf['age_group_id'] = 22
                    futuredf.rename(index=str, columns={'draw_1000': 'draw_0',
                                                        'year': 'year_id',
                                                        'iso3': 'ihme_loc_id'}, inplace=True)
                    futuredf[(futuredf.year_id >= 2018) & (futuredf.year_id <= 2030)]

                    locs = qry.get_sdg_reporting_locations(level_3=True)
                    locs = locs[['location_id', 'ihme_loc_id']]
                    futuredf = futuredf.merge(locs, how='left')

            	if len(futuredf.sex_id.unique()) > 1:
                	futuredf = futuredf.loc[futuredf.sex_id == 3,:]
                if 'scenario' in futuredf.columns:
                    futuredf = futuredf.loc[futuredf.scenario == 0,:]
                futuredf = futuredf[futuredf.year_id != 2017]
            	futuredf = futuredf[['location_id', 'year_id', 'age_group_id', 'sex_id'] + dw.DRAW_COLS]

                if indicator_id in [1033, 1037]:
                    futuredf = futuredf.loc[futuredf.year_id.isin(range(2018, 2031))]

            	df = df.append(futuredf)
            	
            	# set indicator id to the key in the dict
            	df['indicator_id'] = indicator_id

                if len(df.age_group_id.unique() > 1):
                    df = df[df.age_group_id != 159]
            	
            	# should be one age and one sex
            	assert len(df.sex_id.unique()) == 1, 'multiple sexes present'
            	assert len(df.age_group_id.unique()) == 1, 'multiple ages present'

            	dfs.append(df)

        print "concatenating"
        df = pd.concat(dfs, ignore_index=True)

        # set floor of < 1e-12
        for i in xrange(1000):
            df.loc[df['draw_' + str(i)] < 1e-12, 'draw_' + str(i)] = 1e-12
            df.loc[df['draw_' + str(i)].isnull(), 'draw_' + str(i)] = 1e-12

        # remove level 6 (UTLAs) and level 5 india urban/rural (but keep English regions)
        locsdf = qry.get_sdg_reporting_locations().loc[:, ['location_id', 'level']]
        locsdf = locsdf.loc[locsdf['level'] != 6]
        locsdf = locsdf.loc[~( (locsdf['level'] == 5) & (~locsdf['location_id'].isin(range(4618, 4627))) )]
        df = df.merge(locsdf, on = 'location_id')

        # write all the indicator data to the directory
        df = df[dw.INDICATOR_ID_COLS + ['level'] + dw.DRAW_COLS]
        df.columns = df.columns.astype(str)
        print "outputting feather"
        df.to_feather(out_file)

    return df
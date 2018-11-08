import sys
import pandas as pd
import xarray as xr
import numpy as np

import argparse

from functools import partial
from multiprocessing import Pool

sys.path.append('FILEPATH')
from fbd_scenarios_hssa.scenarios import arc_method
from fbd_scenarios_hssa.omega_selection_strategies import use_average_omega_within_threshold

from getpass import getuser
if getuser() == 'USER':
    SDG_REPO = 'FILEPATH'

sys.path.append(SDG_REPO)
import sdg_utils.draw_files as dw

WEIGHTS_TO_TEST = np.arange(0., 2.2, 0.2).tolist()
LOCATION_FILE = 'FILEPATH'


def sdg_df_to_da(df,
                 index_cols=['location_id', 'year_id',
                             'age_group_id', 'sex_id'],
                 draw_cols=['draw_' + str(d) for d in xrange(1000)]):
    '''
    Go from SDG structure to dataarray.
    '''
    df = df.melt(
        id_vars=index_cols,
        value_vars=draw_cols,
        var_name='draw'
    )
    df['draw'] = df['draw'].str.replace('draw_', '').astype(int)
    df = df.set_index(index_cols + ['draw'])

    da = df.to_xarray()['value']

    return da


def da_to_sdg_df(da,
                 index_cols=['location_id', 'year_id',
                             'age_group_id', 'sex_id', 'scenario']):
    '''
    Go from dataarray back to SDG structure.
    '''
    df = da.to_dataframe(name='value')
    df = df.reset_index()
    df['draw'] = 'draw_' + df['draw'].astype(str)
    df = pd.pivot_table(
        df,
        values='value',
        index=index_cols,
        columns='draw'
    ).reset_index().rename_axis(None)

    return df


def load_past_data(sdg_data_type, sdg_data_type_version,
                   sdg_data_type_id, years):
    '''
    Load the formatted SDG indicator level data.
    '''
    # read in file
    df = pd.read_feather(
        'FILEPATH'.format(
            sdg_data_type, sdg_data_type_version, sdg_data_type_id
        )
    )

    df.loc[:, dw.DRAW_COLS] = df.loc[:, dw.DRAW_COLS].applymap(
        lambda x: 1e-12 if x<=1e-12 else x)

    # check demographics
    loc_df = pd.read_csv(LOCATION_FILE)
    loc_df = loc_df.loc[loc_df.level == 3]
    df = df.loc[df.location_id.isin(loc_df.location_id)]
    assert sorted(df.location_id.unique()) == \
        sorted(loc_df.location_id.unique()), \
        'Locations in data not as expected.'
    assert sorted(df.year_id.unique()) == range(years[0], years[2]),\
        'Years in data not as expected.'
    sexes = df.sex_id.unique().tolist()
    if len(sexes) == 3:
        df = df.loc[df.sex_id != 3]
    da = sdg_df_to_da(df)

    return da


def calc_rmse(predicted, observed, years):
    '''
    Get root-mean-square-error from forecasts.
    '''
    predicted = predicted.loc[{'year_id': years}]
    observed = observed.loc[{'year_id': years}]

    rmse = np.sqrt(((predicted - observed) ** 2).mean())

    return rmse.item()


def forecast_past(weight, da, years, output='future', trans='log'):
    '''
    Use mean ARC to project.
    '''
    if trans == 'log':
        da = np.log(da)
    elif trans == 'logit':
        da = np.log(da / (1 - da))
    future_da = arc_method(
        past_data_da=da.loc[{'year_id':range(years[0], years[1])}],
        years=years, weight_exp=weight, reference_scenario='mean',
        truncate=True, truncate_quantiles=(0.025, 0.975)
    )
    future_da = future_da.loc[{'year_id': range(years[1], years[2] + 1)}]
    if trans == 'log':
        da = np.exp(da)
        future_da = np.exp(future_da)
    elif trans == 'logit':
        da = np.exp(da) / (np.exp(da) + 1)
        future_da = np.exp(future_da) / (np.exp(future_da) + 1)
    if output == 'rmse':
        future_da = future_da.loc[{'scenario':0}]
        future_da = future_da.drop('scenario')
        rmse = calc_rmse(
            da.mean(dim='draw'), future_da.mean(dim='draw'),
            range(years[1], years[2])
        )
        return rmse
    elif output=='future':
        return future_da


def determine_weight(da, years, weights, trans='log'):
    '''
    Get RMSE for all weights in test group, use FHS methods to find the one
    to use. If there are five or more age groups, split weight testing into
    three batches.
    '''
    _forecast_past = partial(forecast_past, da=da, years=years, output='rmse', trans=trans)

    if len(weights) <= 11:
        p_num = len(weights)
    else:
        p_num = 11

    if len(np.unique(da['age_group_id'])) >= 5:
        p_nums = [4,4,3]
        weight_list = [weights[0:4], weights[4:8], weights[8:11]]
        
        rmses = []
        for i in range(0, 3):
            print 'batch' + ' ' + str(i + 1)
            pool = Pool(p_nums[i])
            sub_rmses = pool.map(_forecast_past, weight_list[i])
            pool.close()
            pool.join()
            rmses.append(sub_rmses)
            gc.collect()

        rmses = [jj for j in rmses for jj in j]

    else:
        pool = Pool(p_num)
        rmses = pool.map(_forecast_past, weights)
        pool.close()
        pool.join()

    rmse_da = xr.DataArray(rmses, coords=[weights], dims=['weight'])
    norm_rmse_da = rmse_da / rmse_da.min()
    weight = use_average_omega_within_threshold(norm_rmse_da, 0.05)

    rmse_df = rmse_da.to_dataframe(name='value').reset_index()
    rmse_df['selected_weight'] = weight

    return weight, rmse_df


def zero_malaria_elim_locs(future_df):
    print 'zeroing out malaria elim locs'

    elimdf = pd.read_csv('FILEPATH')
    
    elimlocs = elimdf.query('year_id == 2017 and any_malaria_endemic == 0')['location_id'].unique()
    elimdf = future_df.query('location_id in {}'.format(list(elimlocs)))
    endemdf = future_df.query('location_id not in {}'.format(list(elimlocs)))

    elimdf = pd.concat(
            [
                elimdf[INDIC_COLS],
                elimdf[DRAW_COLS].apply(lambda x: x * 0 + 1e-12)
            ],
            axis=1
            )

    future_df = elimdf.append(endemdf)

    return future_df


def save_sdg(da, rmse_df,
             sdg_data_type, sdg_data_type_version, sdg_data_type_id):
    '''
    Write SDG dataframe to forecast storage location.
    '''
    df = da_to_sdg_df(da)

    if sdg_data_type_id == '95':
        df = zero_malaria_elim_locs(df)

    df.to_feather(
        'FILEPATH'.format(
            sdg_data_type, sdg_data_type_version, sdg_data_type_id
        )
    )
    rmse_df.to_csv(
        'FILEPATH'.format(
            sdg_data_type, sdg_data_type_version, sdg_data_type_id
        ),
        index=False
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sdg_data_type', type=str,
                        help='example: codcorrect')
    parser.add_argument('--sdg_data_type_version', type=str,
                        help='example: 77')
    parser.add_argument('--sdg_data_type_id', type=str,
                        help='example: 1020')
    parser.add_argument('--years', type=int, nargs='+',
                        help='example: 1990 2008 2018 2030')
    args = parser.parse_args()

    logit_indicators = ['LOGIT INDICATORS']

    if args.sdg_data_type_id in logit_indicators:
        trans = 'logit'
        print("trans = logit")
    else:
        trans = 'log'
        print("trans = log")

    # load SDG indicator
    print 'loading past data'
    past_da = load_past_data(
        args.sdg_data_type, args.sdg_data_type_version,
        args.sdg_data_type_id, args.years
    )

    # get omega (store possible weights)
    print 'determining weight'
    weight, rmse_df = determine_weight(
        past_da, args.years[0:3], WEIGHTS_TO_TEST,
        trans=trans
    )

    # make forecasts
    print 'forecasting'
    future_da = forecast_past(
        weight, past_da, [args.years[0]] + args.years[2:4],
        trans=trans
    )

    # store future data
    print 'saving forecasts'
    save_sdg(
        future_da, rmse_df, args.sdg_data_type, args.sdg_data_type_version,
        args.sdg_data_type_id
    )


if __name__ == '__main__':
    main()

from typing import Dict, List, NamedTuple

import numpy as np
import pandas as pd

from covid_model_seiir_pipeline.lib import static_vars


def compute_reimposition_threshold(past_deaths, population, reimposition_threshold, max_threshold):
    population = population.reindex(past_deaths.index, level='location_id')
    death_rate = past_deaths / population
    death_rate = (death_rate
                  .groupby('location_id')
                  .apply(lambda x: x.iloc[-14:])
                  .reset_index(level=0, drop=True))
    days_over_death_rate = (
        (death_rate > reimposition_threshold.loc[death_rate.index])
         .groupby('location_id')
         .sum()
    )
    bad_locs = days_over_death_rate[days_over_death_rate >= 7].index
    reimposition_threshold.loc[bad_locs] = max_threshold.loc[bad_locs]

    # Do it a second time to some crazy stuff happening in central europe.
    days_over_death_rate = (
        (death_rate > reimposition_threshold.loc[death_rate.index])
         .groupby('location_id')
         .sum()
    )
    bad_locs = days_over_death_rate[days_over_death_rate >= 7].index
    reimposition_threshold.loc[bad_locs] = 2*max_threshold.loc[bad_locs]
    # Locations that have shown a propensity to impose mandates at 
    # any sign of covid in their populations.
    immediate_lockdown_locations = [
        71, # Australia
    ]
    for location in immediate_lockdown_locations:
        reimposition_threshold.loc[location] = 0.1 / 1_000_000

    return reimposition_threshold


def compute_reimposition_date(deaths, population, reimposition_threshold,
                              min_wait, last_reimposition_end_date) -> pd.Series:
    location_dates = deaths.index.to_frame().reset_index(drop=True)
    deaths = deaths.reset_index(level='observed', drop=True)
    death_rate = ((deaths / population.reindex(deaths.index, level='location_id'))
                  .rename('death_rate'))
    reimposition_threshold = (reimposition_threshold
                              .reindex(death_rate.index)
                              .groupby('location_id')
                              .fillna(method='ffill'))

    over_threshold = death_rate > reimposition_threshold
    last_observed_date = (location_dates[location_dates.observed == 1]
                          .groupby('location_id')
                          .date
                          .max())
    last_observed_date.loc[:] = last_observed_date.max()
    min_reimposition_date = last_observed_date + min_wait

    previously_implemented = last_reimposition_end_date[last_reimposition_end_date.notnull()].index
    min_reimposition_date.loc[previously_implemented] = (
            last_reimposition_end_date.loc[previously_implemented] + min_wait
    )
    min_reimposition_date = min_reimposition_date.loc[location_dates.location_id].reset_index(drop=True)
    after_min_reimposition_date = location_dates['date'] >= min_reimposition_date
    reimposition_date = (death_rate[over_threshold & after_min_reimposition_date.values]
                         .reset_index()
                         .groupby('location_id')['date']
                         .min()
                         .rename('reimposition_date'))
    return reimposition_date


def compute_mobility_lower_bound(mobility: pd.DataFrame, mandate_effect: pd.DataFrame) -> pd.Series:
    min_observed_mobility = mobility.groupby('location_id').min().rename('min_mobility')
    max_mandate_mobility = (mandate_effect
                            .sum(axis=1)
                            .rename('min_mobility')
                            .reindex(min_observed_mobility.index, fill_value=100))
    mobility_lower_bound = min_observed_mobility.where(min_observed_mobility <= max_mandate_mobility,
                                                       max_mandate_mobility)
    return mobility_lower_bound


def compute_rampup(reimposition_date: pd.Series,
                   percent_mandates: pd.DataFrame,
                   days_on: pd.Timedelta) -> pd.DataFrame:
    rampup = pd.merge(reimposition_date, percent_mandates.reset_index(level='date'), on='location_id', how='left')
    rampup['rampup'] = rampup.groupby('location_id')['percent'].apply(lambda x: x / x.max())
    rampup['first_date'] = rampup.groupby('location_id')['date'].transform('min')
    rampup['diff_date'] = rampup['reimposition_date'] - rampup['first_date']
    rampup['date'] = rampup['date'] + rampup['diff_date'] + days_on
    rampup = rampup.reset_index()[['location_id', 'date', 'rampup']]
    return rampup


def compute_new_mobility(old_mobility: pd.Series,
                         reimposition_date: pd.Series,
                         mobility_lower_bound: pd.Series,
                         percent_mandates: pd.DataFrame,
                         days_on: pd.Timedelta) -> pd.Series:
    mobility = pd.merge(old_mobility.reset_index(level='date'), reimposition_date, how='left', on='location_id')
    mobility = mobility.merge(mobility_lower_bound, how='left', on='location_id')

    reimposes = mobility['reimposition_date'].notnull()
    dates_on = ((mobility['reimposition_date'] <= mobility['date'])
                & (mobility['date'] <= mobility['reimposition_date'] + days_on))
    mobility['mobility_explosion'] = mobility['min_mobility'].where(reimposes & dates_on, np.nan)

    rampup = compute_rampup(reimposition_date, percent_mandates, days_on)

    mobility = mobility.merge(rampup, how='left', on=['location_id', 'date'])
    post_reimplementation = ~(mobility['mobility_explosion'].isnull() & mobility['rampup'].notnull())
    mobility['mobility_explosion'] = mobility['mobility_explosion'].where(
        post_reimplementation,
        mobility['min_mobility'] * mobility['rampup']
    )

    idx_columns = ['location_id', 'date']
    mobility = (mobility[idx_columns + ['mobility', 'mobility_explosion']]
                .set_index(idx_columns)
                .sort_index()
                .min(axis=1))
    return mobility


class MandateReimpositionParams(NamedTuple):
    min_wait: pd.Timedelta
    days_on: pd.Timedelta
    reimposition_threshold: pd.Series
    max_threshold: pd.Series


def unpack_parameters(algorithm_parameters: Dict,
                      em_scalars: pd.Series) -> MandateReimpositionParams:
    min_wait = pd.Timedelta(days=algorithm_parameters['minimum_delay'])
    days_on = pd.Timedelta(days=static_vars.DAYS_PER_WEEK * algorithm_parameters['reimposition_duration'])
    reimposition_threshold = (algorithm_parameters['death_threshold'] / 1e6 * em_scalars).rename('threshold')
    max_threshold = (algorithm_parameters['max_threshold'] / 1e6 * em_scalars).rename('threshold')
    return MandateReimpositionParams(min_wait, days_on, reimposition_threshold, max_threshold)

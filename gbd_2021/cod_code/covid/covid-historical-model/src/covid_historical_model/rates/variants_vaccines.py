import sys
from typing import Tuple, List, Dict
from loguru import logger

import pandas as pd

from covid_historical_model.rates import age_standardization
from covid_historical_model.cluster import OMP_NUM_THREADS


def get_ratio_data_scalar(rate_age_pattern: pd.Series,
                          denom_age_pattern: pd.Series,
                          age_spec_population: pd.Series,
                          rate: pd.Series,
                          day_shift: int,
                          escape_variant_prevalence: pd.Series,
                          severity_variant_prevalence: pd.Series,
                          vaccine_coverage: pd.DataFrame,
                          population: pd.Series,
                          daily: pd.Series,
                          location_dates: List,
                          durations: Dict,
                          variant_risk_ratio: float,
                          verbose: bool = True,):
    location_ids = sorted(set([location_id for location_id, date in location_dates]))
    vv_rate, *_ = variants_vaccines(
        rate_age_pattern=rate_age_pattern.copy(),
        denom_age_pattern=denom_age_pattern.copy(),
        age_spec_population=age_spec_population.copy(),
        rate=rate.copy(),
        day_shift=day_shift,
        escape_variant_prevalence=escape_variant_prevalence.copy(),
        severity_variant_prevalence=severity_variant_prevalence.copy(),
        vaccine_coverage=vaccine_coverage.copy(),
        population=population.copy(),
        variant_risk_ratio=variant_risk_ratio,
        verbose=verbose,
    )
    daily_ratio_scalar = (rate / vv_rate).rename('daily_ratio_scalar')

    daily_infections = (daily / rate).dropna().rename('infections')
    daily_infections += 1

    daily_ratio_scalar = daily_ratio_scalar.to_frame().join(daily_infections, how='right')
    daily_ratio_scalar['daily_ratio_exp'] = (daily_ratio_scalar['daily_ratio_scalar']
                                             * daily_ratio_scalar['infections'])
    del daily_ratio_scalar['daily_ratio_scalar']
    daily_ratio_scalar = daily_ratio_scalar.sort_index().groupby(level=0).cumsum()

    ratio_scalars = []
    for location_id, date in location_dates:
        if day_shift - durations['exposure_to_seroconversion'] > 0:
            shifted_date = date + pd.Timedelta(days=day_shift - durations['exposure_to_seroconversion'])
        else:
            shifted_date = date - pd.Timedelta(days=durations['exposure_to_seroconversion'] - day_shift)
        if shifted_date > daily_ratio_scalar.loc[location_id].index.max():
            loc_date_data = daily_ratio_scalar.loc[location_id, daily_ratio_scalar.loc[location_id].index.max()]
        else:
            loc_date_data = daily_ratio_scalar.loc[location_id, shifted_date]
        loc_date_scalar = loc_date_data['daily_ratio_exp'] / loc_date_data['infections']
        ratio_scalars.append(pd.Series(loc_date_scalar, name='ratio_data_scalar',
                                 index=pd.MultiIndex.from_arrays([[location_id], [shifted_date]],
                                                                 names=['location_id', 'date'])))
    ratio_scalars = pd.concat(ratio_scalars)

    return ratio_scalars


def variants_vaccines(rate_age_pattern: pd.Series,
                      denom_age_pattern: pd.Series,
                      age_spec_population: pd.Series,
                      rate: pd.Series,
                      day_shift: int,
                      escape_variant_prevalence: pd.Series,
                      severity_variant_prevalence: pd.Series,
                      vaccine_coverage: pd.DataFrame,
                      population: pd.Series,
                      variant_risk_ratio: float,
                      verbose: bool = True,):
    escape_variant_prevalence = escape_variant_prevalence.reset_index()
    escape_variant_prevalence['date'] += pd.Timedelta(days=day_shift)
    escape_variant_prevalence = (escape_variant_prevalence
                                 .set_index(['location_id', 'date'])
                                 .loc[:, 'escape_variant_prevalence'])
    escape_variant_prevalence = pd.concat([rate, escape_variant_prevalence], axis=1)  # borrow axis
    escape_variant_prevalence = escape_variant_prevalence['escape_variant_prevalence'].fillna(0)
    
    severity_variant_prevalence = severity_variant_prevalence.reset_index()
    severity_variant_prevalence['date'] += pd.Timedelta(days=day_shift)
    severity_variant_prevalence = (severity_variant_prevalence
                                 .set_index(['location_id', 'date'])
                                 .loc[:, 'severity_variant_prevalence'])
    severity_variant_prevalence = pd.concat([rate, severity_variant_prevalence], axis=1)  # borrow axis
    severity_variant_prevalence = severity_variant_prevalence['severity_variant_prevalence'].fillna(0)

    lr_e = [f'cumulative_lr_effective_{variant_suffix}' for variant_suffix in ['wildtype', 'variant']]
    lr_ep = [f'cumulative_lr_effective_protected_{variant_suffix}' for variant_suffix in ['wildtype', 'variant']]
    hr_e = [f'cumulative_hr_effective_{variant_suffix}' for variant_suffix in ['wildtype', 'variant']]
    hr_ep = [f'cumulative_hr_effective_protected_{variant_suffix}' for variant_suffix in ['wildtype', 'variant']]
    vaccine_coverage = (vaccine_coverage
                        .loc[:, lr_e + lr_ep + hr_e + hr_ep]
                        .reset_index())
    vaccine_coverage['date'] += pd.Timedelta(days=day_shift)
    vaccine_coverage = vaccine_coverage.set_index(['location_id', 'date'])
    vaccine_coverage = pd.concat([rate.rename('rate'), vaccine_coverage], axis=1)  # borrow axis
    del vaccine_coverage['rate']
    vaccine_coverage = vaccine_coverage.fillna(0)
    
    # not super necessary...
    numerator = pd.Series(100, index=rate.index)
    numerator /= population
    
    denominator_a = (numerator / rate)
    denominator_ev = (numerator / (rate * variant_risk_ratio))
    denominator_sv = denominator_ev.copy()
    denominator_a *= (1 - (escape_variant_prevalence + severity_variant_prevalence)[denominator_a.index])
    denominator_ev *= escape_variant_prevalence[denominator_ev.index]
    denominator_sv *= severity_variant_prevalence[denominator_sv.index]

    numerator_a = (rate * denominator_a)
    numerator_ev = (rate * variant_risk_ratio * denominator_ev)
    numerator_sv = (rate * variant_risk_ratio * denominator_sv)
    
    if verbose:
        logger.info('Adjusting ancestral...')
    numerator_lr_a, numerator_hr_a, denominator_lr_a, denominator_hr_a = adjust_by_variant_classification(
        numerator=numerator_a,
        denominator=denominator_a,
        variant_suffixes=['wildtype', 'variant',],
        rate_age_pattern=rate_age_pattern,
        denom_age_pattern=denom_age_pattern,
        age_spec_population=age_spec_population,
        vaccine_coverage=vaccine_coverage,
        population=population,
    )
    if verbose:
        logger.info('Adjusting non-escape...')
    numerator_lr_sv, numerator_hr_sv, denominator_lr_sv, denominator_hr_sv = adjust_by_variant_classification(
        numerator=numerator_sv,
        denominator=denominator_sv,
        variant_suffixes=['wildtype', 'variant'],
        rate_age_pattern=rate_age_pattern,
        denom_age_pattern=denom_age_pattern,
        age_spec_population=age_spec_population,
        vaccine_coverage=vaccine_coverage,
        population=population,
    )
    if verbose:
        logger.info('Adjusting escape...')
    numerator_lr_ev, numerator_hr_ev, denominator_lr_ev, denominator_hr_ev = adjust_by_variant_classification(
        numerator=numerator_ev,
        denominator=denominator_ev,
        variant_suffixes=['variant',],
        rate_age_pattern=rate_age_pattern,
        denom_age_pattern=denom_age_pattern,
        age_spec_population=age_spec_population,
        vaccine_coverage=vaccine_coverage,
        population=population,
    )
    
    numerator_lr = numerator_lr_a + numerator_lr_ev + numerator_lr_sv
    denominator_lr = denominator_lr_a + denominator_lr_ev + denominator_lr_sv
    numerator_hr = numerator_hr_a + numerator_hr_ev + numerator_hr_sv
    denominator_hr = denominator_hr_a + denominator_hr_ev + denominator_hr_sv
    
    rate = (numerator_lr + numerator_hr) / (denominator_lr + denominator_hr)
    rate_lr = numerator_lr / denominator_lr
    rate_hr = numerator_hr / denominator_hr
    
    pct_inf_lr = denominator_lr / (denominator_lr + denominator_hr)
    pct_inf_hr = denominator_hr / (denominator_lr + denominator_hr)
    
    return rate, rate_lr, rate_hr, pct_inf_lr, pct_inf_hr


def adjust_by_variant_classification(numerator: pd.Series,
                                     denominator: pd.Series,
                                     variant_suffixes: List[str],
                                     rate_age_pattern: pd.Series,
                                     denom_age_pattern: pd.Series,
                                     age_spec_population: pd.Series,
                                     vaccine_coverage: pd.DataFrame,
                                     population: pd.Series,):
    lr_rate_rr, hr_rate_rr = age_standardization.get_risk_group_rr(
        rate_age_pattern.copy(),
        denom_age_pattern.copy()**0,  # REMOVE THIS IF WE WANT TO USE THE ACTUAL SERO AGE PATTERN
        age_spec_population.copy(),
    )
    rate_lr = (numerator / denominator) * lr_rate_rr
    rate_hr = (numerator / denominator) * hr_rate_rr

    lr_denom_rr, hr_denom_rr = age_standardization.get_risk_group_rr(
        denom_age_pattern.copy()**0,  # REMOVE THIS IF WE WANT TO USE THE ACTUAL SERO AGE PATTERN
        denom_age_pattern.copy()**0,  # REMOVE THIS IF WE WANT TO USE THE ACTUAL SERO AGE PATTERN
        age_spec_population.copy(),
    )
    denominator_lr = denominator * lr_denom_rr
    denominator_hr = denominator * hr_denom_rr

    numerator_lr = rate_lr * denominator_lr
    numerator_hr = rate_hr * denominator_hr

    population_lr, population_hr = age_standardization.get_risk_group_populations(age_spec_population)

    lr_e = [f'cumulative_lr_effective_{variant_suffix}' for variant_suffix in variant_suffixes]
    lr_ep = [f'cumulative_lr_effective_protected_{variant_suffix}' for variant_suffix in variant_suffixes]
    numerator_lr, denominator_lr = vaccine_adjustments(
        numerator_lr, denominator_lr,
        vaccine_coverage[lr_e].sum(axis=1) / population_lr,
        vaccine_coverage[lr_ep].sum(axis=1) / population_lr,
    )
    hr_e = [f'cumulative_hr_effective_{variant_suffix}' for variant_suffix in variant_suffixes]
    hr_ep = [f'cumulative_hr_effective_protected_{variant_suffix}' for variant_suffix in variant_suffixes]
    numerator_hr, denominator_hr = vaccine_adjustments(
        numerator_hr, denominator_hr,
        vaccine_coverage[hr_e].sum(axis=1) / population_hr,
        vaccine_coverage[hr_ep].sum(axis=1) / population_hr,
    )

    numerator_lr *= population_lr
    numerator_lr = numerator_lr.fillna(0)
    numerator_hr *= population_hr
    numerator_hr = numerator_hr.fillna(0)

    denominator_lr *= population_lr
    denominator_lr = denominator_lr.fillna(0)
    denominator_hr *= population_hr
    denominator_hr = denominator_hr.fillna(0)

    return numerator_lr, numerator_hr, denominator_lr, denominator_hr


def vaccine_adjustments(numerator: pd.Series,
                        denominator: pd.Series,
                        effective: pd.Series,
                        protected: pd.Series,) -> Tuple[pd.Series, pd.Series]:    
    numerator *= (1 - (effective[numerator.index] + protected[numerator.index]))
    denominator *= (1 - effective[denominator.index])

    return numerator, denominator

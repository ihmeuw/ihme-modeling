from collections import defaultdict
import functools
import multiprocessing
from typing import Tuple, TYPE_CHECKING

import numpy as np
import pandas as pd
import tqdm

from covid_model_seiir_pipeline.pipeline.regression.model.containers import (
    HospitalCensusData,
    HospitalMetrics,
    HospitalCorrectionFactors,
)
from covid_model_seiir_pipeline.pipeline.regression.model.ode_fit import (
    clean_infection_data_measure,
)

if TYPE_CHECKING:
    from covid_model_seiir_pipeline.pipeline.regression.specification import (
        HospitalParameters,
    )
    from covid_model_seiir_pipeline.pipeline.regression.data import (
        RegressionDataInterface,
    )


def load_admissions_and_hfr(data_interface: 'RegressionDataInterface',
                            n_draws: int, n_cores: int, show_pb: bool) -> Tuple[pd.Series, pd.Series]:
    _runner = functools.partial(
        _load_admissions_and_hfr_draw,
        data_interface=data_interface,
    )
    with multiprocessing.Pool(n_cores) as pool:
        draw_data = list(tqdm.tqdm(pool.imap(_runner, range(n_draws)), total=n_draws, disable=not show_pb))

    admissions, hfr = zip(*draw_data)
    admissions_mean = pd.concat(admissions, axis=1).mean(axis=1).rename('admissions')
    hfr_mean = pd.concat(hfr, axis=1).mean(axis=1).rename('hfr').loc[admissions_mean.index]

    return admissions_mean, hfr_mean


def _load_admissions_and_hfr_draw(draw_id: int,
                                  data_interface: 'RegressionDataInterface') -> Tuple[pd.Series, pd.Series]:
    past_infections_data = data_interface.load_past_infection_data(draw_id)
    infections = clean_infection_data_measure(past_infections_data, 'infections')
    ratio_data = data_interface.load_ratio_data(draw_id)

    admissions = convert_infections(infections, ratio_data.ihr, ratio_data.infection_to_admission)
    admissions = admissions.rename(f'draw_{draw_id}')

    hfr = (ratio_data.ihr / ratio_data.ifr).rename(f'draw_{draw_id}')
    hfr[hfr < 1] = 1

    return admissions, hfr


def convert_infections(infections: pd.Series, ratio: pd.Series, duration: int):
    
    result = (infections
              .groupby('location_id')
              .apply(lambda x: x.reset_index(level='location_id', drop=True).shift(duration, freq='D')))
    result = (result * ratio).groupby('location_id').bfill().dropna()
    return result


def _bound(low, high, value):
    """Helper to fix out of bounds probabilities.
    As written, we can have a negative probability of being in the icu and
    a > 1 probability of being intubated.
    """
    return np.maximum(low, np.minimum(high, value))


def get_p_icu_if_recover(prob_death: pd.Series, hospital_parameters: 'HospitalParameters'):
    """Get probability of going to ICU given the patient recovered
    This fixes the long term average of [# used ICU beds]/[# hospitalized]
    to be expected ICU ratio.
    """
    # noinspection PyTypeChecker
    prob_recover = 1 - prob_death

    icu_ratio = hospital_parameters.icu_ratio
    days_to_death_prob = hospital_parameters.hospital_stay_death + 1
    days_in_hosp_no_icu_prob = hospital_parameters.hospital_stay_recover + 1
    days_in_hosp_icu_prob = hospital_parameters.hospital_stay_recover_icu + 1
    days_in_icu_recover_prob = hospital_parameters.icu_stay_recover + 1

    # Fixme: For some reason, this can be negative.
    prob_icu_if_recover = (
            (icu_ratio * (days_to_death_prob * prob_death + days_in_hosp_no_icu_prob * prob_recover)
             - days_to_death_prob * prob_death)
            / ((icu_ratio * (days_in_hosp_no_icu_prob - days_in_hosp_icu_prob)
                + days_in_icu_recover_prob) * prob_recover)
    )

    return _bound(0, 1, prob_icu_if_recover)


def read_correction_data(correction_path):
    df = pd.read_csv(correction_path)
    df = df.loc[(df.age_group_id == 22) & (df.sex_id == 3), ['location_id', 'date', 'value']]
    return df


def _to_census(admissions: pd.Series, length_of_stay: int) -> pd.Series:
    return (admissions
            .groupby('location_id')
            .transform(lambda x: x.rolling(length_of_stay).sum())
            .fillna(0))


def compute_hospital_usage(admissions: pd.Series,
                           hfr: pd.Series,
                           hospital_parameters: 'HospitalParameters') -> HospitalMetrics:
    prob_death_given_admission = 1 / hfr
    prob_icu = get_p_icu_if_recover(prob_death_given_admission, hospital_parameters)
    prob_no_icu = 1 - prob_icu

    recovered_hospital_admissions = admissions * (1 - prob_death_given_admission)
    # Split people into those who go to ICU and those who don't and count the
    # days they spend in the hospital.
    recovered_hospital_census = (
        _to_census(prob_no_icu * recovered_hospital_admissions, hospital_parameters.hospital_stay_recover)
        + _to_census(prob_icu * recovered_hospital_admissions, hospital_parameters.hospital_stay_recover_icu)
    )
    # Scale down hospitalizations to those who go to ICU and shift forward.
    recovered_icu_admissions = (
        (prob_icu * recovered_hospital_admissions)
        .groupby('location_id')
        .shift(hospital_parameters.hospital_to_icu, fill_value=0)
    )
    # Count number of days those who go to ICU spend there.
    recovered_icu_census = _to_census(recovered_icu_admissions, hospital_parameters.icu_stay_recover)

    # Every death corresponds to a hospital admission shifted back some number
    # of days.
    dead_hospital_admissions = admissions * prob_death_given_admission
    # Count days from admission to get hospital census for those who die.
    dead_hospital_census = _to_census(dead_hospital_admissions, hospital_parameters.hospital_stay_death)
    # Assume people who die after entering the hospital are intubated in the
    # ICU for their full stay.
    dead_icu_admissions = dead_hospital_admissions.copy()
    dead_icu_census = dead_hospital_census.copy()

    def _combine(recovered, dead):
        # Drop data after the last admission since it will be incomplete.
        return (
            (recovered + dead)
            .groupby(['location_id'])
            .apply(lambda x: x.iloc[:-(hospital_parameters.hospital_stay_death - 1)])
            .reset_index(level=0, drop=True)
        )

    return HospitalMetrics(
        hospital_admissions=_combine(recovered_hospital_admissions, dead_hospital_admissions),
        hospital_census=_combine(recovered_hospital_census, dead_hospital_census),
        icu_admissions=_combine(recovered_icu_admissions, dead_icu_admissions),
        icu_census=_combine(recovered_icu_census, dead_icu_census),
    )


def _compute_correction_factor(raw_log_cf: pd.Series,
                               min_date: pd.Timestamp,
                               max_date: pd.Timestamp,
                               smoothing_window: int) -> pd.Series:
    date_index = pd.date_range(min_date, max_date).rename('date')

    data_locs = raw_log_cf.reset_index().location_id.unique().tolist()
    log_cf = []
    for location_id in data_locs:
        raw_log_cf_loc = raw_log_cf.loc[location_id].reset_index()
        raw_log_cf_loc['int_date'] = raw_log_cf_loc['date'].astype(int)
        # This log space mean is a geometric mean in natural units.
        log_cf_loc = raw_log_cf_loc[['int_date', 'log_cf']].rolling(smoothing_window).mean().dropna()
        log_cf_loc['date'] = pd.to_datetime(log_cf_loc['int_date'])
        # Interpolate our moving average to fill gaps.
        log_cf_loc = log_cf_loc.set_index('date').reindex(date_index, method='nearest')
        log_cf_loc['location_id'] = location_id
        log_cf_loc = log_cf_loc.reset_index().set_index(['location_id', 'date']).log_cf
        log_cf.append(log_cf_loc)
    log_cf = pd.concat(log_cf)

    return log_cf


def _fill_missing_locations(log_cf: pd.Series, aggregation_hierarchy: pd.DataFrame) -> pd.Series:
    data_locs = log_cf.reset_index().location_id.unique().tolist()
    # Aggregate up the hierarchy using as many of the children as we can.
    # First build a map between locations and all their children.
    all_children_map = defaultdict(list)
    path_to_top_map = aggregation_hierarchy.set_index('location_id').path_to_top_parent.to_dict()
    for child_id, path_to_top_parent in path_to_top_map.items():
        for parent_id in path_to_top_parent.split(','):
            parent_id = int(parent_id)
            if parent_id != child_id:
                all_children_map[parent_id].append(child_id)

    # Take the mean in log space (geometric mean in normal space) by date of all children.
    parent_ids = aggregation_hierarchy[aggregation_hierarchy.most_detailed == 0].location_id.unique()
    for parent_id in parent_ids:
        children = all_children_map[parent_id]
        modeled_children = set(data_locs).intersection(children)
        if modeled_children:
            parent_log_cf = log_cf.loc[modeled_children].groupby(level='date').mean().reset_index()
            parent_log_cf['location_id'] = parent_id
            parent_log_cf = parent_log_cf.set_index(['location_id', 'date']).log_cf
            log_cf = log_cf.append(parent_log_cf)
            data_locs.append(parent_id)

    # Fill back in with the nearest parent
    levels = sorted(aggregation_hierarchy.level.unique().tolist())
    for level in levels[:-1]:
        parents_at_level = aggregation_hierarchy[aggregation_hierarchy.level == level].location_id.unique()
        for parent_id in parents_at_level:
            assert parent_id in data_locs
            children = aggregation_hierarchy[aggregation_hierarchy.parent_id == parent_id].location_id.unique()
            for child_id in children:
                if child_id not in data_locs:
                    child_log_cf = log_cf.loc[parent_id].reset_index()
                    child_log_cf['location_id'] = child_id
                    child_log_cf = child_log_cf.set_index(['location_id', 'date']).log_cf
                    log_cf = log_cf.append(child_log_cf)
                    data_locs.append(child_id)

    assert not set(data_locs).difference(aggregation_hierarchy.location_id)

    return log_cf


def _safe_log_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """We need to do a bunch of geometric means of ratios. This is just the
    average log difference.  This mean is poorly behaved for numbers between
    0 and 1 and so we add 1 to the numerator and denominator. This is
    approximately correct in all situations we care about and radically
    improves the behavior for small numbers."""
    # noinspection PyTypeChecker
    return (np.log(numerator + 1) - np.log(denominator + 1)).dropna().rename('log_cf')


def calculate_hospital_correction_factors(usage: 'HospitalMetrics',
                                          census_data: 'HospitalCensusData',
                                          aggregation_hierarchy: pd.DataFrame,
                                          hospital_parameters: 'HospitalParameters') -> HospitalCorrectionFactors:
    date = usage.hospital_census.reset_index().date
    min_date, max_date = date.min(), date.max()

    if not hospital_parameters.compute_correction_factors:
        idx = pd.MultiIndex.from_product([
            aggregation_hierarchy.location_id.unique(),
            pd.date_range(min_date, max_date),
        ], names=['location_id', 'date'])
        return HospitalCorrectionFactors(
            hospital_census=pd.Series(1.0, index=idx, name='hospital_census'),
            icu_census=pd.Series(1.0, index=idx, name='icu_census'),
        )

    raw_log_hospital_cf = _safe_log_divide(census_data.hospital_census, usage.hospital_census)

    log_hospital_cf = _compute_correction_factor(
        raw_log_hospital_cf,
        min_date, max_date,
        hospital_parameters.correction_factor_smooth_window,
    )
    hospital_cf = np.exp(_fill_missing_locations(log_hospital_cf, aggregation_hierarchy))
    hospital_cf = hospital_cf.clip(lower=hospital_parameters.hospital_correction_factor_min,
                                   upper=hospital_parameters.hospital_correction_factor_max)

    modeled_icu_log_ratio = _safe_log_divide(usage.icu_census, usage.hospital_census)
    historical_icu_log_ratio = _safe_log_divide(census_data.icu_census, census_data.hospital_census)
    raw_log_icu_ratio_cf = (historical_icu_log_ratio - modeled_icu_log_ratio).dropna()

    log_icu_ratio_cf = _compute_correction_factor(
        raw_log_icu_ratio_cf,
        min_date, max_date,
        hospital_parameters.correction_factor_smooth_window,
    )
    log_icu_cf = (modeled_icu_log_ratio + log_icu_ratio_cf).dropna()
    icu_cf = np.exp(_fill_missing_locations(log_icu_cf, aggregation_hierarchy))
    icu_cf = icu_cf.clip(lower=hospital_parameters.icu_correction_factor_min,
                         upper=hospital_parameters.icu_correction_factor_max)

    return HospitalCorrectionFactors(
        hospital_census=hospital_cf,
        icu_census=icu_cf,
    )

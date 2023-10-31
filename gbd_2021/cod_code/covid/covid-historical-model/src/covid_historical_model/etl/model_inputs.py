from pathlib import Path
from typing import Dict, Tuple
from loguru import logger
import shutil
import contextlib

import pandas as pd
import numpy as np

from covid_shared import shell_tools

from covid_historical_model.etl import db, helpers, estimates


def evil_doings(data: pd.DataFrame, hierarchy: pd.DataFrame, input_measure: str) -> Tuple[pd.DataFrame, Dict]:
    manipulation_metadata = {}
    if input_measure == 'cases':
        pass

    elif input_measure == 'hospitalizations':
        ## late, not cumulative on first day
        is_ndl = data['location_id'] == 89
        data = data.loc[~is_ndl].reset_index(drop=True)
        manipulation_metadata['netherlands'] = 'dropped all hospitalizations'
                
        ## is just march-june 2020
        is_vietnam = data['location_id'] == 20
        data = data.loc[~is_vietnam].reset_index(drop=True)
        manipulation_metadata['vietnam'] = 'dropped all hospitalizations'

        ## is just march-june 2020
        is_murcia = data['location_id'] == 60366
        data = data.loc[~is_murcia].reset_index(drop=True)
        manipulation_metadata['murcia'] = 'dropped all hospitalizations'

        ## partial time series
        pakistan_location_ids = hierarchy.loc[hierarchy['path_to_top_parent'].apply(lambda x: '165' in x.split(',')),
                                              'location_id'].to_list()
        is_pakistan = data['location_id'].isin(pakistan_location_ids)
        data = data.loc[~is_pakistan].reset_index(drop=True)
        manipulation_metadata['pakistan'] = 'dropped all hospitalizations'
        
        ## ECDC is garbage
        ecdc_location_ids = [77, 82, 83, 59, 60, 88, 91, 52, 55]
        is_ecdc = data['location_id'].isin(ecdc_location_ids)
        data = data.loc[~is_ecdc].reset_index(drop=True)
        manipulation_metadata['ecdc_countries'] = 'dropped all hospitalizations'

        ## too late, starts March 2021
        is_haiti = data['location_id'] == 114
        data = data.loc[~is_haiti].reset_index(drop=True)
        manipulation_metadata['haiti'] = 'dropped all hospitalizations'

        ## late, starts Jan/Feb 2021 and is not cumulative (would require imputation)
        is_jordan = data['location_id'] == 144
        data = data.loc[~is_jordan].reset_index(drop=True)
        manipulation_metadata['jordan'] = 'dropped all hospitalizations'
        
        ## too low then too high? odd series
        is_andorra = data['location_id'] == 74
        data = data.loc[~is_andorra].reset_index(drop=True)
        manipulation_metadata['andorra'] = 'dropped all hospitalizations'
        
        ## only Jan-July 2021; also probably too low
        is_malawi = data['location_id'] == 182
        data = data.loc[~is_malawi].reset_index(drop=True)
        manipulation_metadata['malawi'] = 'dropped all hospitalizations'
    
    elif input_measure == 'deaths':
        pass
    
    else:
        raise ValueError(f'Input measure {input_measure} does not have a protocol for exclusions.')
    
    return data, manipulation_metadata


def copy_model_inputs(model_inputs_root: Path, out_dir: Path, verbose: bool = True,):
    path_suffixes = [
        'serology/global_serology_summary.csv',
        'full_data_unscaled.csv',
        'use_at_your_own_risk/full_data_extra_hospital.csv',
        'locations/modeling_hierarchy.csv',
        'locations/covariate_with_aggregates_hierarchy.csv',
        'locations/gbd_analysis_hierarchy.csv',
        'output_measures/population/all_populations.csv',
        'serology/waning_immunity/peluso_assay_sensitivity.xlsx',
        'serology/waning_immunity/perez-saez_n-roche.xlsx',
        'serology/waning_immunity/perez-saez_rbd-euroimmun.xlsx',
        'serology/waning_immunity/perez-saez_rbd-roche.xlsx',
        'serology/waning_immunity/bond.xlsx',
        'serology/waning_immunity/muecksch.xlsx',
        'serology/waning_immunity/lumley_n-abbott.xlsx',
        'serology/waning_immunity/lumley_s-oxford.xlsx',
        'serology/waning_immunity/assay_map.xlsx',
    ]
    if verbose:
        logger.info('Transferring files from `model-inputs`.')
    shell_tools.mkdir(out_dir / 'model_inputs')
    for path_suffix in path_suffixes:
        sub_dirs = '/'.join(path_suffix.split('/')[:-1])
        shell_tools.mkdir(out_dir / 'model_inputs' / sub_dirs, exists_ok=True, parents=True)
        with contextlib.redirect_stdout(None):
            shutil.copyfile(model_inputs_root / path_suffix, out_dir / 'model_inputs' / path_suffix)


def seroprevalence(out_dir: Path, hierarchy: pd.DataFrame, verbose: bool = True,) -> pd.DataFrame:
    # load
    data_path = out_dir / 'model_inputs' / 'serology' / 'global_serology_summary.csv'
    data = pd.read_csv(data_path)
    if verbose:
        logger.info(f'Initial observation count: {len(data)}')

    # date formatting
    if 'Date' in data.columns:
        if 'date' in data.columns:
            raise ValueError('Both `Date` and `date` in serology data.')
        else:
            data = data.rename(columns={'Date':'date'})
    for date_var in ['start_date', 'date']:
        data[date_var] = pd.to_datetime(data[date_var])  # , format='%d.%m.%Y'
    
    # if no start date provided, assume 2 weeks before end date?
    data['start_date'] = data['start_date'].fillna(data['date'] - pd.Timedelta(days=14))
    
    # # use mid-point instead of end date
    # data = data.rename(columns={'date':'end_date'})
    # data['n_midpoint_days'] = (data['end_date'] - data['start_date']).dt.days / 2
    # data['n_midpoint_days'] = data['n_midpoint_days'].astype(int)
    # data['date'] = data.apply(lambda x: x['end_date'] - pd.Timedelta(days=x['n_midpoint_days']), axis=1)
    # del data['n_midpoint_days']

    # convert to m/l/u to 0-1, sample size to numeric
    if not (helpers.str_fmt(data['units']).unique() == 'percentage').all():
        raise ValueError('Units other than percentage present.')
    
    for value_var in ['value', 'lower', 'upper']:
        if data[value_var].dtype.name == 'object':
            data[value_var] = helpers.str_fmt(data[value_var]).replace('not specified', np.nan).astype(float)
        if data[value_var].dtype.name != 'float64':
            raise ValueError(f'Unexpected type for {value_var} column.')
    
    data['seroprevalence'] = data['value'] / 100
    data['seroprevalence_lower'] = data['lower'] / 100
    data['seroprevalence_upper'] = data['upper'] / 100
    data['sample_size'] = (helpers.str_fmt(data['sample_size'])
                           .str.replace(',', '')
                           .replace(('unchecked', 'not specified'), np.nan).astype(float))
    
    data['bias'] = (helpers.str_fmt(data['bias'])
                    .replace(('unchecked', 'not specified'), '0')
                    .fillna('0')
                    .astype(int))
    
    data['manufacturer_correction'] = (helpers.str_fmt(data['manufacturer_correction'])
                                       .replace(('not specified', 'not specifed'), '0')
                                       .fillna('0')
                                       .astype(int))
    
    data['test_target'] = helpers.str_fmt(data['test_target']).str.lower()
    
    data['study_start_age'] = (helpers.str_fmt(data['study_start_age'])
                               .replace(('not specified'), np.nan).astype(float))
    data['study_end_age'] = (helpers.str_fmt(data['study_end_age'])
                             .replace(('not specified'), np.nan).astype(float))
    
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    ## manually specify certain tests when reporting is mixed (might just be US?)
    # Oxford "mixed" is spike
    is_oxford = data['test_name'] == 'University of Oxford ELISA IgG'
    is_mixed = data['test_target'] == 'mixed'
    data.loc[is_oxford & is_mixed, 'test_target'] = 'spike'
    
    # code India 2020 nat'l point (ICMR 3?) as mixed
    is_ind = data['location_id'] == 163
    is_icmr_serosurvey = data['survey_series'] == 'icmr_serosurvey'
    data.loc[is_ind & is_icmr_serosurvey, 'test_target'] = 'mixed'
    
    # code all ICMR round 4 data as spike
    is_icmr_round4 = data['survey_series'] == 'icmr_round4'
    data.loc[is_icmr_round4, 'test_target'] = 'spike'
    data.loc[is_icmr_round4, 'isotype'] = 'IgG'
    
    # Peru N-Roche has the wrong isotype
    is_peru = data['location_id'] == 123
    is_roche = data['test_name'] == 'Roche Elecsys N pan-Ig'
    data.loc[is_peru & is_roche, 'isotype'] = 'pan-Ig'
    
    # New York (from Nov 2020 to Aug 2021, test is Abbott)
    is_ny = data['location_id'] == 555
    is_cdc = data['survey_series'] == 'cdc_series'
    is_post_oct_2020 = pd.Timestamp('2020-10-31') < data['date']
    is_pre_sept_2021 = data['date'] < pd.Timestamp('2021-09-01')
    data.loc[is_ny & is_cdc & is_post_oct_2020 & is_pre_sept_2021, 'isotype'] = 'IgG'
    data.loc[is_ny & is_cdc & is_post_oct_2020 & is_pre_sept_2021, 'test_target'] = 'nucleocapsid'
    data.loc[is_ny & is_cdc & is_post_oct_2020 & is_pre_sept_2021, 'test_name'] = 'Abbott ARCHITECT SARS-CoV-2 IgG immunoassay'
    
    # BIG CDC CHANGE
    # many states are coded as Abbott, seem be Roche after the changes in Nov; recode
    for location_id in [523,  # Alabama
                        526,  # Arkansas
                        527,  # California
                        530,  # Delaware
                        531,  # District of Columbia
                        532,  # Florida
                        536,  # Illinois
                        540,  # Kentucky
                        545,  # Michigan
                        547,  # Mississippi
                        548,  # Missouri
                        551,  # Nevada
                        556,  # North Carolina
                        558,  # Ohio
                        563,  # South Carolina
                        564,  # South Dakota
                        565,  # Tennessee
                        566,  # Texas
                        567,  # Utah
                        572,  # Wisconsin
                        573,  # Wyoming
                       ]:
        is_state = data['location_id'] == location_id
        is_cdc = data['survey_series'] == 'cdc_series'
        is_N = data['test_target'] == 'nucleocapsid'
        is_nov_or_later = data['date'] >= pd.Timestamp('2020-11-01')
        data.loc[is_state & is_cdc & is_nov_or_later & is_N, 'isotype'] = 'pan-Ig'
        data.loc[is_state & is_cdc & is_nov_or_later & is_N, 'test_target'] = 'nucleocapsid'
        data.loc[is_state & is_cdc & is_nov_or_later & is_N, 'test_name'] = 'Roche Elecsys N pan-Ig'
    # some of the new extractions have the wrong isotype
    data.loc[data['test_name'] == 'Roche Elecsys N pan-Ig', 'isotype'] = 'pan-Ig'
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    outliers = []
    data['manual_outlier'] = data['manual_outlier'].astype(float)
    data['manual_outlier'] = data['manual_outlier'].fillna(0)
    data['manual_outlier'] = data['manual_outlier'].astype(int)
    manual_outlier = data['manual_outlier']
    outliers.append(manual_outlier)
    if verbose:
        logger.info(f'{manual_outlier.sum()} rows from sero data flagged as outliers in ETL.')
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    ## SOME THINGS
    # # 1)
    # #    Question: What if survey is only in adults? Only kids?
    # #    Current approach: Drop beyond some threshold limits.
    # max_start_age = 30
    # min_end_age = 60
    # data['study_start_age'] = helpers.str_fmt(data['study_start_age']).replace('not specified', np.nan).astype(float)
    # data['study_end_age'] = helpers.str_fmt(data['study_end_age']).replace('not specified', np.nan).astype(float)
    # too_old = data['study_start_age'] > max_start_age
    # too_young = data['study_end_age'] < min_end_age
    # age_outlier = (too_old  | too_young).astype(int)
    # outliers.append(age_outlier)
    # if verbose:
    #     logger.info(f'{age_outlier.sum()} rows from sero data do not have enough '
    #             f'age coverage (at least ages {max_start_age} to {min_end_age}).')
    
    # 2)
    #    Question: Use of geo_accordance?
    #    Current approach: Drop non-represeentative (geo_accordance == 0).
    data['geo_accordance'] = helpers.str_fmt(data['geo_accordance']).replace(('unchecked', np.nan), '0').astype(int)
    
    # re-code Singh study to be representative of states in which it was conducted except W Bengal
    data.loc[(data['survey_series'] == 'Singh_Dec2020') &
             (data['location_id'] != 4875),
             'geo_accordance'] = 1
    
    # re-code WHO UNITY study to be representative of states in which it was conducted
    data.loc[(data['survey_series'] == 'WHO_Unity_India2021'),
             'geo_accordance'] = 1
    
    # re-code Phenome-India to be representative of Telengana
    data.loc[(data['location_id'] == 4871) &
             (data['survey_series'] == 'phenome_india'),
             'geo_accordance'] = 1
    
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    ## SSA REPRESENTATIVENESS RECODE
    #  all SSA
    ssa_location_ids = (hierarchy
                        .loc[hierarchy['path_to_top_parent'].apply(lambda x: '166' in x.split(',')), 'location_id']
                        .to_list())
    data.loc[data['location_id'].isin(ssa_location_ids), 'geo_accordance'] = 1
    
    # Angola Sebastiao odd points
    data.loc[(data['location_id'] == 168) &
             (data['survey_series'] == 'Sebastiao_Sep2020'),
             'geo_accordance'] = 0
    data.loc[(data['location_id'] == 168) &
             (data['survey_series'] == 'Sebastiao_blooddonors_2020') &
             (data['date'] == pd.Timestamp('2020-07-31')),
             'geo_accordance'] = 0
    
    # Ethiopia Abdella
    data.loc[(data['location_id'] == 179) &
             (data['survey_series'] == 'Abdella_Sep2020'),
             'geo_accordance'] = 0
    
    # Kenya super early point
    data.loc[(data['location_id'] == 180) &
             (data['survey_series'] == 'Crowell_2021'),
             'geo_accordance'] = 0
    
    # Kenya Lucinde
    data.loc[(data['location_id'] == 180) &
             (data['survey_series'] == 'Lucinde_2021'),
             'geo_accordance'] = 0
    
    # Madagascar blood donor IgG after they started pan-Ig
    data.loc[(data['location_id'] == 181) &
             (data['survey_series'] == 'madagascar_blood') & 
             (data['test_name'] == 'ID Vet ELISA IgG') & 
             (data['date'] >= pd.Timestamp('2021-01-01')),
             'geo_accordance'] = 0
    
    # Zambia same study reports 7.6% PCR prev, 2.1% antibody
    data.loc[(data['location_id'] == 191) &
             (data['survey_series'] == 'Hines_July2020'),
             'geo_accordance'] = 0
    
    # Zambia study is young children and healthcare workers
    data.loc[(data['location_id'] == 191) &
             (data['survey_series'] == 'Laban_Dec2020'),
             'geo_accordance'] = 0
    
    # South Africa use most-detailed locs
    data.loc[(data['location_id'] == 196) &
             (data['survey_series'] == 'sanbs_southafrica') & 
             (data['source_population'] == 'South Africa'),
             'geo_accordance'] = 0
    
    # South Africa Hsiao study
    data.loc[(data['location_id'] == 196) &
             (data['survey_series'] == 'Hsiao_2020'),
             'geo_accordance'] = 0
    
    # South Africa Angincourt is too low in PHIRST_C2021
    data.loc[(data['location_id'] == 196) &
             (data['survey_series'] == 'PHIRST_C2021') & 
             (data['source_population'] == 'Agincourt, Mpumalanga province'),
             'geo_accordance'] = 0
    
    # Cote d'Ivoire mining camp not rep (even though data is consistent)
    data.loc[(data['location_id'] == 205) &
             (data['survey_series'] == 'Milleliri_Oct2020'),
             'geo_accordance'] = 0
    
    # Ghana multi-site
    data.loc[(data['location_id'] == 207) &
             (data['survey_series'] == 'ghana_multisite'),
             'geo_accordance'] = 0
    
    # Sierra Leone - must be using awful test
    data.loc[(data['location_id'] == 217) &
             (data['survey_series'] == 'sierra_leone_household'),
             'geo_accordance'] = 0
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    
    geo_outlier = data['geo_accordance'] == 0
    outliers.append(geo_outlier)
    if verbose:
        logger.info(f'{geo_outlier.sum()} rows from sero data do not have `geo_accordance`.')
    data['correction_status'] = helpers.str_fmt(data['correction_status']).replace(('unchecked', 'not specified', np.nan), '0').astype(int)
    
    # 3) Extra drops
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    ## VACCINE-RELATED
    # vaccine debacle, lose all the UK spike data in 2021
    is_uk = data['location_id'].isin([4749, 433, 434, 4636])
    is_spike = data['test_target'] == 'spike'
    is_2021 = data['date'] >= pd.Timestamp('2021-01-01')

    uk_vax_outlier = is_uk & is_spike & is_2021
    outliers.append(uk_vax_outlier)
    if verbose:
        logger.debug(f'{uk_vax_outlier.sum()} rows from sero data dropped due to UK vax issues.')

    # vaccine debacle, lose all the Danish data from Jan 2021 onward
    is_den = data['location_id'].isin([78])
    is_spike = data['test_target'] == 'spike'
    is_2021 = data['date'] >= pd.Timestamp('2021-01-01')

    den_vax_outlier = is_den & is_spike & is_2021
    outliers.append(den_vax_outlier)
    if verbose:
        logger.debug(f'{den_vax_outlier.sum()} rows from sero data dropped due to Denmark vax issues.')

    # vaccine debacle, lose all the Estonia, Belgium, and Netherlands data from June 2021 onward
    is_est_ndl = data['location_id'].isin([58, 76, 89])
    is_spike = data['test_target'] == 'spike'
    is_post_june_2021 = data['date'] >= pd.Timestamp('2021-06-01')

    est_ndl_vax_outlier = is_est_ndl & is_spike & is_post_june_2021
    outliers.append(est_ndl_vax_outlier)
    if verbose:
        logger.debug(f'{est_ndl_vax_outlier.sum()} rows from sero data dropped due to Netherlands and Estonia vax issues.')

    # vaccine debacle, lose all the Puerto Rico data from Feb 2021 onward
    is_pr = data['location_id'].isin([385])
    is_spike = data['test_target'] == 'spike'
    is_2021 = data['date'] >= pd.Timestamp('2021-02-01')

    pr_vax_outlier = is_pr & is_spike & is_2021
    outliers.append(pr_vax_outlier)
    if verbose:
        logger.debug(f'{pr_vax_outlier.sum()} rows from sero data dropped due to Puerto Rico vax issues.')
    
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    ## OTHER FIT-RELATED
    # Kazakhstan collab data
    is_kaz = data['location_id'] == 36
    is_kaz_collab_data = data['survey_series'] == 'kazakhstan_who'

    kaz_outlier = is_kaz & is_kaz_collab_data
    outliers.append(kaz_outlier)
    if verbose:
        logger.debug(f'{kaz_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of Kazakhstan colloborator data.')

    # Saskatchewan
    is_sas = data['location_id'] == 43869
    is_canadian_blood_services = data['survey_series'] == 'canadian_blood_services'

    sas_outlier = is_sas & is_canadian_blood_services
    outliers.append(sas_outlier)
    if verbose:
        logger.debug(f'{sas_outlier.sum()} rows from sero data dropped from Saskatchewan.')

    # King/Snohomish data is too early
    is_k_s = data['location_id'] == 60886
    is_pre_may_2020 = data['date'] < pd.Timestamp('2020-05-01')

    k_s_outlier = is_k_s & is_pre_may_2020
    outliers.append(k_s_outlier)
    if verbose:
        logger.debug(f'{k_s_outlier.sum()} rows from sero data dropped due to noisy (early) King/Snohomish data.')

    # dialysis study
    is_bad_dial_locs = data['location_id'].isin([
        540,  # Kentucky
        543,  # Maryland
        545,  # Michigan
        554,  # New Mexico
        555,  # New York
        560,  # Oregon
        570,  # Washington
        572,  # Wisconsin
    ])
    is_usa_dialysis = data['survey_series'] == 'usa_dialysis'

    dialysis_outlier = is_bad_dial_locs & is_usa_dialysis
    outliers.append(dialysis_outlier)
    if verbose:
        logger.debug(f'{dialysis_outlier.sum()} rows from sero data dropped due to inconsistent results from dialysis study.')

    # North Dakota first round
    is_nd = data['location_id'] == 557
    is_cdc_series = data['survey_series'] == 'cdc_series'
    is_first_date = data['date'] == pd.Timestamp('2020-08-12')

    nd_outlier = is_nd & is_cdc_series & is_first_date
    outliers.append(nd_outlier)
    if verbose:
        logger.debug(f'{nd_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of first commercial lab point in North Dakota.')

    # early Vermont
    is_vermont = data['location_id'] == 568
    is_pre_dec = data['date'] < pd.Timestamp('2020-12-01')

    vermont_outlier = is_vermont & is_pre_dec
    outliers.append(vermont_outlier)
    if verbose:
        logger.debug(f'{vermont_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of early commercial lab points in Vermont.')

    # France early study points
    is_fra = data['location_id'] == 80
    is_frenchpop_stephanelevu = data['survey_series'] == 'frenchpop_stephanelevu'
    is_pre_may_2020 = data['date'] < pd.Timestamp('2020-05-01')

    fra_outlier = is_fra & is_frenchpop_stephanelevu & is_pre_may_2020
    outliers.append(fra_outlier)
    if verbose:
        logger.debug(f'{fra_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of first pre-May survey in France.')

    # high Norway + 2021 point of NRC/UT survey
    is_norway = data['location_id'] == 90
    is_norway_serobank = data['survey_series'] == 'norway_serobank'
    is_august_2020 = data['date'] == pd.Timestamp('2020-08-30')
    is_norway_winter = data['survey_series'] == 'norway_winter'
    is_2021 = data['date'] >= pd.Timestamp('2021-01-01')

    norway_outlier = is_norway & ((is_norway_serobank & is_august_2020) | (is_norway_winter & is_2021))
    outliers.append(norway_outlier)
    if verbose:
        logger.debug(f'{norway_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of high Norway serobank point + 2021 (vax era) point of NRC/UT survey.')

    # Ceuta first round
    is_ceu = data['location_id'] == 60369
    is_ene_covid = data['survey_series'] == 'ene_covid'
    is_pre_june_2020 = data['date'] < pd.Timestamp('2020-06-01')

    ceu_outlier = is_ceu & is_ene_covid & is_pre_june_2020
    outliers.append(ceu_outlier)
    if verbose:
        logger.debug(f'{ceu_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of first survey round in Ceuta.')

    # India national studies, 2020
    is_india = data['location_id'] == 163
    is_2020 = data['date'] <= pd.Timestamp('2020-12-31')

    india_natl_outlier = is_india & is_2020
    outliers.append(india_natl_outlier)
    if verbose:
        logger.debug(f'{india_natl_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of India national data.')
    
    # India ICMR round 2 for some states
    is_bad_icmr_round2_states = data['location_id'].isin([
        4846,  # Chhattisgarh
        4851,  # Gujarat
        4859,  # Madhya Pradesh
        4867,  # Punjab
        4868,  # Rajasthan
        4871,  # Telangana
    ])
    is_icmr_round2 = data['survey_series'] == 'icmr_round2'
    
    icmr_round2_outlier = is_bad_icmr_round2_states & is_icmr_round2
    outliers.append(icmr_round2_outlier)
    if verbose:
        logger.debug(f'{icmr_round2_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of ICMR round 2 for some Indian states.')
    
    # India ICMR round 3 for some states
    is_bad_icmr_round3_states = data['location_id'].isin([
        4844,  # Bihar
        4851,  # Gujarat
        4859,  # Madhya Pradesh
        4867,  # Punjab
        4868,  # Rajasthan
        4871,  # Telangana
    ])
    is_icmr_round3 = data['survey_series'] == 'icmr_round3'
    
    icmr_round3_outlier = is_bad_icmr_round3_states & is_icmr_round3
    outliers.append(icmr_round3_outlier)
    if verbose:
        logger.debug(f'{icmr_round3_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of ICMR round 3 for some Indian states.')
    
    # India Phenome
    is_bad_phenome_india_states = data['location_id'].isin([
        4850,  # Goa
        4853,  # Himachal Pradesh
    ])
    is_phenome_india = data['survey_series'] == 'phenome_india'
    
    phenome_india_outlier = is_bad_phenome_india_states & is_phenome_india
    outliers.append(phenome_india_outlier)
    if verbose:
        logger.debug(f'{phenome_india_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of Phenome India survey data.')
    
    # Chhattisgarh ICMR-RMRC survey
    chhattisgarh_outlier = data['survey_series'] == 'Chhattisgarh_icmr_rmrc'
    outliers.append(chhattisgarh_outlier)
    if verbose:
        logger.debug(f'{chhattisgarh_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of Chhattisgarh ICMR-RMRC survey data.')
    
    # Delhi Sharma study
    is_delhi = data['location_id'] == 4849
    is_sharma = data['survey_series'] == 'sharma_delhi_sero'
    is_pre_sept_2020 = data['date'] < pd.Timestamp('2020-09-01')

    delhi_outlier = is_delhi & is_sharma & is_pre_sept_2020
    outliers.append(delhi_outlier)
    if verbose:
        logger.debug(f'{delhi_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of early portion of Delhi survey data (Sharma, non-ICMR).')

    # Karnataka JAMA and statewide 2
    is_karn = data['location_id'] == 4856
    is_karnataka_mohanan_or_statewide_2 = data['survey_series'].isin(['karnataka_mohanan', 'karnataka_statewide_2'])

    karn_outlier = is_karn & is_karnataka_mohanan_or_statewide_2
    outliers.append(karn_outlier)
    if verbose:
        logger.debug(f'{karn_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of Karnataka survey (Mohanan and "statewide", non-ICMR).')
    
    # Tamil Nadu gov't survey spring 2021
    is_tn = data['location_id'] == 4870
    is_Gov_Tamil_Nadu = data['survey_series'] == 'Gov_Tamil_Nadu'
    is_april_2021 = data['date'] == pd.Timestamp('2021-04-30')

    tn_outlier = is_tn & is_Gov_Tamil_Nadu & is_april_2021
    outliers.append(tn_outlier)
    if verbose:
        logger.debug(f'{tn_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of spring 2021 Tamil Nadu survey (state government, non-ICMR).')

    # Punjab (PAK)
    is_pp = data['location_id'] == 53620
    is_pakistan_july = data['survey_series'] == 'pakistan_july'

    pp_outlier = is_pp & is_pakistan_july
    outliers.append(pp_outlier)
    if verbose:
        logger.debug(f'{pp_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of Punjab (PAK) July survey.')
    
    # DRC ARIACOV
    is_drc = data['location_id'] == 171
    is_ARIACOV_Nov2020 = data['survey_series'] == 'ARIACOV_Nov2020'

    drc_outlier = is_drc & is_ARIACOV_Nov2020
    outliers.append(drc_outlier)
    if verbose:
        logger.debug(f'{drc_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of DRC ARIACOV study.')
    
    # Kenya blood transfusion pre-July
    is_ken = data['location_id'] == 180
    is_kenya_blood = data['survey_series'] == 'kenya_blood'
    is_pre_july_2020 = data['date'] < pd.Timestamp('2020-07-15')

    ken_outlier = is_ken & is_kenya_blood & is_pre_july_2020
    outliers.append(ken_outlier)
    if verbose:
        logger.debug(f'{ken_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of first two months of Kenya blood transfusion surveillance.')
    
    # Malawi Mandolo series pre-July
    is_malawi = data['location_id'] == 182
    is_Mandolo_2021 = data['survey_series'] == 'Mandolo_2021'
    is_pre_july_2020 = data['date'] < pd.Timestamp('2020-07-01')

    malawi_outlier = is_malawi & is_Mandolo_2021 & is_pre_july_2020
    outliers.append(malawi_outlier)
    if verbose:
        logger.debug(f'{malawi_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of super early points from Malawi Mandolo series.')

    # Mozambique INS 2020
    is_moz = data['location_id'] == 184
    is_moz_ins_incovid2020 = data['survey_series'] == 'moz_ins_incovid2020'
    is_2020 = data['date'] <= pd.Timestamp('2020-12-31')

    moz_outlier = is_moz & is_moz_ins_incovid2020 & is_2020
    outliers.append(moz_outlier)
    if verbose:
        logger.debug(f'{moz_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of Mozabique INS survey in 2020.')
    
    # Cameroon study
    is_cameroon = data['location_id'] == 202
    is_Sachathep_zenodo = data['survey_series'] == 'Sachathep_zenodo'

    cameroon_outlier = is_cameroon & is_Sachathep_zenodo
    outliers.append(cameroon_outlier)
    if verbose:
        logger.debug(f'{cameroon_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of Cameroon Sachathep study.')
    
    # Mali first point in study
    is_mali = data['location_id'] == 211
    is_Sagara_2021 = data['survey_series'] == 'Sagara_2021'
    is_2020 = data['date'] <= pd.Timestamp('2020-12-31')

    mali_outlier = is_mali & is_Sagara_2021 & is_2020
    outliers.append(mali_outlier)
    if verbose:
        logger.debug(f'{mali_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of first point from Mali Sagara study.')
    
    # Nigeria Okpala
    is_nigeria = data['location_id'] == 214
    is_Okpala_Dec2020 = data['survey_series'] == 'Okpala_Dec2020'

    nigeria_outlier = is_nigeria & is_Okpala_Dec2020
    outliers.append(nigeria_outlier)
    if verbose:
        logger.debug(f'{nigeria_outlier.sum()} rows from sero data dropped due to implausibility '
                     '(or at least incompatibility) of Nigeria Okpala.')

    # 4) Level threshold - location max > 3%, value max > 1%
    # exemtions -> Norway and Vermont (noisy serial measurements, need low values)
    na_list = [90, 568]
    data['tmp_outlier'] = pd.concat(outliers, axis=1).max(axis=1).astype(int)
    is_maxsub3 = (data
                  .groupby(['location_id', 'tmp_outlier'])
                  .apply(lambda x: x['seroprevalence'].max() <= 0.03)
                  .rename('is_maxsub3')
                  .reset_index())
    is_maxsub3 = data.merge(is_maxsub3, how='left').loc[data.index, 'is_maxsub3']
    del data['tmp_outlier']
    is_sub1 = data['seroprevalence'] <= 0.01
    is_maxsub3_sub1 = (is_maxsub3 | is_sub1) & ~data['location_id'].isin(na_list)
    outliers.append(is_maxsub3_sub1)
    if verbose:
        logger.info(f'{is_maxsub3_sub1.sum()} rows from sero data dropped due to having values'
                     'below 1% or a location max below 3%.')
    
    # 6) drop December 2021 onward
    date_cutoff_outlier = data['date'] > pd.Timestamp('2021-11-20')
    outliers.append(date_cutoff_outlier)
    logger.debug(f'EXCLUDING ALL SERO DATA AFTER 11/20/2021 ({date_cutoff_outlier.sum()} rows).')
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

    keep_columns = ['data_id', 'nid', 'survey_series', 'location_id', 'start_date', 'date',
                    'seroprevalence',  'seroprevalence_lower', 'seroprevalence_upper', 'sample_size',
                    'study_start_age', 'study_end_age',
                    'test_name', 'test_target', 'isotype',
                    'bias', 'bias_type',
                    'manufacturer_correction', 'geo_accordance',
                    'is_outlier', 'manual_outlier']
    data['is_outlier'] = pd.concat(outliers, axis=1).max(axis=1).astype(int)
    data = (data
            .sort_values(['location_id', 'is_outlier', 'survey_series', 'date'])
            .reset_index(drop=True))
    data['data_id'] = data.index
    data = data.loc[:, keep_columns]
    
    if verbose:
        logger.info(f"Final ETL inlier count: {len(data.loc[data['is_outlier'] == 0])}")
        logger.info(f"Final ETL outlier count: {len(data.loc[data['is_outlier'] == 1])}")
    
    return data


def reported_epi(out_dir: Path, input_measure: str, smooth: bool,
                 hierarchy: pd.DataFrame, gbd_hierarchy: pd.DataFrame,
                 excess_mortality: bool = None,
                 excess_mortality_draw: int = None,) -> Tuple[pd.Series, pd.Series]:
    if input_measure == 'deaths':
        if type(excess_mortality) != bool:
            raise TypeError('Must specify `excess_mortality` argument to load deaths.')
        data_path = out_dir / 'model_inputs' / 'full_data_unscaled.csv'
    elif input_measure == 'cases':
        excess_mortality = False
        data_path = out_dir / 'model_inputs' / 'full_data_unscaled.csv'
    elif input_measure == 'hospitalizations':
        excess_mortality = False
        data_path = out_dir / 'model_inputs' / 'use_at_your_own_risk' / 'full_data_extra_hospital.csv'
    else:
        raise ValueError('Invalid input measure.')
    data = pd.read_csv(data_path)
    data = data.rename(columns={'Confirmed': 'cumulative_cases',
                                'Hospitalizations': 'cumulative_hospitalizations',
                                'Deaths': 'cumulative_deaths',})
    
    data['date'] = pd.to_datetime(data['Date'])
    keep_cols = ['location_id', 'date', f'cumulative_{input_measure}']
    data = data.loc[:, keep_cols].dropna()
    data['location_id'] = data['location_id'].astype(int)
    data = data.sort_values(['location_id', 'date']).reset_index(drop=True)
    
    data = (data.groupby('location_id', as_index=False)
            .apply(lambda x: helpers.fill_dates(x, [f'cumulative_{input_measure}']))
            .reset_index(drop=True))
    
    logger.debug(f'EXCLUDING ALL {input_measure.upper()} DATA AFTER 11/30/2021.')
    data = data.loc[data['date'] < pd.Timestamp('2021-12-01')]
    
    data, manipulation_metadata = evil_doings(data, hierarchy, input_measure)
    
    if excess_mortality:
        # NEED TO UPDATE PATH
        em_scalar_data = estimates.excess_mortailty_scalars(excess_mortality)
        if excess_mortality_draw == -1:
            em_scalar_data = em_scalar_data.groupby('location_id', as_index=False)['em_scalar'].mean()
        else:
            em_scalar_data = em_scalar_data.loc[excess_mortality_draw].reset_index(drop=True)
        
        data = data.merge(em_scalar_data, how='left')
        missing_locations = data.loc[data['em_scalar'].isnull(), 'location_id'].astype(str).unique().tolist()
        missing_covid_locations = [l for l in missing_locations if l in hierarchy['location_id'].to_list()]
        missing_gbd_locations = [l for l in missing_locations if l in gbd_hierarchy['location_id'].to_list()]
        missing_gbd_locations = [l for l in missing_gbd_locations if l not in missing_covid_locations]
        if missing_covid_locations:
            logger.warning(f"Missing scalars for the following Covid hierarchy locations: {', '.join(missing_covid_locations)}")
        if missing_gbd_locations:
            logger.warning(f"Missing scalars for the following GBD hierarchy locations: {', '.join(missing_gbd_locations)}")
        data['em_scalar'] = data['em_scalar'].fillna(1)
        data['cumulative_deaths'] *= data['em_scalar']
        del data['em_scalar']
    elif input_measure == 'deaths':
        logger.info('Using unscaled deaths.')
    
    extra_locations = gbd_hierarchy.loc[gbd_hierarchy['most_detailed'] == 1, 'location_id'].to_list()
    extra_locations = [l for l in extra_locations if l not in hierarchy['location_id'].to_list()]
    
    extra_data = data.loc[data['location_id'].isin(extra_locations)].reset_index(drop=True)
    data = helpers.aggregate_data_from_md(data, hierarchy, f'cumulative_{input_measure}')
    data = (data
            .append(extra_data.loc[:, data.columns])
            .sort_values(['location_id', 'date'])
            .reset_index(drop=True))
    
    data[f'daily_{input_measure}'] = (data
                                      .groupby('location_id')[f'cumulative_{input_measure}']
                                      .apply(lambda x: x.diff().fillna(x)))
    data = data.dropna()
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index())
    
    daily_data = data.loc[:, f'daily_{input_measure}']
    if smooth:
        daily_data = (daily_data
                      .groupby('location_id')
                      .apply(lambda x: smooth_t1plus(x))
                      .dropna())
        cumulative_data = (daily_data
                          .groupby('location_id')
                          .cumsum())
    else:
        cumulative_data = daily_data.loc[:, f'cumulative_{input_measure}']

    return cumulative_data, daily_data


def smooth_t1plus(data: pd.Series, window: int = 7) -> pd.Series:
    if len(data) >= window * 2:
        t1_val = data.iloc[0]
        data.iloc[0] = np.nan
        data = (data
                .clip(0, np.inf)
                .rolling(window=window, min_periods=window, center=True)
                .mean())
        add_date = data.loc[~data.notnull().cummax()].index[-1]
        data.loc[add_date] = t1_val
    
    return data


def hierarchy(out_dir:Path, hierarchy_type: str = 'covid_modeling') -> pd.DataFrame:
    if hierarchy_type == 'covid_modeling':
        data_path = out_dir / 'model_inputs' / 'locations' / 'modeling_hierarchy.csv'
        
        data = pd.read_csv(data_path)
        data = data.sort_values('sort_order').reset_index(drop=True)
        
    elif hierarchy_type == 'covid_covariate':
        data_path = out_dir / 'model_inputs' / 'locations' / 'covariate_with_aggregates_hierarchy.csv'
        
        data = pd.read_csv(data_path)
        data = data.sort_values('sort_order').reset_index(drop=True)
        raise ValueError('Incompatible hierarchy.')
    elif hierarchy_type == 'covid_gbd':
        data_path = out_dir / 'model_inputs' / 'locations' / 'gbd_analysis_hierarchy.csv'
        data = pd.read_csv(data_path)
        data = data.sort_values('sort_order').reset_index(drop=True)
    elif hierarchy_type == 'covid_modeling_plus_zaf':
        gbd_path = out_dir / 'model_inputs' / 'locations' / 'gbd_analysis_hierarchy.csv'
        covid_path = out_dir / 'model_inputs' / 'locations' / 'modeling_hierarchy.csv'

        # get ZAF only from GBD
        covid = pd.read_csv(covid_path)
        
        covid_is_zaf = covid['path_to_top_parent'].apply(lambda x: '196' in x.split(','))
        if not covid_is_zaf.sum() == 1:
            raise ValueError('Already have ZAF subnats in Covid hierarchy.')
        sort_order = covid.loc[covid_is_zaf, 'sort_order'].item()
        covid = covid.loc[~covid_is_zaf]

        gbd = pd.read_csv(gbd_path)
        gbd_is_zaf = gbd['path_to_top_parent'].apply(lambda x: '196' in x.split(','))
        gbd = gbd.loc[gbd_is_zaf].reset_index(drop=True)
        gbd['sort_order'] = sort_order + gbd.index

        covid.loc[covid['sort_order'] > sort_order, 'sort_order'] += len(gbd) - 1

        data = pd.concat([covid, gbd]).sort_values('sort_order').reset_index(drop=True)
        
    return data


def population(out_dir: Path, by_age: bool = False) -> pd.Series:
    data_path = out_dir / 'model_inputs' / 'output_measures' / 'population' / 'all_populations.csv'
    data = pd.read_csv(data_path)
    is_2019 = data['year_id'] == 2019
    is_bothsex = data['sex_id'] == 3
    if by_age:
        age_metadata = db.age_metadata()    
        data = (data
                .loc[is_2019 & is_bothsex, ['location_id', 'age_group_id', 'population']])
        data = data.merge(age_metadata)
        data = (data
                .set_index(['location_id', 'age_group_years_start', 'age_group_years_end'])
                .sort_index()
                .loc[:, 'population'])
    else:
        is_allage = data['age_group_id'] == 22
        data = (data
                .loc[is_2019 & is_bothsex & is_allage]
                .set_index('location_id')
                .sort_index()
                .loc[:, 'population'])

    return data


def assay_sensitivity(out_dir: Path,) -> pd.DataFrame:
    peluso_path = out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'peluso_assay_sensitivity.xlsx'
    perez_saez_paths = [
        out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'perez-saez_n-roche.xlsx',
        out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'perez-saez_rbd-euroimmun.xlsx',
        out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'perez-saez_rbd-roche.xlsx',
    ]
    bond_path = out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'bond.xlsx'
    muecksch_path = out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'muecksch.xlsx'
    lumley_paths = [
        out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'lumley_n-abbott.xlsx',
        out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'lumley_s-oxford.xlsx',
    ]
    
    ## PELUSO
    peluso = pd.read_excel(peluso_path)
    peluso['t'] = peluso['Time'].apply(lambda x: int(x.split(' ')[0]) * 30)
    peluso['sensitivity_std'] = (peluso['97.5%'] - peluso['2.5%']) / 3.92
    peluso = peluso.rename(columns={'mean': 'sensitivity_mean',
                                    'AntigenAndAssay': 'assay',
                                    'Hospitalization_status': 'hospitalization_status',})
    peluso = peluso.loc[:, ['assay', 'hospitalization_status', 't', 'sensitivity_mean', 'sensitivity_std']]
    # only need to keep commercial assays
    peluso = peluso.loc[~peluso['assay'].isin(['Neut-Monogram', 'RBD-LIPS', 'RBD-Split Luc',
                                               'RBD-Lum', 'S-Lum', 'N(full)-Lum', 'N-LIPS',
                                               'N(frag)-Lum', 'N-Split Luc'])]
    peluso['source'] = 'Peluso'
    
    ## PEREZ-SAEZ - start at 21 days out
    perez_saez = pd.concat([pd.read_excel(perez_saez_path) for perez_saez_path in perez_saez_paths])
    perez_saez['metric'] = perez_saez['metric'].str.strip()
    perez_saez = pd.pivot_table(perez_saez, index=['t', 'assay'], columns='metric', values='sensitivity').reset_index()
    perez_saez = perez_saez.rename(columns={'mean': 'sensitivity_mean'})
    perez_saez['sensitivity_std'] = (perez_saez['upper'] - perez_saez['lower']) / 3.92
    perez_saez = perez_saez.loc[perez_saez['t'] >= 21]
    perez_saez['t'] -= 21
    perez_saez = pd.concat([
        pd.concat([perez_saez, pd.DataFrame({'hospitalization_status':'Non-hospitalized'}, index=perez_saez.index)], axis=1),
        pd.concat([perez_saez, pd.DataFrame({'hospitalization_status':'Hospitalized'}, index=perez_saez.index)], axis=1)
    ])
    perez_saez['source'] = 'Perez-Saez'
    
    ## BOND - drop 121-150 point, is only 11 people and can't possibly be at 100%; start 21 days out; only keep Abbott
    bond = pd.read_excel(bond_path)
    bond = bond.loc[bond['days since symptom onset'] != '121–150']
    bond['t_start'] = bond['days since symptom onset'].str.split('–').apply(lambda x: int(x[0]))
    bond['t_end'] = bond['days since symptom onset'].str.split('–').apply(lambda x: int(x[1]))
    bond['t'] = bond[['t_start', 't_end']].mean(axis=1)
    for assay in ['N-Abbott', 'S-DiaSorin', 'N-Roche']:
        bond[f'{assay} mean'] = bond[assay].str.split('%').apply(lambda x: float(x[0])) / 100
        bond[f'{assay} lower'] = bond[assay].str.split('% \[').apply(lambda x: float(x[1].split(', ')[0])) / 100
        bond[f'{assay} upper'] = bond[assay].str.split('% \[').apply(lambda x: float(x[1].split(', ')[1].replace(']', ''))) / 100
        bond[f'{assay} std'] = (bond[f'{assay} upper'] - bond[f'{assay} lower']) / 3.92
    bond_mean = pd.melt(bond, id_vars='t', value_vars=['N-Abbott mean', 'S-DiaSorin mean', 'N-Roche mean'],
                        var_name='assay', value_name='sensitivity_mean')
    bond_mean['assay'] = bond_mean['assay'].str.replace(' mean', '')
    bond_std = pd.melt(bond, id_vars='t', value_vars=['N-Abbott std', 'S-DiaSorin std', 'N-Roche std'],
                       var_name='assay', value_name='sensitivity_std')
    bond_std['assay'] = bond_std['assay'].str.replace(' std', '')
    bond = bond_mean.merge(bond_std)
    bond = bond.loc[bond['t'] >= 21]
    bond['t'] -= 21
    bond = pd.concat([
        pd.concat([bond, pd.DataFrame({'hospitalization_status':'Non-hospitalized'}, index=bond.index)], axis=1),
        pd.concat([bond, pd.DataFrame({'hospitalization_status':'Hospitalized'}, index=bond.index)], axis=1)
    ])
    bond = bond.loc[bond['assay'] == 'N-Abbott']
    bond['source'] = 'Bond'
    
    ## MUECKSCH - top end of terminal group is 110 days; only keep Abbott
    muecksch = pd.read_excel(muecksch_path)
    muecksch.loc[muecksch['Time, d'] == '>81', 'Time, d'] = '81-110'
    muecksch['t_start'] = muecksch['Time, d'].str.split('-').apply(lambda x: int(x[0]))
    muecksch['t_end'] = muecksch['Time, d'].str.split('-').apply(lambda x: int(x[1]))
    muecksch['t'] = muecksch[['t_start', 't_end']].mean(axis=1)
    for assay in ['N-Abbott', 'S-DiaSorin', 'RBD-Siemens']:
        muecksch[f'{assay} mean'] = muecksch[assay].str.split(' ').apply(lambda x: float(x[0])) / 100
        muecksch[f'{assay} lower'] = muecksch[assay].str.split(' \[').apply(lambda x: float(x[1].split('-')[0])) / 100
        muecksch[f'{assay} upper'] = muecksch[assay].str.split(' \[').apply(lambda x: float(x[1].split('-')[1].replace(']', ''))) / 100
        muecksch[f'{assay} std'] = (muecksch[f'{assay} upper'] - muecksch[f'{assay} lower']) / 3.92
    muecksch_mean = pd.melt(muecksch, id_vars='t', value_vars=['N-Abbott mean', 'S-DiaSorin mean', 'RBD-Siemens mean'],
                            var_name='assay', value_name='sensitivity_mean')
    muecksch_mean['assay'] = muecksch_mean['assay'].str.replace(' mean', '')
    muecksch_std = pd.melt(muecksch, id_vars='t', value_vars=['N-Abbott std', 'S-DiaSorin std', 'RBD-Siemens std'],
                            var_name='assay', value_name='sensitivity_std')
    muecksch_std['assay'] = muecksch_std['assay'].str.replace(' std', '')
    muecksch = muecksch_mean.merge(muecksch_std)
    muecksch['t'] -= 24
    muecksch = pd.concat([
        pd.concat([muecksch, pd.DataFrame({'hospitalization_status':'Non-hospitalized'}, index=muecksch.index)], axis=1),
        pd.concat([muecksch, pd.DataFrame({'hospitalization_status':'Hospitalized'}, index=muecksch.index)], axis=1)
    ])
    muecksch = muecksch.loc[muecksch['assay'] == 'N-Abbott']
    muecksch['source'] = 'Muecksch'
    
    ## LUMLEY - exclude 180 day point for Oxford that goes back up
    lumley = pd.concat([pd.read_excel(lumley_path) for lumley_path in lumley_paths])
    lumley['metric'] = lumley['metric'].str.strip()
    lumley = pd.pivot_table(lumley, index=['t', 'assay', 'num_60', 'denom_60', 'avg_60'],
                            columns='metric', values='sensitivity').reset_index()
    lumley = lumley.rename(columns={'mean': 'sensitivity_mean'})
    lumley['sensitivity_std'] = (lumley['upper'] - lumley['lower']) / 3.92
    lumley['sensitivity_mean'] *= (lumley['num_60'] / lumley['denom_60']) / lumley['avg_60']
    lumley = pd.concat([
        pd.concat([lumley, pd.DataFrame({'hospitalization_status':'Non-hospitalized'}, index=lumley.index)], axis=1),
        pd.concat([lumley, pd.DataFrame({'hospitalization_status':'Hospitalized'}, index=lumley.index)], axis=1)
    ])
    lumley = lumley.loc[~((lumley['assay'] == 'S-Oxford') & (lumley['t'] == 180))]
    lumley['source'] = 'Lumley'
    
    # combine them all
    keep_cols = ['source', 'assay', 'hospitalization_status', 't', 'sensitivity_mean', 'sensitivity_std',]
    sensitivity = pd.concat([peluso.loc[:, keep_cols],
                             perez_saez.loc[:, keep_cols],
                             bond.loc[:, keep_cols],
                             muecksch.loc[:, keep_cols],
                             lumley.loc[:, keep_cols],]).reset_index(drop=True)
    
    return sensitivity


def assay_map(out_dir: Path,):
    data_path = out_dir / 'model_inputs' / 'serology' / 'waning_immunity' / 'assay_map.xlsx'
    data = pd.read_excel(data_path)
    
    return data


def validate_hierarchies(hierarchy: pd.DataFrame, gbd_hierarchy: pd.DataFrame):
    covid = hierarchy.loc[:, ['location_id', 'path_to_top_parent']]
    covid = covid.rename(columns={'path_to_top_parent': 'covid_path'})
    gbd = gbd_hierarchy.loc[:, ['location_id', 'path_to_top_parent']]
    gbd = gbd.rename(columns={'path_to_top_parent': 'gbd_path'})
    
    data = covid.merge(gbd, how='left')
    is_missing = data['gbd_path'].isnull()
    is_different = (data['covid_path'] != data['gbd_path']) & (~is_missing)
    
    if is_different.sum() > 0:
        raise ValueError(f'Some covid locations are missing a GBD path:\n{data.loc[is_different]}.')
    
    if is_missing.sum() > 0:
        logger.warning(f'Some covid locations are missing in GBD hierarchy and will be added:\n{data.loc[is_missing]}.')
        missing_locations = data.loc[is_missing, 'location_id'].to_list()
        missing_locations = hierarchy.loc[hierarchy['location_id'].isin(missing_locations)]
        gbd_hierarchy = gbd_hierarchy.append(missing_locations)
        
    return gbd_hierarchy

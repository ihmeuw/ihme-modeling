from typing import List, Tuple, Dict
from pathlib import Path
import dill as pickle
from loguru import logger

import pandas as pd
import numpy as np


def evil_doings(data: pd.DataFrame, hierarchy: pd.DataFrame, input_measure: str,
                fh: bool,) -> Tuple[pd.DataFrame, Dict]:
    manipulation_metadata = {}
    
    if fh:
        # is_post_Jun8 = data['date'] > pd.Timestamp('2021-06-08')
        # fl_hierarchy = hierarchy.loc[hierarchy['path_to_top_parent'].apply(lambda x: '532' in x.split(',')),
        #                              'location_id'].to_list()
        # is_fl_hierarchy = data['location_id'].isin(fl_hierarchy)
        # missed_qc = [44633, 725, 807, 794, 1127, 1450, 1714, 2053, 2320, 2555, 2924, 2915, 3042,
        #              2018, 3346, 3358, 3543, 60913,]
        # is_missed_qc = data['location_id'].isin(missed_qc)
        # data = data.loc[~(is_post_Jun8 & (is_fl_hierarchy | is_missed_qc))].reset_index(drop=True)
        # manipulation_metadata['florida_counties'] = f'dropped {input_measure} post June 8th'
        # manipulation_metadata['missed_qc_counties'] = f'dropped {input_measure} post June 8th'
        
        is_md = hierarchy['most_detailed'] == 1
        is_col_hierarchy = hierarchy['path_to_top_parent'].apply(lambda x: '125' in x.split(','))
        # is_pr_hierarchy = hierarchy['path_to_top_parent'].apply(lambda x: '385' in x.split(','))
        col_hierarchy = hierarchy.loc[is_md & is_col_hierarchy]
        # pr_hierarchy = hierarchy.loc[is_md & is_pr_hierarchy]
        fh_col_ids = col_hierarchy['location_id'].to_list()
        # fh_pr_ids = pr_hierarchy['location_id'].to_list()
        fh_col_labels = col_hierarchy['location_ascii_name'].str.replace('[^a-zA-Z ]', '').str.replace(' ', '_').str.lower().to_list()
        # fh_pr_labels = pr_hierarchy['location_ascii_name'].str.replace('[^a-zA-Z ]', '').str.replace(' ', '_').str.lower().to_list()
        fh_col_subnats = list(zip(fh_col_ids, fh_col_labels))
        # fh_pr_subnats = list(zip(fh_pr_ids, fh_pr_labels))
    else:
        fh_col_subnats = []
        # fh_pr_subnats = []
    
    if input_measure == 'cases':
        for location_id, location_label in fh_col_subnats:
            is_fh_col_subnat = data['location_id'] == location_id
            last_date = data.loc[is_fh_col_subnat, 'date'].max()
            last_date_sub_2w = last_date - pd.Timedelta(days=14)
            is_last_2w = data['date'] > last_date_sub_2w
            data = data.loc[~(is_fh_col_subnat & is_last_2w)].reset_index(drop=True)
            manipulation_metadata[location_label] = 'dropped last 14 days of cases'

    elif input_measure == 'hospitalizations':
        ## crazy county-level data
        county_hosp_drop = [(3397, 'fairfax_county'),
                            (725, 'pulaski_county'),
                            (3042, 'knox_county'),
                            (775, 'placer_county'),
                            (933, 'orange_county'),
                            # (891, 'dc_counties'),
                            # (2807, 'lane_county'),
                            # (3173, 'dallas_county'),
                            # (3604, 'kanawha_county'),
                            # (2998, 'union_county'),
                            # (1085, 'ben_hill_county'),
                            (807, 'san_bernadino'),
                            (837, 'denver'),
                            (1312, 'hendricks'),
                            (1694, 'ouachita'),
                            (2343, 'rockingham'),
                            (2555, 'robeson'),
                            (2782, 'umatilla'),
                            (828, 'boulder'),
                            (879, 'broomfield'),
                            (939, 'pinellas'),
                            (1127, 'benewah'),
                            (1493, 'wyandotte'),
                            (2255, 'garden'),
                            (2303, 'phelps'),
                            (2684, 'warren'),
                            (2931, 'charleston'),
                            (2476, 'vance'),]
        for location_id, location_label in county_hosp_drop:
            is_county = data['location_id'] == location_id
            data = data.loc[~is_county].reset_index(drop=True)
            manipulation_metadata[location_label] = 'dropped all hospitalizations'
        
        ## false point in January (from deaths in imputation)
        is_ohio = data['location_id'] == 558
        is_pre_march = data['date'] < pd.Timestamp('2020-02-18')
        data = data.loc[~(is_ohio & is_pre_march)].reset_index(drop=True)
        manipulation_metadata['ohio'] = 'dropped admission before Feb 18'
        
        ## is just march-june 2020
        is_vietnam = data['location_id'] == 20
        data = data.loc[~is_vietnam].reset_index(drop=True)
        manipulation_metadata['vietnam'] = 'dropped all hospitalizations'

        ## is just march-june 2020
        is_murcia = data['location_id'] == 60366
        data = data.loc[~is_murcia].reset_index(drop=True)
        manipulation_metadata['murcia'] = 'dropped all hospitalizations'

        ## under-repored except for Islamabad, which we will keep
        pakistan_location_ids = hierarchy.loc[hierarchy['path_to_top_parent'].apply(lambda x: '165' in x.split(',')),
                                              'location_id'].to_list()
        pakistan_location_ids = [l for l in pakistan_location_ids if l != 53618]
        is_pakistan = data['location_id'].isin(pakistan_location_ids)
        data = data.loc[~is_pakistan].reset_index(drop=True)
        manipulation_metadata['pakistan'] = 'dropped all hospitalizations'
        
        ## ECDC is garbage
        ecdc_location_ids = [77, 82, 83, 59, 60, 88, 91, 52, 55]
        is_ecdc = data['location_id'].isin(ecdc_location_ids)
        data = data.loc[~is_ecdc].reset_index(drop=True)
        manipulation_metadata['ecdc_countries'] = 'dropped all hospitalizations'
        
        ## is a bit high... check w/ new sero data
        is_goa = data['location_id'] == 4850
        data = data.loc[~is_goa].reset_index(drop=True)
        manipulation_metadata['goa'] = 'dropped all hospitalizations'

        ## too low (also starts March 2021)
        is_haiti = data['location_id'] == 114
        data = data.loc[~is_haiti].reset_index(drop=True)
        manipulation_metadata['haiti'] = 'dropped all hospitalizations'
        
        ## too low then too high? odd series
        is_andorra = data['location_id'] == 74
        data = data.loc[~is_andorra].reset_index(drop=True)
        manipulation_metadata['andorra'] = 'dropped all hospitalizations'
        
        ## too low, revisit w/ new sero
        is_ethiopia = data['location_id'] == 179
        data = data.loc[~is_ethiopia].reset_index(drop=True)
        manipulation_metadata['ethiopia'] = 'dropped all hospitalizations'
        
        ## only Jan-July 2021; also probably too low
        is_malawi = data['location_id'] == 182
        data = data.loc[~is_malawi].reset_index(drop=True)
        manipulation_metadata['malawi'] = 'dropped all hospitalizations'
        
        ## too low, revisit w/ new sero
        is_mozambique = data['location_id'] == 184
        data = data.loc[~is_mozambique].reset_index(drop=True)
        manipulation_metadata['mozambique'] = 'dropped all hospitalizations'
        
        ## too low (also starts in May 2021), revisit w/ new sero
        is_zambia = data['location_id'] == 191
        data = data.loc[~is_zambia].reset_index(drop=True)
        manipulation_metadata['zambia'] = 'dropped all hospitalizations'
        
        ## is less than a month of data
        is_zimbabwe = data['location_id'] == 198
        data = data.loc[~is_zimbabwe].reset_index(drop=True)
        manipulation_metadata['zimbabwe'] = 'dropped all hospitalizations'
        
        ## too low (also starts in Feb 2021)
        is_guinea_bissau = data['location_id'] == 209
        data = data.loc[~is_guinea_bissau].reset_index(drop=True)
        manipulation_metadata['guinea_bissau'] = 'dropped all hospitalizations'

    elif input_measure == 'deaths':
        ## false point in January
        is_ohio = data['location_id'] == 558
        is_pre_march = data['date'] < pd.Timestamp('2020-03-01')
        data = data.loc[~(is_ohio & is_pre_march)].reset_index(drop=True)
        manipulation_metadata['ohio'] = 'dropped death before March 1'
        
        for location_id, location_label in fh_col_subnats:
            is_fh_col_subnat = data['location_id'] == location_id
            last_date = data.loc[is_fh_col_subnat, 'date'].max()
            last_date_sub_2w = last_date - pd.Timedelta(days=14)
            is_last_2w = data['date'] > last_date_sub_2w
            data = data.loc[~(is_fh_col_subnat & is_last_2w)].reset_index(drop=True)
            manipulation_metadata[location_label] = 'dropped last 14 days of deaths'
    
    else:
        raise ValueError(f'Input measure {input_measure} does not have a protocol for exclusions.')
    
    return data, manipulation_metadata


def draw_check(n_draws: int, n_draws_in_data: int,) -> bool:
    if n_draws > n_draws_in_data:
        raise ValueError(f'User specified {n_draws} draws; only {n_draws_in_data} draws available in data.')
    elif n_draws < n_draws_in_data:
        logger.warning(f'User specified {n_draws} draws; {n_draws_in_data} draws available in data. '
                       f'Crudely taking first {n_draws} draws from rates.')
        downsample = True
    else:
        downsample = False
    
    return downsample


def load_ifr(rates_root: Path, n_draws: int,) -> pd.DataFrame:
    data_path = rates_root / 'ifr_draws.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.rename(columns={'ifr': 'ratio',
                                'ifr_fe': 'ratio_fe'})
    
    n_draws_in_data = data['draw'].max() + 1    
    downsample = draw_check(n_draws, n_draws_in_data,)
    if downsample:
        data = (data
                .set_index('draw')
                .loc[list(range(n_draws))]
                .reset_index())
    
    data = (data
            .set_index(['location_id', 'draw', 'date'])
            .sort_index()
            .loc[:, ['ratio', 'ratio_fe']])
    
    return data


def load_ifr_rr(rates_root: Path, n_draws: int,) -> pd.DataFrame:
    data_path = rates_root / 'ifr_rr_draws.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    
    n_draws_in_data = data['draw'].max() + 1    
    downsample = draw_check(n_draws, n_draws_in_data,)
    if downsample:
        data = (data
                .set_index('draw')
                .loc[list(range(n_draws))]
                .reset_index())
    
    data = (data
            .set_index(['location_id', 'draw', 'date'])
            .sort_index()
            .loc[:, ['ifr_lr_rr', 'ifr_hr_rr']])
    
    return data


def load_ifr_data(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'ifr_model_data.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.rename(columns={'ifr_mean': 'ratio_mean',
                                'ifr_std': 'ratio_std',})
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index()
            .loc[:, ['ratio_mean', 'ratio_std', 'is_outlier']])
    
    return data


def load_vaccine_data(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'vaccine_coverage.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = (data
            .loc[:, ['location_id', 'date', 'cumulative_all_effective',]]
            .set_index(['location_id', 'date'])
            .sort_index())
    
    return data
    

def load_ihr(rates_root: Path, n_draws: int,) -> pd.DataFrame:
    data_path = rates_root / 'ihr_draws.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.rename(columns={'ihr': 'ratio',
                                'ihr_fe': 'ratio_fe'})
    
    n_draws_in_data = data['draw'].max() + 1    
    downsample = draw_check(n_draws, n_draws_in_data,)
    if downsample:
        data = (data
                .set_index('draw')
                .loc[list(range(n_draws))]
                .reset_index())
    
    data = (data
            .set_index(['location_id', 'draw', 'date'])
            .sort_index()
            .loc[:, ['ratio', 'ratio_fe']])
    
    return data


def load_ihr_data(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'ihr_model_data.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.rename(columns={'ihr_mean': 'ratio_mean',
                                'ihr_std': 'ratio_std',})
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index()
            .loc[:, ['ratio_mean', 'ratio_std', 'is_outlier']])

    
    return data


def load_idr(rates_root: Path, n_draws: int, limits: Tuple[float, float],) -> pd.DataFrame:
    data_path = rates_root / 'idr_draws.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.rename(columns={'idr': 'ratio',
                                'idr_fe': 'ratio_fe'})
    
    n_draws_in_data = data['draw'].max() + 1    
    downsample = draw_check(n_draws, n_draws_in_data,)
    if downsample:
        data = (data
                .set_index('draw')
                .loc[list(range(n_draws))]
                .reset_index())
    
    data = (data
            .set_index(['location_id', 'draw', 'date'])
            .sort_index()
            .loc[:, ['ratio', 'ratio_fe']])
    data = data.clip(*limits)
    
    return data


def load_idr_data(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'idr_model_data.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.rename(columns={'idr_mean': 'ratio_mean',
                                'idr_std': 'ratio_std',})
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index()
            .loc[:, ['ratio_mean', 'ratio_std', 'is_outlier']])

    
    return data


def load_sero_data(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'sero_data.csv'
    data = pd.read_csv(data_path)
    data = (data
            .loc[:, ['location_id', 'infection_date',
                     'seroprevalence', 'seroprevalence_no_vacc',
                     'sero_sample_mean', 'sero_sample_std',
                     'is_outlier']])
    data = data.rename(columns={'infection_date':'date'})
    data['date'] = pd.to_datetime(data['date'])
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index())
    
    return data


def load_cross_variant_immunity(rates_root: Path, n_draws: int,) -> List:
    data_path = rates_root / 'cross_variant_immunity.pkl'
    with data_path.open('rb') as file:
        data = pickle.load(file)
    
    n_draws_in_data = len(data)
    downsample = draw_check(n_draws, n_draws_in_data,)
    if downsample:
        data = data[:n_draws]
    
    return data


def load_variant_risk_ratio(rates_root: Path, n_draws: int,) -> List:
    data_path = rates_root / 'variant_risk_ratio.pkl'
    with data_path.open('rb') as file:
        data = pickle.load(file)
    
    n_draws_in_data = len(data)
    downsample = draw_check(n_draws, n_draws_in_data,)
    if downsample:
        data = data[:n_draws]
    
    return data


def load_escape_variant_prevalence(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'variants.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index()
            .loc[:, ['escape_variant_prevalence']])
    
    return data


def load_reinfection_inflation_factor(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'reinfection_inflation_factor.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index()
            .loc[:, ['inflation_factor']])
    
    return data


def load_test_data(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'testing.parquet'
    data = pd.read_parquet(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index())
    
    return data


def load_em_scalars(rates_root: Path, n_draws: int,) -> pd.DataFrame:
    data_path = rates_root / 'excess_mortality.parquet'
    data = pd.read_parquet(data_path)
    data = data.reset_index()
    
    n_draws_in_data = data['draw'].max() + 1    
    downsample = draw_check(n_draws, n_draws_in_data,)
    if downsample:
        data = (data
                .set_index('draw')
                .loc[list(range(n_draws))]
                .reset_index())
    
    data = (data
            .set_index(['location_id', 'draw',])
            .sort_index())

    return data


def load_durations(rates_root: Path, n_draws: int,) -> List[Dict[str, int]]:
    data_path = rates_root / 'durations.pkl'
    with data_path.open('rb') as file:
        data = pickle.load(file)
    
    n_draws_in_data = len(data)
    downsample = draw_check(n_draws, n_draws_in_data,)
    if downsample:
        data = data[:n_draws]
        
    return data


def load_model_inputs(rates_root:Path, hierarchy: pd.DataFrame,
                      input_measure: str, fh: bool,) -> Tuple[pd.Series, pd.Series, Dict]:
    if fh:
        data_path = rates_root / 'model_inputs' / 'full_data_fh_subnationals_unscaled.csv'
    else:
        if input_measure == 'deaths':
            data_path = rates_root / 'model_inputs' / 'full_data_unscaled.csv'
        elif input_measure == 'cases':
            data_path = rates_root / 'model_inputs' / 'full_data_unscaled.csv'
        elif input_measure == 'hospitalizations':
            data_path = rates_root / 'model_inputs' / 'use_at_your_own_risk' / 'full_data_extra_hospital.csv'
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
    
    data = (data.groupby('location_id', as_index=False)
            .apply(lambda x: fill_dates(x, [f'cumulative_{input_measure}']))
            .reset_index(drop=True))
    
    logger.debug(f'EXCLUDING ALL {input_measure.upper()} DATA AFTER 11/30/2021.')
    data = data.loc[data['date'] < pd.Timestamp('2021-12-01')]
    
    data, manipulation_metadata = evil_doings(data, hierarchy, input_measure, fh,)
    
    data[f'daily_{input_measure}'] = (data
                                      .groupby('location_id')[f'cumulative_{input_measure}']
                                      .apply(lambda x: x.diff())
                                      .fillna(data[f'cumulative_{input_measure}']))
    data = data.dropna()
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index())
    
    cumulative_data = data[f'cumulative_{input_measure}']
    daily_data = data[f'daily_{input_measure}']

    return cumulative_data, daily_data, manipulation_metadata


def fill_dates(data: pd.DataFrame, interp_vars: List[str]) -> pd.DataFrame:
    data = data.set_index('date').sort_index()
    data = data.asfreq('D').reset_index()
    data[interp_vars] = data[interp_vars].interpolate(axis=0, limit_area='inside')
    data['location_id'] = data['location_id'].fillna(method='pad')
    data['location_id'] = data['location_id'].astype(int)

    return data[['location_id', 'date'] + interp_vars]


def trim_leading_zeros(cumul_data: List[pd.Series],
                       daily_data: List[pd.Series],) -> Tuple[pd.Series]:
    cumul = pd.concat(cumul_data, axis=1)
    cumul = cumul.fillna(0)
    cumul = cumul.sum(axis=1)
    cumul = cumul.loc[cumul > 0]
    start_dates = cumul.reset_index().groupby('location_id')['date'].min()
    start_dates -= pd.Timedelta(days=14)
    start_dates = start_dates.rename('start_date').reset_index()
    
    def _trim_leading_zeros(data: pd.Series, start_dates: pd.DataFrame) -> pd.Series:
        data_name = data.name
        data = data.reset_index().merge(start_dates, how='left')
        data = data.loc[~(data['date'] < data['start_date'])]  # do it this way so we keep NaTs
        del data['start_date']
        data = data.set_index(['location_id', 'date']).loc[:, data_name]
        
        return data
    
    trimmed_data = (_trim_leading_zeros(data, start_dates) for data in cumul_data + daily_data)
    
    return trimmed_data


def load_hierarchy(rates_root:Path, fh: bool, gbd: bool,) -> pd.DataFrame:
    if gbd:
        data_path = rates_root / 'model_inputs' / 'locations' / 'gbd_analysis_hierarchy.csv'
        data = pd.read_csv(data_path)
        data = data.sort_values('sort_order').reset_index(drop=True)
    elif fh:
        data_path = rates_root / 'model_inputs' / 'locations' / 'fh_small_area_hierarchy.csv'
        data = pd.read_csv(data_path)
        data = data.sort_values('sort_order').reset_index(drop=True)
    else:
        # data_path = rates_root / 'model_inputs' / 'locations' / 'modeling_hierarchy.csv'
        # data = pd.read_csv(data_path)
        # data = data.sort_values('sort_order').reset_index(drop=True)
        ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
        logger.warning('Using ZAF subnats...')
        gbd_path = rates_root / 'model_inputs' / 'locations' / 'gbd_analysis_hierarchy.csv'
        covid_path = rates_root / 'model_inputs' / 'locations' / 'modeling_hierarchy.csv'

        # get ZAF only from GBD for now
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
        ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    
    return data


def load_population(rates_root: Path) -> pd.DataFrame:
    data_path = rates_root / 'model_inputs' / 'output_measures' / 'population' / 'all_populations.csv'
    data = pd.read_csv(data_path)
    is_2019 = data['year_id'] == 2019
    is_bothsex = data['sex_id'] == 3
    is_alllage = data['age_group_id'] == 22
    data = (data
            .loc[is_2019 & is_bothsex & is_alllage]
            .set_index('location_id')
            .loc[:, ['population']])

    return data


def write_infections_draws(data: pd.DataFrame,
                           infections_draws_dir: Path,):
    draw_col = np.array([c for c in data.columns if c.startswith('draw_')]).item()
    draw = int(draw_col.split('_')[-1])
    data = data.rename(columns={draw_col:'infections_draw'})
    data['draw'] = draw
    
    out_path = infections_draws_dir / f'{draw_col}.csv'
    data.reset_index().to_csv(out_path, index=False)
    # data = data.reset_index()
    # data['date'] = data['date'].astype(str)
    # out_path = infections_draws_dir / f'{draw_col}.parquet'
    # data.to_parquet(out_path, engine='fastparquet', compression='gzip')
    
    return out_path


def write_ratio_draws(data_list: List[pd.Series],
                      estimated_ratio: str,
                      ratio_draws_dir: Path,
                      variant_risk_ratio: List[float],
                      durations: List[int],):
    if estimated_ratio == 'ifr':
        if len(data_list) != 3:
            raise ValueError('IFR, but not 3 elements in data list.')
        data = data_list[0]
        data_lr = data_list[1]
        data_hr = data_list[2]
    else:
        if len(data_list) != 1:
            raise ValueError('Not IFR, but multiple elements in data list.')
        data = data_list[0]
    draw_col = data.name
    draw = int(draw_col.split('_')[-1])
    data = data.rename(f'{estimated_ratio}_draw')
    data = data.to_frame()
    if estimated_ratio == 'ifr':
        data['ifr_lr_draw'] = data['ifr_draw'] * data_lr
        data['ifr_hr_draw'] = data['ifr_draw'] * data_hr
    data['draw'] = draw
    data['duration'] = durations[draw]
    if estimated_ratio in ['ifr', 'ihr']:
        data['variant_risk_ratio'] = variant_risk_ratio[draw]

    out_path = ratio_draws_dir / f'{draw_col}.csv'
    data.reset_index().to_csv(out_path, index=False)
    # data = data.reset_index()
    # data['date'] = data['date'].astype(str)
    # out_path = ratio_draws_dir / f'{draw_col}.parquet'
    # data.to_parquet(out_path, engine='fastparquet', compression='gzip')
    
    return out_path

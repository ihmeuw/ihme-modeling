import pandas as pd
import numpy as np
from multiprocessing import Pool
from sklearn.linear_model import LinearRegression
import os
import pyreadr

from cod_prep.claude.claude_io import get_claude_data
from cod_prep.downloaders.engine_room import get_package_map
from cod_prep.claude.claude_io import get_datasets

from db_queries import get_location_metadata
from db_queries import get_cause_metadata
from db_queries import get_outputs
from db_queries import get_age_metadata

from covid_correction_settings import get_covid_correction_settings

def merge_n_check_left(df1, df2):
    original_shape = df1.shape[0]
    df1 = df1.merge(df2, how='left')
    assert original_shape == df1.shape[0]
    return df1

def check_and_make_dir(setting_index, SETTINGS=get_covid_correction_settings()):
    filepath = SETTINGS[setting_index]
    print("checking if folder exists: ", filepath)
    if not os.path.exists(filepath):
        print("creating folder")
        os.mkdir(filepath)

def multiprocess_lambda(df, actual_year, chunk_lambda_function):
    num_of_chunks = 30
    chunks = np.array_split(df, num_of_chunks)
    p = Pool(num_of_chunks)
    tmp = list(p.apply_async(chunk_lambda_function, args=(chunk, actual_year)) for chunk in chunks)
    p.close()
    p.join()
    tmp_df = pd.DataFrame()
    for chunk in list(range(num_of_chunks)):
        tmp_df = pd.concat([tmp_df, tmp[chunk].get()])
        
    return tmp_df


def populate_list_of_locations_with_2020_data(SETTINGS=get_covid_correction_settings()):
    filepath = SETTINGS['LOCATIONS_WITH_2020_VR']

    df = get_datasets(is_active=True, data_type_id=9, year_id=2020, code_system_id=1,
             force_rerun=False, block_rerun=False)

    df_mccd = get_datasets(is_active=True, data_type_id=9, filter_locations=True,
                    year_id=2020, iso3='IND')

    df = pd.concat([df, df_mccd])

    locs= get_location_metadata(release_id=SETTINGS['RELEASE_ID'], location_set_id=35) 
    locs = locs[['location_id','location_name','ihme_loc_id','level','super_region_name']]
    locs['iso3'] = locs['ihme_loc_id'].apply(lambda x: x[:3])

    df = merge_n_check_left(df, locs[['location_id','iso3']])
    df=  df[['iso3']].drop_duplicates().merge(locs, how='left')[['iso3','location_id','location_name','level','super_region_name']]
    
    df.to_csv(filepath,index=False)

def get_list_of_active_iso3s(SETTINGS=get_covid_correction_settings()):
    filepath = SETTINGS['LOCATIONS_WITH_2020_VR']
    iso3s = list(pd.read_csv(filepath)['iso3'].unique())
    return iso3s

def get_level_3_cause_map(SETTINGS=get_covid_correction_settings()):
    causes = get_cause_metadata(release_id=16, cause_set_id=4)[['cause_id', 'path_to_top_parent', 'level']]
    causes = causes.query("level > 3")
    causes['lvl_3_cause'] = causes.path_to_top_parent.apply(lambda x: int(x.split(',')[3]))
    causes = causes.query("lvl_3_cause != 366")[['cause_id','lvl_3_cause']]
    return causes

def format_causes_and_garbage(df, SETTINGS=get_covid_correction_settings()):
    garbage = df.query("cause_id == 743")
    df = df.query("cause_id != 743")
    lvl_3_map = get_level_3_cause_map()
    df = merge_n_check_left(df, lvl_3_map)
    df['cause_id'] = df['cause_id'].where(df['lvl_3_cause'].isnull(), df['lvl_3_cause'])
    pkg = get_package_map(1)
    pkg = pkg[['code_id', 'package_id']]
    pkg['package_id'] = pkg['package_id'].apply(lambda x: int(x))
    garbage['code_id'].replace(26616, 26614, inplace=True)
    garbage = merge_n_check_left(garbage, pkg)
    assert garbage[garbage['package_id'].isnull()].shape[0] == 0
    garbage['package_id'] = garbage.package_id.astype(int)
    garbage['cause_id'] = garbage.cause_id.astype(str)
    assert garbage['cause_id'].unique() == '743'
    garbage['cause_id'] = "_p_" + garbage['package_id'].astype(str)
    df['cause_id'] = df.cause_id.astype(str)
    df = pd.concat([df,garbage])
    return df

def aggregate_causes_to_level_3(df, SETTINGS=get_covid_correction_settings()):
    lvl_3_map = get_level_3_cause_map()
    df = merge_n_check_left(df, lvl_3_map)
    df['cause_id'] = df['cause_id'].where(df['lvl_3_cause'].isnull(), df['lvl_3_cause'])
    return df

def group_ages(row):
    if row['age_group_id'] in [16,17,18,19,20,30,31,32,235]:
        return "55+"
    elif row['age_group_id'] in [8,9,10,11,12,13,14,15]:
        return "15_54"
    elif row['age_group_id'] in [2,3,6,7,34,238,388,389]:
        return "under_15"
    else:
        assert False

def get_data_temp(): 
    launch_object = {
        'project_id': 15,
        'data_type_id': [9],
        'year_id': range(2020, 2025),
        'is_active': True,
        'filter_locations': True,
        'code_system_id': 1
    }
    ds = get_datasets(**launch_object)
    isos = ds.iso3.unique().tolist()
    nid_etids = ds[['nid', 'extract_type_id']].drop_duplicates().to_records(index=False).tolist()
    dfs = []
    for nid, etid in nid_etids:
        df = pd.read_parquet(FILEPATH.format(nid, etid))
        dfs.append(df)
    df = pd.concat(dfs)
    prev = get_claude_data('redistribution', iso3=isos, is_active=True, data_type_id=9,
                          year_id=range(2015, 2020), code_system_id=1, filter_locations=True)
    df = pd.concat([df, prev])
    return df


def get_and_format_claude_data_for_predictions(iso3s, SETTINGS=get_covid_correction_settings(), 
    retain_age_group_id=False, claude_phase="disaggregation"):
    if claude_phase == "disaggregation":
        df = get_claude_data(
            'disaggregation', is_active=True, iso3=iso3s,
            year_id=list(range(2015, max(SETTINGS['COVID_YEARS']) + 1)),
            force_rerun=False, block_rerun=False, verbose=False,
            data_type_id=9,
            code_system_id=1
            )
        locs = pd.read_csv(SETTINGS['LOCATIONS_WITH_2020_VR'])    
        df = merge_n_check_left(df, locs) 
        df = df.groupby([
            'nid','extract_type_id','site_id','year_id',
            'age_group_id','sex_id','cause_id','code_id','iso3'
            ], as_index=False)['deaths'].sum()
        df = format_causes_and_garbage(df)
    else:
        df = get_claude_data(
            claude_phase, is_active=True, iso3=iso3s,
            year_id=list(range(2015, max(SETTINGS['COVID_YEARS']) + 1)),
            force_rerun=False, block_rerun=False, verbose=False,
            data_type_id=9,
            code_system_id=1
            )
        df_mccd = get_claude_data(claude_phase, is_active=True, data_type_id=9, filter_locations=True,
                            year_id=list(range(2015, max(SETTINGS['COVID_YEARS']) + 1)), iso3='IND',
                            force_rerun=False, block_rerun=False, verbose=False,
                            )
        df = pd.concat([df, df_mccd])
        locs = get_location_metadata(release_id=16, location_set_id=35)
        locs['iso3'] = locs['ihme_loc_id'].apply(lambda x: x[:3])
        locs = locs[['iso3','location_id','location_name','level','super_region_name']]   
        df = merge_n_check_left(df, locs) 
        df = df.groupby([
            'nid','extract_type_id','site_id','year_id',
            'age_group_id','sex_id','cause_id','iso3'
            ], as_index=False)['deaths'].sum()
        df = aggregate_causes_to_level_3(df)

    df = df.query("extract_type_id != 1631")
    df['age_group'] = df.apply(lambda x: group_ages(x), axis=1)
    locs = locs.query("level == 3")
    df = merge_n_check_left(df, locs) 
    if retain_age_group_id:
        df = df.groupby(['location_id','super_region_name','year_id','sex_id','cause_id','age_group','age_group_id'], 
            as_index=False)['deaths'].sum()
    else:
        df = df.groupby(['location_id','super_region_name','year_id','sex_id','cause_id','age_group'], 
            as_index=False)['deaths'].sum()
    return df

def convert_year_column_names_to_strings(df, SETTINGS=get_covid_correction_settings()):
    # convert each column year (int) to a string
    for year in range(2015, SETTINGS['MAX_YEAR_FOR_RANGE']):
        df = df.rename(columns={year:str(year)}) 
    return df

def impute_missing_values(row, year):
    value = row[year]
    if np.isnan(value):
        value = np.nanmean(row[['2015','2016','2017','2018','2019']])
    if pd.isnull(value):
        return 0
    else:
        return value

def chunk_impute_missing(chunk, year):
    assert type(year) == str
    chunk[year] = chunk.apply(lambda x: impute_missing_values(x, year), axis=1)
    return chunk

def impute_missing_values_prior_2020(df, SETTINGS=get_covid_correction_settings()):
    for year in range(2015, 2020):
        df = multiprocess_lambda(df, str(year), chunk_impute_missing)    
    return df

def apply_linear_regression(row, year):
    lr = LinearRegression()
    row.fillna(0, inplace=True)
    x = np.array([2015, 2016, 2017, 2018, 2019]).reshape(-1,1)
    y = np.array(row[['2015','2016','2017','2018','2019']]).reshape(-1,1)
    lr.fit(x,y)

    prediction = lr.predict([[year]])[0][0]
    coef = lr.coef_[0][0]
    if year >= 2020:
        return pd.Series([prediction, coef])
    else:
        return prediction

def chunk_predict_linear_regression(chunk, year):
    if year >= 2020:
        chunk[[f'imputed_{year}',f'lr_coef_{year}']] = chunk.apply(lambda x: apply_linear_regression(x, year), axis=1)
    else:    
        chunk[f'imputed_{year}'] = chunk.apply(lambda x: apply_linear_regression(x, year), axis=1)
    return chunk

def fill_missing_values_post_2020(df, SETTINGS=get_covid_correction_settings()):
    final_df = pd.DataFrame()
    for location_id in df.location_id.unique():
        loc_df = df.query(f"location_id == {location_id}")
        for year in range(2020, SETTINGS['MAX_YEAR_FOR_RANGE']):
            year = str(year)
            if np.isnan(loc_df[year].unique()).all():
                loc_df[year] = -.00001
            loc_df[year].fillna(0, inplace=True)
        final_df = pd.concat([final_df, loc_df])

    return final_df

def predict_years_using_linear_regression(df, SETTINGS=get_covid_correction_settings()):
    for year in range(2019, SETTINGS['MAX_YEAR_FOR_RANGE']):
        df = multiprocess_lambda(df, year, chunk_predict_linear_regression)
    return df

def calculate_difference_between_year_and_pre_covid(df, SETTINGS=get_covid_correction_settings()):
    df['mean_pre_covid'] = df[['2015','2016','2017','2018','2019']].mean(axis=1)
    for year in range(2020, SETTINGS['MAX_YEAR_FOR_RANGE']):
        df[f'{year}_vs_other_years'] = df[f'{year}'] - df['mean_pre_covid']
    return df

def calculate_normalized_std_of_pre_covid(df, SETTINGS=get_covid_correction_settings()):
    df['std'] = df[['2015','2016','2017','2018','2019']].std(axis=1)
    df['std'] = df['std'] / df['std'].max()
    return df

def calculate_min_of_time_series(df, SETTINGS=get_covid_correction_settings()):
    for year in list(range(2020, SETTINGS['MAX_YEAR_FOR_RANGE'])):
        # make a list of years for columns
        years = list(range(2015, year))
        years = [str(i) for i in years]
        
        df[f'{year}_min'] = df[years].min(axis=1)
        df[f'{year}_is_min'] = (df[f'{year}'] == df[f'{year}_min']) * 1
    return df

def calculate_max_of_time_series(df, SETTINGS=get_covid_correction_settings()):
    for year in list(range(2020, SETTINGS['MAX_YEAR_FOR_RANGE'])):
        # make a list of years for columns
        years = list(range(2015, year))
        years = [str(i) for i in years]
        
        df[f'{year}_max'] = df[years].max(axis=1)
        df[f'{year}_is_max'] = (df[f'{year}'] == df[f'{year}_max']) * 1
    return df

def calc_ratios_of_given_year_vs_pre_covid(df, SETTINGS=get_covid_correction_settings()):
    for year in range(2020,SETTINGS['MAX_YEAR_FOR_RANGE']):
        df[f'{year}_vs_2019'] = df[f'{year}'] / (df['2019'] + .00001)
        df[f'{year}_vs_2018'] = df[f'{year}'] / (df['2018'] + .00001)
        df[f'{year}_vs_2017'] = df[f'{year}'] / (df['2017'] + .00001)
        df[f'{year}_vs_2016'] = df[f'{year}'] / (df['2016'] + .00001)
        df[f'{year}_vs_2015'] = df[f'{year}'] / (df['2015'] + .00001)
        df[f'{year}_vs_2019'] = df[f'{year}_vs_2019'].clip(0,3)
        df[f'{year}_vs_2018'] = df[f'{year}_vs_2018'].clip(0,3)
        df[f'{year}_vs_2017'] = df[f'{year}_vs_2017'].clip(0,3)
        df[f'{year}_vs_2016'] = df[f'{year}_vs_2016'].clip(0,3)
        df[f'{year}_vs_2015'] = df[f'{year}_vs_2015'].clip(0,3)    
    
    return df

def calculate_residuals_from_observed_and_regression(df, SETTINGS=get_covid_correction_settings()):
    for year in range(2019, SETTINGS['MAX_YEAR_FOR_RANGE']):
        df[f'imputed_{year}_residual'] = df[f'{year}'] - df[f'imputed_{year}']
    return df

def normalize_time_series(df, SETTINGS=get_covid_correction_settings(), retain_age_group_id=False):
    if retain_age_group_id:
        max_loc_cause_year = df.groupby(['location_id','cause_id','sex_id','age_group','age_group_id'], as_index=False)['deaths'].max()
    else:
        max_loc_cause_year = df.groupby(['location_id','cause_id','sex_id','age_group'], as_index=False)['deaths'].max()
    max_loc_cause_year = max_loc_cause_year.rename(columns={"deaths":"max_deaths"})
    df = merge_n_check_left(df, max_loc_cause_year)
    df['pct_max'] = df['deaths'] / df['max_deaths']
    df['pct_max'].fillna(0, inplace=True)
    return df

def create_features(df, SETTINGS=get_covid_correction_settings(), retain_age_group_id=False, normalize_data=True):
    original_deaths = {}
    for year in range(2020,SETTINGS['MAX_YEAR_FOR_RANGE']):
        if retain_age_group_id:
            original_deaths[f"{year}_deaths"] = df.query(f"year_id=={year}")[['location_id','cause_id','sex_id','deaths','age_group','age_group_id']]
        else:
            original_deaths[f"{year}_deaths"] = df.query(f"year_id=={year}")[['location_id','cause_id','sex_id','deaths','age_group']]
        original_deaths[f"{year}_deaths"].fillna(0, inplace=True)

    if normalize_data:
        df = normalize_time_series(df,retain_age_group_id=retain_age_group_id)
    else:
        df['pct_max'] = df['deaths']
    if retain_age_group_id:
        df = df.pivot(index=['location_id','super_region_name','cause_id','sex_id','age_group','age_group_id'],
        columns=['year_id'], values='pct_max')
    else:
        df = df.pivot(index=['location_id','super_region_name','cause_id','sex_id','age_group'], columns=['year_id'], values='pct_max')
    df.reset_index(inplace=True)
    df = df.query("cause_id != 1048")  
    df = convert_year_column_names_to_strings(df)
    df = impute_missing_values_prior_2020(df)
    df = fill_missing_values_post_2020(df)
    df = predict_years_using_linear_regression(df)
    df = calculate_difference_between_year_and_pre_covid(df)
    df = calculate_normalized_std_of_pre_covid(df)
    df = merge_n_check_left(df, original_deaths['2020_deaths'])
    df['deaths'] = df['deaths'] / 100000
    for year in range(2020,SETTINGS['MAX_YEAR_FOR_RANGE']):
        original_deaths[f"{year}_deaths"].rename(columns={"deaths":f"deaths_{year}"}, inplace=True)
        if retain_age_group_id:
            original_deaths[f"{year}_deaths"] = original_deaths[f"{year}_deaths"].groupby([
                'location_id','cause_id','sex_id','age_group','age_group_id'], as_index=False)[f"deaths_{year}"].sum()
        else:
            original_deaths[f"{year}_deaths"] = original_deaths[f"{year}_deaths"].groupby([
                'location_id','cause_id','sex_id','age_group'], as_index=False)[f"deaths_{year}"].sum()

        df = merge_n_check_left(df, original_deaths[f"{year}_deaths"])

    df['highest_deaths'] = (1 / df['2020']) * df['deaths_2020']
    df['imputed_2020_deaths'] = df['imputed_2020'] * df['highest_deaths'] 
    df['deaths'].fillna(0, inplace=True)
    df['deaths_2020'].fillna(0, inplace=True)
    df['highest_deaths'].fillna(0, inplace=True)
    df['imputed_2020_deaths'].fillna(0, inplace=True)
    df['difference_in_deaths_from_expected'] = df['deaths_2020'] - df['imputed_2020_deaths']
    df = calc_ratios_of_given_year_vs_pre_covid(df)
    df = calculate_residuals_from_observed_and_regression(df)
    df = calculate_min_of_time_series(df)
    df = calculate_max_of_time_series(df)
    return df

def get_svm_columns(model_year):
    model_columns = [
        f"{model_year}_is_max", f"imputed_{model_year}", f"imputed_{model_year}_residual",
        f"{model_year}_vs_other_years","std", f"{model_year}_vs_2019", f"{model_year}_vs_2018", f"{model_year}_vs_2017", 
        f"{model_year}_vs_2016", f"{model_year}_vs_2015", f'{model_year}_is_min', 
        'imputed_2019','imputed_2019_residual', f'lr_coef_{model_year}','deaths',
        f'tag_{model_year}'
    ]
    return model_columns

def get_ihme_covid_results(SETTINGS):
    location_ids = list(pd.read_csv(SETTINGS['LOCATIONS_WITH_2020_VR'])['location_id'].unique())
    location_ids.append(163)
    locs = get_location_metadata(location_set_id=21, release_id=9)
    locs['iso3'] = locs['ihme_loc_id'].apply(lambda x: x[:3])
    most_detailed_locs = locs.query("most_detailed == 1")['location_id'].unique()
    gbd_2021 = get_outputs(    
        "cause",
        release_id=9, 
        year_id=[2020,2021],
        location_id=list(set(most_detailed_locs)),
        sex_id=[1,2],
        age_group_id=list(get_age_metadata(release_id=16)['age_group_id'].unique()),
        cause_id=1048
        )
    gbd_2021 = gbd_2021[['age_group_id','location_id','sex_id','year_id','val']]
    gbd_2021['age_group'] = gbd_2021.apply(lambda x: group_ages(x), axis=1)
    gbd_2021 = gbd_2021.merge(locs[['location_id','iso3']])
    gbd_2021 = gbd_2021.drop('location_id', axis=1).merge(locs.query("level == 3")[['iso3','location_id']])
    gbd_2021 = gbd_2021.groupby(['location_id','age_group','age_group_id','sex_id','year_id'], as_index=False)['val'].sum()
    gbd_2021.to_csv(SETTINGS['COVID_DEATHS'], index=False)
    return gbd_2021
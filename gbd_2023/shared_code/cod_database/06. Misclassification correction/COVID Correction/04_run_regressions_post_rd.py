import pandas as pd
import numpy as np
import time
import scipy as sp
from multiprocessing import Pool

from covid_correction_settings import get_covid_correction_settings
from covid_correction_helper_functions import *

def normalize_x_y(df, column):
    X = df[column].tolist()
    Y = df.change_from_cfac_rate.tolist()
    minx = min(X)
    maxx = max(X)
    X = [((x - minx) / (maxx - minx)) for x in X]
    miny = min(Y)
    maxy = max(Y)
    Y = [((y - miny) / (maxy - miny)) for y in Y]
    X_array = np.array(X).reshape(-1, 1)
    Y_array = np.array(Y).reshape(-1, 1)
    return X, Y, X_array, Y_array

def run_linear_regression(X, Y):
    lr = LinearRegression()
    lr.fit(X, Y)
    return lr.coef_[0][0]

def construct_result_dict(cause_id, iso3, age_group, df, column):
    if iso3 == 'IND':
        df = df[df['year_id'] == 2020]
    tmp = df[df['age_group'] == age_group].copy()
    if len(tmp['change_from_cfac_rate'].unique()) <= 1:
        pearson_r = 0 
        pearson_p = 0
        coef = 0
        insight = "only 1 unique cause value"
    elif len(tmp['covid_rate'].unique()) <= 1:
        pearson_r = 0 
        pearson_p = 0
        coef = 0
        insight = "only 1 unique covid value"
    elif tmp.shape[0] <= 3:
        pearson_r = 0 
        pearson_p = 0
        coef = 0
        insight = "3 or fewer datapoints"
        if (age_group == '55+') & ((pearson_r <= 0) | (pearson_p >= .05)):
            tmp = df[df['age_group_id'].isin([12, 13, 14, 15, 16, 17, 18, 19])].copy()
            if len(tmp['change_from_cfac_rate'].unique()) > 1:
                if len(tmp['change_from_cfac_rate'].unique()) > 1:
                    if tmp.shape[0] > 3:
                            X, Y, X_array, Y_array = normalize_x_y(tmp, column)
                            pearson_r, pearson_p = sp.stats.pearsonr(X, Y)
                            coef = run_linear_regression(X_array, Y_array)
                            insight = "Use Correlation 35_74"
    else:
        X, Y, X_array, Y_array = normalize_x_y(tmp, column)
        pearson_r, pearson_p = sp.stats.pearsonr(X, Y)
        coef = run_linear_regression(X_array, Y_array)
        insight = "no restrictions"
        if (age_group == '55+') & ((pearson_r <= 0) | (pearson_p >= .05)):
            tmp = df[df['age_group_id'].isin([12, 13, 14, 15, 16, 17, 18, 19])].copy()
            if len(tmp['change_from_cfac_rate'].unique()) > 1:
                if len(tmp['change_from_cfac_rate'].unique()) > 1:
                    if tmp.shape[0] > 3:
                            X, Y, X_array, Y_array = normalize_x_y(tmp, column)
                            pearson_r, pearson_p = sp.stats.pearsonr(X, Y)
                            coef = run_linear_regression(X_array, Y_array)
                            insight = "Use Correlation 35_74"
    
    tmp_dct = {
        "cause_id":cause_id,
        "iso3":iso3,
        "age_group":age_group,
        "covid_pearson":np.round(pearson_r,3),
        "covid_p":np.round(pearson_p,3),
        "data_points":tmp.shape[0],
        "coef":coef,
        "insight": insight,
        "column":column
    }
    return tmp_dct

def run_regressions(iso3):
    iso3_df = pd.read_csv(SETTINGS['REGRESSION_DATA_ISO_CAUSE_post_rd'].format(iso3, "all"))
    iso3_df['cause_id'] = iso3_df['cause_id'].astype(str)
    final_stats = pd.DataFrame()
    for cause_id in iso3_df.cause_id.unique():
        df = iso3_df.query(f"cause_id == '{cause_id}'")
        df = df.loc[
            (df.change_from_cfac_rate.notnull()) &
            (df.covid_rate.notnull())
        ]
        
        for age_group in df.age_group.unique():
            tmp_dct = construct_result_dict(cause_id, iso3, age_group, df, "covid_rate")
            df_dict = pd.DataFrame([tmp_dct])
            final_stats = pd.concat([final_stats, df_dict])
    return final_stats

if __name__ == "__main__":    

    SETTINGS = get_covid_correction_settings()
    df = pd.read_csv(SETTINGS['DATAFRAME_WITH_COVID_AND_MOBILITY_post_rd'])
    df = df.loc[
        (df.change_from_cfac_rate.notnull()) &
        (df.covid_rate.notnull())
    ]

    start_time = time.time()
    p = Pool(30)
    final_stats_tmp = list(p.apply_async(run_regressions, args=(iso3,)) for iso3 in df.iso3.unique())
    p.close()
    p.join()
    final_stats = pd.DataFrame()
    for i in list(range(len(final_stats_tmp))):
        final_stats = pd.concat([final_stats, final_stats_tmp[i].get()])
    final_stats['covid_pearson'].fillna(0, inplace=True)
    final_stats['covid_p'].fillna(0, inplace=True)
    locs = pd.read_csv(SETTINGS['LOCATIONS_WITH_2020_VR'])
    locs = locs.query("level == 3")
    final_stats = merge_n_check_left(final_stats, locs)
    final_stats.to_csv(SETTINGS["CORRELATION_ESTIMATES_post_rd"], index=False)


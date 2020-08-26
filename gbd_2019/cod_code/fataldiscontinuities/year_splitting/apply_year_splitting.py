import pandas as pd
import numpy as np
from multiprocessing import Pool
from shock_tools import *


def apply_year_weight_combination(row, year, weight):
    split_row = row.copy()
    year = int(year)
    weight = float(weight)
    split_row['year_id'] = year
    split_row['best'] = row['best'] * weight
    if row['high'] != "NULL":
        split_row['high'] = row['high'] * weight
    if row['high'] != "NULL":
        split_row['low'] = row['low'] * weight
    return split_row


def iterate_through_df_and_split_years(df):
    final = pd.DataFrame()
    for index, row in df.iterrows():
        years = convert_str_to_list(row['years'])
        weights = convert_str_to_list(row['year_weights'])
        if len(years) > 1:
            split_status = 1
        else:
            split_status = 0
        for [year, weight] in zip(years, weights):
            split_row = apply_year_weight_combination(row, year, weight)
            split_row['split_status'] = split_status
            final = final.append(split_row)
    return final


def parallelize(df, func):
    workers = 8
    df_split = np.array_split(df, workers)
    pool = Pool(workers)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def report_years_split(df):
    split_event_count = (df['location_map_data_id'].value_counts() > 1).sum()
    print("{} events were split between multiple years".format(split_event_count))


def run_year_splitting(df):
    df['high'] = df['high'].fillna("NULL")
    df['low'] = df['low'].fillna("NULL")
    
    original_death_count = df.copy()['best'].sum()
    df = parallelize(df, iterate_through_df_and_split_years)
    split_death_count = df['best'].sum()
    assert np.isclose(original_death_count, split_death_count, rtol=1), (
        "deaths before split does not equal deaths after split")
    report_years_split(df)
    
    df['high'].replace("NULL", float("nan"), inplace=True)
    df['low'].replace("NULL", float("nan"), inplace=True)

    df = df.query("year_id >= 1950")

    return df

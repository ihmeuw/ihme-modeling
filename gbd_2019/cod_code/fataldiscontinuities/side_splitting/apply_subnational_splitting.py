import pandas as pd
import numpy as np
from db_queries import get_location_metadata
from db_queries import get_population
from multiprocessing import Pool


def iterate_through_df_and_split_nationals_by_population(all_vars):

    df = all_vars[0]
    locs = all_vars[1]
    pop = all_vars[2]
    worker_id = all_vars[3]
    df.drop("most_detailed", axis=1, inplace=True, errors="ignore")
    df['location_id'] = df['location_id'].apply(lambda x: int(x))
    df = pd.merge(left=df, right=locs[['location_id', 'most_detailed']], how='left')

    final = pd.DataFrame()
    events_split_count = 0
    total_rows = df.copy().shape[0]
    for index, row in df.iterrows():
        if index % max(round(total_rows * .25), 1) == 0:
            print(worker_id, "has comepleted", index, "out of", total_rows)
        event_df = pd.DataFrame()
        if row['most_detailed'] == 0:
            parent_loc_id = row['location_id']
            year_id = row['year_id']
            national_deaths = row['best']
            national_high = float(row['high'])
            national_low = float(row['low'])
            national_pop = pop.query("year_id == {} & location_id == {}".format(
                year_id, parent_loc_id))['population'].iloc[0]
            subnat_ids = locs.query("parent_id == {}".format(parent_loc_id))
            subnat_ids = (subnat_ids['location_id'].unique())

            events_split_count += 1
            for subnat_id in subnat_ids:
                subnat_pop = pop.query("year_id == {} & location_id == {}".format(
                    year_id, subnat_id))['population'].iloc[0]
                pop_percent = float(subnat_pop) / float(national_pop)
                row['not_detailed'] = 1
                row['location_id'] = subnat_id
                row['best'] = national_deaths * pop_percent
                row['pop'] = subnat_pop
                if national_high is not None:
                    row['high'] = national_high * pop_percent
                if national_low is not None:
                    row['low'] = national_low * pop_percent
                event_df = event_df.append(row)

            final = final.append(event_df)
        elif row['most_detailed'] == 1:
            row['not_detailed'] = 0
            final = final.append(row)

    final = final.reset_index(drop=True)
    return final


def parallelize(df, locs, pop, func):
    workers = 20
    df_split = np.array_split(df, workers)
    locs = [locs] * workers
    pop = [pop] * workers
    worker_ids = list(range(1, workers + 1))
    all_vars = list(zip(df_split, locs, pop, worker_ids))
    pool = Pool(workers)
    df = pd.concat(pool.map(func, all_vars))
    pool.close()
    pool.join()
    return df


def run_subnational_splitting(df):
    original_death_count = df.copy()['best'].sum()
    df['high'] = df['high'].astype("float")
    df['low'] = df['low'].astype("float")
    df['location_id'] = df['location_id'].apply(lambda x: int(x))

    all_locations = get_location_metadata(location_set_id=21)
    not_detailed_locations = all_locations.query("most_detailed == 0")
    not_detailed_locations = set(not_detailed_locations['location_id'])
    df_locations = set(df['location_id'])
    has_nationals = bool(len(not_detailed_locations.intersection(df_locations)))
    locs = get_location_metadata(location_set_id=21)
    pop = get_population(location_id=-1, year_id=-1, decomp_step="step1", location_set_id=21)
    count = 0
    while has_nationals:
        count += 1
        print("iteration {}".format(count))
        if count >= 7:
        df = parallelize(df, locs, pop, iterate_through_df_and_split_nationals_by_population)
        df_locations = set(df['location_id'])
        has_nationals = bool(len(not_detailed_locations.intersection(df_locations)))

    split_death_count = df['best'].sum()

    if "not_detailed" not in df.columns:
        df['not_detailed'] = 0

    difference = split_death_count - original_death_count
    assert np.isclose(difference, 0, atol=10), (
        "deaths before split does not equal deaths after split: Difference {}".format(difference))

    df['high'].fillna(0, inplace=True)
    df['low'].fillna(0, inplace=True)
    df['best'] = df['best'].apply(lambda x: float(x))

    df = df.groupby(["source_event_id", "location_id", "cause_id", "year_id",
                     "nid", "source_id", "raw_data_id", "year_split_data_id",
                     "split_status", "not_detailed", "event_name", "sex_id",
                     "age_group_id", "notes"], as_index=False)['low', 'best', 'high'].sum()

    df['high'] = df['high'].replace(0, float('nan'))
    df['low'] = df['low'].replace(0, float('nan'))
    return df

from functools import partial

import pandas as pd
import numpy as np
from multiprocessing import Pool
from db_queries import get_population
from shock_tools import *

from shock_prep.etl.side_splitting_weights import (
    get_weights,
    get_weights_detailed
)


def fill_side_columns_with_non_null_values(df):
    df['deaths_a'] = df['deaths_a'].fillna(0)
    df['deaths_b'] = df['deaths_b'].fillna(0)
    df["side_a"] = df["side_a"].fillna("")
    df["side_b"] = df["side_b"].fillna("")
    return df


def convert_to_lists_of_ints(row, column):
    locs = row[column]
    locs = locs.replace("'", "")
    locs = convert_str_to_list(locs)
    locs = list(map(float, locs))
    for loc in locs:
        assert type(loc) == float, "a location id is not an int"
    return locs


def split_sides_with_known_deaths(side_a, side_b, deaths_a, deaths_b,
                                  best, year, high, low, row, pop,
                                  event_population, event_df):
    deaths_one = best

    if np.isnan(deaths_b):
        deaths_b = 0
    if deaths_b == None:
        deaths_b = 0
    if deaths_a == None:
        deaths_a = 0
    if np.isnan(deaths_a):
        deaths_a = 0

    if len(side_b) == 0:
        deaths_a += deaths_b
        deaths_b = 0

    if side_b is None:
        deaths_a += deaths_b
        deaths_b = 0

    known_deaths = deaths_a + deaths_b
    unkown_deaths = best - known_deaths
    
    side_a_population = pop[(pop['location_id'].isin(side_a)) &
                            (pop['year_id'] == year)]['population'].sum()
    
    side_b_population = pop[(pop['location_id'].isin(side_b)) &
                            (pop['year_id'] == year)]['population'].sum()
    
    side_a_percentage = side_a_population / event_population
    
    side_b_percentage = side_b_population / event_population
    
    # add unknown deaths on, splitting by population
    deaths_a = deaths_a + (unkown_deaths * side_a_percentage)
    deaths_b = deaths_b + (unkown_deaths * side_b_percentage)
    
    # split out deaths by population of location
    for location in side_a:
        # grab the population for that location
        location_pop = pop[(pop['location_id'] == location) &
                           (pop['year_id'] == year)]['population'].sum()

        # find the percentage of population for that event
        population_percentage = location_pop / side_a_population

        row['best'] = deaths_a * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    for location in side_b:
        # grab the population for that location
        location_pop = pop[(pop['location_id'] == int(location)) &
                           (pop['year_id'] == int(year))]['population'].sum()

        # find the percentage of population for that event
        population_percentage = location_pop / side_b_population

        row['best'] = deaths_b * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    assert np.isclose(deaths_one, event_df['best'].sum(), atol=50), "{} SEI {} deaths, {}, {}, {}".format(row['source_event_id'].iloc[0], deaths_one-event_df['best'].sum(), deaths_a, deaths_b, side_b)

    return event_df


def split_sides_for_terrorism(side_a, side_b, deaths_a, deaths_b,
                              best, year, high, low, row, pop,
                              event_population, event_df):
    # add unknown deaths on, splitting by population
    deaths_one = best

    side_b = set(side_b) - set(side_a)

    side_a_population = pop[(pop['location_id'].isin(side_a)) &
                            (pop['year_id'] == year)]['population'].sum()
    
    side_b_population = pop[(pop['location_id'].isin(side_b)) &
                            (pop['year_id'] == year)]['population'].sum()   

    deaths_a = best - 1
    deaths_b = 1
    
    # split out deaths by population of location
    for location in side_a:
        # grab the population for that location
        location_pop = pop[(pop['location_id'] == location) &
                           (pop['year_id'] == year)]['population'].sum()

        # find the percentage of population for that event
        population_percentage = location_pop / side_a_population

        row['best'] = deaths_a * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    for location in side_b:
        # grab the population for that location
        location_pop = pop[(pop['location_id'] == location) &
                           (pop['year_id'] == year)]['population'].sum()

        # find the percentage of population for that event
        population_percentage = location_pop / side_b_population

        row['best'] = deaths_b * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    assert np.isclose(deaths_one, event_df['best'].sum(), atol=10), "{} SEI {} deaths".format(row['source_event_id'], deaths_one-event_df['best'].sum())

    return event_df


def split_sides_for_war(side_a, side_b, deaths_a, deaths_b,
                              best, year, high, low, row, pop,
                              event_population, event_df, location_of_event):
    best = float(best)
    deaths_one = best
    side_a_population = pop[(pop['location_id'].isin(side_a)) &
                            (pop['year_id'] == year)]['population'].sum()
    
    side_b_population = pop[(pop['location_id'].isin(side_b)) &
                            (pop['year_id'] == year)]['population'].sum()
    
    event_locations_population = pop[(pop['location_id'].isin(location_of_event)) &
                                     (pop['year_id'] == year)]['population'].sum()
    side_a_percentage = side_a_population / event_population
    side_b_percentage = side_b_population / event_population
    location_deaths = .99 * best
    if np.isnan(location_deaths):
        location_deaths = 0
    if location_deaths == None:
        location_deaths = 0
    remaining_deaths = best - location_deaths
    # calculate deaths based on percentage of population
    deaths_a = (remaining_deaths * side_a_percentage)
    deaths_b = (remaining_deaths * side_b_percentage)
    for location in location_of_event:
        location_pop = pop[(pop['location_id'] == location) &
                   (pop['year_id'] == year)]['population'].sum()
        # find the percentage of population for that event
        population_percentage = location_pop / event_locations_population
        row['best'] = location_deaths * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1
        
        event_df = event_df.append(row)
    
    # split out deaths by population of location
    for location in side_a:
        # grab the population for that location
        location_pop = pop[(pop['location_id'] == location) &
                           (pop['year_id'] == year)]['population'].sum()

        # find the percentage of population for that event
        population_percentage = location_pop / side_a_population

        row['best'] = deaths_a * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    for location in side_b:
        # grab the population for that location
        location_pop = pop[(pop['location_id'] == location) &
                           (pop['year_id'] == year)]['population'].sum()

        # find the percentage of population for that event
        population_percentage = location_pop / side_b_population

        row['best'] = deaths_b * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    assert np.isclose(deaths_one, event_df['best'].sum(), atol=10), "{} SEI {} deaths".format(str(row['source_event_id'].iloc[0]), deaths_one-event_df['best'].sum())

    return event_df


def split_sides_by_population(side_a, side_b, deaths_a, deaths_b,
                              best, year, high, low, row, pop,
                              event_population, event_df):
    best = float(best)
    deaths_one = best
    side_a_population = pop[(pop['location_id'].isin(side_a)) &
                            (pop['year_id'] == year)]['population'].sum()
    
    side_b_population = pop[(pop['location_id'].isin(side_b)) &
                            (pop['year_id'] == year)]['population'].sum()
    
    side_a_percentage = side_a_population / event_population
    
    side_b_percentage = side_b_population / event_population
    
    
    # calculate deaths based on percentage of population
    deaths_a = (best * side_a_percentage)
    deaths_b = (best * side_b_percentage)
    
    # split out deaths by population of location
    for location in side_a:
        # grab the population for that location
        location_pop = pop[(pop['location_id'] == location) &
                           (pop['year_id'] == year)]['population'].sum()

        # find the percentage of population for that event
        population_percentage = location_pop / side_a_population

        # write over the row's value to append to event_df
        row['best'] = deaths_a * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    for location in side_b:
        # grab the population for that location
        location_pop = pop[(pop['location_id'] == location) &
                           (pop['year_id'] == year)]['population'].sum()

        # find the percentage of population for that event
        population_percentage = location_pop / side_b_population

        row['best'] = deaths_b * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    assert np.isclose(deaths_one, event_df['best'].sum(), atol=10), "{} SEI {} deaths".format(row['source_event_id'], deaths_one-event_df['best'].sum())

    return event_df


def split_sides_with_weights(best, year, high, low, row, pop, event_population,
                             weights, weights_detailed, event_df):
    """
        This function splits the data by side in the case where we have prior data to use.
        The logic:
            - first check if we have more detailed information by side on this event.
                - If so, use that information to allocate as many deaths as possible.
                - If any deaths are remaining, split those by population.
            - Then check the
    """
    assert 1 == len(weights), f"Weights should only have one row, instead got {len(weights)}"

    populations = {
        'a': pop[(pop['location_id'].isin(weights['side_a'].values[0].split(', '))) &
                 (pop['year_id'] == year)]['population'].sum(),

        'b': pop[(pop['location_id'].isin(weights['side_b'].values[0].split(', '))) &
                 (pop['year_id'] == year)]['population'].sum()
    }

    total_side_deaths = {}
    for s in ['a', 'b']:
        if weights['deaths_' + s].sum():
            total_side_deaths[s] = weights['deaths_' + s].values[0]
        elif weights['weight_' + s].sum():
            total_side_deaths[s] = best * weights['weight_' + s].values[0]
        else:
            total_side_deaths[s] = best * (populations[s] / sum(populations.values()))

    wts = pd.merge(weights, weights_detailed, on=['source_id', 'source_event_id'], how='outer')

    for i in range(len(wts)):
        row['location_id'] = wts.iloc[i]['location_id']
        row['split_status'] = 1
        row['high'] = float("nan")
        row['low'] = float("nan")

        side = wts.iloc[i]['side']
        other_side = 'a' if side == 'b' else 'b'

        pop_within_side = pop[(pop['location_id'] == wts.iloc[i]['location_id']) &
                              (pop['year_id'] == year)]['population'].sum()
        pop_within_side = pop_within_side / populations['a'] if side == 'a' else pop_within_side / populations['b']

        if not pd.isnull(wts.iloc[i]['deaths']):
            row['best'] = wts.iloc[i]['deaths']
        elif not pd.isnull(wts.iloc[i]['deaths_' + side]):
            row['best'] = wts.iloc[i]['deaths_' + side] * pop_within_side
        else:
            other_side_deaths = total_side_deaths[other_side]
            row['best'] = (best - other_side_deaths) * wts.iloc[i]['weight']

        event_df = event_df.append(row)

    assert np.isclose(event_df.best.sum(), best, atol=10), "Deaths split by weights do not match"

    return event_df


def iterate_through_df_and_split_sides(df, weights, weights_detailed):
    
    # this is the dataframe that will return all events split out
    final = pd.DataFrame()
    
    df = fill_side_columns_with_non_null_values(df)

    pop = pd.read_csv(FILEPATH)
    print("pop_is_read_in")
    # Get the weights and merge them on
    initial_cols = set(df.columns.tolist())
    df = pd.merge(df, weights, on=['source_id', 'source_event_id'], how='left', suffixes=('', '_w'))
    post_merge_cols = set(df.columns.tolist())

    # iterate through each row of the dataframe
    for index, row in df.iterrows():
        
        # each event_df should be empty to begin with
        event_df = pd.DataFrame()
        
        # extract variables from the row of data
        locations = convert_to_lists_of_ints(row, "location_id")
        side_a = convert_to_lists_of_ints(row, "side_a")
        side_b = convert_to_lists_of_ints(row, "side_b")

        deaths_a = row['deaths_a']
        deaths_b = row['deaths_b']
        weight_a = row['weight_a']
        weight_b = row['weight_b']
        weight_deaths_a = row['deaths_a_w']
        weight_deaths_b = row['deaths_b_w']
        year = row['year_id']
        best = float(row['best'])
        high = row['high']
        low = row['low']
        cause_id = row['cause_id']
        
        row = pd.DataFrame(row).transpose()
        
        # set logic for whether or not sides are present
        has_weights = not pd.isnull(weight_a) or \
                      not pd.isnull(weight_b) or \
                      not pd.isnull(weight_deaths_a) or \
                      not pd.isnull(weight_deaths_b)
        has_side_a = not (side_a == [])
        has_side_b = not (side_b == [])
        has_location = not (np.isnan(locations)).all()
        has_sides = ((has_side_a | has_side_b) and not has_location)
        has_deaths_by_side = ((deaths_a != 0) | (deaths_b != 0))

        if has_weights:
            all_locs = list(set(side_a) | set(side_b))
            event_population = pop[(pop['location_id'].isin(all_locs)) &
                                   (pop['year_id'] == year)]['population'].sum()

            w = weights.loc[
                (weights.source_id == row.source_id.values[0]) &
                (weights.source_event_id == row.source_event_id.values[0])
            ]

            dw = weights_detailed.loc[
                (weights_detailed.source_id == row.source_id.values[0]) &
                (weights_detailed.source_event_id == row.source_event_id.values[0])
            ]

            event_df = split_sides_with_weights(best, year, high, low, row, pop,
                                                event_population, w, dw, event_df)
        elif has_sides:
            all_locs = list(set(side_a) | set(side_b))
            event_population = pop[(pop['location_id'].isin(all_locs)) &
                                   (pop['year_id'] == year)]['population'].sum()
            
            # make sure that no locations are duplicated within sides
            side_a, side_b = deduplicate_locations_within_sides(side_a, side_b)
            
            if has_deaths_by_side:
                event_df = split_sides_with_known_deaths(side_a, side_b, deaths_a, deaths_b,
                                                         best, year, high, low, row, pop,
                                                         event_population, event_df)

            elif cause_id == 855: 
                location_of_event = row['location_of_event'].iloc[0]
                event_df = split_sides_for_war(side_a, side_b, deaths_a, deaths_b,
                                               best, year, high, low, row, pop,
                                               event_population, event_df, location_of_event)
            

            else:
                event_df = split_sides_by_population(side_a, side_b, deaths_a, deaths_b,
                                                     best, year, high, low, row, pop,
                                                     event_population, event_df)
        
        elif has_location:
            original_best = best
            event_population = pop[(pop['location_id'].isin(locations)) &
                                   (pop['year_id'] == year)]['population'].sum()
            assert has_location, "Hmm, there are no sides and no location"
            # if only location IDs are present split out locations by population
            if len(locations) > 1:
                if cause_id == 855:
                    location_of_event = row['location_of_event'].iloc[0]
                    location_of_event = (location_of_event & set(locations))
                    if len(location_of_event) > 0:
                        location_deaths = best * .99
                        best = best - location_deaths
                # locations need to be split out
                for location in set(locations):
                    # grab the population for that location
                    location_pop = pop[(pop['location_id'] == location) &
                                       (pop['year_id'] == year)]['population'].sum()
                    # find the percentage of population for that event
                    population_percentage = location_pop / event_population
                    row['best'] = best * population_percentage
                    if cause_id == 855:
                        if int(location) in location_of_event:
                            row['best'] = row['best'] + (location_deaths / len(location_of_event))

                    row['high'] = float('nan')
                    row['low'] = float('nan')
                    row['location_id'] = location
                    row['split_status'] = 1
                                        event_df = event_df.append(row)
                assert np.isclose(original_best, event_df['best'].sum(), atol=10), "{}, {}, {}, {}, {}".format(original_best, event_df['best'].sum(), locations, best, row['best'])
            else:
                # no locations need to be split out
                row['split_status'] = 0
                event_df = row
                assert np.isclose(original_best, event_df['best'].sum(), atol=10), "{}, {}".format(original_best, event_df['best'].sum())
        event_df.reset_index(drop=True, inplace=True)
        final = final.append(event_df, ignore_index=True)
    cols_to_drop = list(post_merge_cols - initial_cols)
    final.drop(columns=cols_to_drop, inplace=True)
    print("a worker finished")
    return final


def deduplicate_locations_within_sides(side_a, side_b):
    side_a = set(side_a)
    side_b = set(side_b)
    if side_a == side_b:
        side_b = set() 
    side_a = side_a - side_b
    return side_a, side_b


def parallelize(df, func):
    weights = get_weights(df.source_id.tolist())
    weights_detailed = get_weights_detailed(df.source_id.tolist())
    f = partial(func, weights=weights, weights_detailed=weights_detailed)
    workers = 10
    df_split = np.array_split(df, workers)
    pool = Pool(workers)
    df = pd.concat(pool.map(f, df_split))
    pool.close()
    pool.join()
    return df


def add_locations_from_side(df, source):
    try:
        loc_map = pd.read_csv(FILEPATH.format(source))
        loc_map = loc_map.query("map_type_hierarchy_kept == True")
        loc_map = loc_map.query("source_col == 'location_id'")
        loc_map = loc_map[['source_event_id','location_id']]
        loc_map = loc_map.groupby(['source_event_id'], as_index=False).agg({"location_id":set})
        loc_map = loc_map.rename(columns={"location_id":"location_of_event"})
        original_shape = df.copy().shape[0]
        df['source_event_id'] = df['source_event_id'].apply(lambda x: str(x))
        loc_map['source_event_id'] = loc_map['source_event_id'].apply(lambda x: str(x))
        df = pd.merge(left=df, right=loc_map, how='left', on='source_event_id')
        assert original_shape == df.shape[0]
    except:
        pass
    return df


def run_side_splitting(df, source):

    original_death_count = df.copy()['best'].sum()
    df = add_locations_from_side(df, source)
    df['best'] = df['best'].fillna(0)
    print(df['best'].dtype)
    assert np.isclose(df['best'].sum(), original_death_count, atol=50)
    
    print("saving_pop")
    pop = get_population(location_id=-1, decomp_step="step3", year_id=-1, location_set_id=21, gbd_round_id=7)
    pop.to_csv(FILEPATH)
    print("pop saved")

    df = parallelize(df, iterate_through_df_and_split_sides)
    split_death_count = df['best'].sum()
    difference = split_death_count - original_death_count
    assert np.isclose(difference, 0, atol=50), (
        "deaths before split does not equal deaths after split: Difference {}".format(difference))
    print("done side splitting")
    return df

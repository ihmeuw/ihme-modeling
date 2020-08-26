import pandas as pd
import numpy as np
from multiprocessing import Pool
from db_queries import get_population
from shock_tools import *


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
    # side_b will be one death
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

        # write over the row's value to append to event_df
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
        
        # write over the row's value to append to event_df
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

        # write over the row's value to append to event_df
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

        # write over the row's value to append to event_df
        row['best'] = deaths_b * population_percentage
        row['high'] = float("nan")
        row['low'] = float("nan")
        row['location_id'] = location
        row['split_status'] = 1

        # insert the unique locations row into the event df
        event_df = event_df.append(row)

    assert np.isclose(deaths_one, event_df['best'].sum(), atol=10), "{} SEI {} deaths".format(row['source_event_id'], deaths_one-event_df['best'].sum())

    return event_df


def iterate_through_df_and_split_sides(df):
    
    final = pd.DataFrame()
    
    df = fill_side_columns_with_non_null_values(df)

    pop = get_population(location_id=-1, decomp_step="step1", year_id=-1, location_set_id=21)
    
    # iterate through each row of the dataframe
    for index, row in df.iterrows():
        
        event_df = pd.DataFrame()
        
        locations = convert_to_lists_of_ints(row, "location_id")
        side_a = convert_to_lists_of_ints(row, "side_a")
        side_b = convert_to_lists_of_ints(row, "side_b")

        deaths_a = row['deaths_a']
        deaths_b = row['deaths_b']
        year = row['year_id']
        best = float(row['best'])
        high = row['high']
        low = row['low']
        cause_id = row['cause_id']
        
        row = pd.DataFrame(row).transpose()
        
        has_side_a = not (side_a == [])
        has_side_b = not (side_b == [])
        has_location = not (np.isnan(locations)).all()
        has_sides = ((has_side_a | has_side_b) and not has_location)
        has_deaths_by_side = ((deaths_a != 0) | (deaths_b != 0))
                
        if has_sides:
            all_locs = list(set(side_a) | set(side_b))
            event_population = pop[(pop['location_id'].isin(all_locs)) &
                       (pop['year_id'] == year)]['population'].sum()
            
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
                event_df =  split_sides_by_population(side_a, side_b, deaths_a, deaths_b,
                                                      best, year, high, low, row, pop,
                                                      event_population, event_df)
            
        
        elif has_location:
            og_best = best
            event_population = pop[(pop['location_id'].isin(locations)) &
                               (pop['year_id'] == year)]['population'].sum()
            assert has_location, "Hmm, there are no sides and no location"
            if len(locations) > 1:
                if cause_id == 855:
                    location_of_event = row['location_of_event'].iloc[0]
                    location_of_event = (location_of_event & set(locations))
                    if len(location_of_event) > 0:
                        location_deaths = best * .99
                        best = best - location_deaths
                for location in set(locations):
                    location_pop = pop[(pop['location_id'] == location) &
                                       (pop['year_id'] == year)]['population'].sum()
                    
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

                assert np.isclose(og_best, event_df['best'].sum(), atol=10), "{}, {}, {}, {}, {}".format(og_best, event_df['best'].sum(), locations, best, row['best'])

            else:
                row['split_status'] = 0
                event_df = row

                assert np.isclose(og_best, event_df['best'].sum(), atol=10), "{}, {}".format(og_best, event_df['best'].sum())
        
        event_df.reset_index(drop=True, inplace=True)
        final = final.append(event_df, ignore_index=True)
                
    return final


def deduplicate_locations_within_sides(side_a, side_b):
    side_a = set(side_a)
    side_b = set(side_b)

    if side_a == side_b:
        side_b = set() 

    side_a = side_a - side_b
    
    return side_a, side_b


def parallelize(df, func):
    workers = 10
    df_split = np.array_split(df, workers)
    pool = Pool(workers)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def add_locations_from_side(df, source):
    loc_map = pd.read_csv("FILEPATH".format(source))
    loc_map = loc_map.query("map_type_hierarchy_kept == True")
    loc_map = loc_map.query("source_col == 'location_id'")
    loc_map = loc_map[['source_event_id','location_id']]
    loc_map = loc_map.groupby(['source_event_id'], as_index=False).agg({"location_id":set})
    loc_map = loc_map.rename(columns={"location_id":"location_of_event"})
    og_shape = df.copy().shape[0]
    df['source_event_id'] = df['source_event_id'].apply(lambda x: str(x))
    loc_map['source_event_id'] = loc_map['source_event_id'].apply(lambda x: str(x))
    df = pd.merge(left=df, right=loc_map, how='left', on='source_event_id')
    assert og_shape == df.shape[0]
    return df

def run_side_splitting(df, source):

    original_death_count = df.copy()['best'].sum()
    df = add_locations_from_side(df, source)
    df['best'] = df['best'].fillna(0)
    print(df['best'].dtype)
    assert np.isclose(df['best'].sum(), original_death_count, atol=50)
    df = parallelize(df, iterate_through_df_and_split_sides)
    split_death_count = df['best'].sum()
    difference = split_death_count - original_death_count
    assert np.isclose(difference, 0, atol=50), (
        "deaths before split does not equal deaths after split: Difference {}".format(difference))
    # report_locations_split(df)
    return df

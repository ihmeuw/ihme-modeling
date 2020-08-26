"""
These are going to be the functions that create the knockouts. This will be
attached to the input class as a method so no parameters will be required
however every call to the generate a new set of training and test data.
The number of sets created will be determined by the model version ID
specifications.

Edited June 29, 2018 by Hayley Tymeson for use in ST-GPR
"""
import logging
from math import ceil

import numpy as np
import pandas as pd


def generate_sub_frames(df):
    """
    data frame -> data frame, data frame, array, array

    This function is the initiation function for the knockout process as used
    by codem. It creates the necessary objects to be used as inputs in later
    functions detailed below. If this function is being used within the CODEm
    framework the input data frame will be the data frame that was queried in
    the initialization of the input class object.
    """

    dt = df[["idx", "location_id", "year_id", "age_group_id", "sex_id", "data"]]
    no_na_data = dt[dt.data.notnull()]
    locations1 = dt.location_id.unique()
    locations2 = no_na_data.location_id.unique()
    return dt, no_na_data, locations1, locations2


def add_ko(data, no_na_data, location1, location2, knockouts, var):
    """
    (data frame, data frame, str, str, data frame, var) -> data frame

    The first two data frames  that this function takes as inputs should
    be copies of the data frame that is being used for CODEm modeling. The
    first (data) is the full data set while the second (no_na_data) is the data
    set with the missing responses removed. The input location1 should be a
    location_id present in data for which knockouts should be modeled after.
    location2 should be present in no_na_data and will be the data for which
    the knockouts are actually preformed. The last data frame (knockouts), is
    where the results will be appended and used later. (var) is the name of the
    variable which we will be calling this knockout, either "ko_{}".format(ko) or "test2".

    HT: This function seems to be matching patterns of missingness from a randomly selected
    location in the locations WITH data to a randomly selected location in the entire
    prediction frame. It's neat.
    """
    missing_data = data[data["location_id"] == location1]
    data_to_ko = no_na_data[no_na_data["location_id"] == location2]
    missing_data[var] = missing_data["data"].isnull()
    data_to_ko = data_to_ko.merge(missing_data[["year_id", "age_group_id", "sex_id", var]],
                                  how="left", on=["year_id", "age_group_id", "sex_id"])
    data_to_ko = data_to_ko[data_to_ko[var] == True]
    data_to_ko = data_to_ko[["idx", "location_id",
                             "year_id", "age_group_id", "sex_id", var]]
    knockouts = knockouts.append(data_to_ko)
    knockouts = knockouts.drop_duplicates()
    return knockouts


def get_knockout_data_frame(data, knockouts, ko):
    """
    (data frame, data frame, data frame) -> data frame

    Given a full data set(data) used in the codem process and two additional
    data frames which specify the first and second test set of knockouts,
    (knockouts1 and knockouts2, respectively), returns a data frame with three
    columns. The columns ("test", "train1", "train2") are boolean and denote
    whether an observation falls into one of these three categories. If an
    observation is in neither of these categories it is because they are
    missing data for the outcome.
    """
    var = "ko_{}".format(ko)

    partitioned = pd.merge(data, knockouts, how="left")
    partitioned[var][partitioned[var].isnull()] = False
    partitioned[var][partitioned["data"].isnull()] = False
    # partitioned["train"] = ((partitioned[var] == False) &
    #                         (partitioned["data"].isnull() == False))
    #partitioned = partitioned.reset_index()
    return partitioned[['idx', var]]


def knockout_run(data, no_na_data, locations1, locations2, prop_held_out, ko):
    """
    (data frame, data frame, array, array) -> data frame

    Given a full data frame (data) used in CODEm, a subset of that data frame
    where missing data for the outcome variable is removed (no_na_data), and
    two randomized arrays representing the unique locations in each of those
    data sets respectively (locations1, locations2) will return a three column
    data frame with variables ("test", "train1", "train2"). The resulting data
    frame will be of the same length as the original with either a True or
    False value given to each cell denoting whether an observation falls into
    that category.
    """

    knockouts = pd.DataFrame()
    N = float(len(no_na_data))
    while len(knockouts)/N < prop_held_out:
        for i in range(len(locations2)):
            knockouts = add_ko(data, no_na_data, locations1[i],
                               locations2[i], knockouts, "ko_{}".format(ko))
        np.random.shuffle(locations1)
        np.random.shuffle(locations2)

    return get_knockout_data_frame(data, knockouts, ko)


def generate_knockouts(df, holdouts, seed, prop_held_out):
    """
    (data frame, integer, integer) -> list of data frames

    Given a data frame used in the CODEm process, a number of knockout runs to
    complete and an integer to be the seed for random shuffling creates a list
    of data frames which represent different combinations of train, ko_{},.format(ko) and
    test data.
    """
    dataframe = df.copy().reset_index().rename(columns={'index': 'idx'})

    # make ko_0 include full dataset
    dataframe['ko_0'] = np.where(dataframe.data.notnull(), 1, np.nan)

    np.random.seed(seed)
    pd.set_option('chained_assignment', None)
    data, no_na_data, locations1, locations2 = generate_sub_frames(dataframe)
    kos = pd.DataFrame({'idx': data.idx})
    for i in range(holdouts):
        np.random.shuffle(locations1)
        np.random.shuffle(locations2)
        ko_temp = knockout_run(data, no_na_data, locations1,
                               locations2, prop_held_out, i+1)
        kos = kos.merge(ko_temp, on='idx')
    kos = kos.astype(int)
    dataframe = dataframe.merge(kos, on='idx')

    dataframe.drop(columns='idx', inplace=True)

    return dataframe


def add_knockouts_to_dataframe(
        df: pd.DataFrame,
        holdouts: int,
        random_seed: int
) -> pd.DataFrame:
    logging.info('Creating knockouts')
    if not holdouts:
        df['ko_0'] = 1
        return df

    nationals_df = df.loc[df['level'] == 3]
    subnationals_df = df.loc[df['level'] > 3]
    knockouts_df = generate_knockouts(
        nationals_df, holdouts, random_seed, 0.2
    ).append(subnationals_df, sort=True)
    ko_cols = [f'ko_{i}' for i in range(holdouts + 1)]
    knockouts_df[ko_cols] = knockouts_df[ko_cols].fillna(1)
    return knockouts_df

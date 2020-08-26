'''
These are the functions that create the knockouts.
The number of sets created will be determined by the model version ID
specifications.
'''

import numpy as np
import pandas as pd
from math import ceil
import logging

logger = logging.getLogger(__name__)


def generate_sub_frames(df):
    """
    data frame -> data frame, data frame, array, array

    This function is the initiation function for the knockout process as used
    by codem. It creates the necessary objects to be used as inputs in later
    functions detailed below. If this function is being used within the CODEm
    framework the input data frame will be the data frame that was queried in
    the initialization of the input class object.
    """
    data = df[["age", "year", "location_id", "cf"]]
    no_na_data = data[data.cf.notnull()]
    locations1 = data.location_id.unique()
    locations2 = no_na_data.location_id.unique()
    return data, no_na_data, locations1, locations2


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
    variable which we will be calling this knockout, either "test1" or "test2".
    """
    missing_data = data[data["location_id"] == location1]
    data_to_ko = no_na_data[no_na_data["location_id"] == location2]
    missing_data[var] = missing_data["cf"].isnull()
    data_to_ko = data_to_ko.merge(missing_data[["year", "age", var]],
                                  how="left", on=["year", "age"])
    data_to_ko = data_to_ko[data_to_ko[var] == True]
    data_to_ko = data_to_ko[["age", "year", "location_id", var]]
    knockouts = knockouts.append(data_to_ko)
    knockouts = knockouts.drop_duplicates()
    return knockouts


def get_knockout_data_frame(data, knockouts1, knockouts2):
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
    partitioned = pd.merge(data, knockouts1, how="left")
    partitioned = partitioned.merge(knockouts2, how="left")
    partitioned["test1"][partitioned["test1"].isnull()] = False
    partitioned["test2"][partitioned["test2"].isnull()] = False
    partitioned["test1"][partitioned["cf"].isnull()] = False
    partitioned["test2"][partitioned["cf"].isnull()] = False
    partitioned["train"] = ((partitioned["test1"] == False) &
                            (partitioned["test2"] == False) &
                            (partitioned["cf"].isnull() == False))
    partitioned = partitioned.reset_index()
    return partitioned[["train", "test1", "test2"]]


def knockout_run(data, no_na_data, locations1, locations2):
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
    knockouts1 = pd.DataFrame()
    knockouts2 = pd.DataFrame()
    N = float(len(no_na_data))
    for i in range(len(locations2)):
            knockouts1 = add_ko(data, no_na_data, locations1[i],
                                locations2[i], knockouts1, "test1")
            if i == (len(locations2) - 1):
                sample_size = max(0, int(ceil(N * .15) - len(knockouts1)))
                temp = no_na_data.merge(knockouts1, how="left")
                indices = temp[temp.test1.isnull()].index
                indices = np.random.choice(indices, sample_size, replace=False)
                temp = temp.ix[indices][["age", "year", "location_id", "test1"]]
                temp["test1"] = True
                knockouts1 = knockouts1.append(temp)
                sample_size = int(ceil(N * .15))
                temp = no_na_data.merge(knockouts1, how="left")
                indices = temp[temp.test1.isnull()].index
                indices = np.random.choice(indices, sample_size, replace=False)
                knockouts2 = temp.ix[indices][["age", "year", "location_id", "test1"]]
                knockouts2["test1"] = True
                knockouts2 = knockouts2.rename(columns={"test1": "test2"})
                knockouts1.drop_duplicates(inplace=True)
                knockouts2.drop_duplicates(inplace=True)
            if len(knockouts1)/N >= .15:
                break
    for j in range((i + 1), len(locations2)):
            knockouts2 = add_ko(data, no_na_data, locations1[j],
                                locations2[j], knockouts2, "test2")
            if (len(knockouts2)/N) >= .15:
                break
            if j == (len(locations2) - 1):
                sample_size = max(0, int(ceil(N * .15) - len(knockouts2)))
                ko_temp = knockouts1.copy()
                ko_temp = ko_temp.rename(columns={"test1": "test2"})
                temp = no_na_data.merge(knockouts2.append(ko_temp), how="left")
                indices = temp[temp.test2.isnull()].index
                indices = np.random.choice(indices, sample_size, replace=False)
                temp = temp.ix[indices][["age", "year", "location_id", "test2"]]
                temp["test2"] = True
                knockouts2 = knockouts2.append(temp)
                knockouts2.drop_duplicates(inplace=True)
    return get_knockout_data_frame(data, knockouts1, knockouts2)


def generate_knockouts(df, holdouts, seed):
    """
    (data frame, integer, integer) -> list of data frames

    Given a data frame used in the CODEm process, a number of knockout runs to
    complete and an integer to be the seed for random shuffling creates a list
    of data frames which represent different combinations of train, test1, and
    test2 data.
    """
    np.random.seed(seed)
    pd.set_option('chained_assignment', None)
    data, no_na_data, locations1, locations2 = generate_sub_frames(df)
    ko_list = list()
    for i in range(holdouts):
        logger.info(f"Creating holdout {i}")
        np.random.shuffle(locations1)
        np.random.shuffle(locations2)
        ko_temp = knockout_run(data, no_na_data, locations1, locations2)
        ko_temp.columns = ["ko%02d_%s" % (i+1, n) for n in ko_temp.columns]
        ko_list.append(ko_temp)
    logger.info("Creating all data holdout.")
    full_model = pd.DataFrame({x: np.repeat(True, len(df)) for x in ["1", "2", "3"]})
    full_model.columns = ["ko%02d" % (len(ko_list) + 1) + x for x in
                          ["_train", "_test1", "_test2"]]
    full_model["ko%02d" % (len(ko_list) + 1) + "_train"] = df.cf.notnull()
    return ko_list + [full_model]

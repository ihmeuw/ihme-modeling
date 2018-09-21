import numpy as np
import pandas as pd
from math import ceil


def generate_sub_frames(df):
    data = df[["age_group_id", "year_id", "location_id", "data"]]
    no_na_data = data[data.data.notnull()]
    locations1 = data.location_id.unique()
    locations2 = no_na_data.location_id.unique()
    return data, no_na_data, locations1, locations2


def add_ko(data, no_na_data, location1, location2, knockouts, var):
    missing_data = data[data["location_id"] == location1]
    data_to_ko = no_na_data[no_na_data["location_id"] == location2]
    missing_data[var] = missing_data["data"].isnull()
    data_to_ko = data_to_ko.merge(missing_data[["year_id", "age_group_id", var]],
                                  how="left", on=["year_id", "age_group_id"])
    data_to_ko = data_to_ko[data_to_ko[var] == True]
    data_to_ko = data_to_ko[["age_group_id", "year_id", "location_id", var]]
    knockouts = knockouts.append(data_to_ko)
    knockouts = knockouts.drop_duplicates()
    return knockouts


def get_knockout_data_frame(data, knockouts1, knockouts2):
    partitioned = pd.merge(data, knockouts1, how="left")
    partitioned = partitioned.merge(knockouts2, how="left")
    partitioned["test1"][partitioned["test1"].isnull()] = False
    partitioned["test2"][partitioned["test2"].isnull()] = False
    partitioned["test1"][partitioned["data"].isnull()] = False
    partitioned["test2"][partitioned["data"].isnull()] = False
    partitioned["train"] = ((partitioned["test1"] == False) &
                            (partitioned["test2"] == False) &
                            (partitioned["data"].isnull() == False))
    partitioned = partitioned.reset_index()
    return partitioned[["train", "test1", "test2"]]


def knockout_run(data, no_na_data, locations1, locations2):
    knockouts1 = pd.DataFrame()
    knockouts2 = pd.DataFrame()
    N = float(len(no_na_data))
    for i in range(len(locations2)):
            knockouts1 = add_ko(data, no_na_data, locations1[i],
                                locations2[i], knockouts1, "test1")
            if i == (len(locations2) - 1):
                sample_size = max(0, float(ceil(N * .15) - len(knockouts1)))
                temp = no_na_data.merge(knockouts1, how="left")
                indices = temp[temp.test1.isnull()].index
                indices = np.random.choice(indices, sample_size, replace=False)
                temp = temp.ix[indices][["age_group_id", "year_id", "location_id", "test1"]]
                temp["test1"] = True
                knockouts1 = knockouts1.append(temp)
                sample_size = float(ceil(N * .15))
                temp = no_na_data.merge(knockouts1, how="left")
                indices = temp[temp.test1.isnull()].index
                indices = np.random.choice(indices, sample_size, replace=False)
                knockouts2 = temp.ix[indices][["age_group_id", "year_id", "location_id", "test1"]]
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
                sample_size = max(0, float(ceil(N * .15) - len(knockouts2)))
                ko_temp = knockouts1.copy()
                ko_temp = ko_temp.rename(columns={"test1": "test2"})
                temp = no_na_data.merge(knockouts2.append(ko_temp), how="left")
                indices = temp[temp.test2.isnull()].index
                indices = np.random.choice(indices, sample_size, replace=False)
                temp = temp.ix[indices][["age_group_id", "year_id", "location_id", "test2"]]
                temp["test2"] = True
                knockouts2 = knockouts2.append(temp)
                knockouts2.drop_duplicates(inplace=True)
    return get_knockout_data_frame(data, knockouts1, knockouts2)


def generate_knockouts(df, holdouts, seed):
    np.random.seed(seed)
    pd.set_option('chained_assignment', None)
    data, no_na_data, locations1, locations2 = generate_sub_frames(df)
    ko_list = list()
    for i in range(holdouts):
        np.random.shuffle(locations1)
        np.random.shuffle(locations2)
        ko_temp = knockout_run(data, no_na_data, locations1, locations2)
        ko_temp.columns = ["%s" % (n) for n in ko_temp.columns]
        ko_list.append(ko_temp)
    full_model = pd.DataFrame({x: np.repeat(True, len(df)) for x in ["1", "2", "3"]})
    full_model.columns = ["train", "test1", "test2"]
    full_model["train"] = df.data.notnull()
    return ko_list + [full_model]
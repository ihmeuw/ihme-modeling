"""
Functions that are shared across demographics, covariates and cod data.
"""

import pandas as pd
import numpy as np
import logging

from db_queries import get_location_metadata

from codem.reference import db_connect
from codem.data import queryStrings as QS

logger = logging.getLogger(__name__)


def exclude_regions(df, regions_exclude):
    """
    (Pandas data frame, list of regions) -> Pandas data frame

    Given a pandas data frame and a list of regions to exclude, which
    can include id codes for super region, region, country or subnational,
    will remove all of the regions of the data frame.
    """
    exclude = np.array(regions_exclude.split()).astype(int)
    sn_remove = df.location_id.map(lambda x: x not in exclude)
    c_remove = df.country_id.map(lambda x: x not in exclude)
    r_remove = df.region.map(lambda x: x not in exclude)
    sr_remove = df.super_region.map(lambda x: x not in exclude)
    df2 = df[sn_remove & c_remove & r_remove & sr_remove]
    df2.reset_index(drop=True, inplace=True)
    return df2


def get_location_info(location_set_version_id, standard_location_set_version_id, db_connection):
    """
    list -> Pandas Data Frame

    Given a list of country ID numbers will query from the mortality database
    and return a pandas data frame. The data frame contains columns for
    location, super region and region ID.
    """
    logger.info("Querying location information.")
    call = QS.locQueryStr.format(loc_set_ver_id=location_set_version_id,
                                 s_loc_set_ver_id=standard_location_set_version_id)
    df = db_connect.query(call, db_connection)
    df["path_to_top_parent"] = \
        df["path_to_top_parent"].map(lambda x: ",".join((x[2:]).split(",")[:3]))
    arr = np.array(list(df.path_to_top_parent.map(lambda x: x.split(","))))
    df2 = pd.DataFrame(arr.astype(int),
                       columns=["super_region", "region", "country_id"])
    return pd.concat([df[["location_id", "is_estimate", "standard_location"]], df2], axis=1)


def create_age_df(db_connection):
    """
    None -> Pandas data frame

    Creates a Pandas data frame with two columns, all the age groups currently
    used in analysis at IHME as noted by the data base as well as a column with
    the code used for the aggregate group.
    """
    # this age_group_set_id is currently specific to gbd 2016
    call = """
        SELECT age_group_id as all_ages
        FROM shared.age_group_set_list
        WHERE age_group_set_id = 12 AND is_estimate = 1;
    """
    age_df_22 = db_connect.query(call, db_connection)
    age_df_27 = age_df_22.copy(deep=True)
    age_df_22['age'] = 22
    age_df_27['age'] = 27
    return pd.concat([age_df_22, age_df_27], ignore_index=True)


def age_sex_data(df, sex, db_connection):
    """
    (Pandas data frame, integer) -> Pandas Data frame

    Given a Pandas data frame and an integer which represents the desired sex
    of the analysis, will return a data frame with a value for each age group
    and only for the desired sex.
    """
    df2 = df
    age_df = create_age_df(db_connection)
    if len(df2["age"].unique()) == 1:
        df2 = df2.merge(age_df, on="age")
        df2 = df2.drop("age", 1)
        df2 = df2.rename(columns={"all_ages": "age"})
    if len(df2["sex"].unique()) == 1:
        df2["sex"] = sex
    df2 = df2[df2["sex"] == sex]
    return df2

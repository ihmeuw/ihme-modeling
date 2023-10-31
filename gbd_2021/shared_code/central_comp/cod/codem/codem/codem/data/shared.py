"""
Functions that are shared across demographics, covariates and cod data.
"""

import logging

import numpy as np
import pandas as pd

from db_queries.api.public import get_age_metadata
from gbd import constants as gbd

from codem.data import queryStrings as QS
from codem.reference import db_connect

logger = logging.getLogger(__name__)


def exclude_regions(df, regions_exclude):
    """
    (Pandas data frame, list of regions) -> Pandas data frame

    Given a pandas data frame and a list of regions to exclude, which
    can include id codes for super region, region, country or subnational,
    will remove all of the regions of the data frame.
    """
    exclude = np.array(regions_exclude.split()).astype(int)
    loc_remove = df.location_id.isin(exclude)
    lvl4_remove = df.level_4.isin(exclude)
    lvl3_remove = df.level_3.isin(exclude)
    lvl2_remove = df.level_2.isin(exclude)
    lvl1_remove = df.level_1.isin(exclude)
    df2 = df[~(loc_remove | lvl4_remove | lvl3_remove | lvl2_remove | lvl1_remove)]
    df2.reset_index(drop=True, inplace=True)
    return df2


def get_location_info(location_set_version_id, standard_location_set_version_id,
                      db_connection):
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
    df[["level_1", "level_2", "level_3", "level_4"]] = \
        df["path_to_top_parent"].str.split(',', expand=True).iloc[:,[1,2,3,4]]
    df[["level_1", "level_2", "level_3"]] = df[["level_1", "level_2", "level_3"]].applymap(np.int64)
    return df[["location_id", "standard_location", "level_1", "level_2", "level_3", "level_4"]]


def create_age_df(gbd_round_id):
    """
    None -> Pandas data frame

    Creates a Pandas data frame with two columns, all the age groups currently
    used in analysis at IHME as noted by the data base as well as a column with
    the code used for the aggregate group.
    """
    age_df_22 = get_age_metadata(gbd_round_id=gbd_round_id)
    age_df_22 = age_df_22[['age_group_id']]
    age_df_22.rename(columns={'age_group_id': 'all_ages'},inplace=True)
    age_df_27 = age_df_22.copy(deep=True)
    age_df_22['age'] = gbd.age.ALL_AGES
    age_df_27['age'] = gbd.age.AGE_STANDARDIZED
    return pd.concat([age_df_22, age_df_27], ignore_index=True)


def age_sex_data(df, sex, gbd_round_id):
    """
    (Pandas data frame, integer) -> Pandas Data frame

    Given a Pandas data frame and an integer which represents the desired sex
    of the analysis, will return a data frame with a value for each age group
    and only for the desired sex.
    """
    df2 = df
    age_df = create_age_df(gbd_round_id)
    if len(df2["age"].unique()) == 1:
        df2 = df2.merge(age_df, on="age")
        df2 = df2.drop("age", 1)
        df2 = df2.rename(columns={"all_ages": "age"})
    if len(df2["sex"].unique()) == 1:
        df2["sex"] = sex
    df2 = df2[df2["sex"] == sex]
    return df2


def get_ages_in_range(age_start, age_end, gbd_round_id):
    age_df = get_age_metadata(gbd_round_id=gbd_round_id)
    age_df = age_df[['age_group_id', 'age_group_years_start', 'most_detailed']]
    if not age_df.loc[age_df.age_group_id.isin([age_start, age_end])].shape[0] == 2:
        raise RuntimeError("age_start and/or age_end are not valid")
    years_start = age_df[age_df['age_group_id'] == age_start][
        'age_group_years_start'
    ].iat[0]
    years_end = age_df[age_df['age_group_id'] == age_end][
        'age_group_years_start'
    ].iat[0]
    age_list = age_df[
        (age_df['age_group_years_start'] >= years_start) &
        (age_df['age_group_years_start'] <= years_end)
    ]['age_group_id'].unique().tolist()
    age_df.rename(columns={'age_group_id': 'all_ages'}, inplace=True)
    return age_list

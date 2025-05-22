"""
This script pulls all cause VR data from the causes of death teams's
Claude system.
"""


import sys
import getpass
import pandas as pd
import numpy as np
from db_tools import ezfuncs
from db_queries import get_location_metadata

sys.path.append("FILEPATH")
from cod_prep.claude.claude_io import get_claude_data
from cod_prep.utils import (report_if_merge_fail, print_log_message)
from cod_prep.downloaders import add_nid_metadata


# columns to collapse code_id down to cause_id
GROUP_COLS = ['location_id', 'year_id', 'sex_id',
              'age_group_id', 'nid', 'extract_type_id', 'site_id']

# columns to sum together
VAL_COLS = ['deaths']

# Globals / passed in arguments
NEW_RUN_ID = sys.argv[1]
OUT_DIR = "FILEPATH"
OUT_FILENAME = "FILEPATH"
OUT_FILE = "FILEPATH"

# release id
RELEASE_ID = sys.argv[2]
if not isinstance(RELEASE_ID, int):
    RELEASE_ID = int(RELEASE_ID)


def _collapse_to_group_cols(df):
    """Run this on each file get_claude_data touches."""
    # collapse code to cause
    df = df.groupby(GROUP_COLS, as_index=False)[VAL_COLS].sum()
    return df


def get_all_cause_vr(location_set_version_id):
    """Pull VR with all cause mortality."""

    all_cause_filters = {'extract_type_id': [167], 'is_mort_active': True,
                         'force_rerun': True, 'block_rerun': False, 'project_id':15}
    print_log_message("Pulling all cause VR")
    df = get_claude_data(
        phase = "formatted",
        **all_cause_filters
    )
    df = _collapse_to_group_cols(df)
    return df


def add_metadata(df):
    """Add key metadata."""
    print_log_message("Adding key metadata")

    df = add_nid_metadata(
        df, ['source', 'code_system_id', 'parent_nid'],
        force_rerun=True, block_rerun=False, cache_dir='standard',
        project_id=15
    )

    df['representative_id'] = 1
    report_if_merge_fail(df, 'source', 'nid')

    # map extract type
    df = map_extract_type_id(df)

    # map site
    df = map_site_id(df)

    return df


def map_extract_type_id(df):
    """
    Map extract_type_id.

    """
    query = """"""
    eid_dict = \
        ezfuncs.query(
            query, conn_def='cod'
        ).set_index('extract_type_id')['extract_type'].to_dict()
    df['extract_type'] = df['extract_type_id'].map(eid_dict)
    df.drop(['extract_type_id'], axis=1, inplace=True)
    return df


def map_site_id(df):
    """Map site_id to site."""

    df['site'] = ""
    df = df.drop('site_id', axis=1)
    return df


def write_vr_file(df):
    """Write final file to archive and active directories."""

    df.to_csv(OUT_FILE, index=False)
    print_log_message("New VR file written to {}".format(OUT_FILE))


def aggregate_all_ages_groups(df):
    """
    Generate all ages age group.
    """

    # Mark rows for sources that only have the age group id 22
    df.loc[:,"only_has_age_group_id_22"] = 0
    def mark_only_has_age_group_id_22_df(df):
        """helper function to be used in groupbys that marks if a source
        only has age_group_id 22"""
        only_has_age_group_id_22 = (df.age_group_id == 22).all()
        if only_has_age_group_id_22:
            df.loc[:,"only_has_age_group_id_22"] = 1
        return df

    # groupby and mark sources that only have the age group id 22
    dont_group_by_cols = ["deaths", "age_group_id", "only_has_age_group_id_22"]
    groupby_cols = list(set(df.columns) - set(dont_group_by_cols))
    df = df.groupby(by=groupby_cols).apply(mark_only_has_age_group_id_22_df)

    # make a separate dataframe that only contains sources that only have age group id 22
    only_has_age_group_id_22_df = df[df.only_has_age_group_id_22 == 1].copy()
    # drop those sources from the main data frame.
    df = df[df.only_has_age_group_id_22 == 0].copy()

    # drop age group id 22 from the main data frame
    df = df[df.age_group_id != 22]

    # make a separate data frame that is a copy of the main dataframe and
    # make all of the age group ids 22
    all_age_df = df.copy()
    all_age_df['age_group_id'] = 22

    # Collapse it to make all age totals
    group_cols = [col for col in df.columns if col not in VAL_COLS]
    all_age_df = all_age_df.groupby(group_cols, as_index=False).sum()

    # append all three dataframes together.
    df = pd.concat([df, all_age_df, only_has_age_group_id_22_df], ignore_index=True, sort = False)
    df = df.drop("only_has_age_group_id_22", axis=1)  # drop this column

    # check for any duplicates
    duplicates = df.duplicated(keep=False)
    assert duplicates.sum() == 0, "There are duplicates, probably due to there already being an all ages group: \n\n{}\n".format(df[duplicates].sort_values(['sex_id', 'age_group_id', 'year_id']).to_string())

    return df


def aggregate_under_one(df):

    """
    Certain sources / country-years have age_group_id 2, 3, 4, but don't have under-1 aggregated.

    """

    # KNA
    kna_years = [1971,1977,1978] + list(range(1986, 1996)) + list(range(1998,2016))
    df.loc[(df.location_id == 393) & (df.year_id.isin(kna_years)) & (df.age_group_id.isin([2, 3, 4])), "age_group_id"] = 28

    # Need to fill NaNs before collapse so they don't get dropped
    df.loc[df.parent_nid.isnull(), "parent_nid"] = "--filled-na--"

    # collapse
    group_cols = [col for col in df.columns if col not in ["deaths"]]
    assert_msg = "There are Nulls present in the grouping key columns; These rows with Nulls will be dropped.\n{}".format(df[group_cols].isnull().sum().to_string())
    assert df[group_cols].notnull().all().all(), assert_msg
    df = df.groupby(by=group_cols, as_index=False).deaths.sum()

    # put the NaNs back
    df.loc[df.parent_nid == "--filled-na--", "parent_nid"] = np.nan

    return df


def hotfix_drop_sources(df):
    """
    """

    drop_ita = (df.source == "Italy_VR_all_cause") & (df.year_id == 2015)
    df = df[~drop_ita]

    drop_pol = (df.source == "Poland_VR_all_cause") & (df.year_id == 2016)
    df = df[~drop_pol]

    return df

def agg_norway(df, parent_id, child_ids):
    """
    This is used for aggregating the pre-GBD2020 Norwegian subnationals to the post-2020 subnationals.
    """
    output = df.loc[df.location_id.isin(child_ids)]

    if len(output.source.unique()) > 1:
        breakpoint()

    # Temporarily fill NAs in parent_id
    output.loc[output.parent_nid.isnull(), 'parent_nid'] = 'filled_na'
    group_cols = list(set(output.columns) - set(['deaths', 'location_id']))

    # Group and sum
    output = output.groupby(group_cols)['deaths'].sum().reset_index()

    # Set location ID
    output['location_id'] = parent_id

    # Reset parent nids
    output.loc[output.parent_nid == "filled_na", 'parent_nid'] = np.nan

    # Return output
    return output


def prepare_allcause_vr(location_set_version_id, vr_filter=None):
    """Do all things."""

    # get all cause data prepped for mortality
    df = get_all_cause_vr(location_set_version_id)

    print_log_message("Aggregating all ages")
    df = aggregate_all_ages_groups(df)

    print_log_message("Got a dataframe with {} rows".format(len(df)))

    # add names and important metadata
    df = add_metadata(df)

    # remove two sources
    df = hotfix_drop_sources(df)

    # generate under-1 aggregates
    df = aggregate_under_one(df)

    # Perform Norway subnational aggregations manually
    # Define the mapping manually
    norway_subnat_map = {
        60132 : [4921, 4922],
        60133 : [4918, 4919],
        60134 : [4916, 4917],
        60135 : [4912, 4913],
        60136 : [4911, 4914, 4915],
        60137 : [4927, 4928]
    }

    for k, v in norway_subnat_map.items():
        out = agg_norway(df, k, v)
        df = df.loc[~(df.location_id.isin(v))]
        df = pd.concat([df, out], sort=True)

    # age group id 161 (age group 0) should be coded as 28 (<1 year)
    df.loc[df.age_group_id == 161, "age_group_id"] = 28

    # age group id 49 should be coded as 238
    df.loc[df.age_group_id == 49, 'age_group_id'] = 238

    duplicates = df.duplicated(keep=False)
    assert duplicates.sum() == 0, "There are duplicates: \n\n{}\n".format(df[duplicates].sort_values(['sex_id', 'age_group_id', 'year_id']).to_string())

    return df


if __name__ == "__main__":

    # rely on default arg of release id
    location_metadata = get_location_metadata(location_set_id=35, release_id=RELEASE_ID)
    location_set_version_id = location_metadata.location_set_version_id.unique()
    assert len(location_set_version_id) == 1
    location_set_version_id = location_set_version_id[0]

    write = True

    df = prepare_allcause_vr(location_set_version_id)

    if write:
        # write to archive and active directories
        write_vr_file(df)

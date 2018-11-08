import sys
import getpass
import pandas as pd
from db_tools import ezfuncs

sys.path.append("FILEPATH")
from cod_prep.claude.claude_io import get_claude_data
from cod_prep.utils import (
    report_if_merge_fail, cod_timestamp, print_log_message
)
from cod_prep.downloaders import (
    add_nid_metadata, get_code_system_from_id, get_country_level_location_id,
    get_current_location_hierarchy
)
from cod_prep.claude.configurator import Configurator

# import standard configurations
CONF = Configurator('standard')

# columns to collapse code_id down to cause_id
GROUP_COLS = ['location_id', 'year_id', 'sex_id',
              'age_group_id', 'nid', 'extract_type_id', 'site_id']

# columns to sum together
VAL_COLS = ['deaths']

# where to write
new_run_id = sys.argv[1]
OUT_DIR = "FILEPATH".format(new_run_id)
OUT_FILENAME = "allcause_vr_for_mortality"
OUT_FILE = "{}/{}.csv".format(OUT_DIR, OUT_FILENAME)
# OUT_ARCH_FILE = "{}/_archive/{}_{}.csv".format(
#     OUT_DIR, OUT_FILENAME, cod_timestamp())


def _collapse_to_group_cols(df):
    """Run this on each file get_claude_data touches."""
    # collapse code to cause
    df = df.groupby(GROUP_COLS, as_index=False)[VAL_COLS].sum()
    return df


def get_cod_vr(location_set_version_id, vr_filter=None):
    """Pull age/sex split VR with cause of death data."""
    dataset_filters = {
        'data_type_id': [9, 10],
        'location_set_id': 35,
        'is_active': True,
    }
    # optionally, only get VR data for specific sources/nids/iso3s/etc.
    if vr_filter is not None:
        dataset_filters.update(vr_filter)
    print_log_message("Pulling CoD VR")
    df = get_claude_data(
        "disaggregation",
        location_set_version_id=location_set_version_id,
        **dataset_filters
    )
    df = _collapse_to_group_cols(df)
    return df


def get_all_cause_vr(location_set_version_id):
    """Pull age/sex split VR with all cause mortality."""
    all_cause_filters = {'extract_type_id': [167], 'location_set_id': 35}
    print_log_message("Pulling all cause VR")
    df = get_claude_data(
        "formatted",
        location_set_version_id=location_set_version_id,
        **all_cause_filters
    )
    df = _collapse_to_group_cols(df)
    return df


def add_metadata(df):
    """Add key metadata."""
    print_log_message("Adding key metadata")
    df = add_nid_metadata(
        df, ['source', 'code_system_id', 'parent_nid'],
        force_rerun=False, cache_dir='standard'
    )
    # this column is not yet comprehensive in nid metadata file
    df['representative_id'] = 1
    report_if_merge_fail(df, 'source', 'nid')

    # map extract type
    df = map_extract_type_id(df)

    # map site
    df = map_site_id(df)

    return df


def map_extract_type_id(df):
    """Map extract_type_id.

    With addition of extract_type_id 167, there are now (correctly)
    duplicates by source/nid. So keep it! and map to something intelligible.
    """
    query = """QUERY"""
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
    # df.to_csv(OUT_ARCH_FILE, index=False)
    # print_log_message("New VR file written to {}".format(OUT_ARCH_FILE))
    df.to_csv(OUT_FILE, index=False)
    print_log_message("New VR file written to {}".format(OUT_FILE))


def aggregate_to_country_level(orig_df, location_set_version_id):
    """Aggregate sub nationals to country level."""
    df = orig_df.copy()

    # merge on country level location_ids
    location_meta_df = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id)
    country_location_ids = \
        get_country_level_location_id(df.location_id.unique(),
                                      location_meta_df)
    df = df.merge(country_location_ids, how='left', on='location_id')
    report_if_merge_fail(df, 'country_location_id', ['location_id'])

    # aggregate sub national locations to national level
    df = df[df['location_id'] != df['country_location_id']]
    df['location_id'] = df['country_location_id']
    df = df.drop(['country_location_id'], axis=1)
    group_cols = [col for col in df.columns if col not in VAL_COLS]
    df = df.groupby(group_cols, as_index=False)[VAL_COLS].sum()
    df['loc_agg'] = 1

    # append aggregates to original dataframe
    orig_df['loc_agg'] = 0
    df = df.append(orig_df)
    return df


def aggregate_age_groups(df):
    """Generate all ages and under 1 age groups."""

    df = df.copy()
    group_cols = [col for col in df.columns
                  if col not in VAL_COLS]

    all_age_df = df.copy()
    all_age_df['age_group_id'] = 22
    all_age_df = all_age_df.groupby(group_cols, as_index=False).sum()

    df = pd.concat([df, all_age_df], ignore_index=True)

    return df


def prepare_allcause_vr(location_set_version_id, vr_filter=None):
    """Do all the things."""
    # get cod VR data

    # get all cause data prepped for mortality
    df = get_all_cause_vr(location_set_version_id)

    print_log_message("Aggregating under 1 and all ages")
    df = aggregate_age_groups(df)

    print_log_message("Aggregating sub national locations")
    df = aggregate_to_country_level(df, location_set_version_id)

    # append together
    # df = pd.concat([df1, df2])
    print_log_message("Got a dataframe with {} rows".format(len(df)))

    # add names and important metadata
    df = add_metadata(df)

    return df


if __name__ == "__main__":
    # can pass a dictionary of source: [source_list], nid: [nid_list], etc.
    # otherwise, will get ALL VR with cause of death data
    # vr_filter = {}
    location_set_version_id = CONF.get_id('location_set_version')
    write = True

    df = prepare_allcause_vr(location_set_version_id)

    if write:
        # write to archive and active directories
        write_vr_file(df)

"""
Master script for redistribution.

Creates split groups, sends to run_pipeline_redistribution,
and appends the output from run_pipeline_redistribution.

"""
import sys
import os
import time

import pandas as pd
import numpy as np
import getpass

from cod_prep.downloaders import (
    get_current_location_hierarchy,
    get_cause_map,
    get_current_cause_hierarchy,
    get_ages,
    get_pop,
    get_redistribution_locations,
    add_code_metadata,
    add_age_metadata,
    get_package_list,
    get_value_from_nid
)
from cod_prep.utils import (
    wait,
    report_if_merge_fail,
    fill_missing_df,
    submit_cod,
    just_keep_trying,
    print_log_message
)
from configurator import Configurator
from hiv_correction import HIVCorrector
from claude_io import (
    get_phase_output,
    write_phase_output,
    makedirs_safely
)
from run_phase_redistributionworker import main as worker_main

CONF = Configurator('standard')
CACHE_DIR = CONF.get_directory('db_cache')
CODE_DIR = CONF.get_directory('claude_code')
RD_PROCESS_DIR = CONF.get_directory('rd_process_data')
PACKAGE_DIR = CONF.get_directory('rd_package_dir') + '/{csid}'
SG_DIR = RD_PROCESS_DIR + '/{nid}/{extract_type_id}/split_{sg}'

# columns specific to redistribution
RD_COLS = ['global', 'dev_status', 'super_region',
           'region', 'country', 'subnational_level1',
           'subnational_level2', 'sex', 'age',
           'split_group', 'freq']

# columns included as is standard to claude
CLAUDE_COLS = ['nid', 'extract_type_id', 'location_id', 'year_id',
               'age_group_id', 'sex_id',
               'site_id', 'deaths', 'cause_id']


def has_garbage(df):
    """Determine whether or not there are any garbage codes."""
    any_garbage = (df['cause_id'] == 743).any()
    return any_garbage


def verify_packages(df):
    """Verify that packages have been downloaded to the correct directory."""
    # this method currently takes code system name as an arg, but
    # won't all package downloaders will be modified to take nid as an arg?
    # or at the very least take code_system_id?
    csid = int(df['code_system_id'].unique())
    assert len(csid) == 1
    packages = get_package_list(csid)
    for package in packages:
        try:
            pass
            # look in PACKAGE_DIR.format(csid=csid)
            # check that all the packages for an nid are in that folder
            # there will only ever be one csid per nid
        except IOError:
            pass
    raise NotImplementedError


def format_age_groups(df):
    """Convert age groups to simple ages."""
    df = add_age_metadata(df,
                          ['simple_age'],
                          force_rerun=False,
                          block_rerun=True,
                          cache_dir=CACHE_DIR)
    df.rename(columns={'simple_age': 'age'}, inplace=True)
    return df


def drop_zero_deaths(df):
    """Drop rows where there are no deaths."""
    df = df[df['deaths'] > 0]
    return df


def add_rd_locations(df, lsvid):
    """Merge on location hierarchy specific to redistribution."""
    lhh = get_current_location_hierarchy(location_set_version_id=lsvid,
                                         force_rerun=False,
                                         block_rerun=True,
                                         cache_dir=CACHE_DIR)
    rd_lhh = get_redistribution_locations(lhh)
    df = pd.merge(df, rd_lhh, on='location_id', how='left')
    report_if_merge_fail(df, 'global', 'location_id')
    report_if_merge_fail(df, 'dev_status', 'location_id')

    return df


def add_split_group_id_column(df, id_col='split_group'):
    """Add group IDs to a dataset.

    Arguments:
    df : DataFrame
         a pandas DataFrame
    group_cols : str or list-like
                 The columns used to group the data
    id_col : str, default 'split_group_id'
             The name of the column where you want to store the group ids
    credit to Kat Schelonka
    """
    # First collapse data to a single value per group
    # group cols different for US
    # if inlist("`source'", "_US_king_county_ICD10", "_US_king_county_ICD9")
    #   | regexm("`source'", "US_NCHS_")
    # use ghdx title to get US sources? (add_nid_metadata)
    # if source == US:
    # group_cols = ['country', 'subnational_level1', 'site_id',
    #               'nid', 'extract_type_id', 'year_id', 'sex']
    # else:
    #     group_cols = ['country', 'subnational_level1', 'site_id',
    #                   'nid', 'extract_type_id', 'year_id']
    group_cols = ['country', 'subnational_level1', 'site_id', 'nid',
                  'extract_type_id', 'year_id']
    # The aggregator function or column chosen to aggregate doesn't matter
    g = df.groupby(group_cols)[df.columns[-1]].agg(np.min).reset_index()
    # Get rid of all columns except the group columns
    g = g[group_cols]
    # Create ID column that numbers each row (each row is a group)
    g[id_col] = range(1, len(g) + 1)
    return df.merge(g, on=group_cols)


def format_columns_for_rd(df, code_system_id):
    """Ensure necessary columns are appropriately named and present."""
    df.rename(columns={'value': 'cause',
                       'deaths': 'freq'}, inplace=True)
    df['sex'] = df['sex_id']
    missing_cols = []
    for col in RD_COLS:
        if col not in df.columns:
            missing_cols.append(col)
    if len(missing_cols) > 0:
        raise AssertionError(
            "Expected to find ({}) but they were not in "
            "df.columns: ({})".format(missing_cols, df.columns)
        )
    return df


def read_append_split_groups(sg_list, nid, extract_type_id,
                             cause_map):
    """Read and append split groups after redistribution.

    Arguments:
    sg_list : list
            a list of all the split groups for given nid
    nid : int
        the nid in the data

    Returns:
        a pandas dataframe of all split groups
        for a given nid appended together
    """
    sg_dfs = []
    for sg in sg_list:
        filepath = (SG_DIR + '/post_rd.csv').format(
            nid=nid, extract_type_id=extract_type_id, sg=sg)
        sg = just_keep_trying(
            pd.read_csv,
            args=[filepath],
            kwargs={'dtype': {'cause': 'object'}},
            max_tries=250,
            seconds_between_tries=6,
            verbose=True)
        sg = merge_acause_and_collapse(sg, cause_map)
        sg_dfs.append(sg)
    df = pd.concat(sg_dfs)
    return df


def assert_no_lost_deaths():
    """Compare pre and post redistribution death totals."""
    # assert absolute value of total post_rd deaths - pre_rd deaths is not > 1
    return True


def revert_variables(df):
    """Change things back to the bear_bones way."""
    # change 'ZZZ' to cc_code
    #   !This doesn't do anything since we drop cause two lines down!
    #   df.loc[df['cause'] == 'ZZZ', 'cause'] = 'cc_code'
    # rename freq, deaths
    df.rename(columns={'freq': 'deaths'}, inplace=True)
    df = df[CLAUDE_COLS]
    return df


def submit_split_group(nid, extract_type_id, split_group, code_system_id,
                       launch_set_id):
    """Submit a job for a split group."""
    if extract_type_id in [3, 5]:
        slots_needed = 8
    elif extract_type_id == 2:
        slots_needed = 6
    else:
        slots_needed = 2
    jobname = "claude_redistributionworker_{nid}_{extract_type_id}" \
              "_{split_group}".format(
                  nid=nid, extract_type_id=extract_type_id,
                  split_group=split_group)
    shell_language = "python"
    worker = "{cld}/run_phase_redistributionworker.py".format(
        cld=CODE_DIR)
    slots = slots_needed
    params = [
        nid,
        extract_type_id,
        split_group,
        code_system_id
    ]
    log_base_dir = "FILEPATH".format(
        user=getpass.getuser(), launch_set_id=launch_set_id
    )
    jid = submit_cod(
        jobname,
        slots,
        shell_language,
        worker,
        params=params,
        verbose=True,
        logging=True,
        log_base_dir=log_base_dir
    )
    return jid


def write_split_group_input(df, nid, extract_type_id, sg):
    """Write completed split group."""
    indir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)
    makedirs_safely(indir)
    df.to_csv('{}/for_rd.csv'.format(indir), index=False)


def needs_garbage_correction(iso3, data_type_id):
    """Is this country likely to hide HIV in all causes, even garbage?

    Keep reading to find out...
    """
    return iso3 in ["ZAF", "MOZ", "ZWE"] and data_type_id == 9


def delete_split_group_output(nid, extract_type_id, sg):
    """Delete the existing intermediate split group files."""
    indir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)
    for_rd_path = '{}/for_rd.csv'.format(indir)
    post_rd_path = '{}/post_rd.csv'.format(indir)
    for path in [for_rd_path, post_rd_path]:
        if os.path.exists(path):
            os.unlink(path)


def merge_acause_and_collapse(df, cause_map):
    """Add acause column and collapse before appending split groups."""

    cause_map = cause_map[['cause_id', 'value']].copy()
    cause_map = cause_map.rename(columns={'value': 'cause'})

    df = df.merge(cause_map, how='left', on='cause')
    report_if_merge_fail(df, 'cause_id', 'cause')

    # there is some confusion during this cause set version between our
    # db server and the engine room as to which is the right cause set
    # version. Resolve that here.
    if CONF.get_id('cause_set_version') == 229:
        # replace any urinary/gyne with genitourinary
        print_log_message("Fixing urinary/gyne causes for csvid 229.")
        urinary_id = 594
        gyne_id = 603
        genitourinary_id = 982
        df.loc[
            df['cause_id'].isin([gyne_id, urinary_id]),
            'cause_id'
        ] = genitourinary_id

    df = df.drop(['cause', 'split_group'], axis=1)
    df = df.groupby([col for col in df.columns if col != 'freq'],
                    as_index=False).sum()
    return df


def run_phase(df, csvid, nid, extract_type_id, lsvid, pop_run_id, cmvid,
              launch_set_id, remove_decimal, write_diagnostics=True):
    """String together processes for redistribution."""

    # what to do about caching throughout the phase
    read_file_cache_options = {
        'block_rerun': True,
        'cache_dir': CACHE_DIR,
        'force_rerun': False,
        'cache_results': False
    }

    # the iso3 of this data
    iso3 = get_value_from_nid(
        nid, 'iso3', extract_type_id=extract_type_id,
        location_set_version_id=lsvid
    )

    # the code system id
    code_system_id = int(get_value_from_nid(
        nid, 'code_system_id', extract_type_id=extract_type_id))

    # the data type
    data_type_id = get_value_from_nid(
        nid, 'data_type_id', extract_type_id=extract_type_id)

    # cause map
    cause_map = get_cause_map(
        code_map_version_id=cmvid,
        **read_file_cache_options
    )

    orig_deaths_sum = int(df['deaths'].sum())

    if remove_decimal:
        print_log_message("Removing decimal from code map")
        cause_map['value'] = cause_map['value'].apply(
            lambda x: x.replace(".", ""))

    if needs_garbage_correction(iso3, data_type_id):
        print_log_message("Correcting Garbage for {}".format(iso3))
        orig_gc_sum = int(df.query('cause_id == 743')['deaths'].sum())

        cause_meta_df = get_current_cause_hierarchy(
            cause_set_version_id=csvid,
            **read_file_cache_options
        )

        # get age group ids
        age_meta_df = get_ages(
            **read_file_cache_options
        )

        loc_meta_df = get_current_location_hierarchy(
            location_set_version_id=lsvid,
            **read_file_cache_options
        )

        pop_meta_df = get_pop(
            pop_run_id=pop_run_id,
            **read_file_cache_options
        )
        # Move garbage to hiv first
        hiv_corrector = HIVCorrector(df,
                                     iso3,
                                     code_system_id,
                                     pop_meta_df,
                                     cause_meta_df,
                                     loc_meta_df,
                                     age_meta_df,
                                     correct_garbage=True)
        df = hiv_corrector.get_computed_dataframe()
        after_gc_sum = int(df.query('cause_id == 743')['deaths'].sum())
        after_deaths_sum = int(df['deaths'].sum())
        print_log_message("""
            Stage [gc deaths / total deaths]
            Before GC correction [{gco} / {to}]
            After GC correction [{gca} / {ta}]
        """.format(
            gco=orig_gc_sum, to=orig_deaths_sum,
            gca=after_gc_sum, ta=after_deaths_sum
        ))

    df = add_code_metadata(
        df, ['value', 'code_system_id'],
        code_map=cause_map,
        **read_file_cache_options
    )
    # recognizing that it is weird for code_system_id to come from two places,
    # make sure they are consistent
    assert (df['code_system_id'] == code_system_id).all(), "Variable code " \
        "system id {} did not agree with all values of df code " \
        "system id: \n{}".format(
            code_system_id, df.loc[df['code_system_id'] != code_system_id])

    print_log_message("Formatting data for redistribution")
    # do we have all the packages we need?
    # verify_packages(df)
    # format age groups to match package parameters
    df = format_age_groups(df)
    # drop observations with 0 deaths
    df = drop_zero_deaths(df)
    # merge on redistribution location hierarchy
    df = add_rd_locations(df, lsvid)
    # fill in any missing stuff that may have come from rd hierarchy
    df = fill_missing_df(df, verify_all=True)
    # create split groups

    # NO SPLIT GROUP NEEDED
    df = add_split_group_id_column(df)

    # final check to make sure we have all the necessary columns
    df = format_columns_for_rd(df, code_system_id)

    split_groups = list(df.split_group.unique())
    parallel = len(split_groups) > 1

    print_log_message("Submitting/Running split groups")
    for split_group in split_groups:
        # remove intermediate files from previous run
        delete_split_group_output(nid, extract_type_id, split_group)
        # save to file
        split_df = df.loc[df['split_group'] == split_group]
        write_split_group_input(split_df, nid, extract_type_id, split_group)
        # submit jobs or just run them here
        if parallel:
            submit_split_group(nid, extract_type_id, split_group,
                               code_system_id, launch_set_id)
        else:
            worker_main(nid, extract_type_id, split_group, code_system_id)
    if parallel:
        print_log_message("Waiting for splits to complete...")
        # wait until all jobs for a given nid have completed
        # eventually need logic for files not being present
        wait('claude_redistributionworker_{}'.format(nid), 30)
        # This seems to be necessary to wait for files
        print_log_message("Done waiting. Appending them together")
    # append split groups together
    df = read_append_split_groups(split_groups, nid, extract_type_id,
                                  cause_map)

    print_log_message(
        "Done appending files - {} rows assembled".format(len(df)))
    df = revert_variables(df)

    after_deaths_sum = int(df['deaths'].sum())
    before_after_text = """
        Before GC redistribution: {a}
        After GC redistribution: {b}
    """.format(
        a=orig_deaths_sum, b=after_deaths_sum
    )
    diff = abs(orig_deaths_sum - after_deaths_sum)
    # bad if change 2% or 5 deaths, whichever is greater
    # (somewhat arbitrary, just trying to avoid annoying/non-issue failures)
    diff_threshold = max(.02 * orig_deaths_sum, 5)
    if not diff < diff_threshold:
        raise AssertionError("Deaths not close.\n" + before_after_text)
    else:
        print_log_message(before_after_text)

    return df


def main(nid, extract_type_id, csvid, lsvid,
         pop_run_id, cmvid, remove_decimal, launch_set_id):
    """Download data, run phase, and output result."""
    # download data from input database
    df = get_phase_output("misdiagnosiscorrection", nid, extract_type_id)
    # who even needs redistribution?
    if has_garbage(df):
        print_log_message("Running redistribution")
        # run the pipeline
        df = run_phase(df, csvid, nid, extract_type_id,
                       lsvid, pop_run_id, cmvid, launch_set_id, remove_decimal)
    else:
        print_log_message("No redistribution to do.")
        # collapse code id
        val_cols = ['deaths']
        group_cols = list(set(df.columns) - set(['code_id'] + val_cols))
        df = df.groupby(group_cols, as_index=False)[val_cols].sum()

    # write it out
    write_phase_output(df, 'redistribution', nid,
                       extract_type_id, launch_set_id)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    # cause set version id
    csvid = int(sys.argv[3])
    # location_set_version_id
    lsvid = int(sys.argv[4])
    # process version map id
    pop_run_id = int(sys.argv[5])
    # code_map_version_id
    cmvid = int(sys.argv[6])
    # remove decimal
    remove_decimal = sys.argv[7]
    assert remove_decimal in ["True", "False"], \
        "invalid remove_decimal: {}".format(remove_decimal)
    remove_decimal = (remove_decimal == "True")
    # launch set id
    launch_set_id = int(sys.argv[8])
    main(nid, extract_type_id, csvid, lsvid, pop_run_id, cmvid,
         remove_decimal, launch_set_id)

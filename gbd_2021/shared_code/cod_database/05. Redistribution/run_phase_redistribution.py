"""
Master script for redistribution.

Creates split groups, sends to run_pipeline_redistribution,
and appends the output from run_pipeline_redistribution.

"""
import sys
import os

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
    get_value_from_nid
)
from cod_prep.utils import (
    wait_for_job_ids,
    report_if_merge_fail,
    fill_missing_df,
    submit_cod,
    just_keep_trying,
    print_log_message,
    CodSchema,
)
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.hiv_correction import HIVCorrector
from cod_prep.claude.claude_io import (
    get_claude_data,
    write_phase_output,
    makedirs_safely
)
from cod_prep.claude.run_phase_redistributionworker import main as worker_main

CONF = Configurator('standard')
CACHE_DIR = CONF.get_directory('db_cache')
CODE_DIR = CONF.get_directory('claude_code')
RD_PROCESS_DIR = CONF.get_directory('rd_process_data')
PACKAGE_DIR = CONF.get_directory('rd_package_dir') + 'FILEPATH'
SG_DIR = RD_PROCESS_DIR + 'FILEPATH'


def has_garbage(df):
    """Determine whether or not there are any garbage codes."""
    any_garbage = (df['cause_id'] == 743).any()
    return any_garbage


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
    group_cols = ['country', 'subnational_level1', 'site_id', 'nid',
                  'extract_type_id', 'year_id']
    g = df.groupby(group_cols)[df.columns[-1]].agg(np.min).reset_index()
    # Get rid of all columns except the group columns
    g = g[group_cols]
    # Create ID column that numbers each row (each row is a group)
    g[id_col] = list(range(1, len(g) + 1))
    return df.merge(g, on=group_cols)


def format_columns_for_rd(df):
    """Ensure necessary columns are appropriately named and present."""
    df.rename(columns={'value': 'cause',
                       'deaths': 'freq'}, inplace=True)
    df['sex'] = df['sex_id']
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
        filepath = "FILEPATH"
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

def revert_variables(df, start_schema: CodSchema):
    df.rename(columns={'freq': 'deaths'}, inplace=True)
    df = df[set(start_schema.schema) - {"code_id"}]
    return df


def submit_split_group(nid, extract_type_id, split_group, code_system_id,
                       launch_set_id):
    """Submit a job for a split group."""
    if extract_type_id in [1820, 1838, 1850, 1862]:
        slots_needed = 14
    elif extract_type_id in [3, 5]:
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
    worker = "FILEPATH"
    slots = slots_needed
    params = [
        nid,
        extract_type_id,
        split_group,
        code_system_id
    ]
    log_base_dir = "FILEPATH"
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
    df.to_csv('FILEPATH')


def needs_garbage_correction(iso3, data_type_id):
    return iso3 in ["ZAF", "MOZ", "ZWE"] and data_type_id == 9


def delete_split_group_output(nid, extract_type_id, sg):
    """Delete the existing intermediate split group files."""
    indir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)
    for_rd_path = "FILEPATH"
    post_rd_path = "FILEPATH"
    for path in [for_rd_path, post_rd_path]:
        if os.path.exists(path):
            os.unlink(path)


def merge_acause_and_collapse(df, cause_map):
    """Add acause column and collapse before appending split groups."""

    cause_map = cause_map[['cause_id', 'value']].copy()
    cause_map = cause_map.rename(columns={'value': 'cause'})

    df = df.merge(cause_map, how='left', on='cause')
    report_if_merge_fail(df, 'cause_id', 'cause')

    df = df.drop(['cause', 'split_group'], axis=1)
    df = df.groupby([col for col in df.columns if col != 'freq'],
                    as_index=False).sum()
    return df


def run_phase(df, csvid, nid, extract_type_id, project_id, lsvid, pop_run_id, cmvid,
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
        nid, 'iso3', project_id=project_id, extract_type_id=extract_type_id,
        location_set_version_id=lsvid
    )

    # the code system id
    code_system_id = int(get_value_from_nid(
        nid, 'code_system_id', project_id=project_id, extract_type_id=extract_type_id))

    # the data type
    data_type_id = get_value_from_nid(
        nid, 'data_type_id', project_id=project_id, extract_type_id=extract_type_id)

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

    start_schema = CodSchema.infer_from_data(df)

    df = add_code_metadata(
        df, ['value', 'code_system_id'],
        code_map=cause_map,
        **read_file_cache_options
    )
    assert (df['code_system_id'] == code_system_id).all(), "Variable code " \
        "system id {} did not agree with all values of df code " \
        "system id: \n{}".format(
            code_system_id, df.loc[df['code_system_id'] != code_system_id])
    df.drop(columns='code_system_id', inplace=True)

    print_log_message("Formatting data for redistribution")
    # format age groups to match package parameters
    df = format_age_groups(df)
    # drop observations with 0 deaths
    df = drop_zero_deaths(df)
    # merge on redistribution location hierarchy
    df = add_rd_locations(df, lsvid)
    # fill in any missing stuff that may have come from rd hierarchy
    df = fill_missing_df(df, verify_all=True)

    df = add_split_group_id_column(df)

    # final check to make sure we have all the necessary columns
    df = format_columns_for_rd(df)

    split_groups = list(df.split_group.unique())
    parallel = len(split_groups) > 1

    print_log_message("Submitting/Running split groups")
    sg_jids = []
    for split_group in split_groups:
        # remove intermediate files from previous run
        delete_split_group_output(nid, extract_type_id, split_group)
        # save to file
        split_df = df.loc[df['split_group'] == split_group]
        write_split_group_input(split_df, nid, extract_type_id, split_group)
        # submit jobs or just run them here
        if parallel:
            jid = submit_split_group(nid, extract_type_id, split_group,
                                     code_system_id, launch_set_id)
            sg_jids.append(jid)
        else:
            worker_main(nid, extract_type_id, split_group, code_system_id)
    if parallel:
        print_log_message("Waiting for splits to complete...")
        wait_for_job_ids(sg_jids, 30)
        print_log_message("Done waiting. Appending them together")
    # append split groups together
    df = read_append_split_groups(split_groups, nid, extract_type_id, cause_map)
    print_log_message(
        "Done appending files - {} rows assembled".format(len(df)))
    df = revert_variables(df, start_schema)

    after_deaths_sum = int(df['deaths'].sum())
    before_after_text = """
        Before GC redistribution: {a}
        After GC redistribution: {b}
    """.format(
        a=orig_deaths_sum, b=after_deaths_sum
    )
    diff = abs(orig_deaths_sum - after_deaths_sum)
    diff_threshold = max(.02 * orig_deaths_sum, 5)
    if not diff < diff_threshold:
        raise AssertionError("Deaths not close.\n" + before_after_text)
    else:
        print_log_message(before_after_text)

    return df


def main(nid, extract_type_id, project_id, csvid, lsvid,
         pop_run_id, cmvid, remove_decimal, launch_set_id):
    """Download data, run phase, and output result."""
    # download data from input database
    df = get_claude_data("misdiagnosiscorrection", nid, extract_type_id, project_id)
    if has_garbage(df):
        print_log_message("Running redistribution")
        # run the pipeline
        df = run_phase(df, csvid, nid, extract_type_id, project_id,
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
    project_id = int(sys.argv[3])
    # cause set version id
    csvid = int(sys.argv[4])
    # location_set_version_id
    lsvid = int(sys.argv[5])
    # process version map id
    pop_run_id = int(sys.argv[6])
    # code_map_version_id
    cmvid = int(sys.argv[7])
    # remove decimal
    remove_decimal = sys.argv[8]
    assert remove_decimal in ["True", "False"], \
        "invalid remove_decimal: {}".format(remove_decimal)
    remove_decimal = (remove_decimal == "True")
    # launch set id
    launch_set_id = int(sys.argv[9])
    main(nid, extract_type_id, project_id, csvid, lsvid, pop_run_id,
        cmvid, remove_decimal, launch_set_id)

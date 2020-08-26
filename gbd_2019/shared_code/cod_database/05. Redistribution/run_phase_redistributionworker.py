"""Read in split groups and pass them through redistribution.

Split groups are created from run_pipeline_redistribution_master.py
and passed to this script, which then passes them through the
"guts" of redistribution. The master script then appends each
split group togehter post redistribution.
"""
import sys
import os
import pandas as pd
import time
from configurator import Configurator
from cod_prep.claude.redistribution import GarbageRedistributor

CONF = Configurator('standard')
RD_PROCESS_DIR = CONF.get_directory('rd_process_data')
RD_INPUTS_DIR = CONF.get_directory('rd_package_dir')
PACKAGE_DIR = RD_INPUTS_DIR + '/{code_system_id}'
SG_DIR = RD_PROCESS_DIR + '/{nid}/{extract_type_id}/split_{sg}'


def read_cause_map(code_system_id):
    """Read in cause map csv produced by downloading packages."""
    indir = PACKAGE_DIR.format(code_system_id=int(code_system_id))
    df = pd.read_csv('{}/cause_map.csv'.format(indir))
    return df


def read_split_group(nid, extract_type_id, sg):
    """Read in split group dataframe."""
    indir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)
    df = pd.read_csv('{}/for_rd.csv'.format(indir), dtype={'cause': 'object'})
    return df


def fill_zeros(df):
    """Reshape age long to wide then back again to fill in 0s.

    Problem: For data sparse areas we need 0s in the data before
    noise reduction, which will become non-zero data points post
    noise reduction and fill in time trends.

    For now, only do this for VA data. Stata did this for all data,
    post redistribution, by split group. See cod_rdp_worker.do line 284
    """
    group_cols = ['location_id', 'site_id', 'year_id', 'nid', 'extract_type_id',
                  'split_group', 'cause', 'sex_id', 'age_group_id']
    id_vars = [x for x in group_cols if x != 'age_group_id']

    # can't have duplicates for pivot
    df = df.groupby(group_cols, as_index=False)['freq'].sum()

    # reshape wide on age
    df = df.pivot_table(index=id_vars, columns='age_group_id',
                        values='freq', fill_value=0)

    # get columns out of the index
    df = df.reset_index()

    # reshape long on age again
    df = df.melt(id_vars=id_vars, var_name='age_group_id', value_name='freq')

    return df


def write_split_group(df, nid, extract_type_id, sg):
    """Write completed split group."""
    indir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)
    df.to_csv('{}/post_rd.csv'.format(indir), index=False)


def run_pipeline(df, nid, extract_type_id, cause_map,
                 code_system_id, sg, write_diagnostics=True):
    """Run full pipeline, chaining together redistribution processes."""
    redistributor = GarbageRedistributor(code_system_id,
                                         first_and_last_only=False)
    df = redistributor.get_computed_dataframe(df, cause_map)
    # 07/03/2019: don't fill zeros for CHAMPS since we don't want
    # artificial 0's in the data
    if nid != 394317:  # CHAMPS
        df = fill_zeros(df)

    if write_diagnostics:
        outdir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)

        signature_metadata = redistributor.get_signature_metadata()
        signature_metadata.to_csv(
            "{}/signature_metadata.csv".format(outdir), index=False
        )

        proportion_metadata = redistributor.get_proportion_metadata()
        proportion_metadata.to_csv(
            "{}/proportion_metadata.csv".format(outdir), index=False
        )

        magic_table = redistributor.get_diagnostic_dataframe()
        magic_table.to_csv(
            "{}/magic_table.csv".format(outdir), index=False
        )

    return df


def main(nid, extract_type_id, split_group, code_system_id):
    """Main method."""
    start_time = time.time()
    df = read_split_group(nid, extract_type_id, split_group)
    cause_map = read_cause_map(code_system_id)
    df = run_pipeline(df, nid, extract_type_id, cause_map,
                      code_system_id, split_group)
    write_split_group(df, nid, extract_type_id, split_group)
    run_time = time.time() - start_time
    print(run_time)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    split_group = int(sys.argv[3])
    code_system_id = int(sys.argv[4])
    main(nid, extract_type_id, split_group, code_system_id)

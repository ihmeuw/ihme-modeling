
import sys
import os
import pandas as pd
import time
this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, '../..'))
sys.path.append(repo_dir)
from configurator import Configurator
from cod_prep.claude.redistribution import GarbageRedistributor

CONF = Configurator('standard')
RD_PROCESS_DIR = CONF.get_directory('rd_process_data')
RD_INPUTS_DIR = CONF.get_directory('rd_process_inputs')
PACKAGE_DIR = RD_INPUTS_DIR + 'FILEPATH'
SG_DIR = RD_PROCESS_DIR + 'FILEPATH'


def read_cause_map(code_system_id):
    indir = PACKAGE_DIR.format(code_system_id=int(code_system_id))
    df = pd.read_csv('FILEPATH')
    return df


def read_split_group(nid, extract_type_id, sg):
    indir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)
    df = pd.read_csv("FILEPATH")
    return df


def fill_zeros(df):
    id_cols = ['location_id', 'site_id', 'year_id', 'nid', 'extract_type_id',
               'split_group', 'cause', 'sex_id', 'age_group_id']

    df_no_dupes = df.groupby(id_cols, as_index=False)['freq'].sum()

    wide_on_age = df_no_dupes.set_index(id_cols)[['freq']].unstack()['freq']

    for age in wide_on_age.columns:
        wide_on_age[age] = wide_on_age[age].fillna(0)

    long_on_age = wide_on_age.stack().reset_index()
    long_on_age.rename(columns={0: 'freq'}, inplace=True)

    return long_on_age


def write_split_group(df, nid, extract_type_id, sg):
    indir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)
    df.to_csv('FILEPATH'.format(indir), index=False)


def run_pipeline(df, nid, extract_type_id, cause_map,
                 code_system_id, sg, write_diagnostics=True):
    redistributor = GarbageRedistributor(code_system_id,
                                         first_and_last_only=False)
    df = redistributor.get_computed_dataframe(df, cause_map)
    df = fill_zeros(df)

    if write_diagnostics:
        outdir = SG_DIR.format(nid=nid, extract_type_id=extract_type_id, sg=sg)

        signature_metadata = redistributor.get_signature_metadata()
        signature_metadata.to_csv(
            "FILEPATH".format(outdir), index=False
        )

        proportion_metadata = redistributor.get_proportion_metadata()
        proportion_metadata.to_csv(
            "FILEPATH".format(outdir), index=False
        )

        magic_table = redistributor.get_diagnostic_dataframe()
        magic_table.to_csv(
            "FILEPATH".format(outdir), index=False
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

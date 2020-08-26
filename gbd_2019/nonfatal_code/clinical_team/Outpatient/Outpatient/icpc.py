"""
This is the main script for ICPC processing. It reads in formatted
ICPC data, maps it to bundle_id, and adds needed columns.

This script is ran as part of the outpatient process, and is
launched from the outpatient.py run-all script.
"""

import pandas as pd
import sys
from db_queries import get_location_metadata
import db_queries

sys.path.append("FILENAME")


def rename_columns(df):
    """Renames columns to match upload requirements"""
    df = df.rename(
        columns={
            'year_start': 'year_start_id',
            'year_end': 'year_end_id',
            'val': 'cases'
        }
    )
    return df


def get_sample_size_icpc(df, gbd_round_id, decomp_step):
    """Attaches population so it can be used as sample_size."""

    if "sample_size" in df.columns:
        df = df.drop("sample_size", axis=1)

    pop = db_queries.get_population(age_group_id=list(df.age_group_id.unique()),
                                    location_id=list(df.location_id.unique()),
                                    sex_id=[1, 2],
                                    year_id=list(df.year_start_id.unique()),
                                    gbd_round_id=gbd_round_id,
                                    decomp_step=decomp_step)


    pop.rename(columns={'year_id': 'year_start_id'}, inplace=True)
    pop['year_end_id'] = pop['year_start_id']
    pop.drop("run_id", axis=1, inplace=True)

    pre_shape = df.shape[0]


    df = df.merge(pop, how='left', on=['location_id', 'year_start_id',
                                       'year_end_id', 'age_group_id',
                                       'sex_id'])
    assert_msg = """
    number of rows don't match after merge. before it was {} and now it
    is {} for a difference (before - after) of {}
    """.format(pre_shape, df.shape[0], pre_shape - df.shape[0])
    assert pre_shape == df.shape[0], assert_msg

    assert df.population.notnull().all(), 'there are nulls'

    df = df.rename(columns={"population": "sample_size"})

    return df


def add_columns(df, run_id):
    """Add remaning needed columns"""

    df['mean'] = pd.np.nan
    df['lower'] = pd.np.nan
    df['upper'] = pd.np.nan

    df['run_id'] = run_id
    df['source_type_id'] = 11
    df['diagnosis_id'] = 3
    df['estimate_id'] = 25

    return df


def remove_columns(df):
    """Removing columns that are not needed for upload"""
    df = df.drop(['age_group_unit', 'code_system_id',
                  'facility_id', 'metric_id', 'outcome_id', 'source'], axis=1)
    return df


def check(df):
    """Check that df has exactly the right columns and that bundles are
    present"""


    ref = pd.read_csv("FILENAME"
                      "FILEPATH", nrows=100)

    assert set(ref.columns) - set(df.columns) == set(),\
        "Not all needed columns are present"

    assert set(df.columns) - set(ref.columns) == set(),\
        "Too many columns are present"

    assert df.shape[0] > 0, "No rows are present"


def get_icpc_map():
    """Read and prepares the map.
    """

    imap = pd.read_csv("FILEPATH")

    imap = imap.rename(columns={"diagnosis": "cause_code"})

    if 'bundle_level' in imap.columns:
        imap = imap.drop('bundle_level', axis=1)


    rs = imap.groupby('cause_code').size().reset_index()
    rs.columns = ['cause_code', 'counts']
    many_to_many = rs[rs['counts'] > 1].cause_code.unique()
    dups = imap[imap.cause_code.isin(many_to_many)].sort_values('cause_code')

    return imap


def icpc_mapping(df):
    """Map from ICPC code to bundle_id"""

    imap = get_icpc_map()


    pre = df.shape[0]
    df = df.merge(imap, how='left', on='cause_code', validate='many_to_many')
    print('Number of rows changed during mapping. Before: {}. After: {}.'.format(
        pre, df.shape[0]))
    print("The map is a many to many relationship. This causes the number of rows to increase.")


    df = df[df.bundle_id.notnull()]


    assert df.notnull().all().all()
    groupby_cols = list(set(df.columns) - set(['cases', 'cause_code']))
    df = df.groupby(groupby_cols, as_index=False).cases.sum()

    return df


def save_icpc(df, run_id):
    """Save to the outpatient final directory"""
    filepath = "FILEPATH".format(
        run_id)
    print("Saving to {}...".format(filepath))
    df.to_csv(filepath, index=False)
    print("Done.")


def main_icpc(run_id, gbd_round_id, decomp_step):
    """Run all ICPC functions"""
    df = pd.read_hdf("FILEPATH"
                     "FILEPATH".format(run_id))
    df = rename_columns(df)
    df = remove_columns(df)
    df = icpc_mapping(df)
    df = get_sample_size_icpc(
        df, gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    df = add_columns(df, run_id=run_id)

    assert df.shape[0] > 0
    return df


if __name__ == '__main__':

    RUN_ID = sys.argv[1]
    RUN_ID = int(RUN_ID)
    GBD_ROUND_ID = sys.argv[2]
    GBD_ROUND_ID = int(GBD_ROUND_ID)
    DECOMP_STEP = sys.argv[3]

    print(
        "Command-line arguments passed into this script: {}".format(sys.argv[1:]))

    icpc = main_icpc(run_id=RUN_ID, gbd_round_id=GBD_ROUND_ID,
                     decomp_step=DECOMP_STEP)
    save_icpc(df=icpc, run_id=RUN_ID)

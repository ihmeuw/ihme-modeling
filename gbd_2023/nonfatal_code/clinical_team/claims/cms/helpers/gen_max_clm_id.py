"""
MAX data does not have a unique claim identifier column which our schema relies on. In order to
fix this we'll generate our own UUIDs using Python modules and then output a single column CSV
with a 'CLM_ID' column. 'CLM_ID' will be consistent with the naming scheme in MDCR. Then our
process of creating intermediate tables with rename this to 'claim_id'

This will use UUID4 rather than UUID1. The only difference should be that UUID4 is more secure/private
because it does not use a system's network address to generate the ID


outdir = "FILEPATH"
create_all_max_clm_ids(outdir)
"""
import uuid

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants

from cms.helpers.intermediate_tables import funcs
from cms.utils.database import database
from cms.validations import max_clm_ids as test


def create_and_store_clm_id(cms_system, year, outdir):
    """For a given 'cms_system' (see FILEPATH
    for naming structure of 'cms_system') and year, generate a file for each
    staging table which contains a unique claim_id
    """

    db_table = funcs.claims_table_maker(cms_system, year)

    cmsdb = database.CmsDatabase()
    cmsdb.load_odbc()
    row_count = get_rowcount(cmsdb, db_table)

    df = gen_uuid4_clm_df(row_count)

    test.validate_max_df(df, row_count)

    if outdir:
        df.to_csv("FILEPATH", index=False)
    else:
        return df


def create_all_max_clm_ids(outdir):
    ot_state_years = constants.max_state_years
    inp_years = ot_state_years.keys()

    for year in inp_years:
        # generate 1 inp file
        create_and_store_clm_id(cms_system="max_inp", year=year, outdir=outdir)
        for state in ot_state_years[year]:
            # generate number(states) ot files
            create_and_store_clm_id(cms_system=f"max_ot_{state}", year=year, outdir=outdir)

    test.validate_all_max_clm_ids(outdir)
    return


def gen_uuid4_clm_df(n):
    df = pd.DataFrame({"CLM_ID": [str(uuid.uuid4()) for i in range(n)]})
    if len(df) != df.CLM_ID.unique().size:
        diff = df[df.duplicated(keep=False)]
        raise ValueError(f"The CLM_ID column is not actually unique {diff}")
    return df


def get_rowcount(cmsdb, db_table):
    rc = cmsdb.read_table("QUERY")
    assert len(rc) == 1, "Too many rows were returned"
    rc = rc.iloc[0, 0]
    return rc

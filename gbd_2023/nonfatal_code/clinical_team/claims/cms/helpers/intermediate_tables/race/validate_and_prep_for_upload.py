"""
After the main submit and worker scripts are run this will prepare two separate
bene race tables. 1 for Medicare and 1 for Medicaid


sort order is important here, and we need all the data together. We'll sort on bene_id then year_id
"""
import glob

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.helpers.intermediate_tables.race import validate_outputs as vo


def get_race_data(cms_sys):
    """Read all the outputs created by the submit/worker scripts"""

    race_dir = filepath_parser(ini="pipeline.cms", section="table_outputs", section_key="race")
    files = glob.glob("FILEPATH")
    df = pd.concat([pd.read_csv(f) for f in files], sort=False)
    db_order = ["bene_id", "year_id", "rti_race_cd"]

    df = df[db_order]

    return df


def sort_race_df(df):
    """Sort bene_id in ascending order and year_id in descending order"""

    # default sort order for benes to match other code but descending
    # order for year
    df = df.sort_values("year_id", ascending=False).sort_values("bene_id")
    return df


def write_race_df(df, cms_sys):
    """Write the prepped df to a folder for upload to the intermediary db"""

    # check the entire race_bene tables before writing
    vo.validate_bene_race()
    assert df.isnull().sum().sum() == 0, "No Nulls allowed in dB"

    race_dir = filepath_parser(ini="pipeline.cms", section="table_outputs", section_key="race")
    write_path = "FILEPATH"
    df.to_csv(write_path, index=False, header=None)
    return


def main():
    for cms_sys in ["max", "mdcr"]:
        df = get_race_data(cms_sys)
        df = sort_race_df(df)
        write_race_df(df, cms_sys)

    print("Race bene files have successfully written for upload")
    return


if __name__ == "__main__":
    main()

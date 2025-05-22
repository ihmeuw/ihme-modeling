"""
This is a worker script that runs get_claude_data for one year.
"""

import sys
import getpass
import pandas as pd

sys.path.append("FILEPATH")
from cod_prep.claude.claude_io import get_claude_data

assert len(sys.argv) == 3  # 1st arg is file name

if __name__ == "__main__":
    # Globals
    YEAR = sys.argv[1]
    YEAR = pd.to_numeric(YEAR)
    NEW_RUN_ID = sys.argv[2]
    NEW_RUN_ID = pd.to_numeric(NEW_RUN_ID)
    print("Year is {}".format(YEAR))
    print("run_id is {}".format(NEW_RUN_ID))

    # Pull data
    df = get_claude_data(
        phase="formatted",
        data_type_id=9,
        year_id=YEAR,
        is_active=None,
        is_mort_active=True,
        verbose=True,
        attach_launch_set_id=False,
        force_rerun=True,
        block_rerun=False,
        project_id = 15,
        lazy=True
    )

    remove_codes = pd.read_csv("FILEPATH")
    df = (
        df.loc[lambda d: ~d["code_id"].isin(remove_codes), :]
        .groupby(list(set(df.columns) - {"code_id", "deaths"}))
        .agg({"deaths": "sum"})
        .reset_index()
        .compute()
    )

    assert len(df.year_id.unique()) <= 1,\
        "There's more than 1 year of data; Was expecting 1."
    assert int(df.year_id.unique()[0]) == int(YEAR),\
        "The 1 year present in data is not the same as the year we tried to get data for"
    assert df.shape[0] > 0, "DataFrame has no data!"

    # drop all cause vr
    df = df[df.extract_type_id != 167]

    # replace age_group_id -1 with 283
    df.loc[df.age_group_id == -1, 'age_group_id'] = 283

    # replace age group 308 with 26
    df.loc[df.age_group_id == 308, 'age_group_id'] = 26

    assert df.shape[0] > 0, "DataFrame has no data!"

    # drop unneeded columns
    df = df.drop('site_id', axis=1)

    # recollapose
    df = df.groupby(by=[col for col in df.columns if col != "deaths"], as_index=False).deaths.sum()

    # save
    filename = ("FILEPATH")
    df.to_csv(filename, index=False)

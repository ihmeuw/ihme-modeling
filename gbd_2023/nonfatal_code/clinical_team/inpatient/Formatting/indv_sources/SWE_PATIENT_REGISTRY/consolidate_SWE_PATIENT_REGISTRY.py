from typing import Tuple

import numpy as np
import pandas as pd

from inpatient.Formatting.all_sources import fmt_sources_worker
from crosscutting_functions import *

# update this when we have new years of SWE data
OLD_PATH = "FILEPATH"
NEW_PATH = "FILEPATH"


def read_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read in both new and old formatted SWE files.
    """
    df_new = pd.read_hdf(NEW_PATH)
    df_old = pd.read_stata(OLD_PATH)
    return df_new, df_old


def format_old_data(df_old: pd.DataFrame) -> pd.DataFrame:
    """
    Reformat old formatted SWE files to the H5 files' standard.
    """
    df_old.loc[df_old["age_start"] < 0, ["age_start", "age_end"]] = [0, 125]
    df_old.rename(
        columns={"year": "year_start", "sex": "sex_id", "cases": "val", "NID": "nid"},
        inplace=True,
    )

    df_old["year_end"] = df_old["year_start"]
    df_old["age_group_unit"] = 1
    df_old["source"] = "SWE_PATIENT_REGISTRY"
    df_old["facility_id"] = np.where(
        df_old["platform"] == 1, "inpatient unknown", "outpatient unknown"
    )
    df_old["code_system_id"] = 2
    df_old["representative_id"] = 1
    df_old["metric_id"] = 1
    df_old["outcome_id"] = "case"
    df_old["nid"] = df_old["nid"].astype(int)

    df_old = fmt_sources_worker.replace_rounded_under1_ages(df_old)

    df_old = df_old.drop(
        columns=[
            "iso3",
            "subdiv",
            "national",
            "platform",
            "icd_vers",
            "deaths",
        ]
    )

    return df_old


def format_new_data(df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Reformat new formatted SWE files. Only change source name and age start/end floats.
    """
    df_new["source"] = "SWE_PATIENT_REGISTRY"
    df_new = fmt_sources_worker.replace_rounded_under1_ages(df_new)

    return df_new


if __name__ == "__main__":
    df_new, df_old = read_data()
    assert (
        df_new.shape[1] == df_old.shape[1]
    ), "the column numbers between new and old data are different post formatting"
    rows_old = df_old.shape[0]
    rows_new = df_new.shape[0]

    df_old = format_old_data(df_old)
    assert df_old.shape[0] == rows_old, "the number of rows for df_old changed post formatting"
    df_new = format_new_data(df_new)
    assert df_new.shape[0] == rows_new, "the number of rows for df_new changed post formatting"

    df = pd.concat([df_new, df_old], ignore_index=True)

    # write to new file directory
    hosp_prep.write_hosp_file(
        df,
        "FILEPATH",
        backup=True,
    )

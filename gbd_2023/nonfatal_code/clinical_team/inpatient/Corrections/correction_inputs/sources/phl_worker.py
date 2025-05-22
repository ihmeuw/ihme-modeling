import pandas as pd
from crosscutting_functions.pipeline import get_map_version

from inpatient.CorrectionsFactors.correction_inputs import estimate_indv
from inpatient.CorrectionsFactors.correction_inputs.sources import phl_cf_constants


def format_phl_df(year: int) -> pd.DataFrame:
    path = f"{phl_cf_constants.BASE_DIR}/formatted_phl_{year}.parquet"
    df = pd.read_parquet(path, columns=phl_cf_constants.COLS)

    df = df.loc[df.facility_id == "inpatient unknown"]
    df = df.drop("facility_id", axis=1)
    df["adm_date"] = pd.to_datetime(df["adm_date"])

    df["is_otp"] = 0

    return df


def main(run_id: int, year: int) -> None:
    # output path
    write_path = (
        "FILEPATH/bundle_PHL_HICC_{year}.csv"
    )
    df = format_phl_df(year)
    map_version = get_map_version(run_id=run_id)
    df = estimate_indv.main(df, map_version, phl_cf_constants.AGE_GROUP_SET_DICT[year])

    df.to_csv(write_path, index=False)
    print(f"{year} PHL done.")

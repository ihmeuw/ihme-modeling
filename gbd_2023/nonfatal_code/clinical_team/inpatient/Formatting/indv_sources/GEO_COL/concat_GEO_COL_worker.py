import pandas as pd
from crosscutting_functions import general_purpose
from crosscutting_functions.formatting-functions import formatting

from inpatient.Formatting.indv_sources.GEO_COL import constants_GEO_COL


def run_concat() -> None:
    """Concat all years' parquets and save the compiled dataset to disk."""

    all_files = list(constants_GEO_COL.OUTPATH_DIR.glob("output_by_year/*.parquet"))

    # Concatenate all parquet files
    df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)

    # Calculate missing_years after df is defined
    missing_years = set(df["year_start"].unique()) - set(constants_GEO_COL.NIDS.keys())

    if missing_years:
        raise ValueError(f"Year ID(s) {missing_years} may be missing/unexpected.")
    if len(df["year_start"].unique()) != len(all_files):
        raise ValueError(
            "Unexpected number of files found compared to number of unique years in data."
        )

    compare_df = pd.read_hdf(constants_GEO_COL.PREV_DATA)

    subset_df = df[df["year_start"] <= 2020]
    # not enforcing test_results currently for this source

    msg = formatting.test_case_counts(
        df=subset_df, compare_df=compare_df, dfs_must_equal_eachother=False
    )
    if isinstance(msg, list):
        raise RuntimeError(f"Test failed see: {msg}")

    general_purpose.write_hosp_file(df, constants_GEO_COL.FINAL_OUTPATH, backup=True)


if __name__ == "__main__":
    run_concat()

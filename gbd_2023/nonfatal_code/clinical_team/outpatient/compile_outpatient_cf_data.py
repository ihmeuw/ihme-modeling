from pathlib import Path
from typing import List

import pandas as pd
from crosscutting_functions import demographic


def _create_file_list(read_dir: str, pattern: str) -> List[str]:
    """Given a parent directory contain marketscan pipeline outputs and a pattern, eg
    _estimate_18, capture all filepaths matching that pattern and return in a list"""
    files = [file for file in Path(read_dir).glob(pattern)]
    return files


def _concat_files(files: List[str]) -> pd.DataFrame:
    """Read and compile all filepaths into a single dataframe"""
    if len(files) < 100:
        raise RuntimeError(
            f"There are only {len(files)} files, do you want to read fewer than 100 bundles?"
        )
    df = pd.concat([pd.read_parquet(f) for f in files], sort=False, ignore_index=True)
    return df


def _filter_facilities(denominator: pd.DataFrame) -> pd.DataFrame:
    """Retain only the preferred facility IDs we want to include in our denominator"""
    good_facility_ids = [11, 22, 95]
    denominator = denominator[denominator.facility_id.isin(good_facility_ids)]
    return denominator


def _agg(df: pd.DataFrame) -> pd.DataFrame:
    """groupby and aggregate by all columns in the data except facility and sum val"""
    df = (
        df.groupby(df.drop(["facility_id", "val"], axis=1).columns.tolist())
        .agg({"val": "sum"})
        .reset_index()
    )
    return df


def _age_bin(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the standard clinical age binning function to the Marketscan
    single year age data"""
    df = demographic.age_binning(
        df,
        terminal_age_in_data=False,
        drop_age=True,
        break_if_not_contig=False,
        clinical_age_group_set_id=1,
    )
    return df


def _add_year_start_end(df: pd.DataFrame) -> pd.DataFrame:
    df["year_start"], df["year_end"] = df["year_id"], df["year_id"]
    df = df.drop("year_id", axis=1)
    return df


def _save_otp_cf(df: pd.DataFrame, run_id: int) -> None:
    """Output a csv to the run_id to be used by the R script that converts marketscan data
    inputs into the actual correction factor ratio values"""
    base_dir = (
        f"FILEPATH"
    )
    filepath = "FILEPATH"
    print("Saving to {}...".format(filepath))
    df.to_csv(filepath, index=False)
    print("Saved.")


def compile_marketscan_pipeline_results(run_id: int) -> None:
    """Run each of the helper functions above to compile and save
    marketscan CF inputs to disk"""
    read_dir = (
        f"FILEPATH"
    )

    numerator = _concat_files(
        _create_file_list(read_dir=read_dir, pattern="*_estimate_19*")
    )
    denominator = _filter_facilities(
        _concat_files(_create_file_list(read_dir=read_dir, pattern="*_estimate_18*"))
    )

    df = pd.concat([numerator, denominator], sort=False, ignore_index=True)
    df = _agg(df)
    df = _age_bin(df)
    df = _add_year_start_end(df)

    # write data
    _save_otp_cf(df, run_id)

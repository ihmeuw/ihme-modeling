from pathlib import Path

import pandas as pd


def write_wrapper(df: pd.DataFrame, write_path: str, file_format: str) -> None:
    """wraps various pandas.DataFrame.to_{format} methods with a few preferred args"""

    raise_if_not_lu_cms(df, Path(write_path))

    if file_format == "parquet":
        df.to_parquet(f"{write_path}.parquet", index=False, compression="snappy")
    elif file_format == "hdf":
        df.to_hdf(f"{write_path}.H5", mode="w", complib="blosc", complevel=6)
    elif file_format == "csv":
        df.to_csv(f"{write_path}.csv", index=False)
    else:
        raise ValueError(f"Unknown storage file format {file_format}")


def raise_if_not_lu_cms(df: pd.DataFrame, write_path: Path) -> None:
    """Identifiable CMS data must not be moved from the DIRECTORY directory. This checks that the
    second and third elements of the Path.parts exactly match limited use and CMS. It shouldn't
    matter if you're using the prefix DIR or DIR"""
    if df["is_identifiable"].sum() > 0:
        if write_path.parts[2:4] != ("limited_use", "DIRECTORY"):
            raise RuntimeError(
                f"You can not move identifiable CMS data off of DIRECTORY to {write_path}"
            )

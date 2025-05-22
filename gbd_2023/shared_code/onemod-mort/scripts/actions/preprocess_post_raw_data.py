import logging
from time import time
from typing import Any, NamedTuple

import fire
import numpy as np
import pandas as pd
from pplkit.data.interface import DataInterface

logger = logging.getLogger(__name__)


class ColumnFormat(NamedTuple):
    nan: Any
    dtype: str


# columns
IDS = ["location_id", "age_group_id", "sex_id", "year_id"]
AGE = ["age_group_id", "age_group_name", "age_mid"]
LOCATION = [
    "super_region_id",
    "super_region_name",
    "region_id",
    "region_name",
    "national_id",
    "national_name",
    "location_id",
    "location_name",
]
OBS = [
    "source_type_name",
    "mx_updated",
    "mx_adj_updated",
    "sample_size",
    "population",
    "nid",
    "underlying_nid",
    "outlier",
]


def main(directory: str) -> None:
    logging.basicConfig(
        filename="preprocess_post_raw_data.log",
        filemode="w",
        level=logging.INFO,
    )
    start = time()

    logging.info("loading settings")
    dataif = DataInterface(directory=directory)
    for dirname in ["config", "data"]:
        dataif.add_dir(dirname, dataif.directory / dirname)

    settings = dataif.load_config("settings.yaml")
    handoff = settings.pop("handoff")
    for key, value in settings.items():
        if isinstance(value, str):
            dataif.add_dir(key, value, exist_ok=True)
    if handoff == 2:
        OBS.extend(["onemod_process_type", "completeness"])

    logging.info("loading data")
    data = dataif.load_post_raw_data(columns=IDS + OBS).rename(
        columns={"mx_updated": "mx", "mx_adj_updated": "mx_adj"}
    )

    data = create_data_col(data, "obs", ["mx_adj", "mx"], drop=False)
    if handoff == 1:
        logging.info("create data column: handoff1 set obs to mx for VR source")
        index = data.eval("source_type_name == 'VR'")
        data.loc[index, "obs"] = data.loc[index, "mx"]
    data = (
        data.pipe(
            create_data_col,
            "weights",
            ["sample_size", "population"],
            drop=False,
        )
        .pipe(add_source_location_id)
        .pipe(
            format_cols,
            column_map={
                col: ColumnFormat(-1, "int32")
                for col in IDS + ["nid", "underlying_nid", "source_location_id"]
            }
            | {"outlier": ColumnFormat(2, "int32")}
            | {"source_type_name": ColumnFormat("", "str")},
        )
        .pipe(remove_duplicate_data)
        .pipe(overwrite_weights)
    )

    dataif.dump_data(data, "post_raw_data.parquet")

    stop = time()
    logging.info(f"total time: {(stop - start) / 60:.2f} minutes")


def create_data_col(
    data: pd.DataFrame, name: str, from_: list[str], drop: bool = False
) -> pd.DataFrame:
    logging.info(f"create data column: {name} from {from_}")
    data[name] = np.nan
    for col in from_:
        data[name] = data[name].where(data[name].notnull(), data[col])
    if drop:
        data = data.drop(columns=from_)
    return data


def add_source_location_id(data: pd.DataFrame) -> pd.DataFrame:
    data["location_id"] = data["location_id"].astype(int)
    data["source_location_id"] = data["location_id"]
    index = data["location_id"] >= 1_000_000
    data.loc[index, "source_location_id"] = (
        data.loc[index, "location_id"].astype(str).str[:-4].astype(int)
    )
    return data


def format_cols(data: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    logging.info(f"format columns: {column_map}")
    for col, (nan, dtype) in column_map.items():
        data[col] = data[col].fillna(nan).astype(dtype)
    return data


def remove_duplicate_data(data: pd.DataFrame) -> pd.DataFrame:
    logging.info("remove duplicate data")
    logging.info(f"    before nrows: {len(data)}")

    data["obs"] = data["obs"].fillna(-1.0)
    data = data.sort_values(
        IDS + ["source_type_name", "obs", "outlier"], ignore_index=True
    )
    data = data.groupby(IDS + ["source_type_name", "obs"]).first().reset_index()
    data.loc[data["obs"] == -1.0, "obs"] = np.nan

    logging.info(f"    after nrows: {len(data)}")
    return data


def overwrite_weights(data: pd.DataFrame) -> pd.DataFrame:
    """Overwrite weights with the following rules"""
    logging.info("overwrite weights")

    if "completeness" in data.columns:
        logging.info("apply completeness adjustment")
        index = data["completeness"].notna()
        data.loc[index, "weights"] *= data.loc[index, "completeness"]

    scale = 0.005
    sources = ["Census", "DSP"]
    logging.info(f"    shrink {sources} weights by {scale}")
    index = data.eval(f"weights.notna() & source_type_name.isin({sources})")
    data.loc[index, "weights"] *= scale

    scale = 0.1
    sources = ["Survey", "HHD"]
    logging.info(f"    shrink {sources} weights by {scale}")
    index = data.eval(f"weights.notna() & source_type_name.isin({sources})")
    data.loc[index, "weights"] *= scale

    scale = 0.01
    sources = ["VR"]
    location_ids = [180, 193, 195, 198]
    logging.info(f"    shrink {sources} in {location_ids} weights by {scale}")
    index = data.eval(
        f"weights.notna() & source_type_name.isin({sources}) & location_id.isin({location_ids})"
    )
    data.loc[index, "weights"] *= scale

    return data


if __name__ == "__main__":
    fire.Fire(main)

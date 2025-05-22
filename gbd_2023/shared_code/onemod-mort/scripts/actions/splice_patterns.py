import fire
import pandas as pd
from pplkit.data.interface import DataInterface
from scipy.special import expit, logit

IDS = [
    "sex_id",
    "super_region_id",
    "region_id",
    "national_id",
    "location_id",
    "age_group_id",
    "year_id",
]
PRED = ["spxmod_super_region", "spxmod_region", "spxmod"]
VERSIONS = [1950, 1990]


def transform_columns(
    data: pd.DataFrame, columns: list[str], obs_scale: float
) -> pd.DataFrame:
    for col in columns:
        data[col] = logit(data[col] / obs_scale)
    return data


def untransform_columns(
    data: pd.DataFrame, columns: list[str], obs_scale: float
) -> pd.DataFrame:
    for col in columns:
        data[col] = obs_scale * expit(data[col])
    return data


def compute_beta0(
    data: pd.DataFrame,
    year_ref: int = 1990,
    y_ref: str = "spxmod_1990",
    y_alt: str = "spxmod_1950",
) -> pd.DataFrame:
    data_sub = data.query(f"year_id == {year_ref}").reset_index(drop=True)
    data_sub["beta0"] = data_sub.eval(f"{y_ref} - {y_alt}")
    id_cols = ["sex_id", "location_id", "age_group_id"]
    data = data.merge(data_sub[id_cols + ["beta0"]], on=id_cols, how="left")
    return data


def compute_r(
    data: pd.DataFrame, y_ref: str = "spxmod_1990", y_alt: str = "spxmod_1950"
) -> pd.DataFrame:
    data["r"] = data.eval(f"{y_ref} - {y_alt} - beta0")
    return data


def compute_t(data: pd.DataFrame, year_ref: int = 1990) -> pd.DataFrame:
    data["t"] = data.eval(f"year_id - {year_ref}")
    return data


def compute_beta1(
    data: pd.DataFrame, year_ref: int = 1990, n: int = 10
) -> pd.DataFrame:
    data_sub = data.query(
        f"year_id >= {year_ref} & year_id < {year_ref} + {n}"
    ).reset_index(drop=True)

    data_sub["t2"] = data_sub["t"] ** 2
    data_sub["tr"] = data_sub.eval("t * r")
    id_cols = ["sex_id", "location_id", "age_group_id"]
    data_sub = data_sub.groupby(id_cols)[["t2", "tr"]].sum().reset_index()
    data_sub["beta1"] = data_sub.eval("tr / t2")
    data = data.merge(data_sub[id_cols + ["beta1"]], on=id_cols, how="left")
    return data


def compute_beta2(
    data: pd.DataFrame,
    y_ref: str,
    y_alt: str,
    year_ref: int = 1990,
    n: int = 10,
) -> pd.DataFrame:
    data_sub = data.query(
        f"year_id >= {year_ref} & year_id < {year_ref} + {n}"
    ).reset_index(drop=True)
    id_cols = ["sex_id", "location_id", "age_group_id"]
    data_sub = data_sub.groupby(id_cols)[[y_ref, y_alt]].mean().reset_index()
    data_sub["beta2"] = data_sub.eval(f"{y_ref} - {y_alt}")
    data = data.merge(data_sub[id_cols + ["beta2"]], on=id_cols, how="left")
    return data


def _compute_transition_weights(
    data: pd.DataFrame, year_start: int, year_end: int, scale: float
) -> pd.Series:
    return data.eval(
        f"(log(1 + exp({scale} * (year_id - {year_start}))) -"
        f" log(1 + exp({scale} * (year_id - {year_end})))) / {scale}"
    ) / (year_end - year_start)


def compute_w(
    data: pd.DataFrame, year_start: int, year_end: int, scale: float
) -> pd.DataFrame:
    data["w"] = _compute_transition_weights(data, year_start, year_end, scale)
    return data


def compute_v(
    data: pd.DataFrame, year_start: int, year_end: int, scale: float
) -> pd.DataFrame:
    data["v"] = _compute_transition_weights(data, year_start, year_end, scale)
    return data


def compute_extrapolation(
    data: pd.DataFrame,
    year_ref: int = 1990,
    y_ref: str = "spxmod_1990",
    y_alt: str = "spxmod_1950",
    y: str = "spxmod",
) -> pd.DataFrame:
    data[y] = data[y_ref].where(
        data["year_id"] >= year_ref,
        data.eval(f"(beta0 * w + (1 - w) * beta2) + {y_alt} + (beta1 * v) * t"),
    )
    return data


def tidyup(data: pd.DataFrame) -> pd.DataFrame:
    data.drop(
        columns=["beta0", "r", "t", "beta1", "beta2", "w", "v"], inplace=True
    )
    return data


def extrapolate_column(
    data: pd.DataFrame,
    obs_scale: float,
    year_ref: int = 1990,
    y_ref: str = "spxmod_1990",
    y_alt: str = "spxmod_1950",
    y: str = "spxmod",
) -> pd.DataFrame:
    data = (
        data.pipe(
            transform_columns, columns=[y_ref, y_alt], obs_scale=obs_scale
        )
        .pipe(compute_beta0, year_ref=year_ref, y_ref=y_ref, y_alt=y_alt)
        .pipe(compute_r, y_ref=y_ref, y_alt=y_alt)
        .pipe(compute_t, year_ref=year_ref)
        .pipe(compute_beta1, year_ref=year_ref)
        .pipe(compute_beta2, y_ref=y_ref, y_alt=y_alt, year_ref=year_ref)
        .pipe(compute_w, year_start=1977, year_end=1987, scale=0.5)
        .pipe(compute_v, year_start=1982, year_end=1989, scale=1.0)
        .pipe(
            compute_extrapolation,
            year_ref=year_ref,
            y_ref=y_ref,
            y_alt=y_alt,
            y=y,
        )
        .pipe(tidyup)
        .pipe(
            untransform_columns, columns=[y_ref, y_alt, y], obs_scale=obs_scale
        )
    )
    return data


def main(directory: str) -> None:
    dataif = DataInterface(directory=directory)

    component_settings = {
        version: dataif.load_directory(
            f"{version}_component/config/settings.yaml"
        )
        for version in VERSIONS
    }
    data = {
        version: dataif.load(
            component_settings[version]["handoff_predictions"]
            + "/predictions.parquet",
            columns=IDS + PRED,
        ).rename(columns={col: f"{col}_{version}" for col in PRED})
        for version in VERSIONS
    }

    data = data[1950].merge(data[1990], on=IDS, how="outer")
    obs_scale = component_settings[1950].get("obs_scale", 4.0)

    for column in PRED:
        data = extrapolate_column(
            data,
            obs_scale=obs_scale,
            year_ref=1990,
            y_ref=f"{column}_1990",
            y_alt=f"{column}_1950",
            y=column,
        )
    dataif.dump_directory(data[IDS + PRED], "predictions.parquet")


if __name__ == "__main__":
    fire.Fire(main)

"""Format mortality input data.

Note: OneMod and db_queries might have dependency conflicts!

"""

import logging
from time import time
from typing import Any, NamedTuple

import fire
import numpy as np
import pandas as pd
from numpy.typing import NDArray
from pplkit.data.interface import DataInterface

# from scipy.special import logit

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
    "mx",
    "mx_adj",
    "sample_size",
    "population",
    "nid",
    "underlying_nid",
    "outlier",
    "vr_reliability_adj",
    "covid_age_sex",
    "covid_asdr",
    "title",
]


def main(directory: str) -> None:
    logging.basicConfig(
        filename="preprocess.log", filemode="w", level=logging.INFO
    )
    start = time()

    logging.info("loading settings")
    dataif = DataInterface(directory=directory)
    for dirname in ["config", "data"]:
        dataif.add_dir(dirname, dataif.directory / dirname)

    settings = dataif.load_config("settings.yaml")
    handoff = settings.pop("handoff")
    obs_scale = settings.pop("obs_scale", 4.0)
    for key, value in settings.items():
        if isinstance(value, str):
            dataif.add_dir(key, value)
    if handoff == 2:
        OBS.extend(["onemod_process_type", "completeness"])

    covs = get_covs(dataif.load_config("pattern.yaml"))

    logging.info("loading data")
    data = dataif.load_raw_data(columns=list(set(IDS + OBS + covs)))
    loc_meta = dataif.load_loc_meta()
    age_meta = dataif.load_age_meta()

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
        .pipe(additional_outlier_decisions, age_meta)
        .pipe(overwrite_weights)
        .pipe(correct_paf)
        .pipe(modify_sdi)
        .pipe(merge_covid_cdr)
        # .pipe(transform_covid_age_sex, settings.get("obs_scale", 4.0))
        # .pipe(add_use_covid_addon, covid_cov=covid_cov)
        .dropna(subset=covs, ignore_index=True)
    )

    detailed = create_detailed(data, loc_meta)
    dataif.dump_config(detailed, "detailed.csv")
    dataif.dump_data(data, "raw_data.parquet")
    data.drop(
        columns=["mx_adj", "mx", "sample_size", "population"], inplace=True
    )

    data = (
        data.pipe(
            outlier_data,
            columns=["obs", "weights"],
            where=f"obs > {obs_scale} | weights <= 0 | outlier != 0",
        )
        .pipe(
            agg_data,
            "obs",
            "weights",
            IDS + ["source_location_id"],
            others=list(
                set(covs + ["covid_age_sex", "covid_asdr", "covid_cdr"])
            ),
        )
        .pipe(scale_covs, covs, by=["sex_id"])
        .pipe(scale_data, "obs", multiplier=1 / obs_scale)
        .pipe(
            add_trend_covs,
            lwr=0.1,
            upr=0.9,
            scale=0.5,
            by=["sex_id", "location_id"],
        )
        .pipe(add_meta_info, age_meta, loc_meta)
        .pipe(
            format_cols,
            column_map={
                col: ColumnFormat(-1, "int32")
                for col in ["super_region_id", "region_id", "national_id"]
            },
        )
        .pipe(add_super_region_id_trend, [31, 166, 103, 137])
        .pipe(add_high_coverage)
    )

    dataif.dump_data(data, "data.parquet")
    stop = time()
    logging.info(f"total time: {(stop - start) / 60:.2f} minutes")


def add_source_location_id(data: pd.DataFrame) -> pd.DataFrame:
    data["location_id"] = data["location_id"].astype(int)
    data["source_location_id"] = data["location_id"]
    index = data["location_id"] >= 1_000_000
    data.loc[index, "source_location_id"] = (
        data.loc[index, "location_id"].astype(str).str[:-4].astype(int)
    )
    return data


def get_covs(pattern_config: dict) -> list[str]:
    names = [
        b["name"]
        for b in pattern_config["age_pattern"]["xmodel"]["var_builders"]
    ]
    if "time_pattern" in pattern_config:
        names += [
            b["name"]
            for b in pattern_config["time_pattern"]["xmodel"]["var_builders"]
        ]
    names = [
        name
        for name in set(names)
        if not ("intercept" in name or "trend" in name)
    ]
    return names


def format_cols(data: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    logging.info(f"format columns: {column_map}")
    for col, (nan, dtype) in column_map.items():
        data[col] = data[col].fillna(nan).astype(dtype)
    return data


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


def outlier_data(
    data: pd.DataFrame, columns: list[str], where: str
) -> pd.DataFrame:
    logging.info(f"outlier data: {columns} where {where}")
    index = data.eval(where)
    for col in columns:
        data.loc[index, col] = np.nan
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


def agg_data(
    data: pd.DataFrame,
    obs: str,
    weights: str,
    by: list[str],
    others: list[str] | None = None,
) -> pd.DataFrame:
    logging.info(f"aggregate {obs} and {weights} by {by}")
    logging.info(f"    before nrows: {len(data)}")

    no_obs = data.eval("obs.isna() | weights.isna()")
    for col in [obs, weights]:
        data.loc[no_obs, col] = data.loc[no_obs, col].fillna(0.0)

    data[obs] = data.eval(f"{obs} * {weights}")
    data_group = data.groupby(by)
    data = data_group[[obs, weights]].sum().reset_index()
    data[obs] = data.eval(f"{obs} / {weights}")

    data["test"] = data[weights] == 0

    n_obs = data_group.size().rename("n_obs").reset_index()
    data = data.merge(n_obs, on=by, how="left")
    data.loc[data["test"], "n_obs"] = 0

    if others:
        others = data_group[others].first().reset_index()
        data = data.merge(others, on=by, how="left")

    logging.info(f"    after nrows: {len(data)}")
    return data


def correct_covid_age_sex(data: pd.DataFrame) -> pd.DataFrame:
    if "covid_age_sex" in data.columns:
        logging.info("set covid_age_sex value for 2024 to 2023")
        cov = data[IDS + ["covid_age_sex"]].drop_duplicates()
        cov.sort_values(IDS, inplace=True, ignore_index=True)
        cov.loc[cov["year_id"] == 2024, "covid_age_sex"] = cov.loc[
            cov["year_id"] == 2023, "covid_age_sex"
        ].to_numpy()
        data = data.drop(columns=["covid_age_sex"])
        data = data.merge(cov, on=IDS, how="left")
    return data


def correct_paf(data: pd.DataFrame) -> pd.DataFrame:
    if (
        "paf_stand_all" in data.columns
        and data.query("year_id.isin([2023, 2024])")["paf_stand_all"]
        .isna()
        .any()
    ):
        logging.info("set paf_stand_all values for 2023 and 2024")
        data["paf_stand_all"] = data.groupby(
            ["sex_id", "location_id", "age_group_id"]
        )["paf_stand_all"].ffill()
    return data


def modify_sdi(data: pd.DataFrame) -> pd.DataFrame:
    logging.info("modify sdi to remove some crazy swings")

    location_id, year_start, year_end = 349, 1950, 1975
    logging.info(
        f"    clip sdi for location_id={location_id} between {year_start} and {year_end}"
    )
    index = data.eval(f"location_id == {location_id}")
    data.loc[index, "sdi"] = _clip_sdi(data[index].copy(), year_start, year_end)

    location_id, year_start, year_end = 554, 1950, 1965
    logging.info(
        f"    clip sdi for location_id={location_id} between {year_start} and {year_end}"
    )
    index = data.eval(f"location_id == {location_id}")
    data.loc[index, "sdi"] = _clip_sdi(data[index].copy(), year_start, year_end)
    return data


def additional_outlier_decisions(
    data: pd.DataFrame, age_meta: pd.DataFrame
) -> pd.DataFrame:
    logger.info("additional outlier decisions")
    # logger.info("    unoutlier AFG, 1979 census data")
    # age_group_ids = age_meta.query("age_mid < 60.0")["age_group_id"].to_list()
    # index = data.eval(
    #     "location_id == 160 & year_id == 1979 & source_type_name == 'Census' & "
    #     f"age_group_id.isin({age_group_ids}) & "
    #     "outlier == 1"
    # )
    # data.loc[index, "outlier"] = 0

    logger.info("    unoutlier AFG, 2007 HHD for ages 5-84")
    age_group_ids = age_meta.query("age_mid > 5.0 & age_mid < 85.0")[
        "age_group_id"
    ].to_list()
    index = data.eval(
        "location_id == 160 & "
        "year_id == 2007 & "
        "source_type_name == 'HHD' & "
        f"age_group_id.isin({age_group_ids}) & "
        "outlier == 1"
    )
    data.loc[index, "outlier"] = 0

    # logger.info("    unoutlier AFG, SIBS data")
    # index = data.eval(
    #     "location_id == 160 & source_type_name == 'SIBS' & outlier == 1"
    # )
    # data.loc[index, "outlier"] = 0

    return data


# def transform_covid_age_sex(
#     data: pd.DataFrame, obs_scale: float
# ) -> pd.DataFrame:
#     if "covid_age_sex" in data.columns:
#         logging.info("transform covid_age_sex into the logit space")
#         index = data["covid_age_sex"] > 0
#         data.loc[index, "covid_age_sex"] = logit(
#             data.loc[index, "covid_age_sex"] / obs_scale
#         )
#     return data


# def add_use_covid_addon(data: pd.DataFrame, covid_cov: str) -> pd.DataFrame:
#     logging.info(f"add use_covid_addon with {covid_cov}")
#     use_covid_addon = (
#         data[["sex_id", "location_id", "age_group_id", "year_id", "weights"]]
#         .query("year_id.isin([2020, 2021])")
#         .copy()
#     )
#     use_covid_addon["use_covid_addon"] = use_covid_addon.eval("weights == 0")
#     use_covid_addon = (
#         use_covid_addon.groupby(["sex_id", "location_id", "age_group_id"])[
#             "use_covid_addon"
#         ]
#         .any()
#         .reset_index()
#         .groupby(["sex_id", "location_id"])["use_covid_addon"]
#         .all()
#         .reset_index()
#     )

#     data = data.merge(use_covid_addon, on=["sex_id", "location_id"], how="left")
#     data["covid_addon"] = data[covid_cov].where(data["use_covid_addon"], 0.0)
#     data.loc[data["use_covid_addon"], covid_cov] = 0.0
#     return data


def merge_covid_cdr(data: pd.DataFrame) -> pd.DataFrame:
    logging.info("merge covid_cdr to data")
    covid_cdr = (
        pd.read_csv("/path/to/covariates/2024_10_14.02/covid_cdr.csv")
        .drop(columns="age_group_id")
        .rename(columns={"location_id": "source_location_id"})
    )
    data = data.merge(covid_cdr, how="left")
    return data


def _clip_sdi(data: pd.DataFrame, year_start: int, year_end: str) -> NDArray:
    id_cols = ["sex_id", "location_id"]
    data = data.merge(
        data.query(f"year_id == {year_start}")[id_cols + ["sdi"]]
        .drop_duplicates()
        .rename(columns={"sdi": "start_value"}),
        on=id_cols,
        how="left",
    ).merge(
        data.query(f"year_id == {year_end}")[id_cols + ["sdi"]]
        .drop_duplicates()
        .rename(columns={"sdi": "end_value"}),
        on=id_cols,
        how="left",
    )

    data["weights"] = data.eval(
        f"({year_end} - year_id) / ({year_end} - {year_start})"
    )

    return (
        data["sdi"]
        .where(
            data["year_id"] >= year_end,
            data.eval("start_value * weights + end_value * (1 - weights)"),
        )
        .to_numpy()
    )


def scale_covs(
    data: pd.DataFrame, covs: list[str], by: str | list[str] | None = None
) -> pd.DataFrame:
    logging.info(
        f"scale covs={covs} by={by} so that their mins are 0 and max are 1"
    )

    if by:
        if isinstance(by, list) and len(by) == 1:
            by = by[0]
        data_group = data.groupby(by)
        for cov in covs:
            for key, data_sub in data_group:
                index = data_group.groups[key]
                cov_min, cov_max = data_sub[cov].min(), data_sub[cov].max()
                data.loc[index, cov] = (data.loc[index, cov] - cov_min) / (
                    cov_max - cov_min
                )
        return data

    for cov in covs:
        cov_min, cov_max = data[cov].min(), data[cov].max()
        data[cov] = (data[cov] - cov_min) / (cov_max - cov_min)

    return data


def scale_data(
    data: pd.DataFrame, name: str, multiplier: float
) -> pd.DataFrame:
    logging.info(f"scale data={name} by multiplier={multiplier}")
    data[name] = data[name] * multiplier
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
    data = _recover_census_weights(data, scale=scale)

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

    location_ids = [349, 142, 156, 150, 46, 87, 107, 108, 77, 139, 148]
    logging.info(
        f"    overwrite weights in {location_ids} to increase weights in the early age groups"
    )
    index = data.eval(f"location_id.isin({location_ids})")
    data.loc[index, "weights"] = _transform_weights(
        data[index], "weights", ["sex_id"], 1.0
    )

    return data


def _recover_census_weights(data: pd.DataFrame, scale: float) -> pd.DataFrame:
    conditions = [
        "location_id == 176 & year_id.isin([1958])",
        "location_id == 215 & year_id.isin([2012])",
        "location_id == 162 & year_id.isin([2005])",
        "location_id == 164 & year_id.isin([2001, 2011])",
        "location_id == 20 & year_id.isin([1989, 1999])",
        "location_id == 27 & year_id.isin([2016])",
        "location_id == 201 & year_id.isin([2005])",
        "location_id == 193 & year_id.isin([2001])",
        # "location_id == 169 & year_id.isin([1988])",
        "location_id == 202 & year_id.isin([1987])",
        "location_id == 181 & year_id.isin([1993])",
        "location_id == 211 & year_id.isin([2009])",
        "location_id == 182 & year_id.isin([1987])",
        "location_id == 195 & year_id.isin([2001, 2011])",
        "location_id == 197 & year_id.isin([1997, 2007])",
        # "location_id == 198 & year_id.isin([2002, 2022])",
    ]
    query = (
        "(source_type_name == 'Census') & "
        + "("
        + " | ".join([f"({condition})" for condition in conditions])
        + ")"
    )
    index = data.eval(query)
    data.loc[index, "weights"] /= scale
    return data


def _transform_weights(
    data: pd.DataFrame, name: str, by: list[str], scale: float
) -> NDArray:
    """Transform weights so that weights are more concentrated around the
    median. We use a student's T like function.

    f(x) = sign(x) * scale * log(1 + |x| / scale)

    when x is close to 0, f(x) is approximately x. When x is large, f(x) is
    approximately sign(x) * scale * log(|x| / scale). So that larger |x| is, the
    stronger the shrinkage effect is.

    We introduce center and offset to concertrate around give center

    f(x) = sign(x - center) * scale * log(1 + |x - center| / scale) + offset

    and we do the transformation in the logspace.

    """
    weights_median = data.groupby(by)[name].transform("median")
    offset = np.log(weights_median)
    x = np.log(data[name]) - offset
    sign_x = np.sign(x)
    transformed_weights = np.nan_to_num(
        np.exp(sign_x * scale * np.log(1 + np.abs(x) / scale) + offset)
    )
    return transformed_weights


def add_trend_covs(
    data: pd.DataFrame, lwr: float, upr: float, scale: float, by: list[str]
) -> pd.DataFrame:
    logging.info("add covariates: ['trend', 'trend_single', 'trend_double']")
    slope = 1.0 / (data["year_id"].max() - data["year_id"].min())
    data_train = data.query("~test").copy()

    year_span_lwr = (
        data_train.groupby(by)["year_id"].quantile(lwr).rename("year_start")
    )
    year_span_upr = (
        data_train.groupby(by)["year_id"].quantile(upr).rename("year_end")
    )
    year_span = pd.concat([year_span_lwr, year_span_upr], axis=1).reset_index()
    data = data.merge(year_span, on=by, how="left")

    # compute trend covariate
    data["trend"] = data.eval("year_id - year_id.min()")
    # compute trend single covariate
    data["trend_single"] = data.eval(
        "log(1 + exp(@scale * (year_id - year_start))) / @scale"
    )
    # compute trend double covariate
    data["trend_double"] = data.eval(
        "(log(1 + exp(@scale * (year_id - year_start))) -"
        " log(1 + exp(@scale * (year_id - year_end)))) / @scale"
    )
    for col in ["trend", "trend_single", "trend_double"]:
        data[col] *= slope
        data[col] = data[col].fillna(0.0)

    data = data.drop(columns=["year_start", "year_end"])
    return data


def add_meta_info(
    data: pd.DataFrame, age_meta: pd.DataFrame, loc_meta: pd.DataFrame
) -> pd.DataFrame:
    logging.info(
        "add meta info: super_region_id, region_id, national_id, age_mid"
    )
    data = data.merge(
        age_meta[["age_group_id", "age_mid"]], on="age_group_id", how="left"
    ).merge(
        loc_meta[
            ["location_id", "super_region_id", "region_id", "national_id"]
        ].rename(columns={"location_id": "source_location_id"}),
        on="source_location_id",
        how="left",
    )
    return data


def add_super_region_id_trend(
    data: pd.DataFrame, super_region_ids: list[int]
) -> pd.DataFrame:
    logging.info(f"add trend variable for super_region_id {super_region_ids}")
    for super_region_id in super_region_ids:
        data[f"super_region_id_{super_region_id}_trend"] = data.eval(
            f"(super_region_id == {super_region_id}) * trend"
        )
    return data


def add_high_coverage(
    data: pd.DataFrame, threshold: float = 0.8
) -> pd.DataFrame:
    logging.info(f"add high_coverage with threshold={threshold}")

    year_min, year_max = data["year_id"].min(), data["year_id"].max()
    data_train = data.query(
        "obs.notna() & location_id == source_location_id"
    ).copy()
    id_cols = ["sex_id", "location_id", "age_group_id", "year_id"]
    info = (
        data_train[id_cols]
        .drop_duplicates()
        .groupby(id_cols[:-1])
        .size()
        .rename("n")
        .reset_index()
        .merge(
            data[id_cols[:-1]].drop_duplicates(), on=id_cols[:-1], how="outer"
        )
        .fillna(0.0)
    )
    info["coverage"] = info["n"] / (year_max - year_min + 1)
    avg_coverage = info.groupby(id_cols[:-2])["coverage"].mean().reset_index()

    data = data.merge(
        avg_coverage.query(f"coverage >= {threshold}")[id_cols[:-2]].assign(
            high_coverage=True
        ),
        on=id_cols[:-2],
        how="left",
    )
    data["high_coverage"] = data["high_coverage"].notna()

    return data


def create_detailed(data: pd.DataFrame, loc_meta: pd.DataFrame) -> pd.DataFrame:
    logging.info("create detailed config")
    default_config = dict(
        lam_intercept=50.0,
        lam_trend=float("nan"),
        gprior_sd_intercept=1.0,
        gprior_sd_trend=1.0,
        age_scale=0.1,
        gamma_age=8.0,
        gamma_year=12.0,
        nugget=1e-6,
        lam=20.0,
        lam_ridge=0.0,
        lam_uncertainty=5.0,
        anchor=30.0,
        kernel_year_short=0.1,
        kernel_year_const=5.0,
        kernel_year_linear=0.2,
        # kernel_year_linear=0.0,
        modify_recent_years=False,
        use_spxmod_1950=False,
        shrink_under5_year=False,
    )

    detailed = loc_meta[
        ["super_region_id", "region_id", "national_id", "location_id"]
    ].copy()
    for key, value in default_config.items():
        detailed[key] = value

    logging.info("    super region 64 (high income): reduce lam")
    index = detailed.eval("super_region_id == 64")
    detailed.loc[index, "lam"] = 5.0

    logging.info(
        "    super region 166 (africa): decrease lam_intercept and increase lam"
    )
    index = detailed.eval("super_region_id == 166")
    detailed.loc[index, "lam_intercept"] = 1.0
    detailed.loc[index, "lam"] = 50.0

    # Macao Special Administrative Region of China
    logging.info("    location 361 (Macao): decrease lam")
    index = detailed.eval("location_id == 361")
    detailed.loc[index, "lam"] = 5.0

    # Bolivia follow VR data less
    if "vr_reliability_adj" in data.columns:
        location_ids = (
            data.query("vr_reliability_adj == 1")["location_id"]
            .unique()
            .tolist()
        )
        logging.info(f"    locations {location_ids}: increase gamma_year")
        index = detailed.eval(f"location_id.isin({location_ids})")
        detailed.loc[index, "gamma_year"] = 100.0

    # Greenland tune down the overfitting behavior
    logging.info("    location 349 (Greenland): reset gamma_year")
    index = detailed.eval("location_id == 349")
    detailed.loc[index, "gamma_year"] = 50.0

    location_ids = [156, 150]
    logging.info(
        f"    location {location_ids}: decrease lam further to avoid bad extrapolation behavior"
    )
    index = detailed.eval(f"location_id.isin({location_ids})")
    detailed.loc[index, "lam"] = 50.0
    detailed.loc[index, "gamma_year"] = 20.0

    location_ids = [136, 156, 97, 109, 146, 44533, 163]
    logging.info(f"    locations {location_ids} modify recent years")
    index = detailed.eval(f"location_id.isin({location_ids})")
    detailed.loc[index, "modify_recent_years"] = True

    logging.info("    location 114 (Haiti): increase lam more smoothing")
    index = detailed.eval("location_id == 114")
    detailed.loc[index, "lam"] = 50.0

    region_ids = [138]
    logging.info(f"    region_id {region_ids}: trunoff kreg linear trend")
    index = detailed.eval(f"region_id.isin({region_ids})")
    detailed.loc[index, "kernel_year_linear"] = 0.0

    location_id = [198]
    logging.info(f"    location_id {location_id}: turnoff kreg linear trend")
    index = detailed.eval(f"location_id.isin({location_id})")
    detailed.loc[index, "kernel_year_linear"] = 0.0

    location_ids = [77]
    logging.info(
        f"    location_id {location_ids}: turnoff kreg linear trend and add ridge regularization"
    )
    index = detailed.eval(f"location_id.isin({location_ids})")
    detailed.loc[index, "kernel_year_linear"] = 0.0
    detailed.loc[index, "lam_ridge"] = 2.0
    detailed.loc[index, "gamma_year"] = 7.0

    location_ids = [148, 152]
    logging.info(
        f"    location_id {location_ids}: tighten intercepts and localize data influence"
    )
    index = detailed.eval(f"location_id.isin({location_ids})")
    detailed.loc[index, "lam_intercept"] = 500.0
    detailed.loc[index, "gamma_year"] = 20.0
    detailed.loc[index, "lam_ridge"] = 5.0

    location_ids = [35631]
    logging.info(
        f"    location_id {location_ids}: pull location intercept to 0"
    )
    index = detailed.eval(f"location_id.isin({location_ids})")
    detailed.loc[index, "gprior_sd_intercept"] = 0.1

    location_ids = "168, 169, 170, 171, 172, 173, 175, 176, 177, 178, 181, 182, 184, 185, 187, 435, 190, 189, 191, 193, 197, 194, 195, 198, 200, 201, 202, 204, 205, 206, 207, 208, 209, 211, 215, 216, 217, 218, 179, 44861, 44853, 44854, 44857, 44862, 44860, 44859, 44855, 60908, 44856, 94364, 95069, 44852, 214, 25318, 25319, 25320, 25321, 25322, 25323, 25324, 25325, 25326, 25327, 25328, 25329, 25330, 25331, 25332, 25333, 25334, 25335, 25336, 25337, 25338, 25339, 25340, 25342, 25343, 25344, 25345, 25346, 25349, 25350, 25351, 25352, 25353, 25354"
    location_ids = list(map(int, location_ids.split(", "))) + [20]
    logging.info(f"    location_id {location_ids}: tighten intercepts")
    index = detailed.eval(f"location_id.isin({location_ids})")
    detailed.loc[index, "lam_intercept"] = 500.0

    logging.info("    don't transform year for early age groups in SSA")
    index = detailed.eval("super_region_id == 166")
    detailed.loc[index, "shrink_under5_year"] = True

    return detailed


if __name__ == "__main__":
    fire.Fire(main)

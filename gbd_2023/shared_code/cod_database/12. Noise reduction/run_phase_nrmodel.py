import getpass
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

from cod_prep.claude.claude_io import get_claude_data, makedirs_safely
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.redistribution_variance import modelgroup_has_redistribution_variance
from cod_prep.claude.squaring import Squarer
from cod_prep.downloaders import (
    add_location_metadata,
    add_nid_metadata,
    add_survey_type,
    get_ages,
    get_all_related_causes,
    get_current_cause_hierarchy,
    get_current_location_hierarchy,
    get_datasets,
)
from cod_prep.utils import (
    dask_client_scope,
    just_keep_trying,
    log_statistic,
    report_duplicates,
    report_if_merge_fail,
    submit_cod,
    wait,
    write_df,
)
from cod_prep.utils.data_transforms import filter_df, get_id_to_ancestor
from cod_prep.utils.nr_helpers import (
    get_fallback_model_group,
    is_country_vr_non_subnat,
    is_region_vr,
)

pd.options.mode.chained_assignment = None

CONF = Configurator("standard")

class NoiseReductionDependencies:

    def __init__(
        self,
        loc_meta_df: pd.DataFrame,
        cause_meta_df: pd.DataFrame,
        force_rerun: bool = False,
        block_rerun: bool = True,
    ):
        self.loc_meta_df = loc_meta_df
        self.loc_map = (
            self.loc_meta_df.pipe(get_id_to_ancestor, "location_id")
            .rename(columns={"ancestor": "location_id", "location_id": "descendant"})
            .merge(self.loc_meta_df.loc[:, ["location_id", "ihme_loc_id"]])
            .astype({"location_id": str})
            .melt(id_vars="descendant", value_name="key")
            .groupby("key")["descendant"]
            .agg(list)
            .to_dict()
        )
        self.regions = self.loc_meta_df.query("level == 2")["ihme_loc_id"].pipe(set)
        self.country_ids = self.loc_meta_df.query("level == 3")["location_id"].pipe(set)
        self.yll_causes = cause_meta_df.query("yld_only != 1")["cause_id"].pipe(set)

        self.ds = (
            get_datasets(
                is_active=True,
                is_dropped=False,
                filter_locations=True,
                year_id=list(range(1980, CONF.get_id("year_end"))),
                force_rerun=force_rerun,
                block_rerun=block_rerun,
            )
            .merge(
                self.loc_meta_df.pipe(get_id_to_ancestor, "location_id").loc[
                    lambda d: d["ancestor"].isin(
                        self.loc_meta_df.query("level >= 3")["location_id"]
                    ),
                    :,
                ]
            )
            .drop(columns="location_id")
            .rename(columns={"ancestor": "location_id"})
            .merge(
                pd.DataFrame(
                    [
                        [43872, 43872],
                        [43872, 43902],
                        [43908, 43908],
                        [43908, 43938],
                    ],
                    columns=["location_id", "new_location_id"],
                ),
                how="left",
            )
            .assign(
                location_id=lambda d: d["new_location_id"].fillna(
                    d["location_id"], downcast="infer"
                )
            )
            .fillna({"model_group": "NO_NR", "malaria_model_group": "NO_NR"})
            .pipe(
                lambda d: d.merge(
                    d.loc[:, ["model_group", "iso3"]]
                    .drop_duplicates()
                    .assign(
                        fallback_model_group=lambda d_: [
                            get_fallback_model_group(*row) for row in d_.itertuples(index=False)
                        ]
                    )
                )
            )
            .assign(
                malaria_model_group=lambda d: d["malaria_model_group"]
                .mask(
                    (d["location_id"] == 163)
                    & (
                        d.assign(
                            has_meso=d["malaria_model_group"].str.contains("mesoendem", regex=False)
                        )
                        .groupby(["nid", "extract_type_id"])["has_meso"]
                        .transform("any")
                    ),
                    d["malaria_model_group"].str.replace(
                        r"(hypo|hyper)endem", "mesoendem", regex=True
                    ),
                )
                .mask(
                    d["location_id"].isin([43902, 43938]),
                    d["malaria_model_group"].str.replace(
                        r"(meso|hyper)endem", "hypoendem", regex=True
                    ),
                )
                .mask(
                    (d["location_id"] == 179)
                    & (
                        d.assign(
                            has_hypo=d["malaria_model_group"].str.contains("hypoendem", regex=False)
                        )
                        .groupby(["nid", "extract_type_id"])["has_hypo"]
                        .transform("any")
                    ),
                    d["malaria_model_group"].str.replace(
                        r"(meso|hyper)endem", "hypoendem", regex=True
                    ),
                )
                .mask(
                    (d["location_id"] == 214) 
                    & (
                        d.assign(
                            has_meso=d["malaria_model_group"].str.contains("mesoendem", regex=False)
                        )
                        .groupby(["nid", "extract_type_id"])["has_meso"]
                        .transform("any")
                    ),
                    d["malaria_model_group"].str.replace(
                        r"(hypo|hyper)endem", "mesoendem", regex=True
                    ),
                )
            )
            .loc[
                :,
                [
                    "nid",
                    "extract_type_id",
                    "source",
                    "data_type_id",
                    "location_id",
                    "year_id",
                    "survey_type",
                    "model_group",
                    "malaria_model_group",
                    "fallback_model_group",
                ],
            ]
            .drop_duplicates()
        )
        self.sources = self.ds["source"].drop_duplicates()

    def get_input_data_filters(self, model_group: str) -> dict[str, Any]:
        ind_srs_sources = ["India_SRS_states_report", "India_SRS_Maternal_states"]

        filters = {"cause_id": list(self.yll_causes)}
        if model_group.startswith("VR-"):
            filters["data_type_id"] = [9, 10]
            loc_code = model_group.replace("VR-", "")
            try:
                if loc_code == "GRL-AK":
                    filters["location_id"] = [524, 349]
                elif loc_code in self.regions:
                    filters["location_id"] = list(set(self.loc_map[loc_code]) & self.country_ids)
                else:
                    filters["location_id"] = self.loc_map[loc_code]

                if loc_code == "BOL":
                    filters["cause_id"] = [346]
            except KeyError:
                raise ValueError("ERROR")
        elif model_group.startswith("VA-"):
            filters["data_type_id"] = [8, 12]
            filters["_function"] = (
                lambda df: df.merge(
                    self.ds.loc[
                        self.ds["source"] == "CHAMPS", ["nid", "extract_type_id"]
                    ].drop_duplicates(),
                    how="left",
                    indicator="_is_champs",
                )
                .eval("_is_champs = _is_champs == 'both'")
                .compute()
                .loc[
                    lambda d: ~d.groupby(["sex_id", "cause_id"])["_is_champs"].transform("all"),
                    :,
                ]
                .drop(columns="_is_champs")
            )
            loc_code = model_group.replace("VA-", "")
            if loc_code == "SRS-IND":
                filters["source"] = ind_srs_sources
            elif loc_code == "SRS-IDN":
                filters["source"] = ["Indonesia_SRS_2014", "Indonesia_SRS_province"]
            elif loc_code == "Matlab":
                filters["source"] = [
                    "Matlab_1963_1981",
                    "Matlab_1982_1986",
                    "Matlab_1987_2002",
                    "Matlab_2003_2006",
                    "Matlab_2007_2012",
                    "Matlab_2011_2014",
                ]
            elif loc_code == "Nepal-Burden":
                filters["location_id"] = self.loc_map["NPL"]
            elif loc_code == "158":
                filters["location_id"] = list(
                    set(self.loc_map[loc_code]) - set(self.loc_map["IND"])
                )
                filters["source"] = self.sources[
                    ~self.sources.str.match(r"Nepal_Burden_VA|Matlab")
                ].tolist()
            else:
                try:
                    filters["location_id"] = self.loc_map[loc_code]
                    if loc_code == "IND":
                        filters["source"] = self.sources[
                            ~self.sources.str.match(r"India_(SRS|SCD)")
                        ].tolist()
                except KeyError:
                    raise ValueError("ERROR")
        elif model_group.startswith("MATERNAL"):
            if match := re.match(r"MATERNAL-(.+)-([A-Z]{3})$", model_group):
                source, iso3 = match.groups()
                filters["location_id"] = self.loc_map[iso3]
                if source == "HH_SURVEYS":
                    filters["survey_type"] = (
                        self.ds["survey_type"].replace("", pd.NA).dropna().unique()
                    )
                else:
                    filters["source"] = [source]
                filters["_function"] = lambda df: (
                    df.merge(
                        self.loc_meta_df.query("most_detailed == 0 and level == 3").loc[
                            :, ["location_id"]
                        ],
                        how="left",
                        indicator="agg_country",
                    )
                    .merge(
                        self.ds.query(
                            "source in ['Other_Maternal', 'Mexico_BIRMM', 'DHS_maternal']"
                        )
                        .loc[:, ["nid", "extract_type_id"]]
                        .drop_duplicates(),
                        how="left",
                        indicator="agg_source",
                    )
                    .query("agg_source == 'both' or agg_country != 'both'")
                    .drop(columns=["agg_source", "agg_country"])
                )
            else:
                raise ValueError("ERROR")
        elif model_group.startswith("malaria"):
            filters["data_type_id"] = [8, 12]
            filters["cause_id"] = [345] 
            filters["malaria_model_group"] = model_group
            if "IND_SRS" in model_group:
                filters["source"] = ind_srs_sources
            filters["_function"] = lambda df: df.merge(
                self.ds.loc[
                    self.ds["malaria_model_group"] == model_group,
                    ["nid", "extract_type_id", "location_id", "year_id"],
                ].drop_duplicates()
            )
        elif model_group == "Cancer_Registry":
            filters["source"] = ["Cancer_Registry"]
        elif model_group == "CHAMPS":
            filters["data_type_id"] = [12]
        elif model_group == "DHIS2":
            filters["data_type_id"] = [14]
        elif model_group == "POLICE_VIOLENCE-SURVEILLANCE-USA-FE":
            filters["nid"] = 448801
        elif model_group == "POLICE_VIOLENCE-SURVEILLANCE-USA-MPV":
            filters["nid"] = [448802, 448794]
        else:
            raise ValueError(f"Unrecognized model group: {model_group}")
        return filters

    def get_input_nid_extracts_from_model_group(self, model_group: str) -> pd.DataFrame:
        return (
            self.ds.pipe(filter_df, self.get_input_data_filters(model_group))
            .loc[:, ["nid", "extract_type_id"]]
            .drop_duplicates()
        )

    def get_nid_extract_to_model_group(self) -> pd.DataFrame:
        return pd.concat(
            self.get_input_nid_extracts_from_model_group(mg).assign(model_group=mg)
            for mg in (
                self.get_model_group_to_nid_extract()
                .loc[:, "model_group"]
                .drop_duplicates()
            )
        )

    def get_model_group_to_nid_extract(self) -> pd.DataFrame:
        return (
            self.ds.melt(
                id_vars=["nid", "extract_type_id"],
                value_vars=[
                    "model_group",
                    "fallback_model_group",
                    "malaria_model_group",
                ],
                var_name="type",
                value_name="tmp",  
            )
            .rename(columns={"tmp": "model_group"})
            .query('model_group != "NO_NR"')
            .loc[:, ["model_group", "nid", "extract_type_id"]]
            .drop_duplicates()
        )


def model_group_is_run_by_cause(model_group):
    return (
        (model_group.startswith("VR"))
        or (model_group.startswith("MATERNAL"))
        or (model_group.startswith("Cancer"))
        or (model_group == "VA-G")
    )


def get_model_data_path(
    is_input_file: bool,
    model_group: str,
    launch_set_id: int,
    cause_id: Optional[int] = None,
) -> Path:
    return "FILEPATH"


def get_model_data(model_group, project_id, launch_set_id, location_hierarchy, cause_meta_df):
    nr_dependencies = NoiseReductionDependencies(location_hierarchy, cause_meta_df)
    with dask_client_scope():
        model_df = get_claude_data(
            phase="aggregation",
            project_id=project_id,
            nid_extract_records=(
                nr_dependencies.get_input_nid_extracts_from_model_group(model_group).to_records(
                    index=False
                )
            ),
            as_launch_set_id=launch_set_id,
            assert_all_available=True,
            filter_locations=True,
            year_id=list(range(1980, CONF.get_id("year_end"))),
            is_active=True,
            is_dropped=False,
            lazy=True,
        )
        filters = nr_dependencies.get_input_data_filters(model_group)
        model_df = filter_df(model_df, filters)
        if func := filters.get("_function"):
            model_df = func(model_df)
        if isinstance(model_df, dd.DataFrame):
            model_df = model_df.compute()
    add_cols = ["code_system_id"]
    if model_group.startswith(("VA", "MATERNAL", "malaria", "CHAMPS")) or model_group in [
        "VR-RUS", "VR-R9",
        "VR-R15", "VR-IRN", "VR-IRQ", "VR-ARE",
        "VR-R20", "VR-ZWE"
    ]:
        add_cols.append("source")

    if model_group.startswith("MATERNAL-HH_SURVEYS"):
        model_df = add_survey_type(model_df)

    model_df = add_nid_metadata(
        model_df,
        add_cols,
        project_id=project_id,
        force_rerun=False,
        block_rerun=True,
        cache_dir="standard",
        cache_results=False,
    )
    if model_group == "VR-RUS" or model_group == "VR-R9":
        replace_source = "Russia_FMD_ICD9"
        replace_csid = 213
        fmd_conv_10 = model_df["source"] == replace_source
        num_replace = len(model_df[fmd_conv_10])
        assert (
            num_replace > 0
        ), 
        model_df.loc[fmd_conv_10, "code_system_id"] = replace_csid

    if model_group in ["VR-IRN", "VR-IRQ", "VR-ARE", "VR-R15"]:
        iran_sources = ["Iran_Mohsen_special_ICD10", "Iran_2011"]
        iraq_sources = ["Iraq_collab_ICD10"]
        uae_sources = ["UAE_2006_2007", "UAE_Abu_Dhabi"]

        model_df.loc[model_df.source.isin(iran_sources), "code_system_id"] = 9999
        model_df.loc[model_df.source.isin(iraq_sources), "code_system_id"] = 8888
        model_df.loc[model_df.source.isin(uae_sources), "code_system_id"] = 7777

    if model_group == 'VR-RUS':
        model_df.loc[model_df.source == 'Russia_FMD_ICD9', 'code_system_id'] = 9999

    if model_group in ["VR-ZWE", "VR-R20"]:
        model_df.loc[model_df.source.isin(["Zimbabwe_2007", "Zimbabwe_95"]), "code_system_id"] = 9999

    report_if_merge_fail(model_df, "code_system_id", ["nid", "extract_type_id"])

    return model_df


def drop_problematic_regional_data(df, model_group, location_hierarchy, cause_meta_df):
    if model_group == "VR-R15":
        iran_locs = location_hierarchy.query("iso3 == 'IRN'").location_id.unique().tolist()

        glomerulo = get_all_related_causes("glomerulo", cause_meta_df=cause_meta_df)
        pud = get_all_related_causes("digest_pud", cause_meta_df=cause_meta_df)
        gyne = get_all_related_causes("gyne", cause_meta_df=cause_meta_df)

        df = df.loc[
            ~(
                (df.location_id.isin(iran_locs))
                & (df.cause_id.isin(glomerulo + pud + gyne))
                & (df.year_id.isin([2013, 2014, 2015, 2016]))
            )
        ]

        sau_loc = int(location_hierarchy.query("ihme_loc_id == 'SAU'").location_id.iloc[0])
        rhd = get_all_related_causes("cvd_rhd", cause_meta_df=cause_meta_df)

        df = df.loc[
            ~(
                (df.location_id == sau_loc) & (df.year_id == 2022) &
                (df.cause_id.isin(rhd))
            )
        ]


    if model_group == "VR-R3":
        df = add_nid_metadata(
            df,
            "data_type_id",
            project_id=CONF.get_id("project"),
            force_rerun=False,
            block_rerun=True,
        )

        china_locs = location_hierarchy.query("iso3 == 'CHN'").location_id.unique().tolist()

        other_cvd = get_all_related_causes("cvd_other", cause_meta_df=cause_meta_df)

        df = df.loc[
            ~(
                (df.data_type_id == 10)
                & (df.location_id.isin(china_locs))
                & (df.cause_id.isin(other_cvd))
                & (df.year_id.isin(list(range(1990, 2004))))
            )
        ]
        df = df.drop("data_type_id", axis=1)

    if model_group == "VR-R14":
        brazil_locs = location_hierarchy.query("iso3 == 'BRA'").location_id.unique().tolist()

        uti = get_all_related_causes("urinary_nephritis", cause_meta_df=cause_meta_df)

        df = df.loc[
            ~(
                (df.location_id.isin(brazil_locs))
                & (df.cause_id.isin(uti))
                & (df.year_id.isin(list(range(2004, 2018))))
            )
        ]

    return df


def square_dhs_data(model_df, cause_meta_df, age_meta_df, location_hierarchy):
    dhs = model_df["survey_type"] == "DHS"
    non_dhs = model_df[~dhs]
    dhs = model_df[dhs]

    if len(dhs) > 0:
        nid_loc_df = model_df[
            ["nid", "location_id", "site_id", "extract_type_id"]
        ].drop_duplicates()
        squarer = Squarer(
            cause_meta_df, age_meta_df, location_meta_df=location_hierarchy, data_type="DHS"
        )
        dhs = squarer.get_computed_dataframe(dhs)

        dhs["code_system_id"].fillna(177, inplace=True)
        dhs["source"].fillna("DHS_maternal", inplace=True)
        dhs["survey_type"].fillna("DHS", inplace=True)

        dhs = nid_loc_df.merge(dhs, on=["location_id", "site_id"], how="right")

        tls = dhs.query("location_id == 19")
        dhs = dhs.query("location_id != 19")
        tls = tls.drop_duplicates(
            subset=[
                "location_id",
                "year_id",
                "cause_id",
                "age_group_id",
                "sex_id",
                "nid_y",
                "extract_type_id_y",
            ],
            keep="first",
        )
        dhs = pd.concat([tls, dhs], ignore_index=True)

        dhs.loc[dhs["nid_y"].isnull(), "nid_y"] = dhs["nid_x"]
        dhs.loc[dhs["extract_type_id_y"].isnull(), "extract_type_id_y"] = dhs["extract_type_id_x"]

        dhs.drop(["extract_type_id_x", "nid_x", "iso3"], axis=1, inplace=True)
        dhs.rename(columns={"nid_y": "nid", "extract_type_id_y": "extract_type_id"}, inplace=True)

        dhs.loc[dhs["sample_size"] == 0, "sample_size"] = 0.5

        model_df = pd.concat([dhs, non_dhs], ignore_index=True)

    assert model_df.notnull().values.any()
    report_duplicates(
        model_df,
        [
            "year_id",
            "sex_id",
            "location_id",
            "cause_id",
            "age_group_id",
            "nid",
            "extract_type_id",
            "site_id",
        ],
    )

    return model_df


def get_code_system_cause_ids(df):
    df = df.copy()
    df = df[["cause_id", "code_system_id"]].drop_duplicates()
    cs_cause_dict = (
        df.groupby("code_system_id").apply(lambda x: x["cause_id"].values.tolist()).to_dict()
    )
    return cs_cause_dict


def restrict_to_cause_ids(code_system_cause_dict, df):
    df = df.copy()
    df_list = []
    for code_system_id, cause_ids in code_system_cause_dict.items():
        cs_df = df.loc[
            (
                (df["code_system_id"] == code_system_id)
                & (df["cause_id"].isin(code_system_cause_dict[code_system_id]))
            )
        ]
        df_list.append(cs_df)
    df = pd.concat(df_list)
    df = df.loc[~((df.year_id < 2020) & (df.cause_id == 1048))]

    return df


def format_for_nr(df, location_hierarchy):
    locs = df[["location_id"]].drop_duplicates()
    locs = add_location_metadata(
        locs,
        add_cols=["ihme_loc_id", "path_to_top_parent"],
        merge_col="location_id",
        location_meta_df=location_hierarchy,
    )
    report_if_merge_fail(locs, "path_to_top_parent", "location_id")
    locs["country_id"] = locs["path_to_top_parent"].str.split(",").apply(lambda x: int(x[3]))
    locs["subnat_id"] = locs["ihme_loc_id"].apply(lambda x: int(x.split("_")[1]) if "_" in x else 0)
    locs["iso3"] = locs["ihme_loc_id"].str.slice(0, 3)
    different_locations = locs["country_id"] != locs["location_id"]

    locs.loc[different_locations, "iso3"] = locs["iso3"].apply(lambda x: x + "_subnat")
    locs = locs[["location_id", "country_id", "subnat_id", "iso3"]]
    df = df.merge(locs, on="location_id", how="left")
    report_if_merge_fail(locs, "country_id", "location_id")

    df["is_loc_agg"] = (
        df.eval("country_id == location_id")
        & (df["iso3"] != "GRL")
    ).astype(int)

    df = df.loc[df["sample_size"] > 0]

    return df


def create_year_bins(df):
    year_to_num_locs = (
        df.astype({"year_id": int})
        .groupby("year_id")["location_id"]
        .apply(pd.Series.nunique)
        .sort_index(ascending=False)
        .to_dict()
    )
    year_to_bin = {}
    for year, num_locs in year_to_num_locs.items():
        if year not in year_to_bin:
            if num_locs == 1:
                if year != 1980:
                    year_bin = f"{year - 1}_{year}"
                    pair_year = year - 1
                else:
                    bin_1981 = [x for x in year_to_bin.values() if "1981" in x]
                    if len(bin_1981) > 0:
                        year_bin = bin_1981[0]
                    else:
                        year_bin = "1980_1981"
                    pair_year = 1981
                year_to_bin[year] = year_bin
                year_to_bin[pair_year] = year_bin
            else:
                year_bin = str(year)
                year_to_bin[year] = year_bin

    df["year_bin"] = df["year_id"].map(year_to_bin)
    report_if_merge_fail(df, "year_bin", "year_id")
    return df


def create_dsp_year_bins(df):
    bin_map = {}
    for year in range(1980, 2025):
        if year < 2015:
            bin_map.update({year: str(year)})
        else:
            bin_map.update({year: "2015_onwards"})
    df["year_bin"] = df.year_id.map(bin_map)
    report_if_merge_fail(df, "year_bin", "year_id")
    return df

def apply_china_ms_model_adjustment(df, cause_meta_df):
    start = df.shape
    ms = get_all_related_causes('neuro_ms', cause_meta_df=cause_meta_df)
    msdf = df.loc[df.cause_id.isin(ms)]
    df = df.loc[~df.cause_id.isin(ms)]

    inc_cols = msdf.columns.tolist()
    msdf = add_nid_metadata(msdf, 'source')
    draw_cols = [x for x in msdf.columns if 'draw_' in x]
    cf_cols = ['cf']

    base = msdf.loc[
        (msdf.year_id >= 1991) & (msdf.year_id <= 2002) & (msdf.source == 'China_1991_2002')
    ]
    base['year_id'] = base.year_id.astype(int)
    base = base[
        ['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'cause_id', 'source'] + cf_cols + draw_cols
    ].sort_values(by='cf', ascending=False).drop_duplicates(
        subset=['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'cause_id'], keep='first'
    )
    report_duplicates(base, ['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'cause_id'])
    base = base.drop('source', axis=1)

    year_map = {x: x+12 for x in range(1991, 2003)}
    year_map2 = {x: x+24 for x in range(1991, 1998)}

    base2 = base.query("year_id <= 1997")
    base['year_id'] = base.year_id.map(year_map)
    base2['year_id'] = base2.year_id.map(year_map2)
    base = pd.concat([base, base2])

    report_duplicates(base, ['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'cause_id'])
    msdf = msdf.merge(
        base, on=['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'cause_id'],
        how='left', suffixes=('_orig', '_repl')
    )

    for col in cf_cols + draw_cols:
        msdf[col + "_repl"] = msdf[col + "_repl"].fillna(msdf[col + "_orig"])
        msdf.loc[msdf[col + "_orig"] < msdf[col + "_repl"], col+"_orig"] = msdf[col + "_repl"]
        msdf = msdf.rename(columns={col+"_orig": col})

    msdf = msdf[inc_cols]
    df = pd.concat([df, msdf])
    assert df.notnull().values.all()
    assert df.shape == start
    return df


def create_fallback_flag(df, cause_meta_df):

    transmission_sensitive_causes = pd.read_csv(CONF.get_resource("transmission_sensitive_causes"))
    transmission_sensitive_causes = set(
        transmission_sensitive_causes.acause.apply(
            get_all_related_causes, args=(cause_meta_df,)
        ).explode()
    )
    df["fallback_flag"] = 0
    df.loc[df.cause_id.isin(transmission_sensitive_causes), "fallback_flag"] = 1
    return df


def calculate_mean_deaths_per_year(df, model_group):
    df = add_nid_metadata(
        df,
        ["data_type_id"],
        project_id=CONF.get_id("project"),
        force_rerun=False,
        block_rerun=True,
        cache_results=False,
    )
    mean_death_cols = ["location_id", "age_group_id", "sex_id", "cause_id", "data_type_id"]
    if model_group == "VR-CHN":
        mean_death_cols += ["code_system_id"]
    assert df.data_type_id.notnull().all()
    df["mean_deaths_per_year"] = (
        df.assign(deaths=df["cf"] * df["sample_size"])
        .groupby(mean_death_cols)
        .deaths.transform(np.mean)
    )
    assert df.mean_deaths_per_year.notnull().all()
    df = df.drop("data_type_id", axis="columns")
    return df


def write_nrmodel_data(df, model_group, launch_set_id, cause_id=None):
    out_path = get_model_data_path(
        is_input_file=True,
        model_group=model_group,
        launch_set_id=launch_set_id,
        cause_id=cause_id,
    )
    makedirs_safely(out_path.parent)
    if launch_set_id == 0:
        out_path.unlink(missing_ok=True)
        get_model_data_path(
            is_input_file=False,
            model_group=model_group,
            launch_set_id=launch_set_id,
            cause_id=cause_id,
        ).unlink(missing_ok=True)
    write_df(df, out_path)


def determine_worker(model_group: str) -> str:
    claude_dir = CONF.get_directory("claude_code")
    if model_group.startswith(("VA", "malaria", "CHAMPS")):
        worker = "FILEPATH"
    else:
        worker = "FILEPATH"
    return worker


def run_phase_by_model_group(model_df, model_group, launch_set_id, partition="all.q"):
    write_nrmodel_data(model_df, model_group, launch_set_id)
    if pd.Series(["draw_" in x for x in model_df.columns]).any():
        num_draws = CONF.get_resource("uncertainty_draws")
    else:
        num_draws = 0
    params = [
        model_group,
        str(launch_set_id),
        CONF.get_directory("nr_process_data"),
        str(num_draws),
    ]

    worker = determine_worker(model_group)
    submit_cod(
        worker,
        job_name=f"claude_nrmodelworker_{model_group}",
        memory=40 if model_group == "VA-G" else 2,
        params=params,
        logging=True,
        log_base_dir="FILEPATH",
        partition=partition,
        needs_j_drive=False,
    )
    wait("claude_nrmodelworker_{model_group}".format(model_group=model_group), 30)

def run_phase_by_cause(model_df, cause_meta_df, model_group, launch_set_id, partition):
    nocause = model_df[model_df["cause_id"].isnull()]
    if len(nocause) > 0:
        raise AssertionError("ERROR")
    model_df = model_df.astype({"cause_id": int})

    if is_country_vr_non_subnat(model_group) and model_group not in ["VR-GRL-AK", "VR-BOL"]:
        causes = pd.read_csv(CONF.get_resource("transmission_sensitive_causes"))
        causes = set(causes.acause.apply(get_all_related_causes, args=(cause_meta_df,)).explode())
        causes = list(causes.intersection(set(model_df.cause_id)))
    else:
        causes = list(set(model_df["cause_id"]))
    causes = [int(cause) for cause in causes]

    log_base_dir = "FILEPATH"
    worker = determine_worker(model_group)
    multiple_locations = model_df["location_id"].nunique() > 1

    num_draws = CONF.get_resource("uncertainty_draws")
    if not modelgroup_has_redistribution_variance(model_group):
        num_draws = 0

    for cause_id, cause_model_df in model_df.groupby("cause_id"):
        write_nrmodel_data(cause_model_df, model_group, launch_set_id, cause_id=cause_id)
        params = [
            model_group,
            str(launch_set_id),
            CONF.get_directory("nr_process_data"),
            str(num_draws),
            str(cause_id),
        ]
        time.sleep(5)
        submit_cod(
            worker,
            job_name=f"claude_nrmodelworker_{model_group}_{cause_id}",
            memory=10 if multiple_locations else 4,
            cores=1,
            params=params,
            logging=True,
            log_base_dir=log_base_dir,
            partition=partition,
            needs_j_drive=False,
        )

    wait(f"claude_nrmodelworker_{model_group}", 30)

    for cause_id in causes:
        out_path = get_model_data_path(
            is_input_file=False,
            model_group=model_group,
            launch_set_id=launch_set_id,
            cause_id=cause_id,
        )
        just_keep_trying(
            os.path.exists,
            args=[out_path],
            max_tries=250,
            seconds_between_tries=6,
            verbose=True,
        )
    causes_out_path = Path(
        "FILEPATH"
    )
    causes_out = (
        model_df[list(set(model_df).intersection({"cause_id", "fallback_flag"}))]
        .drop_duplicates()
        .astype(int)
    )
    report_duplicates(causes_out, "cause_id")
    causes_out.to_csv(causes_out_path, index=False)


def main(model_group: str, launch_set_id: int) -> None:
    read_file_cache_options = {
        "block_rerun": True,
        "cache_dir": CONF.get_directory("db_cache"),
        "force_rerun": False,
        "cache_results": False,
    }
    location_hierarchy = get_current_location_hierarchy(
        location_set_version_id=CONF.get_id("location_set_version"), **read_file_cache_options
    )

    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=CONF.get_id("cause_set_version"), **read_file_cache_options
    )
    age_meta_df = get_ages(**read_file_cache_options)

    model_df = get_model_data(
        model_group, CONF.get_id("project"), launch_set_id, location_hierarchy, cause_meta_df
    )

    if len(model_df) == 0:
        return

    if "deaths" in model_df.columns:
        model_df = model_df.drop("deaths", axis=1)

    code_system_cause_dict = get_code_system_cause_ids(model_df)

    if (model_group.startswith("VR")) or (model_group.startswith("Cancer")):
        data_type = "VR"
    elif model_group.startswith("VA-"):
        data_type = "VA"
    elif model_group == "CHAMPS":
        data_type = "CHAMPS"
    else:
        data_type = None
    if data_type:
        squarer = Squarer(cause_meta_df, age_meta_df, data_type=data_type)
        model_df = squarer.get_computed_dataframe(model_df)
    if "HH_SURVEYS" in model_group:
        model_df = square_dhs_data(model_df, cause_meta_df, age_meta_df, location_hierarchy)


    model_df = restrict_to_cause_ids(code_system_cause_dict, model_df)

    model_df = drop_problematic_regional_data(
        model_df, model_group, location_hierarchy, cause_meta_df
    )

    model_df = format_for_nr(model_df, location_hierarchy)

    if model_group == "VR-CHN":
        model_df = apply_china_ms_model_adjustment(model_df, cause_meta_df)
        model_df = create_dsp_year_bins(model_df)

    if is_region_vr(model_group):
        model_df = model_df.groupby("is_loc_agg").apply(create_year_bins).reset_index(drop=True)

        model_df = create_fallback_flag(model_df, cause_meta_df)

    if model_group.startswith("VR"):
        model_df = calculate_mean_deaths_per_year(model_df, model_group)

    partition = os.getenv("SLURM_JOB_PARTITION")
    if model_group_is_run_by_cause(model_group):
        run_phase_by_cause(model_df, cause_meta_df, model_group, launch_set_id, partition=partition)
    else:
        run_phase_by_model_group(model_df, model_group, launch_set_id, partition=partition)


if __name__ == "__main__":
    main(
        model_group=str(sys.argv[1]),
        launch_set_id=int(sys.argv[2]),
    )

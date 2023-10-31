import re
import string
from pathlib import Path
import db_queries as dbq
import numpy as np
from shock_prep.utils import dataframe_utils as df_utils, data_structures as ds, trees
SHOCK_SOURCES = Path(FILEPATH)

def add_location_id(df, use_name_col="location_name", raw_name_col="raw", use_level=True):
    loc_meta_df = (
        dbq.get_location_metadata(21, gbd_round_id=7)
        .loc[:, ["level", use_name_col, "location_id"]]
        .rename(columns={use_name_col: raw_name_col, "location_id": "new_location_id"})
        .assign(
            **{
                "level": lambda d: np.where(d["level"] > 3, 4, d["level"]),
                raw_name_col: lambda d: d[raw_name_col].astype(str).str.lower(),
            }
        )
        .drop_duplicates()
    )
    if not use_level:
        loc_meta_df.drop(columns="level", inplace=True)
    if "location_id" not in df:
        df["location_id"] = np.nan
    df = df.merge(loc_meta_df, how="left")
    df["location_id"].fillna(df["new_location_id"], inplace=True)
    return df.drop(columns="new_location_id")

def has_level_3(df):
    return (
        df.assign(is_level_3=lambda d: d["level"] == 3)
        .groupby("source_event_id", as_index=False)
        .agg({"is_level_3": "any"})
        .query("is_level_3 == True")
        .loc[:, ["source_event_id"]]
    )

def clean_location_names(df, col_name="raw"):
    extract_parens_pat = re.compile(r"([^()]+)\(([^()]+)\)")
    df[col_name] = (
        df[col_name].str.strip().str.lower().str.replace("?", "").str.split("[,;]")
    ) 
    df = df_utils.unnest(df, col_name)
    df[col_name] = df[col_name].map(lambda s: ds.flatten(re.findall(extract_parens_pat, s)) or [s])
    df = df_utils.unnest(df, col_name)
    df[col_name] = df[col_name].str.strip(" " + string.punctuation)
    return df[~df[col_name].isin(["", "the", "and"])]

def clean(df):
    location_id_cols = ["country", "admin1", "admin2", "admin3"]
    return (
        df.loc[:, ["source_event_id"] + location_id_cols]
        .melt(
            id_vars=["source_event_id"],
            value_vars=location_id_cols,
            var_name="level",
            value_name="raw",
        )
        .dropna(subset=["raw"])
        .assign(level=lambda d: d.level.map({"country": 3, "admin1": 4, "admin2": 4, "admin3": 4}))
        .pipe(clean_location_names)
    )

def string_match(df):
    return (
        df.pipe(add_location_id, "ihme_loc_id")
        .pipe(add_location_id, "location_name")
        .pipe(add_location_id, "location_ascii_name")
        .pipe(add_location_id, "location_name_short")
        .pipe(add_location_id, "lancet_label")
        .dropna()
        .astype({"location_id": int})
        .sort_values("level")
        .drop_duplicates(["source_event_id", "location_id"], keep="first")

        .pipe(lambda d: d.merge(has_level_3(d)))
    )


def _multiple_locs_for_source_event(df):
    return (
        df.groupby("source_event_id", as_index=False)
        .agg({"location_id": "count"})
        .query("location_id > 1")
        .loc[:, ["source_event_id"]]
    )


def _get_maximally_detailed_locations(df):
    best_idxs = df["level"] == df["level"].min()
    best_locs = df.loc[best_idxs, "location_id"]
    kept = set(best_locs)
    new_locs = trees.filter_locs_for_ancestors_present(df.loc[~best_idxs, "location_id"], kept)
    kept = trees.keep_only_detailed_locs(kept | new_locs)
    return kept

def _add_available_location_detail(df):
    if df.empty:
        return df[["source_event_id", "location_id"]]
    return (
        df.groupby("source_event_id")
        .apply(lambda g: _get_maximally_detailed_locations(g))
        .to_frame(name="location_id")
        .reset_index()
        .assign(tmp=0)
        .pipe(df_utils.unnest, "location_id")
        .drop(columns="tmp")
    )

def keep_maximally_detailed_locations(df):
    df = df_utils.cascade(
        df,
        dict(
            property_map=_multiple_locs_for_source_event, keep_label="mul_locs", add_keep_label=True
        ),
        dict(
            property_map=_add_available_location_detail,
            keep_label="resolved_locs",
            add_keep_label=True,
        ),
    )

    return df.loc[
        ~df["mul_locs"].fillna(False) | df["resolved_locs"].fillna(False),
        ["source_event_id", "location_id"],
    ]

def _get_urban_rural_df():
    loc_meta_df = dbq.get_location_metadata(21, gbd_round_id=7)
    urban_rural = loc_meta_df[loc_meta_df.location_name.str.contains(", Rural|, Urban")].copy()
    is_urban = urban_rural.location_name.str.contains(", Urban")
    urban_rural.loc[is_urban, "urban_rural"] = 1
    urban_rural.urban_rural.fillna(0, inplace=True)

    return urban_rural.loc[:, ["parent_id", "urban_rural", "location_id"]].rename(
        columns={"location_id": "new_location_id", "parent_id": "location_id"}
    )

def add_urban_rural_detail(df, raw):
    df = df.merge(
        raw.loc[:, ["source_event_id", "urban_rural"]].dropna().drop_duplicates(), how="left"
    ).merge(_get_urban_rural_df(), how="left")
    df["location_id"].update(df["new_location_id"])
    return df.drop(columns=["new_location_id", "urban_rural"])

def format_final_string_matched_df(df, loc_col_name):
    return (
        df.astype({loc_col_name: int})
        .astype({loc_col_name: str})
        .groupby("source_event_id", as_index=False)
        .agg({loc_col_name: lambda l: ", ".join(l)})
    )

def string_match_sides(df, side_col):
    return (
        df.loc[:, ["source_event_id", side_col]]
        .dropna()
        .assign(tmp=0)  # HACK
        .pipe(clean_location_names, side_col)
        .drop(columns="tmp")
        .drop_duplicates()
        .pipe(add_location_id, "ihme_loc_id", raw_name_col=side_col, use_level=False)
        .pipe(add_location_id, "location_name", raw_name_col=side_col, use_level=False)
        .pipe(add_location_id, "location_ascii_name", raw_name_col=side_col, use_level=False)
        .pipe(add_location_id, "location_name_short", raw_name_col=side_col, use_level=False)
        .pipe(add_location_id, "lancet_label", raw_name_col=side_col, use_level=False)
        .drop(columns=side_col)
        .rename(columns={"location_id": side_col})
        .dropna()
        .astype({side_col: int})
        .drop_duplicates()
    )

def add_string_matched_sides(df, raw):
    side_a = raw.pipe(string_match_sides, "side_a").pipe(format_final_string_matched_df, "side_a")
    side_b = raw.pipe(string_match_sides, "side_b").pipe(format_final_string_matched_df, "side_b")
    return df.merge(side_a, how="outer").merge(side_b, how="outer").drop_duplicates()

def generate_string_matched_file(df, source, version="test"):
    df = (
        df.pipe(clean)
        .pipe(string_match)
        .pipe(keep_maximally_detailed_locations)
        .pipe(add_urban_rural_detail, df)
        .pipe(format_final_string_matched_df, "location_id")
        .pipe(add_string_matched_sides, df)
    )
    map_path = SHOCK_SOURCES.joinpath(
        "FILEPATH"
    )
    map_path_archive = map_path.parent.joinpath(
        "FILEPATH"
    )
    if not df.empty:
        df.to_csv(map_path, index=False)
        df.to_csv(map_path_archive, index=False)
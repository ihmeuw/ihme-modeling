import os 
import pandas as pd
from cod_prep.utils.data_transforms import get_id_to_ancestor
from cod_prep.downloaders import (
    get_current_location_hierarchy,
    get_datasets,
    get_nid_metadata,
)

def make_run_directory(WORK_DIR):
    if not os.path.exists(WORK_DIR):
        print(f"creating folder at: {WORK_DIR}")
        os.makedirs(WORK_DIR)
    else:
        print(f"folder already exists at: {WORK_DIR}")
        

def format_disjoint_demographics(ROOT_DIR, WORK_DIR):    
    (
        pd.read_excel(FILEPATH)
        .melt(
            id_vars=["location_id", "cause_id"],
            value_vars=["male_ages", "female_ages"],
            var_name="sex_id",
            value_name="age_group_id",
        )
        .replace({"sex_id": {"male_ages": 1, "female_ages": 2}})
        .assign(
            age_group_id=lambda d: d["age_group_id"]
            .str.replace(r"\s", "", regex=True)
            .str.split(",")
        )
        .explode("age_group_id")
        .assign(data_type_id=9)
        .astype(int)
        .loc[:, ["data_type_id", "location_id", "age_group_id", "sex_id", "cause_id"]]
        .to_csv(FILEPATH, index=False)
    )
    
def get_demographics_to_run(WORK_DIR) -> pd.DataFrame:
    loc_meta = get_current_location_hierarchy(force_rerun=False)
    loc_to_most_detailed = (
        loc_meta.pipe(get_id_to_ancestor, "location_id")
        .loc[
            lambda d: d["location_id"].isin(
                loc_meta.query("most_detailed == 1")["location_id"]
            ),
            :,
        ]
        .rename(
            columns={"ancestor": "location_id", "location_id": "detailed_location_id"}
        )
    )
    return (
        pd.read_csv(WORK_DIR / "disjoint_demographics.csv")
        .merge(loc_to_most_detailed)
        .drop(columns="location_id")
        .rename(columns={"detailed_location_id": "location_id"})
    )

def assign_gold_ref(ds):
    ds['gold_ref'] = False
    ds.loc[ds['code_system_id'] == 1, ['gold_ref']] = True

    return ds

def get_years_to_use(OVERLAP, CONF) -> pd.DataFrame:
    ds = get_datasets(
        code_system_id=[1, 4, 5, 6, 8],
        data_type_id=[9, 10],
        year_id=range(1980, CONF.get_id("year_end")),
        is_active=True,
        force_rerun=False,
        need_data_drop_status=False,
        need_location_metadata=False,
        need_malaria_model_group=False,
        need_nr_model_group=False,
        need_survey_type=False,
    )
    id_cols = ["data_type_id", "location_id"]
    assert not ds.loc[:, id_cols + ["year_id"]].duplicated().any()

    ds = assign_gold_ref(ds)

    return ds.sort_values(id_cols + ["year_id"]).assign(
        consecutive_year=lambda d: (
            d.groupby(id_cols + ["gold_ref"])["year_id"].apply(
                lambda s: s.diff().shift(-1).fillna(1) == 1
            )
        ),
        cs_id_diff=lambda d: d.groupby(id_cols)["gold_ref"].diff().fillna(0) != 0,
        before=lambda d: (
            d.groupby(id_cols)["cs_id_diff"].apply(
                lambda s: s.shift(-1)[::-1].rolling(OVERLAP).sum()[::-1] > 0
            )
        ),
        after=(
            lambda d: d.groupby(id_cols)["cs_id_diff"]
            .rolling(OVERLAP)
            .sum()
            .droplevel(id_cols)
            > 0
        ),
        use_year=lambda d: d["consecutive_year"] & (d["before"] | d["after"]),
    )


def get_nid_loc_years(
    years_to_use: pd.DataFrame, demographics_to_run: pd.DataFrame
) -> pd.DataFrame:
    common_id_cols = list(set(years_to_use.columns) & set(demographics_to_run.columns))
    assert (
        years_to_use.merge(
            demographics_to_run.loc[:, common_id_cols].drop_duplicates(), how="right"
        )
        .groupby(common_id_cols + ["gold_ref"], dropna=False)["use_year"]
        .sum()
        >= 1
    ).all(), "Trying to run a crosswalk without enough data available"
    return (
        .merge(demographics_to_run.loc[:, common_id_cols].drop_duplicates())
        .loc[
            :,
            [
                "nid",
                "extract_type_id",
                "location_id",
                "year_id",
                "use_year"
            ],
        ]
        .drop_duplicates()
    )
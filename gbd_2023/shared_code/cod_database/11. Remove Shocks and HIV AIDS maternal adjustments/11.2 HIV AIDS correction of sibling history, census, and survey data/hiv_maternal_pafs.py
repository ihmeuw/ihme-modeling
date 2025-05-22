import pandas as pd

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import add_age_metadata, add_nid_metadata
from cod_prep.utils import CodSchema


class HIVMatPAFs(CodProcess):

    calc_cf_col = "cf"
    all_cf_cols = ["cf", "cf_raw", "cf_cov", "cf_corr", "cf_rd"]

    def __init__(self, project_id):
        self.project_id = project_id
        self.configurator = Configurator("standard")
        self.cache_dir = self.configurator.get_directory("db_cache")
        self.maternal_hiv_props_path = self.configurator.get_directory("maternal_hiv_props")

    def get_computed_dataframe(self, df, cause_meta_df, location_meta_df):
        restricted_maternal_df = self.restrict_to_maternal_data(df, cause_meta_df)
        if restricted_maternal_df is None:
            return df
        appended_pafs = self.append_maternal_pafs(restricted_maternal_df.year_id.unique())
        orig_snnpr = appended_pafs.loc[appended_pafs['location_id']==44858]
        sidama = orig_snnpr.copy()
        sidama['location_id'] = 60908
        sw = orig_snnpr.copy()
        sw['location_id'] = 94364
        appended_pafs.loc[appended_pafs['location_id'] == 44858, 'location_id'] = 95069
        appended_pafs = pd.concat([appended_pafs,sidama, sw])

        merged_data = self.merge_data_and_proportions(restricted_maternal_df, appended_pafs)
        percent_maternal = self.generate_percentages(merged_data)
        split_maternal = self.generate_splits(percent_maternal)
        hiv_cfs = self.create_maternal_hiv_cfs(split_maternal)
        cleaned = self.clean_adjusted_data(hiv_cfs)
        final = self.append_adjusted_orig(df, restricted_maternal_df, cleaned)
        final = final.groupby(CodSchema.infer_from_data(final).id_cols, as_index=False).agg(
            {
                "sample_size": "mean",
                "cf": "sum",
                "cf_raw": "sum",
                "cf_cov": "sum",
                "cf_corr": "sum",
                "cf_rd": "sum",
            }
        )
        return final

    def restrict_to_maternal_data(self, df, cause_meta_df):
        df = df.copy()
        maternal_metadata = cause_meta_df.loc[cause_meta_df["cause_id"] == 366]
        age_start = maternal_metadata["yll_age_start"]
        assert len(age_start) == 1
        age_start = age_start.iloc[0]
        age_end = maternal_metadata.yll_age_end
        assert len(age_end) == 1
        age_end = age_end.iloc[0]

        data = add_age_metadata(
            df,
            add_cols=["simple_age"],
            merge_col="age_group_id",
            force_rerun=False,
            block_rerun=True,
            cache_results=False,
            cache_dir=self.cache_dir,
        )
        data.rename(columns={"simple_age": "age"}, inplace=True)
        maternal_data = data.loc[
            (df["cause_id"] == 366)
            & (data["age"] >= age_start)
            & (data["age"] <= age_end)
            & (data["sex_id"] == 2)
            & (data["year_id"] >= 1980)
        ]
        maternal_data.drop("age", axis=1, inplace=True)
        if len(maternal_data) == 0:
            return None
        else:
            return maternal_data

    def append_maternal_pafs(self, years):
        props = pd.DataFrame()
        for year in years:
            year = int(year)
            props_path = "FILEPATH"
            data = pd.read_csv(props_path)
            props = props.append(data)
        props = props.rename(columns={"year": "year_id"})
        return props

    def duplicate_national_props(self, props_df, loc_df):
        subnational = loc_df.loc[
            loc_df["level"] > 3, ["location_id", "parent_id", "level", "path_to_top_parent"]
        ]

        subnational.loc[subnational["level"] == 5, "parent_id"] = (
            subnational["path_to_top_parent"].str.split(",").str[3].astype(int)
        )

        subnational = subnational.loc[subnational["parent_id"].isin(self.need_subnational_props)]

        subnational = subnational.loc[
            ~((subnational["parent_id"] == 62) & (subnational["level"] == 4))
        ]
        subnational = subnational[["location_id", "parent_id"]]
        subnational.rename(
            columns={"location_id": "child_location_id", "parent_id": "location_id"},
            inplace=True,
        )

        subnational = props_df.merge(subnational, on="location_id")
        subnational.drop("location_id", axis=1, inplace=True)
        subnational.rename(columns={"child_location_id": "location_id"}, inplace=True)
        props_df = pd.concat([props_df, subnational])
        assert not props_df.duplicated().any()
        return props_df

    def merge_data_and_proportions(self, data, props):
        merged_data = data.merge(props, on=["location_id", "age_group_id", "year_id"], how="left")
        assert merged_data.notnull().values.all()
        return merged_data

    def generate_percentages(self, df):
        df["pct_maternal"] = 1 - df["pct_hiv"]
        df.loc[df["pct_maternal"].isnull(), "pct_maternal"] = 1
        df.loc[df["pct_hiv"].isnull(), "pct_hiv"] = 0
        df.loc[df["pct_maternal_hiv"].isnull(), "pct_maternal_hiv"] = 0
        assert all(x > 0 for x in df["pct_maternal"])
        assert (
            df[["pct_maternal", "pct_hiv", "pct_maternal_hiv"]].notnull().values.any()
        )
        assert all(abs(df["pct_maternal"] + df["pct_hiv"]) - 1) < 0.0001
        assert (df["pct_maternal_hiv_vr"] <= 0.13).all()
        assert not (df["cause_id"] == 741).any()
        return df

    def generate_splits(self, df):
        df = add_nid_metadata(
            df,
            add_cols="data_type_id",
            project_id=self.project_id,
            block_rerun=True,
            cache_dir=self.cache_dir,
            force_rerun=False,
        )
        df.loc[df["data_type_id"].isin([7, 5]), "split_maternal"] = 1
        df.loc[df["split_maternal"].isnull(), "split_maternal"] = 0
        df.loc[df["split_maternal"] == 0, "pct_maternal"] = 1
        df.loc[df["split_maternal"] == 0, "pct_maternal_hiv"] = df["pct_maternal_hiv_vr"]
        df.loc[df["split_maternal"] == 0, "pct_hiv"] = 0
        df.drop(columns=["pct_maternal_hiv_vr", "data_type_id"], inplace=True)
        return df

    def create_maternal_hiv_cfs(self, df):
        df = df.copy()

        maternal_hiv_df = df.copy()
        maternal_hiv_df["cf"] = maternal_hiv_df["cf"] * maternal_hiv_df["pct_maternal_hiv"]
        maternal_hiv_df["cause_id"] = 741
        maternal_hiv_df["cf_raw"] = 0
        maternal_hiv_df["cf_cov"] = 0
        maternal_hiv_df["cf_corr"] = 0
        maternal_hiv_df["cf_rd"] = 0

        maternal_df = df.copy()
        maternal_df["cf"] = maternal_df["cf"] * maternal_df["pct_maternal"]
        maternal_df["cause_id"] = 366
        df = pd.concat([maternal_hiv_df, maternal_df], ignore_index=True)

        return df

    def clean_adjusted_data(self, df):
        mat_hiv = df.copy(deep=True)
        if len(mat_hiv) > 0:
            assert {741, 366} == set(mat_hiv.cause_id.unique())
            mat_hiv = mat_hiv.loc[mat_hiv["cause_id"] != 366]
            mat_hiv["cause_id"] = 366
        df = pd.concat([df, mat_hiv], ignore_index=True)
        df.drop(
            columns=["pct_hiv", "pct_maternal_hiv", "pct_maternal", "split_maternal"],
            inplace=True,
        )
        df = df.groupby(CodSchema.infer_from_data(df).id_cols + ["sample_size"], as_index=False)[
            self.all_cf_cols
        ].sum()

        assert (df["cf"] < 1.1).all()
        df.loc[df["cf"] > 1, "cf"] = 1

        return df

    def append_adjusted_orig(self, orig, maternal_data, adjusted):
        data = orig.merge(maternal_data, how="left", indicator=True)
        data = data.loc[data["_merge"] != "both"]
        data.drop("_merge", axis=1, inplace=True)
        data = data.append(adjusted, ignore_index=True)
        return data

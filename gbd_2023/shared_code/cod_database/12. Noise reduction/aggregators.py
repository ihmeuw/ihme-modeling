import pandas as pd

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import (
    add_cause_metadata,
    add_envelope,
    add_location_metadata,
    add_population,
    get_country_level_location_id,
)
from cod_prep.utils import CodSchema, report_duplicates, report_if_merge_fail

pd.options.mode.chained_assignment = None

class CauseAggregator(CodProcess):
    def __init__(self, df, cause_meta_df, source, data_type_id):
        self.df = df
        self.schema = CodSchema.infer_from_data(df)
        self.cause_meta_df = cause_meta_df
        self.source = source
        self.data_type_id = data_type_id
        self.conf = Configurator('standard')

        self.skip_vr_sources = [
            "China_1991_2002", "ICD9_USSR_Tabulation", "Iran_2011", "Iran_Mohsen_special_ICD10", "Iraq_collab_ICD10",
            "Libya_VR_2006_2008", "MOZ_MoH_urban", "Madagascar_Antananarivo", "Malaysia_VR", "Mali_Bamako",
            "Nigeria_VR", "PHL_VSR_1980_1990", "PHL_VSR_1991_1998", "QAT_VR_1984_1985", "Sri_Lanka_93_96",
            "Tonga_2002_2004", "UAE_2006_2007", "UAE_Abu_Dhabi", "Zimbabwe_2007", "Zimbabwe_95"
        ]
        if 'MCCD' in self.source:
            self.skip_vr_sources += [self.source]

        self.va_agg_nids = pd.read_csv(
            self.conf.get_resource('va_aggregation_nids')
        )[['nid', 'extract_type_id']].drop_duplicates().to_records(index=False).tolist()

        self.war_and_disaster_exceptions = [945, 729]

    def data_is_aggregate_eligible_va(self, df):
        nid = df.nid.iloc[0]
        etid = df.extract_type_id.iloc[0]

        if (nid, etid) in self.va_agg_nids:
            return True
        else:
            return False

    def get_computed_dataframe(self):
        agg_eligible_va = self.data_is_aggregate_eligible_va(self.df)

        if (self.data_type_id in [9, 10]) and (self.source not in self.skip_vr_sources):
            df = self.simple_aggregate()
        elif (self.data_type_id == 8) and (agg_eligible_va):
            df = self.simple_aggregate()
        else:
            df = self.level_3_aggregate()

        agg_dict = {c: "sum" for c in self.schema.value_cols}
        agg_dict["sample_size"] = "mean" 

        df = df.groupby(self.schema.id_cols, as_index=False).agg(agg_dict)
        return df

    def get_diagnostic_dataframe(self):
        df = self.get_computed_dataframe()
        df = add_cause_metadata(
            df, ["parent_id", "level"], merge_col="cause_id", cause_meta_df=self.cause_meta_df
        )
        return df

    def simple_aggregate(self):
        df = add_cause_metadata(
            self.df,
            ["secret_cause", "parent_id", "level"],
            merge_col="cause_id",
            cause_meta_df=self.cause_meta_df,
        )
        secret_causes = df.loc[df["secret_cause"] == 1]
        if len(secret_causes) > 0:
            raise AssertionError(
                "The following secret causes are still "
                "in the data: \n{}".format(secret_causes["cause_id"].unique())
            )
        cause_levels = sorted(list(range(2, 6, 1)), reverse=True)
        for level in cause_levels:
            level_df = df[df["level"] == level]

            if level == 3:
                level_df = level_df.loc[~level_df.cause_id.isin(self.war_and_disaster_exceptions)]

            if len(level_df) > 0:
                level_df["cause_id"] = level_df["parent_id"]
                level_df["level"] = level_df["level"] - 1
                level_df.drop("parent_id", axis=1, inplace=True)
                level_df = add_cause_metadata(
                    level_df,
                    ["parent_id"],
                    merge_col=["cause_id"],
                    cause_meta_df=self.cause_meta_df,
                )
                df = pd.concat([level_df, df], ignore_index=True)

        return df

    def level_3_aggregate(self):
        df = add_cause_metadata(
            self.df,
            ["secret_cause", "parent_id", "level"],
            merge_col="cause_id",
            cause_meta_df=self.cause_meta_df,
        )
        secret_causes = df.loc[df["secret_cause"] == 1]
        if len(secret_causes) > 0:
            raise AssertionError(
                "The following secret causes are still "
                "in the data: \n{}".format(secret_causes["cause_id"].unique())
            )

        for level in [5, 4]:
            level_df = df[df["level"] == level]
            if len(level_df) > 0:
                level_df["cause_id"] = level_df["parent_id"]
                level_df["level"] = level_df["level"] - 1
                level_df.drop("parent_id", axis=1, inplace=True)
                level_df = add_cause_metadata(
                    level_df,
                    ["parent_id"],
                    merge_col=["cause_id"],
                    cause_meta_df=self.cause_meta_df,
                )
                df = pd.concat([level_df, df], ignore_index=True)
        df = df.loc[df.level >= 3]
        return df


class LocationAggregator(CodProcess):
    val_cols = ["deaths", "deaths_rd", "deaths_corr", "deaths_cov", "deaths_raw"]

    def __init__(self, df, location_meta_df):
        self.df = df
        self.location_meta_df = location_meta_df
        self.conf = Configurator("standard")
        self.nid_replacements = self.conf.get_resource("nid_replacements")

    def get_computed_dataframe(self, type="simple"):
        if type != "simple":
            df = self.aggregate_locations()
        else:
            df = self.simple_aggregate()
        df = self.change_nid_for_aggregates(df)
        return df

    def simple_aggregate(self):
        df = self.df.copy()
        country_location_ids = get_country_level_location_id(
            df.location_id.unique(), self.location_meta_df
        )
        df = df.merge(country_location_ids, how="left", on="location_id")
        report_if_merge_fail(df, "country_location_id", ["location_id"])
        df = df[df["location_id"] != df["country_location_id"]]
        df["location_id"] = df["country_location_id"]
        df = df.drop(["country_location_id"], axis=1)

        group_cols = [col for col in df.columns if col not in self.val_cols]
        group_cols.remove("site_id")
        df = df.groupby(group_cols, as_index=False)[self.val_cols].sum()

        df["site_id"] = 2

        df = df.append(self.df)

        return df

    def aggregate_locations(self):
        df = add_location_metadata(
            self.df,
            ["parent_id", "level"],
            merge_col="location_id",
            location_meta_df=self.location_meta_df,
        )
        max_level = df["level"].max()
        loc_levels = list(range(4, max_level + 1))
        loc_levels.reverse()
        for level in loc_levels:
            level_df = df.loc[df["level"] == level]
            if len(level_df) > 0:
                level_df["location_id"] = level_df["parent_id"]
                level_df["level"] = df["level"] - 1
                level_df.drop("parent_id", axis=1, inplace=True)
                level_df = add_location_metadata(
                    level_df,
                    ["parent_id"],
                    merge_col=["location_id"],
                    location_meta_df=self.location_meta_df,
                )
                group_cols = [col for col in level_df.columns if col not in self.val_cols]
                group_cols.remove("site_id")
                level_df = level_df.groupby(group_cols, as_index=False)[self.val_cols].sum()
                level_df["site_id"] = 2
                df = df.append(level_df, ignore_index=True)
        df.drop(["parent_id", "level"], axis=1, inplace=True)
        return df

    def change_nid_for_aggregates(self, df):
        nid_df = pd.read_csv(self.nid_replacements)
        nid_df.rename(
            columns={"match_location_id": "location_id", "Old NID": "nid", "NID": "new_nid"},
            inplace=True,
        )
        nid_df = nid_df[["location_id", "nid", "new_nid"]]
        start_length = len(df)
        df = df.merge(nid_df, how="left", on=["location_id", "nid"])
        df.loc[df["new_nid"].notnull(), "nid"] = df["new_nid"]
        df = df.drop("new_nid", axis=1)
        if df["nid"].isnull().any():
            raise AssertionError("There are observations with missing nids")
        if len(df.loc[df["nid"] == 103215]) > 0:
            raise AssertionError("There are observations with nid 103215")
        end_length = len(df)
        if start_length != end_length:
            raise AssertionError(
                "Observations have either "
                "been added or dropped: {}".format(start_length - end_length)
            )
        return df


class AgeAggregator(CodProcess):

    def __init__(self, df, pop_df, env_df, age_weight_df):
        self.df = df
        self.pop_df = pop_df
        self.env_df = env_df
        self.age_weight_df = age_weight_df
        self.cf_final_col = ["cf_final"]
        self.draw_cols = [x for x in self.df.columns if "cf_draw_" in x]
        if len(self.draw_cols) > 0:
            self.cf_final_col = self.draw_cols + ["cf_final"]
        self.cf_cols = ["cf_raw", "cf_cov", "cf_rd", "cf_corr", "cf_agg"] + self.cf_final_col
        self.deaths_cols = ["deaths" + x.split("cf")[1] for x in self.cf_cols]
        self.id_cols = [
            "nid",
            "extract_type_id",
            "location_id",
            "site_id",
            "year_id",
            "age_group_id",
            "sex_id",
            "cause_id",
        ]

    def get_computed_dataframe(self):
        df = self.df.copy()

        all_age_df = self.make_all_ages_group(df)

        age_weight_dict = (
            self.age_weight_df.drop_duplicates(["age_group_id", "age_group_weight_value"])
            .set_index("age_group_id")["age_group_weight_value"]
            .to_dict()
        )
        age_standard_df = self.make_age_standardized_group(df, age_weight_dict)

        assert len(age_standard_df) == len(
            all_age_df
        )

        df = pd.concat([self.df, all_age_df, age_standard_df], ignore_index=True)

        if "floor_flag" in df.columns:
            df.fillna(value={"cf_final_pre_floor": df["cf_final"], "floor_flag": 0}, inplace=True)

        report_duplicates(df, self.id_cols)

        return df

    def make_deaths(self, df):
        if "mean_env" not in df.columns:
            df = add_envelope(df, env_df=self.env_df)
            report_if_merge_fail(
                df, "mean_env", ["sex_id", "age_group_id", "year_id", "location_id"]
            )
        for cf_col in self.cf_cols:
            df["deaths" + cf_col.split("cf")[1]] = df[cf_col] * df["mean_env"]

        df = df.drop("mean_env", axis=1)

        return df

    def make_cause_fractions(self, df):
        if "mean_env" not in df.columns:
            df = add_envelope(df, env_df=self.env_df)
            report_if_merge_fail(
                df, "mean_env", ["sex_id", "age_group_id", "year_id", "location_id"]
            )
        for col in self.deaths_cols:
            df["cf" + col.split("deaths")[1]] = df[col] / df["mean_env"]
        df = df.drop("mean_env", axis=1)
        return df

    def make_all_ages_group(self, df):
        df = df.copy()
        df = self.make_deaths(df)
        df = df.drop(self.cf_cols, axis=1)
        df["age_group_id"] = 22
        df = df.groupby(self.id_cols, as_index=False)[self.deaths_cols + ["sample_size"]].sum()
        df = self.make_cause_fractions(df)
        df = df.drop(self.deaths_cols, axis=1)

        report_duplicates(df, self.id_cols)

        return df

    def make_age_standardized_group(self, df, age_weight_dict):
        df = df.copy()
        df["weight"] = df["age_group_id"].map(age_weight_dict)
        report_if_merge_fail(df, "weight", "age_group_id")

        df = add_population(df, pop_df=self.pop_df)
        report_if_merge_fail(df, "population", ["sex_id", "age_group_id", "year_id", "location_id"])
        df = add_envelope(df, env_df=self.env_df)
        report_if_merge_fail(df, "mean_env", ["sex_id", "age_group_id", "year_id", "location_id"])

        for col in self.cf_cols:
            df[col] = ((df[col] * df["mean_env"]) / df["population"]) * df["weight"]

        df["age_group_id"] = 27
        df = df.drop(["weight", "population", "mean_env"], axis=1)
        df = df.groupby(self.id_cols, as_index=False)[self.cf_cols + ["sample_size"]].sum()

        report_duplicates(df, self.id_cols)

        return df
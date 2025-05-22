import numpy as np
import pandas as pd

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders.causes import get_parent_and_childen_causes
from cod_prep.downloaders.locations import add_location_metadata, get_country_level_location_id
from cod_prep.utils import CodSchema, expand_to_u5_age_detail, report_if_merge_fail

pd.options.mode.chained_assignment = None

CONF = Configurator("standard")
N_DRAWS = CONF.get_resource("uncertainty_draws")

class RTIAdjuster(CodProcess):
    death_cols = ["deaths", "deaths_corr", "deaths_cov", "deaths_raw", "deaths_rd"]
    rti_sources = ["Various_RTI"]

    def __init__(self, df, cause_meta_df, age_meta_df, location_meta_df):
        self.df = df
        self.merge_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
        self.orig_cols = df.columns
        self.cmdf = cause_meta_df
        self.amdf = age_meta_df
        self.lmdf = location_meta_df

    def get_computed_dataframe(self):
        inj_trans_causes = list(
            self.cmdf[self.cmdf.acause.str.startswith("inj_trans")].cause_id.unique()
        )
        df = self.df[self.df.cause_id.isin(inj_trans_causes)]
        df = df.groupby(
            ["location_id", "year_id", "sex_id", "age_group_id", "nid", "extract_type_id"]
        ).apply(self.cf_without_cc_code)
        df = self.apply_rti_fractions(df)
        df = self.cleanup(df)
        self.diag_df = df.copy()
        return df

    def cf_without_cc_code(self, df):
        df["cf"] = df["cf"] / df.cf.sum()
        return df

    def apply_rti_fractions(self, df):
        rti_fraction_input = pd.read_csv(CONF.get_resource("rti_fractions_data"))
        rti_fraction_input = expand_to_u5_age_detail(rti_fraction_input)
        df = df.merge(rti_fraction_input, how="left", on=self.merge_cols)
        df["cf"] = df["cf"] * df["rti_fractions"]
        return df

    def cleanup(self, df):
        df = df.drop(["rti_fractions"], axis=1)
        return df

    def get_diagnostic_dataframe(self):
        try:
            return self.diag_df
        except AttributeError:
            print(
                "ERROR"
            )
        return pd.DataFrame()


class MaternalHIVRemover(CodProcess):
    death_cols = ["deaths", "deaths_corr", "deaths_cov", "deaths_raw", "deaths_rd"]

    def __init__(self, df, env_meta_df, env_hiv_meta_df, source, nid):
        self.df = df
        self.maternal_env_nids = CONF.get_resource("maternal_env_nids")
        self.maternal_ages = list(range(7, 16))
        self.env_meta_df = env_meta_df
        self.env_hiv_meta_df = env_hiv_meta_df
        self.merge_cols = ["age_group_id", "location_id", "year_id", "sex_id"]
        self.orig_cols = df.columns
        self.source = source
        self.nid = nid
        self.maternal_cause_id = 366
        self.cc_code = 919

    def get_computed_dataframe(self):
        df = self.flag_observations_to_adjust()
        env_df = self.calculate_hiv_envelope_ratio()
        df = self.adjust_deaths(df, env_df)

        self.diag_df = df.copy()

        df = self.cleanup(df)
        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        try:
            return self.diag_df
        except AttributeError:
            print(
                "ERROR"
            )
            return pd.DataFrame()

    def flag_observations_to_adjust(self):
        df = self.df.copy()
        try:
            nid_df = pd.read_csv(self.maternal_env_nids)
            if self.nid in nid_df["nid"]:
                df["used_env"] = 1
            else:
                df["used_env"] = 0
        except IOError:
            df["used_env"] = 0

        if self.maternal_cause_id in df.cause_id.unique():
            df.loc[
                (df["used_env"] == 0)
                & (df["sex_id"] == 2)
                & (df["cause_id"] == self.cc_code)
                & (df["age_group_id"].isin(self.maternal_ages)),
                "adjust",
            ] = 1
            df["adjust"] = df["adjust"].fillna(0)
        else:
            df["adjust"] = 0

        return df

    def calculate_hiv_envelope_ratio(self):
        self.env_hiv_meta_df.rename(columns={"mean_env": "mean_hiv_env"}, inplace=True)
        env_df = self.env_hiv_meta_df.merge(self.env_meta_df, on=self.merge_cols)
        env_df["hiv_ratio"] = env_df["mean_env"] / env_df["mean_hiv_env"]
        env_df.drop(
            ["lower_x", "upper_x", "lower_y", "upper_y", "run_id_x", "run_id_y"],
            axis=1,
            inplace=True,
        )
        env_df.loc[env_df['hiv_ratio'] > 1, 'hiv_ratio'] = 1

        assert (
            (env_df["hiv_ratio"] <= 1.001) & (env_df["hiv_ratio"] > 0)
        ).values.all()

        return env_df

    def adjust_deaths(self, df, env_df):
        df = df.merge(env_df, on=self.merge_cols, how="left")

        df.loc[df["hiv_ratio"] < 0.05, "hiv_ratio"] = 0.05

        for col in self.death_cols:
            df[col + "_adj"] = df[col].copy()
            df.loc[df["adjust"] == 1, col + "_adj"] = df["hiv_ratio"] * df[col + "_adj"]

        return df

    def cleanup(self, df):
        for col in self.death_cols:
            df[col] = df[col + "_adj"].copy()
        df = df[self.orig_cols]
        return df


class SampleSizeCauseRemover(CodProcess):
    adjust_causes = [729, 945]
    cf_cols = ["cf", "cf_rd", "cf_corr", "cf_cov", "cf_raw"]

    def __init__(self, cause_meta_df):
        self.cause_meta_df = cause_meta_df
        self.affected_causes = get_parent_and_childen_causes(self.adjust_causes, self.cause_meta_df)

    def get_computed_dataframe(self, df):
        df = self.set_adjustment_causes(df)
        df = self.set_affected_causes(df)
        df = self.adjust_sample_size(df)
        df = self.remake_cf(df)
        self.diag_df = df.copy()
        df = self.cleanup(df)
        return df

    def get_diagnostic_dataframe(self):
        try:
            return self.diag_df
        except AttributeError:
            print(
                "ERROR"
            )
            return pd.DataFrame()

    def set_adjustment_causes(self, df):
        df = df.copy()
        df["is_adjustment_cause"] = 0
        is_adjustment_cause = df["cause_id"].isin(self.adjust_causes)
        df.loc[is_adjustment_cause, "is_adjustment_cause"] = 1
        return df

    def set_affected_causes(self, df):
        df = df.copy()
        df["is_affected_cause"] = 0
        df.loc[df["cause_id"].isin(self.affected_causes), "is_affected_cause"] = 1
        return df

    def adjust_sample_size(self, df):
        df = df.copy()
        is_adjust_cause = df["is_adjustment_cause"] == 1
        df.loc[is_adjust_cause, "deaths_remove"] = df["cf"] * df["sample_size"]
        df["deaths_remove"] = df["deaths_remove"].fillna(0)

        demo_cols = CodSchema.infer_from_data(
            df,
            metadata={
                "is_adjustment_cause": {"col_type": "cause"},
                "is_affected_cause": {"col_type": "cause"},
            },
        ).demo_cols
        df["sample_size_remove"] = df.groupby(demo_cols)["deaths_remove"].transform(
            "sum"
        )

        df["sample_size_adj"] = df["sample_size"]
        is_affected_cause = df["is_affected_cause"] == 1
        df.loc[~is_affected_cause, "sample_size_adj"] = (
            df["sample_size_adj"] - df["sample_size_remove"]
        )

        return df

    def remake_cf(self, df):
        df = df.copy()
        for cf_col in self.cf_cols:
            df[cf_col] = (df[cf_col] * df["sample_size"]) / df["sample_size_adj"]
            df[cf_col] = df[cf_col].fillna(0)
        return df

    def cleanup(self, df):
        df = df.copy()
        df = df.drop(
            [
                "is_affected_cause",
                "is_adjustment_cause",
                "sample_size",
                "deaths_remove",
                "sample_size_remove",
            ],
            axis=1,
        )
        df.rename(columns={"sample_size_adj": "sample_size"}, inplace=True)
        return df


class Raker(CodProcess):
    def __init__(self, df, source, double=False):
        self.df = df
        self.source = source
        self.double = double
        self.merge_cols = ["sex_id", "age_group_id", "cause_id", "year_id", "iso3"]
        self.cf_cols = ["cf_final"]
        self.draw_cols = [x for x in self.df.columns if "cf_draw_" in x]
        if len(self.draw_cols) > 0:
            self.cf_cols = self.draw_cols + ["cf_final"]
        self.death_cols = ["deaths" + x.split("cf")[1] for x in self.cf_cols]
        self.agg_cols = [(x + "_agg") for x in self.death_cols] + ["sample_size_agg"]
        self.sub_cols = [(x + "_sub") for x in self.death_cols] + ["sample_size_sub"]
        self.death_prop_cols = [(x + "_prop") for x in self.death_cols]

    def get_computed_dataframe(self, location_hierarchy):
        if self.double:
            df = self.double_rake(self.df, location_hierarchy)
        else:
            df = self.standard_rake(self.df, location_hierarchy)
        df.drop("is_nat", axis=1, inplace=True)
        return df

    def standard_rake(self, df, location_hierarchy):
        start = len(df)

        df = self.add_iso3(df, location_hierarchy)
        df = self.flag_aggregates(df, location_hierarchy)

        if 0 in df["is_nat"].unique():

            df = self.make_deaths(df)

            aggregate_df = self.prep_aggregate_df(df)
            subnational_df = self.prep_subnational_df(df)
            sub_and_agg = subnational_df.merge(aggregate_df, on=self.merge_cols, how="left")
            for death_col in self.death_cols:
                sub_and_agg.loc[
                    sub_and_agg["{}_agg".format(death_col)].isnull(),
                    "{}_agg".format(death_col),
                ] = sub_and_agg["{}_sub".format(death_col)]
            sub_and_agg.loc[
                sub_and_agg["sample_size_agg"].isnull(), "sample_size_agg"
            ] = sub_and_agg["sample_size_sub"]
            df = df.merge(sub_and_agg, how="left", on=self.merge_cols)

            end = len(df)
            assert start == end
            df = self.replace_metrics(df)
            df = self.cleanup(df)
        return df

    def cleanup(self, df):
        df = df.drop(self.sub_cols + self.agg_cols + self.death_prop_cols + self.death_cols, axis=1)
        return df

    def add_iso3(self, df, location_hierarchy):
        df = add_location_metadata(df, "ihme_loc_id", location_meta_df=location_hierarchy)
        df["iso3"] = df["ihme_loc_id"].str[0:3]
        df.drop(["ihme_loc_id"], axis=1, inplace=True)
        return df

    def prep_subnational_df(self, df):
        df = df[df["is_nat"] == 0]
        sub_total = df.groupby(self.merge_cols, as_index=False)[
            self.death_cols + ["sample_size"]
        ].sum()

        for death_col in self.death_cols:
            sub_total.loc[sub_total[death_col] == 0, death_col] = 0.0001
            sub_total.rename(columns={death_col: death_col + "_sub"}, inplace=True)
        sub_total.rename(columns={"sample_size": "sample_size_sub"}, inplace=True)

        sub_total = sub_total[self.merge_cols + self.sub_cols]

        return sub_total

    def flag_aggregates(self, df, location_hierarchy):
        country_locations = get_country_level_location_id(
            df.location_id.unique(), location_hierarchy
        )
        df = df.merge(country_locations, how="left", on="location_id")
        df.loc[df["location_id"] == df["country_location_id"], "is_nat"] = 1
        df.loc[df["location_id"] != df["country_location_id"], "is_nat"] = 0
        df = df.drop("country_location_id", axis=1)
        return df

    def fix_inf_death_props(self, row):
        prop_values = row[self.death_prop_cols].values.tolist()
        if np.inf in prop_values:
            maxfill = max(p for p in prop_values if p != np.inf)
            prop_values = [p if p != np.inf else maxfill for p in prop_values]
            row[self.death_prop_cols] = prop_values

        return row

    def replace_metrics(self, df):
        if self.source in ["Other_Maternal", "DHS_maternal"]:
            df["prop_ss"] = df["sample_size_agg"] / df["sample_size_sub"]
            df.loc[df["is_nat"] == 0, "sample_size"] = df["sample_size"] * df["prop_ss"]

        for death_col in self.death_cols:
            df["{}_prop".format(death_col)] = (
                df["{}_agg".format(death_col)] / df["{}_sub".format(death_col)]
            )

        if df["iso3"].tolist()[0] == "PHL":
            df = df.apply(self.fix_inf_death_props, axis=1)

        for death_col in self.death_cols:
            df.loc[df["is_nat"] == 0, death_col] = df[death_col] * df["{}_prop".format(death_col)]

            cf_col = "cf" + death_col.split("deaths")[1]
            df.loc[df["is_nat"] == 0, cf_col] = df[death_col] / df["sample_size"]
            df.loc[df[cf_col] > 1, cf_col] = 1

        return df

    def prep_aggregate_df(self, df):
        df = df[df["is_nat"] == 1]

        for death_col in self.death_cols:
            df = df.rename(columns={death_col: death_col + "_agg"})

        df = df.rename(columns={"sample_size": "sample_size_agg"})

        df = df[self.merge_cols + self.agg_cols]
        df = df.groupby(self.merge_cols, as_index=False).sum()

        return df

    def make_deaths(self, df):
        for cf_col in self.cf_cols:
            df["deaths" + cf_col.split("cf")[1]] = df[cf_col] * df["sample_size"]
        return df

    def rake_detail_to_intermediate(self, df, location_hierarchy, intermediate_locs):
        df = add_location_metadata(df, ["parent_id"], location_meta_df=location_hierarchy)
        dfs = []
        for loc in intermediate_locs:
            temp = df.loc[(df.parent_id == loc) | (df.location_id == loc)]
            temp.loc[temp.location_id == loc, "location_id"] = temp["parent_id"]
            temp.drop("parent_id", axis=1, inplace=True)
            temp = self.standard_rake(temp, location_hierarchy)
            dfs.append(temp)
        df = pd.concat(dfs, ignore_index=True)
        df = df.loc[df.is_nat == 0]
        return df

    def double_rake(self, df, location_hierarchy):
        start = len(df)
        df = add_location_metadata(df, ["level"], location_meta_df=location_hierarchy)
        report_if_merge_fail(df, "level", "location_id")

        national = df["level"] == 3
        intermediate = df["level"] == 4
        detail = df["level"] == 5
        assert len(df[detail]) > 0, (
            "ERROR"
        )
        assert len(df[intermediate]) > 0, (
            "ERROR"
        )
        intermediate_locs = df[intermediate].location_id.unique().tolist()
        df.drop("level", axis=1, inplace=True)

        non_national = df.loc[detail | intermediate]
        single_raked_detail_locs = self.rake_detail_to_intermediate(
            non_national, location_hierarchy, intermediate_locs
        )
        detail_and_national = pd.concat(
            [single_raked_detail_locs, df.loc[national]], ignore_index=True
        )
        detail_df = self.standard_rake(detail_and_national, location_hierarchy)
        detail_df = detail_df.loc[detail_df.is_nat == 0]

        intermediate_and_national = df.loc[intermediate | national]
        intermediate_and_national = self.standard_rake(
            intermediate_and_national, location_hierarchy
        )

        df = pd.concat([detail_df, intermediate_and_national], ignore_index=True)
        assert start == len(df)
        return df


class AnemiaAdjuster(CodProcess):
    cf_cols = ["cf", "cf_raw", "cf_cov", "cf_corr", "cf_rd"]
    anemia_cause_id = 390

    def __init__(self):
        self.anemia_props_path = CONF.get_resource("va_anemia_proportions")
        self.location_set_version_id = CONF.get_id("location_set_version")

    def get_computed_dataframe(self, df):
        original_columns = list(df.columns)

        orig_deaths_sum = (df["cf"] * df["sample_size"]).sum()

        anemia_props = pd.read_csv(self.anemia_props_path)
        anemia_props = expand_to_u5_age_detail(anemia_props)

        anemia_df = df.loc[df["cause_id"] == self.anemia_cause_id]
        anemia_df = add_location_metadata(
            anemia_df,
            "ihme_loc_id",
            location_set_version_id=self.location_set_version_id,
            force_rerun=False,
        )
        anemia_df["iso3"] = anemia_df["ihme_loc_id"].str.slice(0, 3)
        unique_iso3s = list(anemia_df["iso3"].unique())
        merge_props = anemia_props.loc[anemia_props["iso3"].isin(unique_iso3s)]
        unique_years = list(anemia_df.year_id.unique())
        years_under_90 = [u for u in unique_years if u < 1990]
        if len(years_under_90) > 0:
            props_90 = merge_props.query("year_id == 1990")
            for copy_year in years_under_90:
                copy_props = props_90.copy()
                copy_props["year_id"] = copy_year
                merge_props = merge_props.append(copy_props, ignore_index=True)
        anemia_df = anemia_df.merge(
            merge_props,
            on=["iso3", "year_id", "age_group_id", "sex_id", "cause_id"],
            how="left",
        )
        self.diag_df = anemia_df

        sum_to_one_id_cols = list(set(original_columns) - set(self.cf_cols))
        assert np.allclose(anemia_df.groupby(sum_to_one_id_cols)["anemia_prop"].sum(), 1)

        anemia_df["cause_id"] = anemia_df["target_cause_id"]
        for cf_col in self.cf_cols:
            anemia_df[cf_col] = anemia_df[cf_col] * anemia_df["anemia_prop"]

        anemia_df = anemia_df[original_columns]

        df = df.loc[df["cause_id"] != self.anemia_cause_id]
        df = df.append(anemia_df, ignore_index=True)

        sum_cols = self.cf_cols
        group_cols = list(set(df.columns) - set(sum_cols))
        df = df.groupby(group_cols, as_index=False)[sum_cols].sum()

        new_deaths_sum = (df["cf"] * df["sample_size"]).sum()

        assert np.allclose(orig_deaths_sum, new_deaths_sum)

        return df

    def get_diagnostic_dataframe(self):
        if self.diag_df is not None:
            return self.diag_df
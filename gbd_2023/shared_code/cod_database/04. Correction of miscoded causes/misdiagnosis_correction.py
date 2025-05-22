import os
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import logit

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import (
    add_code_metadata,
    add_envelope,
    add_location_metadata,
    get_current_cause_hierarchy,
    get_all_related_causes,
    get_cause_map,
    get_clean_package_map_for_misdc,
    get_env,
    get_package_map,
    remove_five_plus_digit_icd_codes,
)
from cod_prep.utils import (
    CodSchema,
    clean_icd_codes,
    get_norway_subnational_mapping,
    print_log_message,
    report_duplicates,
    report_if_merge_fail,
)


class MisdiagnosisCorrector(CodProcess):
    conf = Configurator()
    block_rerun = {"force_rerun": False, "block_rerun": True}
    process_dir = conf.get_directory("mc_process_data")
    mcod_prob_path = conf.get_resource("misdiagnosis_prob_path")
    cc_code = 919
    location_set_version_id = conf.get_id("location_set_version")

    def __init__(
        self,
        nid,
        extract_type_id,
        code_system_id,
        code_map_version_id,
        adjust_id,
        remove_decimal,
    ):
        self.nid = nid
        self.extract_type_id = extract_type_id
        self.code_system_id = code_system_id
        self.code_map_version_id = code_map_version_id
        self.cause_set_version = self.conf.get_id('cause_set_version')
        self.adjust_id = adjust_id
        self.remove_decimal = remove_decimal
        self.dismod_dir = self.conf.get_directory("misdc_dismod_dir").format(
            adjust_id=self.adjust_id
        )
        if adjust_id == 543:
            self.misdiagnosis_version_id = 4
        else:
            self.misdiagnosis_version_id = 3
        self.misdiagnosis_path = self.mcod_prob_path.format(
            adjust_id=self.adjust_id,
            version_id=self.misdiagnosis_version_id,
            code_system_id=self.code_system_id,
        )

        self.cause_package_update = self.get_cause_package_update()

        self.adjust_at_dismod_level = (self.adjust_id != 543) and (
            self.conf.config_type in ["us_counties", "race_ethnicity"]
        )

    def induce_dem_cols(self, df):
        """
        Induce demographic columns using CodSchema
        """
        self.dem_cols = CodSchema.infer_from_data(df).demo_cols
        self.orig_dem_cols = self.dem_cols

    def aggregate_dementia_subcauses(self, df):
        """
        There are new dementia secret subcauses as of GBD2022. These are for
        diagnostics at the mapping stage, but msdc is built such that all dementia is
        assigned to neuro_dementia, so aggregate up here.
        """
        cause_meta = get_current_cause_hierarchy(
            cause_set_id=4, cause_set_version_id=self.cause_set_version,
            force_rerun=False, block_rerun=True
        )
        dementia = get_all_related_causes(self.adjust_id, cause_meta_df=cause_meta)
        dementia.remove(self.adjust_id)

        if len(df.loc[df.cause_id.isin(dementia)]) == 0:
            return df

        cause_map = get_cause_map(
            code_map_version_id=self.code_map_version_id,
            force_rerun=False, block_rerun=True
        )
        dem_code_id = int(cause_map.loc[cause_map.cause_id == self.adjust_id].code_id.iloc[0])

        df.loc[df.cause_id.isin(dementia), 'code_id'] = dem_code_id
        df.loc[df.cause_id.isin(dementia), 'cause_id'] = self.adjust_id
        df = df.groupby(self.dem_cols + ['cause_id', 'code_id'], as_index=False).deaths.sum()

        return df

    def get_cause_package_update(self):
        """
        The MCoD proportions and empirical limits were produced in GBD 2017 - remap causes and
        packages to their closest GBD 2020 equivalents

        This file lists mappings from GBD 2017 causes/garbage packages to their closest
        GBD 2020 equivalents. This mapping is not perfect, since causes/packages
        are often partially decomposed with codes going to different places. It is also
        not exhaustive - certain cause/packages that are not used in misdc are ignored.
        It does attempt to cover all cases where a cause/package is deleted or created by
        providing a reasonable mapping where most of the codes agree

        NOTE: there are proportions for several packages that are not used due to
        code system/package mismatches or overlaps in garbage codes between packages
        For deleted packages whose codes were split into multiple other existing packages, allow
        these codes to use the proportions for their new packages
        """
        update = pd.read_csv(self.conf.get_resource("misdc_cause_package_update"))
        assert update.notnull().values.all()
        update = update.astype({"old": str, "new": str})
        assert set(update.old).isdisjoint(set(update.new))
        return update

    def store_intermediate_data(self, df, move_df, orig_df):
        """
        Write intermediate files for tracking deaths moved and for uncertainty.
        """
        adjust_dir = Path(self.process_dir, str(self.adjust_id))
        adjust_dir.mkdir(exist_ok=True, parents=True)
        if len(move_df) > 0:
            id_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
            df = df.groupby(self.dem_cols + ["cause_id"], as_index=False).deaths.sum()
            ss_df = self.get_ss(df)

            draws_df = self.load_dismod(
                location_id=list(df.location_id.unique()),
                year_id=list(df.year_id.unique()),
                file_name="best_draws",
                expand_parent_to_child=True,
            )
            draw_cols = [d for d in list(draws_df) if "draw_" in d]

            draws_df = draws_df[id_cols + draw_cols + ["est_frac", "population", "est_mx"]].merge(
                move_df.loc[move_df.map_id == str(self.adjust_id)], how="right"
            )
            draws_df = draws_df.merge(ss_df)
            draws_df["added_scalar"] = draws_df["misdiagnosed_scaled"] / (
                draws_df["est_frac"] * draws_df["sample_size"]
            )
            draws_df["completeness_scalar"] = draws_df["sample_size"] / (
                draws_df["est_mx"] * draws_df["population"]
            )
            draws_df = pd.concat(
                [
                    draws_df[self.dem_cols],
                    draws_df[draw_cols].apply(
                        lambda x: x
                        * draws_df["population"]
                        * draws_df["completeness_scalar"]
                        * draws_df["added_scalar"]
                    ),
                    draws_df[["misdiagnosed_scaled"]],
                ],
                axis=1,
            )

            draws_df = draws_df.merge(df.loc[df.cause_id == self.adjust_id], how="right")
            draws_df["deaths"] = draws_df["deaths"] - draws_df["misdiagnosed_scaled"]

            draws_df["deaths_variance"] = (
                draws_df[draw_cols].apply(lambda x: x + draws_df["deaths"]).var(axis=1)
            )
            draws_df["deaths_mean"] = (
                draws_df[draw_cols].apply(lambda x: x + draws_df["deaths"]).mean(axis=1)
            )

            draws_df[draw_cols] = draws_df[draw_cols].apply(
                lambda x: logit((x + 1e-5) / (x + 1e-5 + draws_df["deaths"]))
            )
            draws_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            draws_df[draw_cols] = draws_df[draw_cols].fillna(logit(1 - 1e-5))
            draws_df["logit_frac_variance"] = draws_df[draw_cols].var(axis=1)
            draws_df["logit_frac_mean"] = draws_df[draw_cols].mean(axis=1)

            draws_df = draws_df[
                self.dem_cols
                + ["deaths_mean", "deaths_variance", "logit_frac_mean", "logit_frac_variance"]
            ]
            out_file = adjust_dir / "ui" / f"{self.nid}_{self.extract_type_id}.csv"
            out_file.parent.mkdir(exist_ok=True, parents=True)
            draws_df.to_csv(out_file, index=False)

        move_df["pct_moved"] = move_df["misdiagnosed_scaled"] / move_df["orig_deaths"]
        out_file = adjust_dir / "pct" / f"{self.nid}_{self.extract_type_id}.csv"
        out_file.parent.mkdir(exist_ok=True, parents=True)
        move_df.to_csv(out_file, index=False)

        out_file = adjust_dir / "orig" / f"{self.nid}_{self.extract_type_id}.csv"
        out_file.parent.mkdir(exist_ok=True, parents=True)
        orig_df.to_csv(out_file, index=False)

    def assign_packages(self, df, version_string=None):
        """Load packages from redistribution location."""
        package_df = get_clean_package_map_for_misdc(
            self.code_system_id,
            remove_decimal=self.remove_decimal,
            version_string=version_string,
            block_rerun=True,
            force_rerun=False,
        )
        df = df.merge(package_df, how="left")
        df.loc[df.map_id.astype(str) == "nan", "map_id"] = df["cause_id"]
        df["map_id"] = df["map_id"].astype(str)
        return df

    def add_map_ids(self, df, version_string=None, trim_codes=True):
        """Assign map value to garbage based on package id."""
        df = add_code_metadata(
            df, ["value"], code_map_version_id=self.code_map_version_id, **self.block_rerun
        )
        df["value"] = clean_icd_codes(df["value"], self.remove_decimal)
        if (self.code_system_id in [1, 6]) & (trim_codes):
            df = remove_five_plus_digit_icd_codes(df, code_system_id=self.code_system_id, trim=True)
        df = self.assign_packages(df, version_string=version_string)
        garbage_cause_id = df.cause_id == 743
        garbage_map_id = df.map_id.str.contains("_p_", na=False)
        bad_codes = df.loc[
            ~garbage_cause_id & garbage_map_id, ["value", "map_id", "cause_id"]
        ].drop_duplicates()
        assert len(bad_codes) == 0, "Code(s) mapped to both a cause and a package: {}".format(
            bad_codes
        )
        bad_garbage = df.loc[
            garbage_cause_id & ~garbage_map_id, ["value", "map_id"]
        ].drop_duplicates()
        assert len(bad_garbage) == 0, "Code(s) mapped to garbage but not a package: {}".format(
            bad_garbage
        )
        df.drop("value", axis=1, inplace=True)
        return df

    def get_ss(self, df):
        """Get total deaths in dataset (by demographic group)."""
        df = df.groupby(self.dem_cols, as_index=False).deaths.sum()
        df.rename(index=str, inplace=True, columns={"deaths": "sample_size"})
        return df

    def remap_locations(self, loc_ids):

        df = pd.DataFrame({"location_id": loc_ids})
        add_cols = ["parent_id", "level", "iso3"]
        df = add_location_metadata(
            df,
            add_cols,
            location_set_version_id=self.location_set_version_id,
            **self.block_rerun,
        )
        assert df[add_cols].notnull().values.all()
        df["original_location_id"] = df["location_id"]
        self.norway = False

        if self.adjust_id == 544:
            df.loc[df.parent_id.isin([16, 51, 86, 165, 214]), "location_id"] = df["parent_id"]
            df.loc[df.location_id == 393, "location_id"] = 111
            df.loc[df.location_id.isin([367, 396]), "location_id"] = 86
            df.loc[df.location_id.isin([320, 380]), "location_id"] = 26

            norway_remap = get_norway_subnational_mapping().rename(
                columns={
                    "location_id_new": "original_location_id",
                    "location_id_old": "location_id_pull",
                }
            )
            if df.location_id.isin(norway_remap.original_location_id.unique()).any():
                self.norway = True
            df = df.merge(norway_remap, how="left", on="original_location_id")
            df.location_id.update(df["location_id_pull"])

        df.loc[(df.level == 5) & (df.iso3 == "USA"), "location_id"] = df["parent_id"]

        self.location_remap = df[["original_location_id", "parent_id", "location_id"]].copy()
        return df.location_id.unique().tolist()

    def remap_years(self, year_ids):
        """
        Determine year of model result to use.

        Parkinson's model only estimates up to 2017, dementia
        and afib go up to 2022. For any years after this,
        use the last available year.
        """
        df = pd.DataFrame({"year_id": year_ids})
        df["original_year_id"] = df["year_id"]
        if self.adjust_id in [500, 543]:
            df.loc[df.year_id >= 2023, "year_id"] = 2022
        elif self.adjust_id == 544:
            df.loc[df.year_id >= 2018, "year_id"] = 2017
        self.year_remap = df[["original_year_id", "year_id"]].drop_duplicates()
        return df.year_id.unique().tolist()

    def expand_original_ids(self, df, remap, column, orig_ids):
        """
        Update a given id column of a dataframe using a mapping
        from id --> original id, duplicating rows as necessary
        when 1 id goes to multiple original ids.

        This function is used to remap dismod model results to
        the location/years that they will be used for in the data,
        ensuring that the model results will merge successfully
        with the data. For example, is used to map the national
        Philippines model result for Parkinson's
        to all PHL subnationals.
        """
        orig_col = "original_" + column
        df = (
            df.merge(remap[[column, orig_col]], how="left", on=column)
            .assign(**{column: lambda d: d[orig_col]})
            .drop(orig_col, axis="columns")
        )
        assert df.notnull().values.all()
        assert set(df[column]) == set(orig_ids)
        return df

    def aggregate_to_original_locations(self, df, orig_loc_ids):
        """
        Part two of the fix for the new merged Norway subnationals. Aggregate
        the detailed locations to the merged ones
        """
        dem_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
        report_duplicates(df, dem_cols)

        nor_env = get_env(
            env_run_id=self.conf.get_id("NOR_old_env_run"),
            release_id=self.conf.get_id("NOR_old_release"),
            with_hiv=True,
            with_shock=True,
            force_rerun=False,
            block_rerun=True,
        )
        nor_env_2020 = nor_env.loc[nor_env.year_id == 2019]
        nor_env_2021 = nor_env.loc[nor_env.year_id == 2019]
        nor_env_2022 = nor_env.loc[nor_env.year_id == 2019]
        nor_env_2020["year_id"] = 2020
        nor_env_2021["year_id"] = 2021
        nor_env_2022["year_id"] = 2022
        nor_env = pd.concat(
            [nor_env, nor_env_2020, nor_env_2021, nor_env_2022],
            ignore_index=True
        )
        df = add_envelope(df, env_df=nor_env)
        report_if_merge_fail(df, "mean_env", dem_cols)

        df = self.expand_original_ids(df, self.location_remap, "location_id", orig_loc_ids)

        def produce_aggregates(d):
            draw_cols = [d for d in list(df) if "draw_" in d]
            aggregates = {
                "est_frac": (d["est_frac"] * d["mean_env"]).sum() / d["mean_env"].sum(),
                "est_mx": (d["est_mx"] * d["population"]).sum() / d["population"].sum(),
                "population": d["population"].sum(),
            }
            for draw_col in draw_cols:
                aggregates[draw_col] = (d[draw_col] * d["population"]).sum() / d["population"].sum()
            return pd.Series(aggregates)

        df = df.groupby(dem_cols).apply(produce_aggregates).reset_index()
        assert df.notnull().values.all()
        assert set(df.location_id) == set(orig_loc_ids)
        return df

    def load_dismod(self, location_id, year_id, file_name="best", expand_parent_to_child=True):
        """Retreive interpolated mortality data from DisMod model.

        Note: for dementia (cause_id 543) we're using model outputs from Emma. This
        result comes from "interpolate_custom_dementia_analysis" in the explore
        folder for misdiagnosis correction.
        """
        orig_loc_ids = location_id
        orig_year_ids = year_id
        location_id = self.remap_locations(location_id)
        year_id = self.remap_years(year_id)

        dm_files = sorted(os.listdir(self.dismod_dir))
        assert (
            file_name + ".h5" in dm_files
        ), "file_name {}.h5 not found. " "files found in {}: {}".format(
            file_name, self.dismod_dir, dm_files
        )
        path = "{}/{}.h5".format(self.dismod_dir, file_name)
        read_hdf_kwargs = {"path_or_buf": path}
        if len(location_id) <= 30 and len(year_id) <= 30:
            read_hdf_kwargs.update(
                {"where": "location_id={} and year_id={}".format(location_id, year_id)}
            )
            df = pd.read_hdf(**read_hdf_kwargs)
        elif len(year_id) <= 30:
            read_hdf_kwargs.update({"where": "year_id={}".format(year_id)})
            df = pd.read_hdf(**read_hdf_kwargs)
            df = df.loc[df["location_id"].isin(location_id)]
        else:
            df = pd.read_hdf(**read_hdf_kwargs)
            df = df.loc[(df["location_id"].isin(location_id)) & (df["year_id"].isin(year_id))]

        df = self.expand_original_ids(df, self.year_remap, "year_id", orig_year_ids)

        child_to_parent = (
            (self.location_remap["location_id"] == self.location_remap["parent_id"])
            | (self.location_remap["location_id"] == self.location_remap["original_location_id"])
        ).all()
        if self.norway:
            df = self.aggregate_to_original_locations(df, orig_loc_ids)
        elif (not child_to_parent) or expand_parent_to_child:
            df = self.expand_original_ids(df, self.location_remap, "location_id", orig_loc_ids)
        return df

    def calculate_location_time_distribution(self, df):
        df = df.groupby(self.orig_dem_cols + ["original_location_id"], as_index=False)[
            "deaths"
        ].sum()
        loc_dist = df.groupby(self.dem_cols, as_index=False).apply(
            lambda d: d.assign(pct=d["deaths"] / d["deaths"].sum())
        )
        loc_dist = loc_dist[self.orig_dem_cols + ["pct", "original_location_id"]]
        return loc_dist

    def get_deficit(self, df):
        """
        Identify missing deaths in VR based on CSMR from DisMod.
        """
        df = df.copy()
        orig_loc_ids = list(df.location_id.unique())
        est_df = self.load_dismod(
            orig_loc_ids,
            list(df.year_id.unique()),
            expand_parent_to_child=(not self.adjust_at_dismod_level),
        )
        est_loc_ids = list(est_df.location_id.unique())
        est_matches_orig = set(orig_loc_ids) == set(est_loc_ids)
        dismod_id_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
        ids_match = set(self.dem_cols) == set(
            dismod_id_cols + ["nid", "extract_type_id", "site_id"]
        )

        if (self.adjust_id != 543) & ((not est_matches_orig) | (not ids_match)):
            self.dem_cols = dismod_id_cols + ["nid", "extract_type_id", "site_id"]
            df = df.rename(columns={"location_id": "original_location_id"})
            df = df.merge(
                self.location_remap[["original_location_id", "location_id"]],
                how="left",
                on="original_location_id",
                validate="many_to_one",
            )
            assert df.location_id.notnull().all()
            all_cause_loc_time_dist = self.calculate_location_time_distribution(df)
            df = df.groupby(self.dem_cols + ["cause_id", "map_id"], as_index=False)["deaths"].sum()
            assert df.location_id.isin(est_df.location_id.unique()).all()

        ss_df = self.get_ss(df)
        est_df = est_df.merge(ss_df)
        df = (
            df.loc[df.cause_id == self.adjust_id]
            .groupby(self.dem_cols, as_index=False)
            .deaths.sum()
        )
        df = df.merge(est_df[self.dem_cols + ["est_frac", "sample_size"]], how="right")
        df["deaths"].fillna(0, inplace=True)
        df["deficit"] = (df["est_frac"] * df["sample_size"]) - df["deaths"]

        if self.adjust_id == 543:
            df.loc[(df["deaths"] == 0) & (df["deficit"] < 0), "deficit"] = 0
        else:
            df.loc[df.deficit < 0, "deficit"] = 0

        if (self.adjust_id != 543) & ((not est_matches_orig) | (not ids_match)):
            start_deficit = df["deficit"].sum()
            df = df.merge(all_cause_loc_time_dist, how="left", on=self.dem_cols)
            report_if_merge_fail(df, "pct", self.dem_cols)
            df["location_id"] = df["original_location_id"]
            df["deficit"] = df["deficit"] * df["pct"]
            assert np.isclose(df.deficit.sum(), start_deficit)
            assert df.location_id.isin(orig_loc_ids).all()
            self.dem_cols = self.orig_dem_cols

        df = df[self.dem_cols + ["deficit"]]
        assert df.notnull().values.all()
        return df

    def apply_cause_package_update(self, df, col=None):
        """For a given df, create duplicate rows with a different map_id"""
        col_new = col + "_new"
        cause_package_update = self.cause_package_update.rename(
            columns={"old": col, "new": col_new}
        )
        add_rows = pd.merge(df, cause_package_update, how="inner", on=col)
        add_rows[col] = add_rows[col_new]
        add_rows = add_rows.drop(col_new, axis="columns")
        df = df.append(add_rows)
        assert df[col].notnull().all()
        return df

    def add_mcod_props(self, df):
        """
        Add misdiagnosis probabilities calculated from multiple cause data and subset to
        map_ids that have misdiagnosis probability > 0.
        """
        prop_df = pd.read_hdf(self.misdiagnosis_path, key="data")
        prop_df["cause_mapped"] = prop_df["cause_mapped"].astype(str)
        prop_df = prop_df.loc[prop_df.recode_deaths > 0]
        prop_df = self.apply_cause_package_update(prop_df, col="cause_mapped")

        prop_df = prop_df.groupby(["cause_mapped", "age_group_id", "sex_id"], as_index=False)[
            ["deaths", "recode_deaths"]
        ].sum()
        prop_df["ratio"] = prop_df["recode_deaths"] / (prop_df["deaths"] + prop_df["recode_deaths"])
        assert prop_df.notnull().values.all()
        df = df.merge(
            prop_df[["cause_mapped", "age_group_id", "sex_id", "ratio"]],
            left_on=["map_id", "age_group_id", "sex_id"],
            right_on=["cause_mapped", "age_group_id", "sex_id"],
            how="inner",
        )
        df.drop("cause_mapped", axis=1, inplace=True)
        return df

    def get_dementia_map_id(self, df):
        """Return the package_id with the '_p_' prefix for dementia.

        We created special garbage packages for the purpose of moving deaths from AD
        to garbage. The package name for all of them is 'Dementia'.
        """
        package_map = get_package_map(self.code_system_id, **self.block_rerun)
        dementia = package_map.loc[package_map["package_name"] == "Dementia"]
        assert len(dementia) > 0, "{} is missing the Dementia package".format(self.code_system_id)
        package_ids = dementia["package_id"].unique()
        package_ids = ["_p_" + str(x) for x in package_ids]
        assert len(package_ids) == 1, "More than one dementia package in {}".format(
            self.code_system_id
        )
        return package_ids[0]

    def add_adjust_id_rows(self, df):
        """Add row of deaths to be added to adjust cause."""
        adjust_df = df.groupby(self.dem_cols, as_index=False)["misdiagnosed_scaled", "deaths"].sum()
        adjust_df["map_id"] = str(self.adjust_id)
        df = pd.concat([df, adjust_df], ignore_index=True, sort=True)
        return df

    def apply_empirical_caps(self, df):
        """Based on 5-star VR countries, enforce caps."""
        lim_df = pd.read_csv(
            self.conf.get_resource("misdc_limits").format(
                adjust_id=self.adjust_id, code_system_id=self.code_system_id
            )
        )
        lim_df["map_id"] = lim_df["map_id"].astype(str)
        lim_df = self.apply_cause_package_update(lim_df, col="map_id")
        lim_df = (
            lim_df.groupby(["age_group_id", "sex_id", "map_id"])["pct_limit"]
            .quantile(0.95)
            .reset_index()
        )
        assert lim_df.notnull().values.all()

        df = df.merge(lim_df, how="left")
        df.loc[df.pct_limit.isnull(), "pct_limit"] = 0.95
        df["deaths_limit"] = df["deaths"] * df["pct_limit"]
        df.loc[df.misdiagnosed_scaled > df.deaths_limit, "misdiagnosed_scaled"] = df["deaths_limit"]
        df.drop(["pct_limit", "deaths_limit"], axis=1, inplace=True)
        return df

    def get_deaths_to_move(self, df):
        """Determine how many deaths to move from each code."""
        df = df.groupby(self.dem_cols + ["cause_id", "map_id"], as_index=False).deaths.sum()
        def_df = self.get_deficit(df)
        df = self.add_mcod_props(df)
        df["misdiagnosed"] = df["deaths"] * df["ratio"]
        df["potential_misdiagnosed"] = df.groupby(self.dem_cols).misdiagnosed.transform("sum")
        df = df.merge(def_df)
        if self.adjust_id == 543:
            df = df[df["deficit"] >= 0]
        df["misdiagnosed_scaled"] = df["misdiagnosed"] * (
            df["deficit"] / df["potential_misdiagnosed"]
        )
        df = self.apply_empirical_caps(df)
        df = self.add_adjust_id_rows(df)
        df = df[self.dem_cols + ["map_id", "misdiagnosed_scaled", "deaths"]]
        df = df.rename(columns={"deaths": "orig_deaths"})
        return df

    def get_deaths_to_move_away(self, df, senility_map_id):
        """Determine how many deaths to move away from alzheimer's/dementia.

        As of GBD 2019, we are changing the level of AD, and now in some cases we will be
        moving AD deaths to the senility garbage package to be redistributed proportionally.
        """
        deficit_df = self.get_deficit(df).query("deficit < 0")
        df = df.groupby(self.dem_cols + ["cause_id", "map_id"], as_index=False).deaths.sum()
        df = df.loc[df["cause_id"] == self.adjust_id]
        df["misdiagnosed"] = df["deaths"]
        df["potential_misdiagnosed"] = df.groupby(self.dem_cols).misdiagnosed.transform("sum")
        df = df.merge(deficit_df)
        df["misdiagnosed_scaled"] = df["misdiagnosed"] * (
            df["deficit"] / df["potential_misdiagnosed"]
        )

        senility_df = df.copy()
        senility_df["map_id"] = senility_map_id

        df = pd.concat([df, senility_df], ignore_index=True, sort=True)
        df = df[self.dem_cols + ["map_id", "misdiagnosed_scaled", "deaths"]]
        df = df.rename(columns={"deaths": "orig_deaths"})

        assert (df["misdiagnosed_scaled"] < 0).all()

        return df

    def get_code_ids_from_map_ids(self, map_id):
        cs_map = get_cause_map(code_map_version_id=self.code_map_version_id, **self.block_rerun)
        pkg_map = get_clean_package_map_for_misdc(
            self.code_system_id, remove_decimal=self.remove_decimal, **self.block_rerun
        )
        assert isinstance(map_id, str)
        if map_id.startswith("_p_"):
            values = pkg_map.loc[pkg_map["map_id"] == map_id, "value"].values
            codes = cs_map.loc[cs_map.value.isin(values), "code_id"].values
            cause_id = 743
            assert len(codes) > 0, "No code_ids matching {} in the cause map".format(map_id)
        else:
            codes = cs_map.loc[cs_map.cause_id == int(map_id), "code_id"].values
            cause_id = int(map_id)
            if len(codes) == 0:
                codes = cs_map.loc[cs_map.cause_id == self.cc_code, "code_id"].values
                cause_id = self.cc_code
        code_id = codes[0]
        code_dict = {map_id: code_id}
        cause_dict = {map_id: cause_id}
        return code_dict, cause_dict

    def merge_on_scaled(self, df, move_df, senility_map_id=None):
        """Attach scaled misdiagnosed deaths.

        There could be multiple codes for a single adjust_id, but eventually they'll
        all be going to the same cause_id. If adjust_id is not present in dataset,
        look in map and add to first code.

        If adjust_id is not in map, move to cc_code (aka denominator).
        """
        keep_cols = self.dem_cols + ["map_id", "misdiagnosed_scaled"]
        df = df.merge(move_df[keep_cols], how="outer")
        if df.cause_id.isnull().values.any():
            permitted_null_map_ids = [str(self.adjust_id)]
            if self.adjust_id == 543:
                permitted_null_map_ids.append(senility_map_id)
            null_map_ids = df.loc[df.cause_id.isnull(), "map_id"].unique()
            assert set(null_map_ids).issubset(set(permitted_null_map_ids))
            for map_id in permitted_null_map_ids:
                code_dict, cause_dict = self.get_code_ids_from_map_ids(map_id)
                df.loc[df["code_id"].isnull(), "code_id"] = df["map_id"].map(code_dict)
                df.loc[df["cause_id"].isnull(), "cause_id"] = df["map_id"].map(cause_dict)
            df["deaths"].fillna(0, inplace=True)
            for idvar in [i for i in list(df) if i.endswith("_id")] + ["nid"]:
                if df[idvar].dtype == "float64":
                    df[idvar] = df[idvar].astype(int)

        return df

    def death_jumble(self, df, move_df, senility_map_id=None):
        """Use values we've calculated to actually move deaths in main dataframe."""
        df = self.merge_on_scaled(df, move_df, senility_map_id)
        df["cause_total"] = df.groupby(self.dem_cols + ["map_id"]).deaths.transform("sum")
        df["misdiagnosed_scalar"] = df.apply(
            lambda x: (x["cause_total"] + x["misdiagnosed_scaled"]) / x["cause_total"]
            if x["map_id"] == str(self.adjust_id) and x["cause_total"] > 0
            else (
                (x["cause_total"] - x["misdiagnosed_scaled"]) / x["cause_total"]
                if x["map_id"] != str(self.adjust_id) and x["cause_total"] > 0
                else 0
            ),
            axis=1,
        )
        df["misdiagnosed_scalar"].fillna(1, inplace=True)
        df["deaths"] = df["deaths"] * df["misdiagnosed_scalar"]
        df.loc[(df.map_id == str(self.adjust_id)) & (df.cause_total == 0), "deaths"] = df[
            "misdiagnosed_scaled"
        ]
        if self.adjust_id == 543:
            df.loc[(df.map_id == senility_map_id) & (df.cause_total == 0), "deaths"] = (
                df["misdiagnosed_scaled"] * -1
            )
        df = df.groupby(self.dem_cols + ["code_id", "cause_id"], as_index=False).deaths.sum()
        return df

    def get_computed_dataframe(self, df):
        """For specified cause, use independent miscode probabilities from multiple
        cause data and mortality rates from DisMod to adjust miscoded deaths."""
        self.induce_dem_cols(df)
        if self.adjust_id == 543:
            df = self.aggregate_dementia_subcauses(df)
        start_deaths = df["deaths"].sum()
        start_deaths_target = df.loc[df.cause_id == self.adjust_id, "deaths"].sum()
        start_deaths_cc = df.loc[df.cause_id == self.cc_code, "deaths"].sum()

        df = df.loc[df.deaths > 0]

        print_log_message("Adding map_id column for package_ids")
        df = self.add_map_ids(df)
        orig_df = df.copy()

        print_log_message("Getting deaths to move")
        move_df = self.get_deaths_to_move(df)
        if self.adjust_id == 543:
            senility_map_id = self.get_dementia_map_id(df)
            take_df = self.get_deaths_to_move_away(df, senility_map_id)
            move_df = pd.concat([move_df, take_df], ignore_index=True, sort=True)
        else:
            senility_map_id = None
        print_log_message("Jumbling up deaths")
        df = self.death_jumble(df, move_df, senility_map_id)

        print_log_message("Checking deaths jumbled well")
        end_deaths = df["deaths"].sum()
        end_deaths_target = df.loc[df.cause_id == self.adjust_id, "deaths"].sum()
        end_deaths_cc = df.loc[df.cause_id == self.cc_code, "deaths"].sum()

        assert (
            abs(int(start_deaths) - int(end_deaths)) <= 5
        ), "Bad jumble - added/lost deaths " "(started: {}, ended: {})".format(
            str(int(start_deaths)), str(int(end_deaths))
        )
        assert (df["deaths"] >= 0).all(), "There are negative deaths!"

        print_log_message("Storing intermediate data")
        self.store_intermediate_data(df, move_df, orig_df)

        deaths_moved = int(
            (end_deaths_target + end_deaths_cc) - (start_deaths_target + start_deaths_cc)
        )
        print_log_message("Deaths moved: {}".format(deaths_moved))
        return df

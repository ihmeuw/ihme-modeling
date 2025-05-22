import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import (
    add_cause_metadata,
    add_code_metadata,
    add_nid_metadata,
    get_all_related_causes,
    get_garbage_from_package,
)
from cod_prep.utils import CodSchema, report_if_merge_fail
from cod_prep.utils.redistribution_utils import distribute


class BridgeMapper(CodProcess):
    VA = 8

    def __init__(self, source, cause_meta_df, code_system, project_id):
        self.source = source
        self.code_system = code_system
        self.project_id = project_id
        self.conf = Configurator("standard")
        self.bridge_map_path = Path(self.conf.get_directory("bridge_maps"))
        self.cause_meta_df = cause_meta_df
        self.cache_options = {
            "force_rerun": False,
            "block_rerun": True,
            "cache_results": False,
            "cache_dir": "standard",
        }

    def get_computed_dataframe(self, df):
        df = add_nid_metadata(
            df, ["data_type_id"], project_id=self.project_id, **self.cache_options
        )
        has_verbal_autopsy = self.VA in df["data_type_id"].unique()
        df.drop(columns="data_type_id", inplace=True)

        schema = CodSchema.infer_from_data(df)

        if self.needs_bridging(has_verbal_autopsy):
            file_name = self.get_file_name(has_verbal_autopsy)
            map_df = pd.read_csv(self.bridge_map_path / file_name)
            map_df = map_df[["acause", "bridge_code"]]
            self.validate_bridge_map(map_df, self.cause_meta_df)

            bridge_mapped = add_cause_metadata(
                df, ["acause"], merge_col="cause_id", cause_meta_df=self.cause_meta_df
            )
            bridge_mapped.loc[bridge_mapped["cause_id"] == 606, "acause"] = "gyne_femaleinfert"
            report_if_merge_fail(bridge_mapped, "acause", "cause_id")
            bridge_mapped.drop(["cause_id"], axis=1, inplace=True)

            bridge_mapped = self.redistribute_zz_bridge_codes(bridge_mapped, map_df, schema=schema)

            bridge_mapped = bridge_mapped.merge(map_df, how="left", on="acause")
            bridge_mapped = self.acause_to_bridge_code(bridge_mapped)
            bridge_mapped = add_cause_metadata(
                bridge_mapped,
                ["cause_id"],
                merge_col="acause",
                cause_meta_df=self.cause_meta_df,
            )

            bridge_mapped.loc[bridge_mapped["acause"] == "gyne_femaleinfert", "cause_id"] = 606
            report_if_merge_fail(bridge_mapped, "cause_id", "acause")
            self.diag_df = bridge_mapped
            bridge_mapped = self.clean_up(bridge_mapped, schema)
            return bridge_mapped
        else:
            self.diag_df = df
            df = self.clean_up(df, schema)
            return df

    def validate_bridge_map(self, map_df, cause_meta_df):
        assert (set(map_df.acause)).intersection(
            set(map_df.bridge_code)
        ) == set()

        map_df = add_cause_metadata(
            map_df,
            ["cause_id", "level", "is_estimate"],
            merge_col="acause",
            cause_meta_df=cause_meta_df,
        )
        valid_acauses = cause_meta_df.acause.unique()
        cause_ids = map_df.cause_id.unique()
        acauses = map_df.acause.unique()
        bridge_codes = map_df.loc[~map_df.bridge_code.str.startswith("ZZ-")].bridge_code.unique()
        zz_codes = (
            map_df.loc[map_df.bridge_code.str.startswith("ZZ-")]
            .bridge_code.str.replace("ZZ-", "_")
            .unique()
        )

        assert (
            set(acauses) - set(valid_acauses) == set()
        )
        assert (
            set(bridge_codes) - set(valid_acauses) == set()
        )
        assert (
            set(zz_codes) - set(valid_acauses) == set()
        )

        map_df["children"] = map_df.cause_id.apply(
            lambda x: get_all_related_causes(x, cause_meta_df)
        )
        map_df = map_df.loc[(map_df.level > 2) | (map_df.is_estimate == 1)]
        map_df = map_df.loc[~map_df.bridge_code.str.startswith("ZZ-")]
        map_df = map_df.loc[~map_df.acause.isin(["neo_nmsc", "ntd_leish"])]
        map_df["missing_children"] = map_df.children.apply(lambda x: set(x) - set(cause_ids))
        missing_children = map_df.loc[map_df.missing_children != set()].acause.unique().tolist()
        if missing_children:
            raise AssertionError(
                "You are trying to bridge map these causes: "
                + str(missing_children)
                + ", but not all of their children are present."
            )
        map_df["targets"] = map_df.children.apply(
            lambda x: len(map_df.loc[map_df.cause_id.isin(x)].bridge_code.unique())
        )
        multiple_targets = map_df.loc[map_df.targets != 1].acause.unique().tolist()
        if multiple_targets:
            raise AssertionError(
                "You are tryin to bridge map these causes: "
                + str(multiple_targets)
                + ", but some of their children map to different targets."
            )

    def needs_bridging(self, has_verbal_autopsy):
        code_systems_to_bridge_map = [
            "ICD9_detail",
            "ICD9_BTL",
            "ICD10_tabulated",
            "ICD8_detail",
            "ICD8A",
            "Iran_Mohsen_special_ICD10",
            "China_1991_2002",
            "India_SCD_states_rural",
            "India_MCCD_states_ICD10",
            "India_MCCD_states_ICD9",
            "India_SRS_states_report",
            "Russia_FMD_1989_1998",
            "ICD9_USSR_Tabulation",
            "INDEPTH_ICD10_VA",
            "India_Maharashtra_SCD",
            "India_CRS",
            "PHL_VSR_1999_2005",
        ]
        special_sources_to_bridge_map = [
            "Russia_FMD_ICD9",
            "Iran_Mohsen_special_ICD10",
            "Iran_2011",
            "India_SRS_states_report",
            "India_MCCD_Orissa_ICD10",
            "ETH_DHIS2",
        ]
        if (
            has_verbal_autopsy
            | (self.code_system in code_systems_to_bridge_map)
            | (self.source in special_sources_to_bridge_map)
        ):
            return True
        else:
            return False

    def get_file_name(self, has_verbal_autopsy):
        source_to_sheet = {
            "India_MCCD_Orissa_ICD10": "India_MCCD_states_ICD10",
            "India_MCCD_Delhi_ICD10": "India_MCCD_states_ICD10",
            "Thailand_Public_Health_Statistics": "ICD10_tabulated",
            "India_SRS_states_report": "India_SRS_states_report",
            "UKR_databank_ICD10_tab": "ICD10_tabulated",
            "Russia_FMD_ICD9": "Russia_FMD_1989_1998",
            "Iran_Mohsen_special_ICD10": "Iran_Mohsen_special_ICD10",
            "Iran_2011": "Iran_Mohsen_special_ICD10",
            "ETH_DHIS2": "DHIS2",
        }
        if has_verbal_autopsy and (self.source != "India_SRS_states_report"):
            file_name = "INDEPTH_ICD10_VA"
        else:
            file_name = source_to_sheet.get(self.source, self.code_system)
        return file_name + ".csv"

    def redistribute_zz_bridge_codes(self, df, map_df, schema: CodSchema):
        grouping_cols = schema.demo_cols
        val_cols = schema.value_cols
        start_deaths = {col: df.groupby(grouping_cols)[col].sum() for col in val_cols}

        zz_code_idxs = map_df["bridge_code"].str.startswith("ZZ-")
        zz_code_targets = (
            map_df.loc[zz_code_idxs, ["bridge_code"]]
            .drop_duplicates()
            .assign(acause=lambda d: d["bridge_code"].str.replace("ZZ-", "_"))
            .merge(self.cause_meta_df, on="acause")
            .sort_values(["level", "acause"], ascending=False)
            .loc[:, "bridge_code"]
            .tolist()
        )
        all_causes_to_zz_codes = set(map_df.loc[zz_code_idxs, "acause"])

        for zz_code in zz_code_targets:
            child_cause_ids = get_all_related_causes(
                zz_code.strip().replace("ZZ-", "_"), self.cause_meta_df
            )
            child_causes = self.cause_meta_df.loc[
                self.cause_meta_df["cause_id"].isin(child_cause_ids), "acause"
            ].tolist()

            acauses_to_redistribute = map_df.loc[map_df["bridge_code"] == zz_code, "acause"]
            to_redistribute = df["acause"].isin(acauses_to_redistribute)
            valid_child_causes = set(child_causes) - all_causes_to_zz_codes


            values_to_include = {
                "acause": valid_child_causes,
            }
            for col in grouping_cols:
                values_to_include[col] = df.loc[to_redistribute, col].unique()
            distributed = distribute(
                df[to_redistribute],
                based_on=df[df["acause"].isin(valid_child_causes)],
                distribute_over="acause",
                within=grouping_cols,
                value_col="deaths",
                values_to_include=values_to_include,
                base_value=0.001,
            )
            report_if_merge_fail(distributed, check_col="acause", merge_cols=grouping_cols)
            df = df.merge(
                distributed[grouping_cols + ["acause", "deaths"]],
                how="outer",
                on=grouping_cols + ["acause"],
                suffixes=("", "_new"),
            )
            df[val_cols + ["deaths_new"]] = df[val_cols + ["deaths_new"]].fillna(0)
            df.loc[df["acause"].isin(acauses_to_redistribute), "deaths"] = 0
            df["deaths"] += df["deaths_new"]
            df.drop(columns="deaths_new", inplace=True)

            for col in val_cols:
                end_deaths = df.groupby(grouping_cols)[col].sum()
                assert np.allclose(start_deaths[col], end_deaths)
        return df

    def acause_to_bridge_code(self, df):
        df["acause"].update(df["bridge_code"].str.replace("ZZ-", "_"))
        return df

    def get_diagnostic_dataframe(self):
        if self.diag_df is None:
        else:
            return self.diag_df

    def clean_up(self, df, schema):
        df = df.groupby(schema.id_cols, as_index=False)[schema.value_cols].sum()
        return df


class GBDCauseMapper(CodProcess):
    unnecessary_causes = [920, 744]
    cache_dir = str()

    def __init__(self, project_id, cause_set_version_id, code_map):
        self.cg = Configurator("standard")
        self.cache_dir = self.cg.get_directory("db_cache")
        self.cause_set_version_id = cause_set_version_id
        self.code_map = code_map
        self.project_id = project_id

    def get_computed_dataframe(self, df, code_system_id):

        df = self.special_cause_reassignment(df, code_system_id)

        df = add_code_metadata(df, ["cause_id"], code_system_id, code_map=self.code_map)
        report_if_merge_fail(df, "cause_id", "code_id")

        self.assert_valid_mappings(df, code_system_id)
        df = self.drop_unnecessary_causes(df, self.unnecessary_causes)
        df = self.collapse_and_sum_by_deaths(df)
        return df

    def drop_unnecessary_causes(self, df, unnecessary_causes):
        df = df.copy()
        df = df[~df["cause_id"].isin(unnecessary_causes)]
        return df

    def special_cause_reassignment(self, df, code_system_id):
        cache_args = {
            "force_rerun": False,
            "block_rerun": True,
            "cache_dir": "standard",
            "cache_results": False,
        }
        df = add_nid_metadata(df, "source", project_id=self.project_id, **cache_args)

        if (df["source"] == "India_SRS_states_report").any() and (code_system_id == 1):
            assert (df["source"] == "India_SRS_states_report").all()

            df = add_code_metadata(df, "value", code_system_id=code_system_id, **cache_args)

            custom_grbg = pd.read_csv(self.cg.get_resource("srs_custom_garbage_groups"))
            custom_grbg = custom_grbg.query("active == 1")
            custom_grbg["value"] = custom_grbg["srs_custom_garbage_group"]
            custom_grbg = add_code_metadata(
                custom_grbg,
                "code_id",
                code_system_id=code_system_id,
                merge_col="value",
                **cache_args,
            )
            custom_grbg = custom_grbg.rename(columns={"code_id": "new_code_id"})
            custom_grbg = custom_grbg[["package_id", "new_code_id"]]

            gp_dfs = []
            for package_id in custom_grbg.package_id.unique():
                gp_df = get_garbage_from_package(
                    code_system_id, package_id, package_arg_type="package_id"
                )
                assert len(gp_df) != 0
                gp_dfs.append(gp_df)
            gp_df = pd.concat(gp_dfs, ignore_index=True)

            gp_df = gp_df.merge(custom_grbg, how="left")
            report_if_merge_fail(gp_df, "new_code_id", "package_id")
            gp_df = gp_df[["value", "new_code_id"]]
            gp_df["value"] = gp_df["value"].str.strip()

            df = df.merge(gp_df, how="left", on="value")
            df.loc[df["new_code_id"].notnull(), "code_id"] = df["new_code_id"]
            df["code_id"] = df["code_id"].astype(int)
            df = df.drop(["new_code_id", "value"], axis=1)

        df = df.drop("source", axis=1)

        china_cdc_2008 = (df["nid"] == 270005) & (df["extract_type_id"] == 2)
        five_dig_code = df["code_id"] == 13243
        df.loc[china_cdc_2008 & five_dig_code, "code_id"] = 13242
        if code_system_id == 1:
            B342_code_id = self.code_map.loc[self.code_map.value == "B34.2"].code_id.iloc[0]
            U07_code_id = self.code_map.loc[self.code_map.value == "U07"].code_id.iloc[0]
            df.loc[
                (df.year_id.isin([2020, 2021])) & (df.code_id == B342_code_id), "code_id"
            ] = U07_code_id
        elif code_system_id == 6:
            code_ids_034 = (
                self.code_map.loc[
                    self.code_map.value.isin(["043", "043.0", "043.1", "043.2", "043.3"])
                ]
                .code_id.unique()
                .tolist()
            )
            covid_code_id = self.code_map.loc[self.code_map.value == "480.3"].code_id.iloc[0]
            df.loc[
                (df.year_id.isin([2020, 2021])) & (df.code_id.isin(code_ids_034)), "code_id"
            ] = covid_code_id

        return df

    def collapse_and_sum_by_deaths(self, df):
        schema = CodSchema.infer_from_data(df)
        return df.groupby(schema.id_cols, as_index=False)[schema.value_cols].sum()

    def assert_valid_mappings(self, df, code_system_id):
        df = add_code_metadata(
            df,
            ["value"],
            code_system_id,
            force_rerun=False,
            block_rerun=True,
            cache_dir=self.cache_dir,
        )
        report_if_merge_fail(df, "value", "code_id")
        df = add_cause_metadata(
            df,
            ["acause"],
            cause_set_version_id=self.cause_set_version_id,
            force_rerun=False,
            block_rerun=True,
            cache_dir=self.cache_dir,
        )
        report_if_merge_fail(df, "acause", "cause_id")
        check_df = df.loc[df["value"].str.startswith("acause_")]
        check_df["implied_acause"] = check_df["value"].str.replace("acause_", "", 1)

        check_df.loc[check_df["value"].str.contains("acause__gc"), "implied_acause"] = "_gc"
        bad_df = check_df.loc[check_df["acause"] != check_df["implied_acause"]]
        if len(bad_df) > 0:
            bad_stuff = bad_df[["value", "acause"]].drop_duplicates()
            warnings.warn("These code values do not match their acause: " "\n{}".format(bad_stuff))

        bad_acauses = ["acause_digest_gastrititis", "acause_hiv_tb", "acause_tb_drug"]

        bad_df = df.loc[df["value"].isin(bad_acauses)].value.unique()
        if len(bad_df) > 0:
            raise AssertionError("Found these bad code values in the data: {}".format(bad_stuff))

    def check_mohsens_map(self):
        raise NotImplementedError

    def get_diagnostic_data_frame(self):
        raise NotImplementedError

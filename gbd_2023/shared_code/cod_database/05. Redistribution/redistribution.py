import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Hashable, List, Optional, Tuple

import numpy as np
import pandas as pd

from cod_prep.claude.claude_io import makedirs_safely
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders.packages import CodeStringMapper, Package, get_rdp_package_dir
from cod_prep.utils import CodSchema
from cod_prep.utils import data_transforms as dt
from cod_prep.utils import misc, write_df

def process_redistribution_target_restrictions(
    target_restrictions: pd.DataFrame,
    cause_meta_df: pd.DataFrame,
    allow_distribution_onto_yld_only: bool,
    allow_logic_overrides_to_override_cause_hierarchy: bool,
) -> pd.DataFrame:
    return (
        cause_meta_df.loc[
            :, ["cause_id", "acause", "male", "female", "yll_age_start", "yll_age_end", "yld_only"]
        ]
        .astype({"male": bool, "female": bool})
        .merge(
            target_restrictions,
            on="acause",
            how="outer",
            validate="one_to_one",
        )
        .assign(
            male=lambda d: np.where(d["male"] == False, "sex_id != 1", None), 
            female=lambda d: np.where(d["female"] == False, "sex_id != 2", None),  
            yll_age_start=lambda d: "age >= " + d["yll_age_start"].astype("string"),
            yll_age_end=lambda d: "age <= " + d["yll_age_end"].astype("string"),
            yld_only=lambda d: np.where( 
                (not allow_distribution_onto_yld_only) & (d["yld_only"] == 1),
                "False",
                None,
            ),
            restrictions=lambda d: d.loc[
                :, ["male", "female", "yll_age_start", "yll_age_end", "statement", "yld_only"]
            ].apply(lambda r: " and ".join(r.dropna()), axis=1),
        )
        .assign(
            restrictions=lambda d: d["restrictions"].mask(
                d["statement"].notna(),
                d["statement"]
                if allow_logic_overrides_to_override_cause_hierarchy
                else d["restrictions"],
            )
        )
        .query("restrictions != ''")
        .loc[:, ["cause_id", "restrictions"]]
    )


@dataclass
class RedistributionData:


    demo: pd.DataFrame
    cause: pd.DataFrame
    rd_demo: pd.DataFrame
    restricted_rd_demo_code_combos: pd.DataFrame
    rd_demo_prop_combos: pd.DataFrame
    residual_code_id: int

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        proportional_cols: List[Hashable],
        col_meta: Optional[Dict[str, Dict[str, Any]]],
        conf: Configurator,
        loc_meta_df: pd.DataFrame,
        age_meta_df: pd.DataFrame,
        cause_meta_df: pd.DataFrame,
        cause_map: pd.DataFrame,
        remove_decimal: bool,
        residual_code: str,
        allow_distribution_onto_yld_only: bool,
        allow_logic_overrides_to_override_cause_hierarchy: bool,
    ) -> "RedistributionData":
        loc_meta_df = loc_meta_df.assign(
            country_id=lambda d: d["path_to_top_parent"]
            .str.split(",")
            .map(lambda x: x[3] if len(x) > 3 else None)
            .astype(float),
            admin1_or_above_id=lambda d: d["path_to_top_parent"]
            .str.replace("4749,", "", regex=False)
            .str.split(",")
            .map(lambda x: x[min(len(x) - 1, 4)])
            .astype(int),
            tmp_developed=lambda d: d["developed"].astype(float) == 1,
            developed=lambda d: d["country_id"].map(d.set_index("location_id")["tmp_developed"]),
        )

        demo_cols = CodSchema.infer_from_data(df, metadata=col_meta).demo_cols
        demo, cause = df.pipe(dt.extract_sub_table, demo_cols, id_col="demographic_id")
        cause = (
            cause.merge(cause_map.loc[:, ["code_id", "value"]])
            .drop(columns="code_id")
            .pipe(
                CodeStringMapper(
                    code_system_id=cause_map["code_system_id"].iat[0],
                    remove_decimal=remove_decimal,
                    contains_ranges=False,
                    cause_map=cause_map.rename(columns={"value": "code"}),
                    cause_metadata=cause_meta_df,
                    expand_acauses_to_most_detailed=True,
                ).maybe_assign_code_id,
                "value",
                package_version_id=-1,
            )
            .drop(columns="value")
        )
        rd_demo, demo = (
            demo.merge(loc_meta_df.loc[:, ["location_id", "country_id", "admin1_or_above_id"]])
            .pipe(dt.make_unique_index_col, proportional_cols, "proportion_id")
            .drop(columns=["admin1_or_above_id"])
            .pipe(
                dt.extract_sub_table,
                ["country_id", "year_id", "age_group_id", "sex_id"],
                "rd_demo_id",
                drop_sub_columns=False,
            )
        )
        demo = demo.drop(columns="country_id")

        country_remap = pd.read_csv(
            conf.get_resource("redistribution_location_remap_for_weights")
        ).set_index("country_id")["new_country_id"]
        rd_demo = (
            rd_demo.replace({"country_id": country_remap})
            .merge(
                loc_meta_df.loc[
                    :, ["country_id", "region_id", "super_region_id", "developed"]
                ].drop_duplicates()
            )
            .merge(
                age_meta_df.rename(columns={"simple_age": "age"}).loc[:, ["age_group_id", "age"]],
                validate="many_to_one",
            )
            .astype(
                {
                    c: int
                    for c in [
                        "country_id",
                        "region_id",
                        "super_region_id",
                        "year_id",
                        "sex_id",
                    ]
                }
            )
            .loc[
                :,
                [
                    "rd_demo_id",
                    "country_id",
                    "region_id",
                    "super_region_id",
                    "developed",
                    "year_id",
                    "age",
                    "sex_id",
                ],
            ]
        )

        restrictions, cause_restriction_map = process_redistribution_target_restrictions(
            pd.read_csv(conf.get_resource("redistribution_demographic_target_restrictions")),
            cause_meta_df,
            allow_distribution_onto_yld_only=allow_distribution_onto_yld_only,
            allow_logic_overrides_to_override_cause_hierarchy=allow_logic_overrides_to_override_cause_hierarchy,
        ).pipe(dt.extract_sub_table, "restrictions", "restriction_id")
        restricted_rd_demo_code_combos = (
            pd.concat(
                rd_demo.assign(
                    evaluated=lambda d: d.eval(restriction),
                    restriction_id=restriction_id,
                ).loc[:, ["rd_demo_id", "restriction_id", "evaluated"]]
                for restriction, restriction_id in restrictions.itertuples(index=False)
            )
            .query("not evaluated")
            .merge(cause_restriction_map)
            .merge(cause_map.loc[:, ["cause_id", "code_id"]])
            .loc[:, ["rd_demo_id", "code_id"]]
        )

        rd_demo_prop_combos = demo.loc[:, ["rd_demo_id", "proportion_id"]].drop_duplicates()
        residual_code_id = cause_map.loc[lambda d: d["value"] == residual_code, "code_id"].item()
        return cls(
            demo,
            cause,
            rd_demo,
            restricted_rd_demo_code_combos,
            rd_demo_prop_combos,
            residual_code_id,
        )

    def to_dataframe(self) -> pd.DataFrame:
        return (
            self.demo.drop(columns=["rd_demo_id", "proportion_id"])
            .merge(self.cause, on="demographic_id")
            .drop(columns="demographic_id")
        )

    def get_package_anti_filters(self) -> List[Tuple[str, str, Callable[[pd.Series], pd.Series]]]:
        countries = set(self.rd_demo["country_id"].drop_duplicates().astype(str))
        min_year = str(self.rd_demo["year_id"].min())
        max_year = str(self.rd_demo["year_id"].max())
        return [
            ("country_id", "==", lambda s: ~s.isin(countries)),
            ("year_id", "<", lambda s: s <= min_year),
            ("year_id", "<=", lambda s: s < min_year),
            ("year_id", ">", lambda s: s >= max_year),
            ("year_id", ">=", lambda s: s > max_year),
            ("year_id", "==", lambda s: (s < min_year) | (s > max_year)),
        ]

    def collapse_cause(self) -> "RedistributionData":
        cause = self.cause.groupby([c for c in self.cause if c != "deaths"], as_index=False).agg(
            {"deaths": "sum"}
        )
        return RedistributionData(
            demo=self.demo,
            cause=cause,
            rd_demo=self.rd_demo,
            restricted_rd_demo_code_combos=self.restricted_rd_demo_code_combos,
            rd_demo_prop_combos=self.rd_demo_prop_combos,
            residual_code_id=self.residual_code_id,
        )


def get_rd_demo_wgt_group_map(package: Package, rd_demo: pd.DataFrame) -> pd.DataFrame:
    shared_wgt_group_ids = np.full(len(rd_demo), np.nan)
    for swg_id, expr in package.get_source_exprs().itertuples(index=False):
        evaluated = rd_demo.eval(expr)
        if not np.isnan(shared_wgt_group_ids[evaluated]).all():
            raise RuntimeError(
                f"There are overlaps in the logic groups for package {package.package_name} "
                f"(shared package version ID {package.shared_package_version_id}) "
                f"""with logic expression "{expr}" (shared weight group ID {swg_id})."""
            )
        shared_wgt_group_ids[evaluated] = swg_id

    failed = np.isnan(shared_wgt_group_ids)

    if failed.any():
        with pd.option_context("display.max_columns", 50, "display.max_rows", 100):
            raise RuntimeError(
                "Some garbage demographics were not matched:\n" + str(rd_demo.loc[failed, :])
            )
    return pd.DataFrame(
        {
            "rd_demo_id": rd_demo.loc[:, "rd_demo_id"].to_numpy(),
            "shared_wgt_group_id": shared_wgt_group_ids.astype(int),
        }
    )


def get_weights(
    package: Package,
    non_garbage: pd.DataFrame,
    demo: pd.DataFrame,
    rd_demo_wgt_group_map: pd.DataFrame,
    restricted_rd_demo_code_combos: pd.DataFrame,
    rd_demo_prop_combos: pd.DataFrame,
    residual_code_id: int,
) -> pd.DataFrame:
    return (
        rd_demo_wgt_group_map.merge(package.wgt)  
        .drop(columns="shared_wgt_group_id")  
        .merge(package.target)  
        .merge(restricted_rd_demo_code_combos, how="left", indicator="is_restricted")
        .eval("is_restricted = is_restricted == 'both'")  
        .merge(rd_demo_prop_combos) 
        .pipe(
            lambda d: d.merge(
                non_garbage.query("deaths != 0")
                .loc[lambda ng: ng["code_id"].isin(package.target["code_id"]), :]
                .merge(demo.loc[:, ["demographic_id", "proportion_id"]])
                .loc[lambda ng: ng["proportion_id"].isin(d["proportion_id"]), :]
                .groupby(["proportion_id", "code_id"], as_index=False)
                .agg({"deaths": "sum"}),
                on=["proportion_id", "code_id"],
                how="left",
            )
        ) 
        .assign(
            no_data=lambda d: (
                d.assign(no_data=d["deaths"].isna() | d["is_restricted"])
                .groupby(["rd_demo_id", "proportion_id", "shared_group_id"])["no_data"]
                .transform("all")
            ),
            no_targets=lambda d: (
                d.groupby(["rd_demo_id", "proportion_id", "shared_group_id"])[
                    "is_restricted"
                ].transform("all")
            ),
            deaths=lambda d: (
                (d["deaths"].fillna(0) + (0.001 if package.create_targets else 0)).mask(
                    d["is_restricted"], 0
                )
                .mask((d["no_data"] & ~d["is_restricted"]) | d["no_targets"], 1)
            ),
            wgt=lambda d: (
                d["wgt"]
                * d["deaths"]
                / d.groupby(["rd_demo_id", "proportion_id", "shared_group_id"])["deaths"].transform(
                    "sum"
                )
            ),
            code_id=lambda d: d["code_id"].mask(d["no_targets"], residual_code_id),
        )
        .query("wgt > 0")
        .groupby(["rd_demo_id", "proportion_id", "code_id"], as_index=False)
        .agg({"wgt": "sum"})
        .assign(
            wgt=lambda d: d["wgt"]
            / d.groupby(["rd_demo_id", "proportion_id"])["wgt"].transform("sum")
        )
        .loc[:, ["rd_demo_id", "proportion_id", "wgt", "code_id"]]
    )


def execute_package(
    data: RedistributionData, package: Package
) -> Tuple[RedistributionData, pd.DataFrame]:
    pid = package.package_id
    is_package_garbage = data.cause["code_id"].isin(package.codes)
    if not is_package_garbage.any():
        redistributed = (
            pd.DataFrame(columns=["demographic_id", "code_id", "deaths"])
            .astype({"demographic_id": int, "code_id": int, "deaths": float})
            .assign(package_id=package.package_id)
        )
        return data, redistributed
    garbage, non_garbage = dt.split(data.cause, lambda _: is_package_garbage)
    garbage = (
        garbage.groupby("demographic_id", as_index=False)
        .agg({"deaths": "sum"})
        .merge(data.demo.loc[:, ["demographic_id", "rd_demo_id", "proportion_id"]])
    )
    start_deaths = garbage["deaths"].sum()
    package = package.filter(data.get_package_anti_filters())
    rd_demo_wgt_group_map = get_rd_demo_wgt_group_map(
        package,
        data.rd_demo.loc[lambda d: d["rd_demo_id"].isin(garbage["rd_demo_id"]), :],
    )

    weights = get_weights(
        package,
        non_garbage,
        data.demo,
        rd_demo_wgt_group_map,
        data.restricted_rd_demo_code_combos,
        data.rd_demo_prop_combos,
        data.residual_code_id,
    )

    redistributed = (
        garbage.merge(weights, how="left", on=["rd_demo_id", "proportion_id"])
        .pipe(misc.report_if_merge_fail, "code_id", ["rd_demo_id", "proportion_id"]).eval(
            "deaths = deaths * wgt"
        ) 
    )
    end_deaths = redistributed["deaths"].sum()
    if not np.isclose(start_deaths, end_deaths):
        raise RuntimeError(
            "Number of deaths changed while executing "
            f"{package.package_name} ({package.package_id}):\n"
            f"Start: {start_deaths}, end: {end_deaths}"
        )

    out = pd.concat(
        [
            non_garbage.loc[:, data.cause.columns],
            redistributed.loc[:, data.cause.columns],
        ]
    )
    return RedistributionData(
        demo=data.demo,
        cause=out,
        rd_demo=data.rd_demo,
        restricted_rd_demo_code_combos=data.restricted_rd_demo_code_combos,
        rd_demo_prop_combos=data.rd_demo_prop_combos,
        residual_code_id=data.residual_code_id,
    ), redistributed.loc[:, data.cause.columns].assign(package_id=package.package_id)


class Redistributor:
    def __init__(
        self,
        conf: Configurator,
        code_system_id: int,
        proportional_cols: List[Hashable],
        loc_meta_df: pd.DataFrame,
        age_meta_df: pd.DataFrame,
        cause_meta_df: pd.DataFrame,
        cause_map: pd.DataFrame,
        remove_decimal: bool,
        col_meta: Optional[Dict[str, Dict[str, Any]]] = None,
        residual_code: str = "ZZZ",
        diagnostics_path: Optional[Path] = None,
        collapse_interval: int = 10,
        allow_distribution_onto_yld_only: bool = True,
        allow_logic_overrides_to_override_cause_hierarchy: bool = True,
    ):
        self.conf = conf
        self.proportional_cols = proportional_cols
        self.loc_meta_df = loc_meta_df
        self.age_meta_df = age_meta_df
        self.cause_meta_df = cause_meta_df
        self.cause_map = cause_map
        self.remove_decimal = remove_decimal
        self.packages = self.get_all_packages(self.conf, code_system_id)
        self.col_meta = col_meta
        self.residual_code = residual_code
        self.diagnostics_path = diagnostics_path
        self.collapse_interval = collapse_interval
        self.allow_distribution_onto_yld_only = allow_distribution_onto_yld_only
        self.allow_logic_overrides_to_override_cause_hierarchy = (
            allow_logic_overrides_to_override_cause_hierarchy
        )

    @staticmethod
    def get_all_packages(conf: Configurator, code_system_id: int) -> List[Package]:
        package_dir = get_rdp_package_dir(conf, code_system_id)
        package_ids = json.loads(package_dir.joinpath("package_list.json").read_text())
        return [Package.load(conf, code_system_id, package_id) for package_id in package_ids]

    def _write_diagnostics_chunk(self, all_weights: List[pd.DataFrame], chunk: int) -> None:
        if self.diagnostics_path:
            write_df(
                pd.concat(all_weights),
                self.diagnostics_path / "magic_tables" / f"{chunk}.parquet",
            )

    def run_redistribution(self, df: pd.DataFrame) -> pd.DataFrame:
        data = RedistributionData.from_dataframe(
            df,
            proportional_cols=self.proportional_cols,
            conf=self.conf,
            loc_meta_df=self.loc_meta_df,
            age_meta_df=self.age_meta_df,
            cause_meta_df=self.cause_meta_df,
            cause_map=self.cause_map,
            remove_decimal=self.remove_decimal,
            col_meta=self.col_meta,
            residual_code=self.residual_code,
            allow_distribution_onto_yld_only=self.allow_distribution_onto_yld_only,
            allow_logic_overrides_to_override_cause_hierarchy=self.allow_logic_overrides_to_override_cause_hierarchy,
        )
        if self.diagnostics_path:
            shutil.rmtree(self.diagnostics_path, ignore_errors=True)
            makedirs_safely(self.diagnostics_path)
            write_df(df, self.diagnostics_path / "original.parquet")
            write_df(data.demo, self.diagnostics_path / "demo.parquet")
            makedirs_safely(self.diagnostics_path.joinpath("magic_tables"))
        diagnostics = []
        for i, package in enumerate(self.packages):
            data, pkg_diagnostics = execute_package(data, package)
            if self.diagnostics_path:
                diagnostics.append(pkg_diagnostics)
            if (i + 1) % self.collapse_interval == 0:
                data = data.collapse_cause()
                self._write_diagnostics_chunk(diagnostics, chunk=i // self.collapse_interval)
                diagnostics = []
        if diagnostics:
            self._write_diagnostics_chunk(
                diagnostics, chunk=len(self.packages) // self.collapse_interval
            )
        return data.collapse_cause().to_dataframe()

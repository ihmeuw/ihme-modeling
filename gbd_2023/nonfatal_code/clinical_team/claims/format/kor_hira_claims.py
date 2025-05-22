import argparse
import functools
import platform
from datetime import datetime
from functools import lru_cache
from typing import Callable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from clinical_db_tools.clinical_metadata_utils.api.pipeline_wrappers import (
    ClaimsWrappers,
)
from crosscutting_functions import demographic, legacy_pipeline
from crosscutting_functions.pipeline import get_release_id
from crosscutting_functions.mapping import clinical_mapping
from db_queries import get_population
from loguru import logger

from inpatient.Clinical_Runs.utils import constants

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"


class KorHiraClaims:
    def __init__(
        self,
        run_id: int,
        clinical_data_type_id: int = 5,
        clinical_age_group_set_id: int = 7,
        write_final_data: bool = False,
    ):
        """
        Args:
            run_id (int): Clinical run_id which exists in run_metadata.
            clinical_data_type_id (int, optional): Type ID for data.
                Defaults to 5 for claims - flagged under KOR.
            clinical_age_group_set_id (int, optional): Clinical age binning method ID.
                Defaults to 3.
            write_final_output (bool): Write final estimate data out. True writes out.
                If False, will return the final estimate table instead of writting out.

        """

        self.run_id = run_id
        self.map_version = self.get_map_version()
        self.pop_run_id = self.get_pop_run_id()
        self.clinical_data_type_id = clinical_data_type_id
        self.clinical_age_group_set_id = clinical_age_group_set_id
        self.release_id = get_release_id(run_id=run_id)
        self.write_final_output = write_final_data

    def create_claims_data(self) -> Union[pd.DataFrame, None]:
        """
        Main function used to create estimates for KOR_HIRA dataset.
        Produces estimate id 27 only for KOR claims source.
        Will not return anything if saving final data. For development,
        if not writing final data, will return the final concatted dataframe.
        """

        self.create_logger()
        logger.info(f"Class Attributes: {self.__dict__}")
        self.pull_data()
        self.make_estimate_27()

        if self.write_final_output:
            self.write_final_data()
        else:
            return self.df_estimate_27

    @lru_cache()
    def pull_run_metadata(self) -> pd.DataFrame:
        cw = ClaimsWrappers(
            run_id=self.run_id,
            clinical_data_type_id=self.clinical_data_type_id,
            odbc_profile=constants.RunDBSettings.cw_profile,
        )
        run_metadata = cw.pull_run_metadata()
        return run_metadata

    def get_pop_run_id(self) -> int:
        run_metadata = self.pull_run_metadata()
        return run_metadata.population_run_id.iloc[0]

    def get_map_version(self) -> int:
        run_metadata = self.pull_run_metadata()
        return run_metadata.map_version.iloc[0]

    def create_logger(self) -> None:
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        log_path = f"{self.base_dir}/FILEPATH/{timestamp}.log"
        logger.add(sink=log_path)

    def write_final_data(self) -> None:
        write_path = (
            f"{self.base_dir}/FILEPATH/kor_hira_claims.csv"
        )
        self.df_estimate_27.to_csv(write_path, index=False)

    @property
    def base_dir(self):
        return "FILEPATH"

    def gbd_func_seq(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a function sequence to create bundle estimates for a
        subsetted dataframe. Once sequence is created, applies it to
        the subset df.

        Args:
            df (pd.DataFrame): DataFrame only contatining records pertinent
                to a specific estimate.
        """

        func_seq = [
            self.agg_to_bundle,
            self.apply_age_group_id,
            self.make_zeros,
            self.apply_age_sex_restrictions,
            self.fill_missing_square_data,
            self.apply_gbd_pop,
        ]

        func_composition = self.compose(*func_seq)
        df = func_composition(df)

        return df

    def compose(self, *funcs: Union[List[Callable], Tuple[Callable]]):
        """
        Creates and executes a sequence of functions.
        """
        return functools.reduce(lambda f, g: lambda x: g(f(x)), funcs)

    def make_zeros(self, df: pd.DataFrame) -> pd.DataFrame:
        """takes a dataframe and returns the square of every age/sex/cause
        type(bundle or baby seq) which **exists** in the given
        dataframe but only the years available for each location id
        """
        return legacy_pipeline.make_zeros(df, cols_to_square="val")

    def fill_missing_square_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill Null feture values (e.g. nid, age_end) for newly created squared rows.
        """
        col_order = df.columns.tolist()
        og = df[df["val"] != 0].copy()
        # This is only discharges
        df = df[df["val"] == 0].copy()

        missing_col_vals = [e for e in df.columns.tolist() if all(df[e].isnull())]
        df.drop(missing_col_vals, axis=1, inplace=True)

        exclude = ["val", "bundle_id", "bundle_measure"]
        id_cols = [e for e in df.columns.tolist() if e not in exclude]
        feature_vals = [e for e in og.columns.tolist() if e not in exclude]

        df = df.merge(og[feature_vals].drop_duplicates(), how="left", on=id_cols)
        df = df.merge(
            og[["bundle_id", "bundle_measure"]].drop_duplicates(),
            how="left",
            on="bundle_id",
        )

        df = pd.concat([og, df], sort=False, ignore_index=True)

        if df.isnull().sum().sum() != 0:
            raise RuntimeError("There are null values")

        return df[col_order]

    def apply_claims_shape(self, df: pd.DataFrame, estimate_id: int) -> pd.DataFrame:
        """
        Prep data for final formatting in clean_final_bundle for a given estimate.
        """
        df.rename({"val": "cases"}, axis=1, inplace=True)
        df["cases"] = df["cases"].astype(int)
        # create the rate estimate column
        df["mean"] = df["cases"] / df["sample_size"]

        # create upper lower with nulls. Filled in upload
        df["upper"] = np.nan
        df["lower"] = np.nan

        df["source"] = "KOR_HIRA"
        df["estimate_id"] = estimate_id
        df["run_id"] = self.run_id
        df["diagnosis_id"] = 1

        cols = [
            "age_group_id",
            "sex_id",
            "location_id",
            "year_start",
            "year_end",
            "bundle_id",
            "estimate_id",
            "diagnosis_id",
            "source",
            "nid",
            "representative_id",
            "cases",
            "mean",
            "lower",
            "upper",
            "sample_size",
            "run_id",
            "population_run_id",
        ]

        df = df[cols]

        for col in df.columns[df.columns.str.endswith("_id")]:
            df[col] = df[col].astype(int)

        return df

    def apply_gbd_pop(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply GBD population as sample size
        """
        # 31, 32, 235 are ids for 85-89, 90-94, & 95+ respectively
        # which requires custom pull from get_population
        ids_85p = [31, 32, 235]

        # get population for sample size
        pop = get_population(
            release_id=self.release_id,
            year_id=df.year_start.unique().tolist(),
            location_id=df.location_id.unique().tolist(),
            age_group_id=df.age_group_id.unique().tolist() + ids_85p,
            sex_id=[1, 2],
            run_id=self.pop_run_id,
        )

        # Custom sample size summing for irregular 85+ age group of KOR
        # which are not captured in pop pull for this country
        pop_85p = (
            pop[pop.age_group_id.isin(ids_85p)]
            .groupby(["year_id", "sex_id", "location_id", "run_id"])
            .agg(population=("population", "sum"))
            .reset_index()
        )
        pop_85p["age_group_id"] = 160
        pop = pd.concat([pop, pop_85p])

        pop.rename(
            columns={
                "population": "sample_size",
                "run_id": "population_run_id",
                "year_id": "year_start",
            },
            inplace=True,
        )

        df = df.merge(
            pop, how="left", on=["age_group_id", "location_id", "sex_id", "year_start"]
        )

        if df.sample_size.isnull().sum() > 0:
            raise RuntimeError("Missing demographic population")

        return df

    def apply_age_group_id(
        self,
        df: pd.DataFrame,
        clinical_age_group_set_id: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Append GBD age group ids. clinical_age_group_set_id defaults to object attr.
        """
        if not clinical_age_group_set_id:
            clinical_age_group_set_id = self.clinical_age_group_set_id
        return demographic.all_group_id_start_end_switcher(
            df, clinical_age_group_set_id=clinical_age_group_set_id, remove_cols=False
        )

    def apply_age_sex_restrictions(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop(["age_start", "age_end"], axis=1, inplace=True)
        return clinical_mapping.apply_restrictions(
            df,
            age_set="age_group_id",
            cause_type="bundle",
            map_version=self.map_version,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
        )

    def agg_to_bundle(self, df: pd.DataFrame, remove_facility: bool = True) -> pd.DataFrame:
        """
        Map from cause code to bundle and re-aggergate counts.
        Remove facility aggs both inp and otp counts. Turning the flag off is primarly used
        for dev work. Aggregates over diagnosis_id. To prepare an estimate,
        secondary dx will need removed before passing into this function.
        """
        df = clinical_mapping.map_to_gbd_cause(
            df,
            input_type="cause_code",
            output_type="bundle",
            retain_active_bundles=True,
            map_version=self.map_version,
        )
        exclude = ["val", "cause_code", "code_system_id", "diagnosis_id"]
        if remove_facility:
            exclude.append("facility_id")
        cols = [e for e in df.columns if e not in exclude]

        return df.groupby(cols).agg({"val": "sum"}).reset_index()

    def pull_data(self) -> None:
        """
        Pull inp/otp formatted data with primary dx only. Prepared
        using FILEPATH/01_format_KOR_HIRA.py
        Saves this dataframe as a class attribute to be further refined based
        on specific estimate id.
        """
        path = (
            root + r"FILEPATH/formatted_KOR_HIRA.H5"
        )
        logger.info(f"Pulling input data from: {path}")
        df = pd.read_hdf(path)

        # there is a single row where sex_id is 3
        if df[df.sex_id == 3].val.sum() > 5:
            raise RuntimeError("This data should be split")

        self.df = df[df.sex_id.isin([1, 2])]

    def make_estimate_27(self) -> None:
        """Creates estimate ID 27. Defined as: inp + otp. KOR provides primary dx only.
        Using all year, all facility, all dx self.df from pull_data, default
        as source data is combined inp & outp with no disaggregation for this source.
        """
        df = self.df
        df = self.gbd_func_seq(df)
        df = self.apply_claims_shape(df=df, estimate_id=27)

        self.df_estimate_27 = df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run_id",
        type=int,
        action="store",
        required=True,
        help="ID for this CI run",
    )
    parser.add_argument(
        "--write",
        type=bool,
        action="store",
        required=True,
        help="Indication of whether data should be written out",
    )
    args = parser.parse_args()

    kor_claims = KorHiraClaims(run_id=args.run_id, write_final_data=args.write)
    kor_claims.create_claims_data()

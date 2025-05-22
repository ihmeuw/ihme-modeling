import argparse
import functools
import platform
from datetime import datetime
from functools import lru_cache
from typing import Callable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import (
    ClaimsWrappers,
)
from crosscutting_functions import demographic, general_purpose, legacy_pipeline
from crosscutting_functions.formatting-functions import formatting
from crosscutting_functions.pipeline import get_release_id
from crosscutting_functions.mapping import clinical_mapping, clinical_mapping_db
from db_queries import get_population
from loguru import logger

from inpatient.Clinical_Runs.utils import constants


class SgpMediClaims:
    def __init__(
        self,
        run_id: int,
        clinical_data_type_id: int = 3,
        clinical_age_group_set_id: int = 3,
        provided_age_group_set_id: int = 2,
        write_final_data: bool = False,
    ):
        """
        Args:
            run_id (int): Clinical run_id which exists in run_metadata.
            clinical_data_type_id (int, optional): Type ID for data.
                Defaults to 3 for claims.
            clinical_age_group_set_id (int, optional): Age set to formatted data to.
                Defaults to 3 for claims.
            provided_age_group_set_id (int, optional): Age set that data is prebinned to.
                Defaults to 2.
            write_final_output (bool): Write final estimate data out. True writes out.
                If False, will return the final estimate table instead of writting out.

        """
        self.run_id = run_id
        self.clinical_data_type_id = clinical_data_type_id
        self.provided_age_group_set_id = provided_age_group_set_id
        self.map_version = self.get_map_version()
        self.pop_run_id = self.get_pop_run_id()
        self.clinical_age_group_set_id = clinical_age_group_set_id
        self.release_id = get_release_id(run_id=run_id)
        self.write_final_output = write_final_data

    def create_claims_data(self) -> Union[pd.DataFrame, None]:
        """
        Main function used to create estimates for SGP Mediclaims dataset.
        Produces estimate id 17 (inpatient only, ) only for SGP MediClaims source.
        Will not return anything if saving final data. For development,
        if not writing final data, will return the final concatted dataframe.
        """

        self.create_logger()
        logger.info(f"Class Attributes: {self.__dict__}")
        self.pull_data_preformat()
        self.make_estimate_17()
        self.df = general_purpose.to_int(df=self.df)

        if self.write_final_output:
            self.write_final_data()
        else:
            return self.df

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
        log_path = f"FILEPATH/{timestamp}.log"
        logger.add(sink=log_path)

    def write_final_data(self) -> None:
        write_path = (
            f"FILEPATH/sgp_mediclaims.csv"
        )
        self.df.to_csv(write_path, index=False)
        logger.info(f"Data file has been outputted at: {write_path}")

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
        Fill Null feature values (e.g. nid, age_end) for newly created squared rows.
        """
        col_order = df.columns.tolist()
        og = df[df["val"] != 0].copy()
        # This is only discharges
        df = df[df["val"] == 0].copy()

        missing_col_vals = [e for e in df.columns.tolist() if all(df[e].isnull())]
        df = df.drop(missing_col_vals, axis=1)

        exclude = ["val", "bundle_id"]
        id_cols = [e for e in df.columns.tolist() if e not in exclude]
        feature_vals = [e for e in og.columns.tolist() if e not in exclude]

        df = df.merge(og[feature_vals].drop_duplicates(), how="left", on=id_cols)
        df = df.merge(
            og[["bundle_id"]].drop_duplicates(),
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
        df = df.rename(columns={"val": "cases"})
        df["cases"] = df["cases"].astype(int)
        # create the rate estimate column
        df["mean"] = df["cases"] / df["sample_size"]

        # create upper lower with nulls. Filled in upload
        df["upper"] = np.nan
        df["lower"] = np.nan

        df["estimate_id"] = estimate_id
        df["run_id"] = self.run_id
        df["diagnosis_id"] = 3
        df["representative_id"] = 1

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

        df = df.drop_duplicates().reset_index(drop=True)

        return df

    def apply_gbd_pop(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply GBD population as sample size
        """
        # get population for sample size
        pop = get_population(
            release_id=self.release_id,
            year_id=df.year_start.unique().tolist(),
            location_id=df.location_id.unique().tolist(),
            age_group_id=df.age_group_id.unique().tolist(),
            sex_id=[1, 2],
            run_id=self.pop_run_id,
        )

        pop = pop.rename(
            columns={
                "population": "sample_size",
                "run_id": "population_run_id",
                "year_id": "year_start",
            },
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
        df = df.drop(["age_start", "age_end"], axis=1)
        return clinical_mapping.apply_restrictions(
            df,
            age_set="age_group_id",
            cause_type="bundle",
            map_version=self.map_version,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
        )

    def validate_bundles_in_map(self, df: pd.DataFrame) -> None:
        """Makes sure the bundles in the table all belong to the ma[ version.

        Raises:
            AssertionError: A bundle ID is found outside the expected map version.
        """
        # validate that all matched bundle IDs exists in relevant map version
        ci_bundles = clinical_mapping_db.get_active_bundles(map_version=self.map_version)
        check_bundle = [
            b for b in df["bundle_id"].unique() if b not in ci_bundles["bundle_id"].unique()
        ]
        if len(check_bundle) != 0:
            raise AssertionError(
                f"There are bundle_id's not in this CI map version: {check_bundle}"
            )

    def sum_under1(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregates a dataframe by 'gbcols'. Expects df of ONLY under 1 data."""
        gbcols = ["gender", "discharge_year", "bundle_name", "bundle_id", "measure"]
        df = df.groupby(gbcols).agg({"countnum": "sum"}).reset_index()
        df["age_start"] = 0
        df["age_end"] = 1

        return df

    def adjust_u2(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sums under 1 ages to align with 0-1 age group and convert the days/months
        age bins into years.

        Args:
            df (pd.DataFrame): Dataframe of binned ages, but ONLY under 2 yeas ages.

        Raises:
            ValueError: Unexpected number of age bins for under 2.
            ValueError: Unexpected number of age bins for 0-1.
            ValueError: Unexpected number of age bins for 1-2.

        Returns:
            pd.DataFrame: Under 2 age groups corrected and in year format.
        """

        given_ages = demographic.get_hospital_age_groups(
            clinical_age_group_set_id=self.provided_age_group_set_id
        )

        n_under_2 = given_ages.loc[(given_ages["age_end"] <= 2), "age_group_id"].nunique()
        n_1_2 = given_ages.loc[
            (given_ages["age_end"] <= 2) & (given_ages["age_start"] >= 1), "age_group_id"
        ].nunique()

        if df["age_bin"].nunique() != n_under_2:
            raise ValueError("Data age-bins and expected age bins do not align.")

        if df.loc[df["age_bin"] != "12-23", "age_bin"].nunique() != (n_under_2 - n_1_2):
            raise ValueError("Setting ages will change data for age 0-1.")

        tmp_under1 = self.sum_under1(
            df=df.loc[df["age_bin"] != "12-23"].reset_index(drop=True)
        )

        tmp_1_2 = df.loc[df["age_bin"] == "12-23"].reset_index(drop=True)
        if tmp_1_2["age_bin"].nunique() != n_1_2:
            raise ValueError("Setting ages will change data for age 1-2.")

        tmp_1_2["age_start"] = 1
        tmp_1_2["age_end"] = 2

        df = pd.concat([tmp_under1, tmp_1_2], ignore_index=True)

        df["age_group_unit"] = "years"
        return df

    def pull_data_preformat(self) -> None:
        """
        Pull collaborator-provided data and preformat so that claims shape
        can be applied later.
        Saves this dataframe as a class attribute to be further refined based
        on specific estimate id.
        """
        if platform.system() == "Linux":
            root = "FILEPATH"
        else:
            root = "FILEPATH"

        base = "FILEPATH"
        fn_base = "FILEPATH"
        fn_type = "FILEPATH"
        path = f"{root}/{base}/{fn_type}"

        logger.info(f"Pulling input data files from: {path} and concatenating inc & prev data")
        files = [
            f"{path}_INC_PROCESSING_Y2023M11D27.XLSX",
            f"{path}_PREV_PROCESSING_Y2023M11D27.XLSX",
        ]

        df = pd.concat([pd.read_excel(f) for f in files], ignore_index=True)

        # preliminary formatting to align columns
        df[["age_bin", "age_group_unit"]] = df["agegrp"].str.split(" ", n=1, expand=True)
        df[["age_start", "age_end"]] = df["age_bin"].str.split("-", n=1, expand=True)
        df["age_start"] = df["age_start"].astype(float)

        tmp_over2 = df.loc[df["age_group_unit"] == "years"].reset_index(drop=True)
        tmp_under2 = df.loc[df["age_group_unit"] != "years"].reset_index(drop=True)

        tmp_over2["age_end"] = (
            tmp_over2["age_end"].astype(float) + 1
        )
        tmp_under2 = self.adjust_u2(df=tmp_under2)

        df = pd.concat([tmp_under2, tmp_over2], ignore_index=True)

        df["year_end"] = df["discharge_year"]
        df = df.rename(
            columns={"gender": "sex_id", "discharge_year": "year_start", "countnum": "val"}
        )
        df["sex_id"] = df["sex_id"].replace(["M", "F"], [1, 2])
        df["nid"] = np.nan
        df["source"] = "SGP"
        df["location_id"] = 69
        df = df.drop(["agegrp", "age_bin"], axis=1)

        # apply nids
        nid_dict = {
            1991: 336846,
            1992: 336845,
            1993: 336844,
            1994: 336843,
            1995: 336842,
            1996: 336841,
            1997: 336840,
            1998: 336839,
            1999: 336838,
            2000: 336837,
            2001: 336836,
            2002: 336835,
            2003: 336834,
            2004: 336833,
            2005: 336832,
            2006: 336831,
            2007: 336830,
            2008: 336829,
            2009: 336828,
            2010: 336827,
            2011: 336826,
            2012: 336825,
            2013: 336824,
            2014: 336822,
            2015: 336820,
            2016: 336817,
            2017: 406980,
            2018: 507450,
            2019: 507451,
            2020: 507452,
            2021: 507453,
        }
        df = formatting.fill_nid(df, nid_dict)

        # split rule for sex id
        if df[df["sex_id"] == 3].val.sum() > 5:
            raise RuntimeError("This data should be split")
        self.validate_bundles_in_map(df=df)
        self.df = df[df["sex_id"].isin([1, 2])]

    def make_estimate_17(self) -> None:
        """Creates estimate ID 17. Defined as: inpatient-only claims, all dx, deduped.
        Using all year, all facility, all dx self.df from pull_data_preformat.
        """
        self.df = self.gbd_func_seq(df=self.df)
        self.df = self.apply_claims_shape(df=self.df, estimate_id=17)


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

    sgp_claims = SgpMediClaims(run_id=args.run_id, write_final_data=args.write)
    sgp_claims.create_claims_data()

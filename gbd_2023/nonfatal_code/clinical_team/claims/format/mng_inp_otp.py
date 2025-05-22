import functools
import warnings
from datetime import datetime
from functools import lru_cache
from typing import Callable, List, Optional, Tuple, Union

import click
import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import (
    ClaimsWrappers,
)
from crosscutting_functions.clinical_metadata_utils.database import Database
from crosscutting_functions import demographic, legacy_pipeline
from crosscutting_functions.pipeline import get_release_id
from crosscutting_functions.mapping import clinical_mapping
from db_queries import get_population
from loguru import logger

from inpatient.Clinical_Runs.utils import constants
from clinical_info.Plotting.vet_new_sources.micro_claims_vetting_plots.MNG_H_INFO import (
    national_age_plots,
)


class MngClaims:
    def __init__(
        self,
        run_id: int,
        clinical_age_group_set_id: int = 7,
        release_id: Union[int, None] = None,
        write_final_data: bool = False,
        make_national_plots: bool = False,
    ):
        """
        Args:
            run_id (int): Clinical run_id which exists in run_metadata.
            clinical_age_group_set_id (int, optional): Clinical age binning method ID.
                Defaults to 7.
            release_id (Union[int, None], optional): GBD release ID for shared functions.
                Defaults to None which will use the most current release.
            write_final_output (bool): Write final estiamte data out. True writes out.
                If False, will return the final estimate table instead of writting out.
            make_national_age_plots (bool): Make National age plots for bundle level
                estimates. Only applies if write_final_data is True since the plotting relies
                on the final output saved to disk.

        NOTE: For MNG_H_INFO, Clinical does not process injuries codes alongside other
        ICD-codes because the collaborators have provided a separate dataset
        (MNG_NATIONAL_INJURY_SURVEILLANCE_SYSTEM) that the BIRDS team processes, given the
        E and N coding detail they can incorporate in their matrix.

        """

        self.run_id = run_id
        self.map_version = self.get_map_version()
        self.pop_run_id = self.get_pop_run_id()
        self.clinical_age_group_set_id = clinical_age_group_set_id
        if not release_id:
            self.release_id = get_release_id(run_id=run_id)
        else:
            self.release_id = release_id
        self.write_final_output = write_final_data
        self.make_national_age_plots = make_national_plots

    def create_claims_data(self) -> Union[pd.DataFrame, None]:
        """
        Main function used to create estimates for MNG_H_INFO dataset.
        Produces estimate ids 16, 18, and 27 together.
        Will not return anything if saving final data. For development,
        if not writing final data, will return the final concatted dataframe.
        """

        self.create_logger()
        logger.info(f"Class Attributes: {self.__dict__}")
        self.pull_data()
        self.make_estimate_16()
        self.make_estimate_18()
        self.make_estimate_27()

        if self.write_final_output:
            self.write_final_data()
            if self.make_national_age_plots:
                self.plot_final_data()
        else:
            return pd.concat(
                [self.df_estimate_16, self.df_estimate_18, self.df_estimate_27],
                ignore_index=True,
            )

    @lru_cache()
    def pull_run_metadata(self) -> pd.DataFrame:
        cw = ClaimsWrappers(self.run_id, constants.RunDBSettings.cw_profile)
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
        write_path = f"{self.base_dir}/FILEPATH/mng_claims.csv"
        mng_df = pd.concat(
            [self.df_estimate_16, self.df_estimate_18, self.df_estimate_27], ignore_index=True
        )
        mng_df.to_csv(write_path, index=False)

    def plot_final_data(self) -> None:
        national_age_plots.create_national_age_plots(run_id=self.run_id)

    @property
    def base_dir(self):
        return "FILEPATH"

    def gbd_func_seq(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a function sequence to create bundle estimates for a
        subsetted dataframe. Once sequence is created, applies it to
        the subset df.

        Args:
            df (pd.DataFrame): DataFrame only containing records pertinent
                to a specific estimate.
        """

        func_seq = [
            self.agg_to_bundle,
            self.apply_age_group_id,
            self.make_zeros,
            self.apply_age_sex_restrictions,
            self.fill_missing_square_data,
            self.apply_gbd_pop,
            self.agg_new_terminal_age,
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
        df = df[df["val"] == 0].copy()
        pre = df.shape[0]

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

        if pre != df.shape[0]:
            raise RuntimeError("Mismatch in number of rows while filling NA columns")
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

        df["source"] = "MNG_H_INFO"
        df["source_type_id"] = 10
        df["estimate_id"] = estimate_id
        df["run_id"] = self.run_id
        df["diagnosis_id"] = 3

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
            "source_type_id",
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
        # get population for sample size\
        pop = get_population(
            release_id=self.release_id,
            year_id=df.year_start.unique().tolist(),
            location_id=df.location_id.unique().tolist(),
            age_group_id=df.age_group_id.unique().tolist(),
            sex_id=[1, 2],
            run_id=self.pop_run_id,
        )
        pop.rename(
            columns={"population": "sample_size", "run_id": "population_run_id"},
            inplace=True,
        )

        pop.rename({"year_id": "year_start"}, axis=1, inplace=True)
        df = df.merge(
            pop, how="left", on=["age_group_id", "location_id", "sex_id", "year_start"]
        )
        if df.sample_size.isnull().sum() > 0:
            raise RuntimeError("Missing demographic population")
        return df

    def agg_new_terminal_age(self, df: pd.DataFrame, terminal_age: int = 85) -> pd.DataFrame:
        """MNG needs to have the terminal age group lowered to algin
        with other micro-claims sources.
        """
        db = Database()
        db.load_odbc(odbc_profile="SERVER")

        age_group_id = "DATABASE"
        age_group_name = "DATABASE"
        years_start = "DATABASE"
        years_end = "DATABASE"

        age_qu = "QUERY"
        # Get valid GBD age groups with age start and end
        age_groups = db.query(age_qu)
        # Find GBD terminal age group(s) defined by 'terminal_age'
        terminal_groups = age_groups.loc[
            (age_groups.age_group_years_start == terminal_age)
            & (age_groups.age_group_years_end == 125)
        ]

        # Some ages have more than 1 terminal age group.
        # Ex: age 70 has '70+ years' and '70 plus'.  Both are 70-125.
        if terminal_groups.shape[0] > 1:
            print(terminal_groups)
            terminal_groups = terminal_groups.loc[
                terminal_groups.age_group_name.str.endswith("plus")
            ]
            msg = f"More than one terminal age group with age start '{terminal_age}'"
            ext = "Chosing age group ID with name ending in 'plus'"
            warnings.warn(f"{msg}. {ext}. \nChoice: \n{terminal_groups}")

        # Some ages do not have an associated terminal age group.
        if terminal_groups.shape[0] == 0:
            raise RuntimeError(f"GBD terminal age group for {terminal_age}+ not established.")

        terminal_groups = terminal_groups.reset_index(drop=True)
        terminal_group_id = terminal_groups.age_group_id[0]

        # Get all age groups >= than terminal age. Includes age group with new terminal age.
        # These are the age groups to aggregate into the new terminal age group.
        agg_age_groups = age_groups.loc[
            (age_groups.age_group_years_end > terminal_age), "age_group_id"
        ].to_list()
        # Assign chosen terminal age group ID to all of these age groups
        df.loc[df.age_group_id.isin(agg_age_groups), "age_group_id"] = terminal_group_id

        # Aggregate case counts and population estimate for the agg_age_groups
        cols = list(df.columns)
        for c in ["val", "sample_size"]:
            cols.remove(c)

        return df.groupby(cols).sum().reset_index()

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

        return demographic.group_id_start_end_switcher(
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
        for dev work. Aggregates over diagnosis_id. To prepare an estimate such
        as estimate_id 14, secondary dx will need removed before passing into
        this function.
        """
        df = clinical_mapping.map_to_gbd_cause(
            df,
            input_type="cause_code",
            output_type="bundle",
            retain_active_bundles=True,
            map_version=self.map_version,
        )
        exclude = ["val", "cause_code", "code_system_id", "diagnosis_id", "outcome_id"]
        if remove_facility:
            exclude.append("facility_id")
        cols = [e for e in df.columns if e not in exclude]

        return df.groupby(cols).agg({"val": "sum"}).reset_index()

    def pull_data(self) -> None:
        """
        Pull inp/otp formatted data with primary and secondary dx's. Prepared
        using FILEPATH/01_format_MNG_H_INFO.py
        No injury data in this set, processed separately. All years.
        Saves this dataframe as a class attribute to be further refined based
        on specific estimate id.
        """
        path = (
            "FILEPATH/formatted_MNG_H_INFO.H5"
        )
        logger.info(f"Pulling input data from: {path}")
        df = pd.read_hdf(path)

        # there is a single row where sex_id is 3
        if df[df.sex_id == 3].val.sum() > 5:
            raise RuntimeError("This data should be split")

        if df.facility_id.nunique() > 2:
            unexpected = set(df.facility_id.unique()) - set(
                ["inpatient unknown", "outpatient unknown"]
            )
            msg = f"Unexpected facility_id values: {unexpected}. Address in formatting."
            warnings.warn(msg)
            counts = df[["facility_id"]].value_counts()
            for uf in unexpected:
                msg = f"{counts[uf]} aggregate records will be missing due to facility '{uf}'"
                warnings.warn(msg)
            raise RuntimeError("Data was formatted with unexpected facility_id present")

        self.df = df[df.sex_id.isin([1, 2])]

    def make_estimate_27(self) -> None:
        """Creates estimate ID 27. Defined as: inp + otp, primary & secondary.
        Using all year, all facility, all dx self.df from pull_data, subsets to
        only data years that are found in both inpatient and outpatient records.
        """

        if (
            len(
                set(["inpatient unknown", "outpatient unknown"])
                - set(self.df.facility_id.unique())
            )
            != 0
        ):
            raise KeyError("Missing expected inpatient and outpatient facility tags.")

        # Grab years that have both inp and otp data
        temp = (
            self.df[["year_start", "facility_id"]]
            .drop_duplicates()
            .sort_values(by=["year_start", "facility_id"])
            .reset_index(drop=True)
        )
        temp = temp.groupby("year_start")["facility_id"].nunique().reset_index(name="vals")
        inp_otp_years = temp[temp.vals == 2].year_start.unique().tolist()
        df = self.df[self.df.year_start.isin(inp_otp_years)]

        df = self.gbd_func_seq(df)
        df = self.apply_claims_shape(df=df, estimate_id=27)

        self.df_estimate_27 = df

    def make_estimate_16(self) -> None:
        """Creates estimate ID 16. Defined as: inp, primary & secondary.
        Using all year, all facility, all dx self.df from pull_data, subsets to
        only inpatient records.
        """
        logger.info("Creating Estimate ID: 16")
        # Filter to only inptient records
        if "inpatient unknown" not in self.df.facility_id.unique():
            raise KeyError("Inpatient flag 'inpatient unknown' is not present or has changed.")
        df = self.df.loc[self.df.facility_id == "inpatient unknown"]
        df = self.gbd_func_seq(df)
        df = self.apply_claims_shape(df=df, estimate_id=16)
        self.df_estimate_16 = df

    def make_estimate_18(self) -> None:
        """Creates estimate ID 18. Defined as: otp, primary & secondary.
        Using all year, all facility, all dx self.df from pull_data, subsets to
        only outpatient records.
        """
        logger.info("Creating Estimate ID: 18")
        # Filter to only outptient records
        if "outpatient unknown" not in self.df.facility_id.unique():
            raise KeyError(
                "Outpatient flag 'outpatient unknown' is not present or has changed."
            )
        df = self.df.loc[self.df.facility_id == "outpatient unknown"]
        df = self.gbd_func_seq(df)
        df = self.apply_claims_shape(df=df, estimate_id=18)
        self.df_estimate_18 = df


@click.command()
@click.option("--run_id", required=True, type=click.INT, help="CI run id directory")
@click.option(
    "--clinical_age_group_set_id",
    required=False,
    type=click.INT,
    help="CI age group id for output",
)
@click.option("--release_id", required=False, type=click.INT, help="release id")
def main(**kwargs):
    mng_claims = MngClaims(**kwargs)
    mng_claims.create_claims_data()

import pathlib
import warnings
from typing import Optional, Union

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.api.ddl import DDL
from crosscutting_functions.db_connector.database import Database, DBManager
from crosscutting_functions.nid_tables import nid_run_history
from crosscutting_functions import demographic
from crosscutting_functions.mapping import clinical_mapping_db

from crosscutting_functions import uncertainty
from inpatient.Upload.tools.data_manager import ClinicalDM
from inpatient.Upload.tools.filter import FilterPipeline
from inpatient.Upload.tools.filters.main import (
    duplicates,
    empty_mean,
    impossible_incidence,
    impossible_prevalence,
    inf_mean,
    mean_greater_than_upper,
)
from inpatient.Upload.tools.logger import ClinicalLogger
from inpatient.Upload.tools.testing import TestingPipeline
from inpatient.Upload.tools.tests.main import (
    contains_column,
    lower_less_than_mean,
    no_duplicates,
    no_extra_years,
    not_null,
    prevalence_under_1,
    unique_column,
    upper_greater_than_mean,
)

odbc_profile = "clinical"
DBopen = DBManager(odbc_profile)
pd.options.mode.chained_assignment = None


class UploaderError(Exception):
    pass


class Uploader:
    """A base class for uploaders.

    Args:
        name (str): Job name.
        run_id (int): Clinical Run ID in run_metadata.
        max_upload_year (Optional[int]): Maximum year allowed in final table. All years
            greater than this year will be removed (this year is kept). Defaults to None
            which keeps all years present.
        year_end_cap (Optional[int]): Inpatient year-bin end cap adjustment value.
            The year bin that this year belongs to will be capped to this value.
            Defaults to None which keeps standard year bins.

    """

    def __init__(
        self,
        name: str,
        run_id: int,
        max_upload_year: Optional[int] = None,
        year_end_cap: Optional[int] = None,
    ):
        self.name = name
        self.run_id = run_id
        self.max_upload_year = max_upload_year
        self.year_end_cap = year_end_cap
        self._has_loaded = False

        # setup
        self.dm = ClinicalDM(run_id=run_id)
        self.log = ClinicalLogger(path=self.dm.log_folder, name=name)
        self.dm.setup_logger(self.log)

        base = filepath_parser(ini="pipeline.clinical", section="clinical", section_key="runs")
        self.run_base = FILEPATH

    def load_df(self, df: pd.DataFrame) -> None:
        size_est = df.memory_usage().sum() / 1024 / 1024
        self.log.log(
            f"Loaded {len(df)} rows x {len(df.columns)} columns, " f"~{size_est:.2f}Mb"
        )
        self.df = df
        self.log.set_groups(self.df, ["bundle_id", "estimate_id"])
        self._has_loaded = True

    def load_csv(self, file_name: str) -> None:
        self.df = self.dm.load_file(self.run_base / file_name)
        self.df["run_id"] = self.run_id
        self.log.set_groups(self.df, ["bundle_id", "estimate_id"])
        self._has_loaded = True

    def format_columns(self) -> None:
        self.log.log("Formatting data")
        column_names = {
            "age_group_id": "age_group_id",
            "cases": "cases",
            "bundle_id": "bundle_id",
            "diagnosis_id": "diagnosis_id",
            "estimate_id": "estimate_id",
            "location_id": "location_id",
            "lower": "lower",
            "mean": "mean",
            "nid": "merged_nid",
            "representative_id": "representative_id",
            "run_id": "run_id",
            "sample_size": "sample_size",
            "sex_id": "sex_id",
            "source": "source",
            "source_type_id": "source_type_id",
            "upper": "upper",
            "year_end_id": "year_end",
            "year_start_id": "year_start",
        }
        self.df.rename(columns=column_names, inplace=True)
        self.df = self.df[[col for col in column_names.values() if col in self.df]]

    def merge_bundle_info(self) -> None:
        # Add measure ID and duration
        self.log.log("Merging `measure_id` and `duration` from the CM onto the data")

        db = Database()
        db.load_odbc("clinical")

        map_version = db.query(QUERY
        ).map_version

        dm = pd.merge(
            clinical_mapping_db.create_bundle_durations(map_version[0]),
            clinical_mapping_db.get_bundle_measure(map_version[0]),
            how="outer",
            on=["bundle_id"],
        )

        # Swap to measure_id
        dm["measure_id"] = np.where(dm.bundle_measure == "prev", 5, 6)

        dm.drop(["map_version", "bundle_measure"], axis=1, inplace=True)
        dm.rename({"bundle_duration": "duration"}, axis=1, inplace=True)

        # Ensure all the bundles in our DF have a measure_id to map to
        missing_bundles = set(self.df["bundle_id"].unique()) - set(dm.bundle_id.tolist())

        if missing_bundles:
            raise ValueError(
                f"The map is missing measures for the following bundles: \n{missing_bundles}"
            )

        # Create dictionaries from bundle to measure_id/duration
        # to use in mapping bundles to measure_id/duration
        bundle_to_measure = dict(zip(dm.bundle_id, dm.measure_id))
        bundle_to_duration = dict(zip(dm.bundle_id, dm.duration))

        self.df["measure_id"] = self.df.bundle_id.map(bundle_to_measure)
        self.df["duration"] = self.df.bundle_id.map(bundle_to_duration)

    def calculate_uncertainty(self) -> None:
        self.log.log(f"Creating uncertainty for {len(self.df)} rows")
        self.df = uncertainty.fill(self.df)

    def cap_estimates(self) -> None:
        for col in ["lower", "mean", "upper", "standard_error"]:
            self.df.loc[self.df[col] < 0, col] = 0
            self.df.loc[(self.df[col] > 1) & (self.df.measure_id == 5), col] = 1

    def remove_extra_years(self) -> None:
        if self.max_upload_year:
            self.df = self.df.loc[self.df["year_end"] <= self.max_upload_year].reset_index(
                drop=True
            )
        else:
            warnings.warn(
                f"Uploading years: \n{sorted(self.df['year_end'].unique().tolist())}"
            )

    def update_year_bin(self) -> None:
        p = pd.read_pickle(FILEPATH
        )
        # make sure this is only applicable to 5-year estimate runs
        # currently not applicable to claims or otp
        if p.bin_years:
            if self.year_end_cap:
                year = pd.DataFrame(
                    [[self.year_end_cap, self.year_end_cap]],
                    columns=["year_start", "year_end"],
                )
                year_end_max = demographic.year_binner(year)["year_end"][0]
                if year_end_max != max(self.df["year_end"]):
                    raise ValueError(
                        "Expected max year_end given cap does not match "
                        "the actual max year_end in df"
                    )
                self.df.loc[self.df["year_end"] == year_end_max, "year_end"] = int(
                    self.year_end_cap
                )

    def check_if_loaded(self) -> None:
        if not self._has_loaded:
            raise UploaderError("Must load before processing.")

    def process(self) -> None:
        if "nid" in self.df.columns:
            self.upload_nids()
        self.check_if_loaded()

    def upload_nids(self) -> None:
        self.check_if_loaded()
        df_up = self.df[["nid", "estimate_id"]].drop_duplicates().reset_index(drop=True)

        db = Database()
        db.load_odbc("clinical")

        estimate_ids = str(df_up.estimate_id.unique().tolist())[1:-1]
        ci_dtype_estimate = db.query(QUERY
        )

        df_up = df_up.merge(ci_dtype_estimate, on="estimate_id")

        nid_hist = nid_run_history.NidRunHistory(df=df_up, run_id=self.run_id)
        nid_hist.process()

    def test(self) -> None:
        self.check_if_loaded()
        pipeline = TestingPipeline(df=self.df, logger=self.log)
        assert pipeline(
            not_null(ignore=["duration", "measure_id"]),
            no_duplicates,
            contains_column(column="run_id"),
            unique_column(column="run_id"),
            prevalence_under_1,
            lower_less_than_mean,
            upper_greater_than_mean,
            no_extra_years(max_upload_year=self.max_upload_year),
        ), "Tests failed"

    def save(self, path: Union[str, pathlib.Path]) -> None:
        self.check_if_loaded()
        self.dm.save(self.df, path)

    def backup_and_upload(self, path: str, odbc_profile: str) -> None:
        self.check_if_loaded()
        path = pathlib.Path(self.run_base / path)

        columns_to_upload = [
            "age_group_id",
            "bundle_id",
            "cases",
            "diagnosis_id",
            "estimate_id",
            "location_id",
            "measure_id",
            "duration",
            "lower",
            "mean",
            "merged_nid",
            "representative_id",
            "run_id",
            "sample_size",
            "sex_id",
            "source_type_id",
            "standard_error",
            "uncertainty_type_id",
            "uncertainty_type_value",
            "upper",
            "year_end",
            "year_start",
        ]

        self.dm.save(self.df.fillna("\\N")[columns_to_upload], path)

        schema = "clinical"
        table = "final_estimates_bundle"

        db = Database()
        db.load_odbc(odbc_profile)

        self.log.log(f"Uploading to {db.credentials.host} - {schema}.{table}\n")
        input("Press enter to continue")

        db.write(FILEPATH, schema, table)


class InpatientUploader(Uploader):
    """An Uploader for inpatient data"""

    def __init__(
        self,
        run_id: int,
        name: str = "inpatient_uploader",
        year_end_cap: Optional[int] = None,
    ):
        super().__init__(
            name=name,
            run_id=run_id,
            year_end_cap=year_end_cap,
        )
        self.clinical_data_type_id = 1

    def process(self, validate: bool = True) -> None:
        super().process()

        # Create a cases column from mean and sample size
        self.log.log("Adding `cases` = `mean` * `sample_size`")
        self.df["cases"] = self.df["mean"] * self.df.sample_size

        # source_type_id = 10 is Facility - inpatient
        self.log.log("Adding `source_type_id` = 10")
        self.df["source_type_id"] = 10

        # Filter out rows with empty means and means > upper
        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(empty_mean, mean_greater_than_upper, duplicates)

        # Formatting:
        # Rename all columns for consistency. Also, drop columns we don't
        # need for the upload.
        self.format_columns()

        # Merge measure_id and duration for each bundle. This info is
        # needed to calculate uncertainty and filter out incidence/prevalence
        # rows.
        self.merge_bundle_info()

        # Uncertainty
        # Calculate uncertainty for our estimates. Prevalence estimates
        # > 1 will break the uncertainty code so we need to filter out
        # those rows first (may as well filter out duplicates here as well).
        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(impossible_prevalence)
        self.calculate_uncertainty()

        # Filter our rows with impossible prevalence or incidence estimates
        # if we are running validations
        if validate:
            # This assert is breaking. Code should be able to progress
            # if 3419 is in bundle_id list. See CCMHD-10469
            # assert 3419 not in self.df.bundle_id.unique(), (
            #     f"Bundle 3419 should not be validated"
            # )
            df_list = []
            if 3419 in self.df.bundle_id.unique():
                self.log.log("Removing bundle 3419 from validations")
                df_list.append(self.df[self.df.bundle_id == 3419])
                self.df = self.df[self.df.bundle_id != 3419]

            pipeline = FilterPipeline(df=self.df, logger=self.log)
            self.df = pipeline(impossible_prevalence, impossible_incidence)
            df_list.append(self.df)
            self.df = pd.concat(df_list)
        # Cap estimates (e.g. a lower estimate of -0.1 will be capped at 0)
        # also cap standard errors to 1 if the measure is prev
        self.cap_estimates()
        self.update_year_bin()

    def test(self) -> None:
        pipeline = TestingPipeline(df=self.df, logger=self.log)
        assert pipeline(
            not_null(
                ignore=[
                    "duration",
                    "measure_id",
                    "sample_size",
                    "effective_sample_size",
                    "cases",
                ]
            ),
            no_duplicates,
            contains_column(column="run_id"),
            unique_column(column="run_id"),
            prevalence_under_1,
            lower_less_than_mean,
            upper_greater_than_mean,
            no_extra_years(max_upload_year=self.max_upload_year),
        ), "Tests failed"


class OutpatientUploader(Uploader):
    """An Uploader for outpatient data"""

    def __init__(
        self,
        run_id: int,
        name: str = "outpatient_uploader",
        max_upload_year: Optional[int] = None,
    ):
        super().__init__(name=name, run_id=run_id, max_upload_year=max_upload_year)
        self.clinical_data_type_id = 2

    def process(self) -> None:
        super().process()

        # Add a mean from cases and sample_size
        self.log.log("Adding `mean` = `cases`/`sample_size`")
        self.df["mean"] = self.df.cases / self.df.sample_size

        self.format_columns()
        self.merge_bundle_info()

        # Drop duplicates and any prevalence > 1
        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(
            duplicates,
            impossible_prevalence,
        )
        self.calculate_uncertainty()

        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(impossible_prevalence, impossible_incidence)

        self.cap_estimates()
        self.remove_extra_years()

    def test(self) -> None:
        return super().test()


class ClaimsUploader(Uploader):
    """An uploader for claims data"""

    def __init__(
        self,
        run_id: int,
        name: str = "claims_uploader",
        clinical_data_type_id: int = 3,
        max_upload_year: Optional[int] = None,
    ):
        super().__init__(name=name, run_id=run_id, max_upload_year=max_upload_year)
        if clinical_data_type_id not in [3, 4, 5]:
            msg = "Claims clinical_data_type_id must be one of 3, 4, 5"
            ext = f"found '{clinical_data_type_id}'"
            raise ValueError(f"{msg}, {ext}.")
        self.clinical_data_type_id = clinical_data_type_id

    def process(self) -> None:
        super().process()
        # Drop rows with empty means
        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(
            empty_mean,
            inf_mean,
        )

        self.format_columns()
        self.merge_bundle_info()

        # Drop duplicates and any prevalence > 1
        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(
            duplicates,
            impossible_prevalence,
        )
        self.calculate_uncertainty()

        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(impossible_prevalence, impossible_incidence)

        self.cap_estimates()
        self.remove_extra_years()

    def test(self) -> None:
        return super().test()


def add_final_bundle_estimates_partition(run_id: int, odbc_profile: str) -> None:
    # code to add a new partition to final estimates bundles if necessary
    ddl = DDL(odbc_profile)
    cur = ddl._generate_cursor()
    cur.callproc("epi.add_partition", ("clinical", "final_estimates_bundle", run_id))
    cur.close()
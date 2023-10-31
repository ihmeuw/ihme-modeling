import pathlib
import os
import warnings

import pandas as pd

from clinical_info.Upload.maternal_ratio import MaternalRatioBuilder
from clinical_info.Upload.uncertainty import Uncertainty

from clinical_info.Upload.tools.logger import ClinicalLogger
from clinical_info.Upload.tools.data_manager import ClinicalDM, Pipeline
from clinical_info.Upload.tools.testing import TestingPipeline
from clinical_info.Upload.tools.tests.main import *
from clinical_info.Upload.tools.filter import FilterPipeline
from clinical_info.Upload.tools.filters.main import *
from clinical_info.Upload.tools.io.database import DBManager, Database
from clinical_info.Database.NID_Tables import nid_run_history
from clinical_info.Database.bundle_relationships.relationship_methods import (
    ParentInjuries,
    MaternalRatios,
)
from clinical_info.Mapping import clinical_mapping as cm

DBopen = DBManager("clinical")
pd.options.mode.chained_assignment = None


class UploaderError(Exception):
    pass


class Uploader:
    """ A base class for uploaders"""

    def __init__(self, name, run_id):
        self.name = name
        self.run_id = run_id
        self._has_loaded = False

        # setup
        self.dm = ClinicalDM(run_id=run_id)
        self.log = ClinicalLogger(path=self.dm.log_folder, name=name)
        self.dm.setup_logger(self.log)

        self.run_base = f"FILEPATH"

    def load_df(self, df):
        size_est = df.memory_usage().sum() / 1024 / 1024
        self.log.log(
            f"Loaded {len(df)} rows x {len(df.columns)} columns, " f"~{size_est:.2f}Mb"
        )
        self.df = df
        self.log.set_groups(self.df, ["bundle_id", "estimate_id"])
        self._has_loaded = True

    def load_csv(self, file_name):
        self.df = self.dm.load_file(self.run_base + file_name)
        self.df["run_id"] = self.run_id
        self.log.set_groups(self.df, ["bundle_id", "estimate_id"])
        self._has_loaded = True

    def format_columns(self):
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

    def merge_bundle_info(self):
        # Add measure ID and duration
        self.log.log("Merging `measure_id` and `duration` from the CM onto the data")

        dm = pd.merge(
            cm.create_bundle_durations(),
            cm.get_bundle_measure(),
            how="outer",
            on=["bundle_id"],
        )

        # Swap to measure_id
        dm["measure_id"] = np.where(dm.bundle_measure == "prev", 5, 6)

        dm.drop(["map_version", "bundle_measure"], axis=1, inplace=True)
        dm.rename({"bundle_duration": "duration"}, axis=1, inplace=True)

        # Append the parent inj bundles
        parent_method = ParentInjuries(run_id=self.run_id)
        parent_method.get_tables()
        parent_df = parent_method.bundle_relationship.copy()
        parent_df.rename(
            columns={
                "origin_bundle_id": "child_bundle_id",
                "output_bundle_id": "parent_bundle_id",
            },
            inplace=True,
        )
        par_bids = parent_df.parent_bundle_id.unique().tolist()
        temp = pd.DataFrame(
            data={
                "bundle_id": par_bids,
                "measure_id": [6 for _ in np.arange(len(par_bids))],
                "duration": [90 for _ in np.arange(len(par_bids))],
            }
        )
        dm = pd.concat([dm, temp], sort=False)

        # Ensure all the bundles in our DF have a measure_id to map to
        missing_bundles = set(self.df["bundle_id"].unique()) - set(
            dm.bundle_id.tolist()
        )
        assert not missing_bundles, (
            f"The map is missing measures for the "
            f"following bundles: {missing_bundles}"
        )

        # Create dictionaries from bundle to measure_id/duration
        # to use in mapping bundles to measure_id/duration
        bundle_to_measure = dict(zip(dm.bundle_id, dm.measure_id))
        bundle_to_duration = dict(zip(dm.bundle_id, dm.duration))

        self.df["measure_id"] = self.df.bundle_id.map(bundle_to_measure)
        self.df["duration"] = self.df.bundle_id.map(bundle_to_duration)

    def calculate_uncertainty(self):
        self.log.log(f"Creating uncertainty for {len(self.df)} rows")
        uc = Uncertainty(self.df)
        uc.fill()
        self.df = uc.df

    def cap_estimates(self):
        for col in ["lower", "mean", "upper"]:
            self.df.loc[self.df[col] < 0, col] = 0
            self.df.loc[(self.df[col] > 1) & (self.df.measure_id == 5), col] = 1

    def check_if_loaded(self):
        if not self._has_loaded:
            raise UploaderError("Must load before processing.")
        pass

    def process(self):
        if "nid" in self.df.columns:
            self.upload_nids()
        self.check_if_loaded()

    def upload_nids(self):
        self.check_if_loaded()
        df_up = self.df[["nid", "estimate_id"]].drop_duplicates().reset_index(drop=True)

        db = Database()
        db.load_odbc("clinical")

        estimate_ids = str(df_up.estimate_id.unique().tolist())[1:-1]
        ci_dtype_estimate = db.query("QUERY")

        df_up = df_up.merge(ci_dtype_estimate, on="estimate_id")

        nid_hist = nid_run_history.NID_Run_History(df=df_up, run_id=self.run_id)
        nid_hist.process()

    def test(self):
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
        ), "Tests failed"

    def save(self, path):
        self.check_if_loaded()
        self.dm.save(self.df, path)

    def backup_and_upload(self, path, odbc_profile):
        self.check_if_loaded()
        path = pathlib.Path(self.run_base + path)

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

        db.write(path / "backup.csv", schema, table)


class InpatientUploader(Uploader):
    """ An Uploader for inpatient data"""

    def __init__(self, run_id, name="inpatient_uploader"):
        super().__init__(name=name, run_id=run_id)

    def process(self, validate=True, maternal_ratios=True):
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

        # Compute ratios before filtering out rows from the data.
        # Ratios's aren't prevalence or incididence so don't run
        # incidence/prevalence filters on them.
        if maternal_ratios:
            self.log.log("Creating ratio bundles")
            # ratios = {
            #     6113: (76, 825),     # Eclampsia            / severe preeclampsia
            #     6116: (76, 6107),    # Eclampsia            / pre-eclampsia
            #     6119: (6107, 75),    # Pre-eclampsia        / total HDoP
            #     6122: (6110, 6107),  # HELLP                / pre-eclampsia
            #     6125: (825, 6107),   # severe Pre-eclampsia / pre-eclampsia
            # }

            mat_re = MaternalRatios(run_id=self.run_id)
            mat_re.get_tables()
            mat_re.convert_to_tuple_dict()

            # Check if the DF has any bundles that builds ratios
            builder = MaternalRatioBuilder(self.log)
            ratio_df = builder.ratio_bundles(self.df, mat_re.ratios)
            # TODO These should be made automatically in the maternal code
            ratio_df["run_id"] = self.run_id
            ratio_df["source_type_id"] = 10
            ratio_df["measure_id"] = 6
            ratio_df["diagnosis_id"] = 1

        # Filter our rows with impossible prevalence or incidence estimates
        # if we are running validations
        if validate:
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
        self.cap_estimates()

        # Add the ratio bundle estimates to the rest of the data
        if maternal_ratios:
            self.log.log(f"Appending {len(ratio_df)} maternal ratio rows")
            self.df = self.df.append(ratio_df, sort=False)

    def test(self):
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
        ), "Tests failed"


class OutpatientUploader(Uploader):
    """ An Uploader for outpatient data"""

    def __init__(self, run_id, name="outpatient_uploader"):
        super().__init__(name=name, run_id=run_id)

    def process(self):
        super().process()

        # Add a mean from cases and sample_size
        self.log.log("Adding `mean` = `cases`/`sample_size`")
        self.df["mean"] = self.df.cases / self.df.sample_size

        self.format_columns()
        self.merge_bundle_info()

        # Drop duplicates and any prevalence > 1
        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(duplicates, impossible_prevalence,)
        self.calculate_uncertainty()

        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(impossible_prevalence, impossible_incidence)

        self.cap_estimates()

    def test(self):
        return super().test()


class ClaimsUploader(Uploader):
    """ An uploader for claims data"""

    def __init__(self, run_id, name="claims_uploader"):
        super().__init__(name=name, run_id=run_id)

    def process(self):
        super().process()
        # Drop rows with empty means
        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(empty_mean, inf_mean,)

        self.format_columns()
        self.merge_bundle_info()

        # Drop duplicates and any prevalence > 1
        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(duplicates, impossible_prevalence,)
        self.calculate_uncertainty()

        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(impossible_prevalence, impossible_incidence)

        self.cap_estimates()

    def test(self):
        return super().test()


# if __name__ == "__main__":
#     uploader = InpatientUploader(run_id=3)
#     uploader.load('FILEPATH')
#     uploader.process()
#     uploader.test()
#     uploader.upload()

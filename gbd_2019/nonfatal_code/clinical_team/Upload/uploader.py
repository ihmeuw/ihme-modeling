import pathlib
import os
import warnings

import pandas as pd

from maternal_ratio import MaternalRatioBuilder
from uncertainty import Uncertainty

from tools.logger import ClinicalLogger
from tools.data_manager import ClinicalDM, Pipeline
from tools.testing import TestingPipeline
from tools.tests.main import *
from tools.filter import FilterPipeline
from tools.filters.main import *
from tools.io.database import DBManager, Database

DBopen = DBManager('clinical')
pd.options.mode.chained_assignment = None


class UploaderError(Exception): pass


class Uploader:
    """ A base class for uploaders"""
    def __init__(self, name, run_id):
        self.name = name
        self.run_id = run_id
        self._has_loaded = False


        self.dm = ClinicalDM(run_id=run_id)
        self.log = ClinicalLogger(path=self.dm.log_folder, name=name)
        self.dm.setup_logger(self.log)

        self._upload_base = (f"FILEPATH"
                            "FILEPATH")

    def load_df(self, df):
        size_est = df.memory_usage().sum() / 1024 / 1024
        self.log.log(f"Loaded {len(df)} rows x {len(df.columns)} columns, "
                         f"~{size_est:.2f}Mb")
        self.df = df
        self.log.set_groups(self.df, ["bundle_id", "estimate_id"])
        self._has_loaded = True

    def load_csv(self, file_name):
        self.df = self.dm.load_file(self.upload_base + file_name)
        self.df['run_id'] = self.run_id
        self.log.set_groups(self.df, ["bundle_id", "estimate_id"])
        self._has_loaded = True

    def format_columns(self):
        self.log.log("Formatting data")
        column_names = {'age_group_id': 'age_group_id',
                        'cases': 'cases',
                        'bundle_id': 'bundle_id',
                        'diagnosis_id': 'diagnosis_id',
                        'estimate_id': 'estimate_id',
                        'location_id': 'location_id',
                        'lower': 'lower',
                        'mean': 'mean',
                        'nid': 'merged_nid',
                        'representative_id': 'representative_id',
                        'run_id': 'run_id',
                        'sample_size': 'sample_size',
                        'sex_id': 'sex_id',
                        'source': 'source',
                        'source_type_id': 'source_type_id',
                        'upper': 'upper',
                        'year_end_id': 'year_end',
                        'year_start_id': 'year_start'}
        self.df.rename(columns=column_names, inplace=True)
        self.df = self.df[[col for col in column_names.values()
                           if col in self.df]]

    def merge_bundle_info(self):

        self.log.log(
            "Merging `measure_id` and `duration` from the DB onto the data"
        )
        query = ("SQL")
        with DBopen(query) as table:

            missing_bundles = (set(self.df["bundle_id"].unique()) -
                               set(table.bundle_id))
            assert not missing_bundles, (f"The DB is missing measures for the "
                                         f"following bundles: {missing_bundles}"
            )


            bundle_to_measure = dict(zip(table.bundle_id, table.measure_id))
            bundle_to_duration = dict(zip(table.bundle_id, table.duration))

            self.df['measure_id'] = self.df.bundle_id.map(bundle_to_measure)
            self.df['duration'] = self.df.bundle_id.map(bundle_to_duration)

    def calculate_uncertainty(self):
        self.log.log(f"Creating uncertainty for {len(self.df)} rows")
        uc = Uncertainty(self.df)
        uc.fill()
        self.df = uc.df

    def cap_estimates(self):
        for col in ['lower', 'mean', 'upper']:
            self.df.loc[self.df[col] < 0, col] = 0
            self.df.loc[(self.df[col] > 1) & (self.df.measure_id == 5), col] = 1

    def check_if_loaded(self):
        if not self._has_loaded:
            raise UploaderError("Must load before processing.")
        pass

    def process(self):
        self.check_if_loaded()

    def test(self):
        self.check_if_loaded()
        pipeline = TestingPipeline(df=self.df, logger=self.log)
        assert pipeline(not_null(ignore=["duration", "measure_id"]),
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
        path = pathlib.Path(path)

        columns_to_upload = ['age_group_id',
                             'bundle_id',
                             'cases',
                             'diagnosis_id',
                             'estimate_id',
                             'location_id',
                             'lower',
                             'mean',
                             'merged_nid',
                             'representative_id',
                             'run_id',
                             'sample_size',
                             'sex_id',
                             'source_type_id',
                             'standard_error',
                             'uncertainty_type_id',
                             'uncertainty_type_value',
                             'upper',
                             'year_end',
                             'year_start']

        self.dm.save(self.df.fillna("\\N")[columns_to_upload], path)

        schema = "clinical"
        table = "final_estimates_bundle"

        db = Database()
        db.load_odbc(odbc_profile)

        self.log.log(
            f"Uploading to {db.credentials.host} - {schema}.{table}\n"
        )
        input("Press enter to continue")

        db.write(path / "FILEPATH", schema, table)


class InpatientUploader(Uploader):
    """ An Uploader for inpatient data"""
    def __init__(self, run_id, name="inpatient_uploader"):
        super().__init__(
            name=name,
            run_id=run_id
        )

    def process(self, validate=True, maternal_ratios=False):
        super().process()


        self.log.log("Adding `cases` = `mean` * `sample_size`")
        self.df['cases'] = self.df['mean'] * self.df.sample_size


        self.log.log("Adding `source_type_id` = 10")
        self.df['source_type_id'] = 10


        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(empty_mean,
                           mean_greater_than_upper,
                           duplicates
        )




        self.format_columns()




        self.merge_bundle_info()





        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(impossible_prevalence)
        self.calculate_uncertainty()




        if maternal_ratios:
            self.log.log("Creating ratio bundles")
            ratios = {
                6113: (76, 667),
                6116: (76, 6107),
                6119: (6107, 75),
                6122: (6110, 6107),
                6125: (667, 6107),
            }


            builder = MaternalRatioBuilder(self.log)
            ratio_df = builder.ratio_bundles(self.df, ratios)

            ratio_df['run_id'] = self.run_id
            ratio_df['source_type_id'] = 10
            ratio_df['representative_id'] = 1



        if validate:





            df_list = []
            if 3419 in self.df.bundle_id.unique():
                self.log.log('Removing bundle 3419 from validations')
                self.df = self.df[self.df.bundle_id != 3419]
                df_list.append(self.df[self.df.bundle_id == 3419])

            pipeline = FilterPipeline(df=self.df, logger=self.log)
            self.df = pipeline(impossible_prevalence,
                               impossible_incidence
            )
            df_list.append(self.df)
            self.df = pd.concat(df_list)

        self.cap_estimates()


        if maternal_ratios:
            self.log.log(f"Appending {len(ratio_df)} maternal ratio rows")
            self.df = self.df.append(ratio_df, sort=False)

    def test(self):
        pipeline = TestingPipeline(df=self.df, logger=self.log)
        assert pipeline(not_null(ignore=["duration", "measure_id",
                                         "sample_size", "effective_sample_size",
                                         "cases"]
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
        super().__init__(
            name=name,
            run_id=run_id
        )

    def process(self):
        super().process()


        self.log.log("Adding `mean` = `cases`/`sample_size`")
        self.df['mean'] = self.df.cases / self.df.sample_size

        self.format_columns()
        self.merge_bundle_info()


        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(duplicates,
                           impossible_prevalence,
        )
        self.calculate_uncertainty()

        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(impossible_prevalence,
                           impossible_incidence
        )

        self.cap_estimates()

    def test(self):
        return super().test()

class ClaimsUploader(Uploader):
    """ An uploader for claims data"""
    def __init__(self, run_id, name="claims_uploader"):
        super().__init__(
            name=name,
            run_id=run_id
        )

    def process(self):
        super().process()


        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(empty_mean,
                           inf_mean,
        )

        self.format_columns()
        self.merge_bundle_info()


        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(duplicates,
                           impossible_prevalence,
        )
        self.calculate_uncertainty()

        pipeline = FilterPipeline(df=self.df, logger=self.log)
        self.df = pipeline(impossible_prevalence,
                           impossible_incidence
        )

        self.cap_estimates()

    def test(self):
        return super().test()

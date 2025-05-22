import logging
import os

import pandas as pd

from aggregator.aggregators import AggSynchronous
from aggregator.operators import Sum
from db_queries import get_age_weights, get_demographics, get_envelope, get_population
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from gbd import constants as gbd_constants
from hierarchies import dbtrees
from ihme_cc_gbd_schema.common import ModelStorageMetadata

import hybridizer.reference.db_connect as db_connect
from hybridizer.database import get_mortality_run_id

logger = logging.getLogger(__name__)


class ModelData:
    """
    Stores and manipulates the data and its properties for a given
    model_version_id
    """

    def __init__(
        self,
        model_version_id: int,
        model_folder: str,
        data_draws: pd.DataFrame,
        index_columns: list,
        envelope_column: str,
        pop_column: str,
        data_columns: list,
        conn_def: str,
        location_set_id: int,
        release_id: int,
        sex_id: int,
        user: str,
        envelope_proc_version_id: int,
        population_proc_version_id: int,
        refresh_id: int,
    ) -> None:
        # Assign inputs to attributes
        self.model_version_id: int = model_version_id
        self.model_folder: str = model_folder
        self.data_draws: pd.DataFrame = data_draws
        self.index_columns: list = index_columns
        self.envelope_column: str = envelope_column
        self.pop_column: str = pop_column
        self.data_columns: list = data_columns
        self.conn_def: str = conn_def
        self.location_set_id: int = location_set_id
        self.release_id: int = release_id
        self.sex_id: int = sex_id
        self.user: str = user
        self.envelope_run_id: int = get_mortality_run_id(envelope_proc_version_id)
        self.population_run_id: int = get_mortality_run_id(population_proc_version_id)
        self.refresh_id: int = refresh_id

        # Define other attributes
        self.demographics = get_demographics(gbd_team="cod", release_id=self.release_id)
        self.data_summaries = None
        self.draw_folder = os.path.join(self.model_folder, "draws")
        self.draw_file = os.path.join(self.draw_folder, "deaths_{sex_name}.h5")
        self.storage_pattern = os.path.relpath(self.draw_file, self.model_folder)
        self.draw_file_key = "data"

        # Methods that run when ModelData object is instantiated
        self.check_missing_locations()
        self.add_envelope_and_population()

    def add_envelope_and_population(self) -> None:
        """
        Global and Data-rich models save population and envelope used to data, but
        we need to override this in case the hybrid was run on mismatch versions
        to the column is consisten across all locations.
        """
        logger.info("Adding envelope and population columns to dataframe")
        # pull population and envelope data frames
        env_df = get_envelope(
            location_id=self.data_draws.location_id.unique().tolist(),
            year_id=self.data_draws.year_id.unique().tolist(),
            age_group_id=self.data_draws.age_group_id.unique().tolist(),
            sex_id=self.sex_id,
            with_hiv=self._get_envelope_with_hiv(),
            release_id=self.release_id,
            run_id=self.envelope_run_id,
        )
        env_df = env_df[["location_id", "year_id", "age_group_id", "mean"]]
        env_df.rename(columns={"mean": self.envelope_column}, inplace=True)
        pop_df = get_population(
            location_id=self.data_draws.location_id.unique().tolist(),
            year_id=self.data_draws.year_id.unique().tolist(),
            age_group_id=self.data_draws.age_group_id.unique().tolist(),
            sex_id=self.sex_id,
            release_id=self.release_id,
            run_id=self.population_run_id,
        )
        pop_df = pop_df[["location_id", "year_id", "age_group_id", "population"]]
        pop_df = pop_df.rename(columns={"population": self.pop_column})
        # replace previous columns and re-calculate deaths using current envelope
        self.data_draws[self.data_columns] = (
            self.data_draws[self.data_columns].values
            / self.data_draws[[self.envelope_column]].values
        )
        self.data_draws = self.data_draws.drop(
            [self.envelope_column, self.pop_column], axis=1
        )
        self.data_draws = self.data_draws.merge(
            env_df, on=["location_id", "year_id", "age_group_id"]
        )
        self.data_draws = self.data_draws.merge(
            pop_df, on=["location_id", "year_id", "age_group_id"]
        )
        self.data_draws[self.data_columns] = (
            self.data_draws[self.data_columns].values
            * self.data_draws[[self.envelope_column]].values
        )
        self.data_draws = self.format_draws(self.data_draws)

    def format_draws(self, data: pd.DataFrame) -> None:
        """
        Drops all columns except for index columns, envelope and pop columns,
        and data columns

        :param self:
        :param data: dataframe
            pandas dataframe to edit
        :return: dataframe
            pandas dataframe with only index, envelope, pop, and data columns
            retained
        """
        logger.info("Formatting model draws.")
        return data[
            self.index_columns + [self.envelope_column, self.pop_column] + self.data_columns
        ]

    def check_missing_locations(self) -> None:
        """
        Prints any missing locations, i.e. locations that are in estimated
        but not in draw_locations

        :param self:
        """
        logger.info("Check for missing locations.")
        draw_locations = self.data_draws["location_id"].drop_duplicates().tolist()
        estimated_locations = self.demographics["location_id"]
        if len(set(estimated_locations) - set(draw_locations)) > 0:
            logger.debug(
                "The following location IDs are missing from the draws {}".format(
                    ", ".join(
                        [str(x) for x in list(set(estimated_locations) - set(draw_locations))]
                    )
                )
            )
            raise Exception("Locations missing from the draws!")
        else:
            logger.debug("No missing locations!")

    def aggregate_locations(self) -> None:
        """
        Aggregate data up the location hierarchy and assign it to the data_draws
        attribute

        :param self:
        """
        logger.info("Aggregating locations.")

        io_mock = {}
        source = DrawSource({"draw_dict": io_mock, "name": "tmp"}, mem_read_func)
        sink = DrawSink({"draw_dict": io_mock, "name": "tmp"}, mem_write_func)
        sink.push(self.data_draws)
        # location
        operator = Sum(
            index_cols=[col for col in self.index_columns if col != "location_id"],
            value_cols=[self.envelope_column, self.pop_column] + self.data_columns,
        )
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in self.index_columns if col != "location_id"],
            aggregate_col="location_id",
            operator=operator,
        )
        loc_tree = dbtrees.loctree(
            location_set_id=self.location_set_id, release_id=self.release_id
        )
        aggregator.run(loc_tree)
        self.data_draws = source.content()

    def save_draws(self) -> None:
        """
        Saves the draws information to an hdf

        :param self:
        """
        logger.info("Saving draws.")
        sex_dict = {1: "male", 2: "female"}
        sex_name = sex_dict[self.sex_id]
        draws_filepath = self.draw_file.format(sex_name=sex_name)
        if not os.path.exists(self.draw_folder):
            os.makedirs(self.draw_folder)
        self._output_storage_metadata(sex_name=sex_name)
        self.data_draws.to_hdf(
            draws_filepath,
            self.draw_file_key,
            mode="w",
            format="table",
            data_columns=["location_id", "year_id", "sex_id", "age_group_id", "cause_id"],
        )

    def _output_storage_metadata(self, sex_name: str) -> None:
        """Outputs draw storage metadata if it does not exist."""
        output_file = os.path.join(self.model_folder, "version_metadata.json")
        if not os.path.exists(output_file):
            storage_metadata = ModelStorageMetadata.from_dict(
                {
                    "storage_pattern": self.storage_pattern.format(sex_name=sex_name),
                    "h5_tablename": self.draw_file_key,
                }
            )
            storage_metadata.to_file(directory=self.model_folder)

    def get_full_envelope(self) -> None:
        """
        For models with age restrictions, the all-ages group should use envelope
        for all ages. Pulls the envelope for most detailed age groups and
        all ages.
        """
        logger.info("Pulling in the envelope for all ages, and most-detailed age groups")
        # get the envelope, then merge in with the aggregated dfs
        env_df = get_envelope(
            location_id=self.data_draws.location_id.unique().tolist(),
            year_id=self.data_draws.year_id.unique().tolist(),
            age_group_id=self.data_draws.age_group_id.unique().tolist(),
            sex_id=self.sex_id,
            with_hiv=self._get_envelope_with_hiv(),
            release_id=self.release_id,
            run_id=self.envelope_run_id,
        )
        env_df = env_df[["location_id", "year_id", "age_group_id", "mean"]]
        env_df.rename(columns={"mean": self.envelope_column}, inplace=True)
        self.data_draws = self.data_draws.drop(self.envelope_column, axis=1)
        self.data_draws = self.data_draws.merge(
            env_df, on=["location_id", "year_id", "age_group_id"]
        )

    def generate_all_ages(self) -> None:
        """
        Adds the draws in all specific age groups to get the counts for all ages
        combined, then stores it in data_draws attribute

        :param self:
        """
        logger.info("Generating draws for all ages.")
        self.data_draws = self.data_draws.loc[
            self.data_draws["age_group_id"] != gbd_constants.age.ALL_AGES
        ]
        data = self.format_draws(self.data_draws)
        data = data.loc[data["age_group_id"].isin(self.demographics["age_group_id"])]
        # sum by indices (age, sex, location) to get the sum over all age groups
        data["age_group_id"] = gbd_constants.age.ALL_AGES
        data = data.groupby(self.index_columns).sum().reset_index()
        self.data_draws = pd.concat([self.data_draws, data])

    def generate_age_standardized(self) -> None:
        """
        Standardizes data_draws using age group weights from database

        :param self:
        """
        logger.info("Generating age-standardized draws.")
        # prep draws for merge
        self.data_draws = self.data_draws.loc[
            self.data_draws["age_group_id"] != gbd_constants.age.AGE_STANDARDIZED
        ]
        data = self.format_draws(self.data_draws)
        data = data.loc[data["age_group_id"].isin(self.demographics["age_group_id"])]
        # merge on age-weights and make adjusted rate
        age_standard_data = get_age_weights(release_id=self.release_id)
        data = pd.merge(data, age_standard_data, on="age_group_id")
        data[self.data_columns] = (
            data[self.data_columns].values
            * data[["age_group_weight_value"]].values
            / data[[self.pop_column]].values
        )
        # collapsing to generate ASR
        data["age_group_id"] = gbd_constants.age.AGE_STANDARDIZED
        data = data.groupby(self.index_columns).sum().reset_index()
        self.data_draws = pd.concat([self.data_draws, data])

    def generate_summaries(self) -> None:
        """
        Summarizes model data and stores summaries in data_summaries attribute

        :param self:
        """
        logger.info("Generate data summaries")
        # Copy draws
        self.data_summaries = self.data_draws.copy(deep=True)
        for column in self.data_columns:
            self.data_summaries.loc[
                self.data_summaries["age_group_id"] != gbd_constants.age.AGE_STANDARDIZED,
                column,
            ] = (
                self.data_summaries.loc[
                    self.data_summaries["age_group_id"] != gbd_constants.age.AGE_STANDARDIZED,
                    column,
                ]
                / self.data_summaries.loc[
                    self.data_summaries["age_group_id"] != gbd_constants.age.AGE_STANDARDIZED,
                    self.envelope_column,
                ]
            )
        self.data_summaries = self.data_summaries[self.index_columns + self.data_columns]
        self.data_summaries["mean_cf"] = self.data_summaries[self.data_columns].mean(axis=1)
        self.data_summaries["lower_cf"] = self.data_summaries[self.data_columns].quantile(
            0.025, axis=1
        )
        self.data_summaries["upper_cf"] = self.data_summaries[self.data_columns].quantile(
            0.975, axis=1
        )
        # Generate other columns
        self.data_summaries["model_version_id"] = self.model_version_id
        self.data_summaries = self.data_summaries[
            self.index_columns + ["model_version_id", "mean_cf", "lower_cf", "upper_cf"]
        ]

    def save_summaries(self) -> None:
        """
        Saves data_summaries to a csv file

        :param self:
        """
        logger.info("Save data summaries.")
        summary_filepath = self.model_folder + "/summaries.csv"
        self.data_summaries.to_csv(summary_filepath, index=False)

    def upload_summaries(self) -> None:
        """Writes data_summaries to the database."""
        logger.info("Upload summaries.")
        db_connect.wipe_database_upload(
            model_version_id=self.model_version_id, conn_def=self.conn_def
        )
        data = self.data_summaries[
            [
                "model_version_id",
                "year_id",
                "location_id",
                "sex_id",
                "age_group_id",
                "mean_cf",
                "lower_cf",
                "upper_cf",
            ]
        ].reset_index(drop=True)
        db_connect.write_data(df=data, db="cod", table="model", conn_def=self.conn_def)

    def _get_envelope_with_hiv(self) -> int:
        """Returns whether to include HIV in envelope.

        Mid GBD 2023 it was determined that CODEm should use the with-HIV envelope rather
        than without. CoD refreshes released after refresh_id 68 incorporate HIV into their
        data. For any model using a refresh >= 69, use the with-HIV envelope for all
        get_envelope() calls.
        """
        _FIRST_HIV_REFRESH = 69
        with_hiv = 1 if self.refresh_id >= _FIRST_HIV_REFRESH else 0
        return with_hiv

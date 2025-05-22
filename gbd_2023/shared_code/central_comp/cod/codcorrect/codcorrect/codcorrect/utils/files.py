"""File-related functions and file system abstractions."""

import copy
import os
import pathlib
from typing import Dict, List, Optional, Union, cast

import pandas as pd

import db_queries
import gbd.constants as gbd_constants
from draw_sources.draw_sources import DrawSink, DrawSource
from ihme_cc_gbd_schema.common import ModelStorageMetadata

from codcorrect.legacy.utils.constants import (
    Columns,
    DataBases,
    ModelVersionTypeId,
    SummaryType,
)

# File path directory constants
AGGREGATED: str = "aggregated"
COD: str = "cod"
DEATHS: str = "1"
DIAGNOSTICS: str = "diagnostics"
DRAWS: str = "draws"
GBD: str = "gbd"
INPUT_FILES: str = "input_files"
LOGS: str = "logs"
MULTI: str = "multi"
PARAMETERS: str = "parameters"
RESCALED: str = "rescaled"
ROOT: str = "FILEPATH"
SHOCKS: str = "shocks"
SINGLE: str = "single"
STDERR: str = "stderr"
STDOUT: str = "stdout"
SUMMARIES: str = "summaries"
UNAGGREGATED: str = "unaggregated"
UNSCALED: str = "unscaled"
YEARS_PLACEHOLDER: str = "YEARS_PLACEHOLDER"
YEARS_START_PLACEHOLDER: str = "YEARS_START_PLACEHOLDER"
YLLS: str = "4"

# File constants
PARAMETERS_FILE_FORMAT: str = f"{{root_path}}/{{version_id}}/{PARAMETERS}/parameters.pkl"


# Misc constants
CAUSE_AGG_WORKERS: int = 10
SUMMARIZE_WORKERS: int = 2
H5_DEFAULT_KEY: str = "data"
H5_DRAWS_KEY: str = "draws"

# Recursive type alias:
# We use this in the FileSystem class for the data structure
Directory = Dict[str, "Directory"]


class FileSystem:
    """File system abstraction for CodCorrect.

    FileSystem has functions for creating/accessing file paths used in CodCorrect runs.
    Each folder has its own caching function, ie cache_inputs, cache_results.

    It could be represented as:

    FILE_SYSTEM = {
        "draws": {
            "deaths": {},
            "ylls": {},
        },
        "inputs": {},
        "logs": {
            "step1": {}
            "step2": {},
            "step3": {}
        }
    }

    Above, we omit the root (as it's assumed), and represent each subdirectory as its own
    dictionary, where its name is the key and its values are its own subdirectories,
    represented in the same way. If a directory does not have any subdirectories within it,
    this is represented with an empty dictionary.

    When the file system is created (make_file_system), we create a set of all the folders
    for record keeping purposes:

    directories_set = {
        "FILEPATHS"
    }

    Arguments:
        output_path: the root of the file system. Expected parameter to instantiate
        year_ids: All year ids for the CodCorrect run, used to dynamically create
            year-specific directories in some cases
        year_start_ids: Year start ids for the CodCorrect run, used to dynamically create
            year-specific percent change directories in some cases
        year_end_ids: Year start ids for the CodCorrect run, used to dynamically create
            year-specific percent change directories in some cases

    Additional Fields:
        FILE_SYSTEM: a human-readable version of what the project file system structure
            looks like. New folders/edits go here and require a corresponding caching function.
        file_system: a copy of FILE_SYSTEM that is editable by class instances
        directories_set: A set of a directories in the file system
    """

    FILE_SYSTEM: Directory = {
        AGGREGATED: {
            RESCALED: {DEATHS: {}, YLLS: {}},
            SHOCKS: {DEATHS: {}, YLLS: {}},
            UNSCALED: {DEATHS: {}, YLLS: {}},
        },
        DIAGNOSTICS: {},
        DRAWS: {DEATHS: {}, YLLS: {}},
        INPUT_FILES: {},
        LOGS: {STDERR: {}, STDOUT: {}},
        PARAMETERS: {},
        SUMMARIES: {
            COD: {DEATHS: {YEARS_PLACEHOLDER: {}}},
            GBD: {
                SINGLE: {DEATHS: {YEARS_PLACEHOLDER: {}}, YLLS: {YEARS_PLACEHOLDER: {}}},
                MULTI: {
                    DEATHS: {YEARS_START_PLACEHOLDER: {}},
                    YLLS: {YEARS_START_PLACEHOLDER: {}},
                },
            },
        },
        UNAGGREGATED: {
            RESCALED: {DEATHS: {}, YLLS: {}},
            SHOCKS: {DEATHS: {}, YLLS: {}},
            UNSCALED: {DEATHS: {}, YLLS: {}, DIAGNOSTICS: {DEATHS: {}}},
        },
    }

    def __init__(
        self,
        output_path: str,
        year_ids: List[int],
        year_start_ids: Optional[List[int]],
        year_end_ids: Optional[List[int]],
    ):
        self.output_path = output_path
        self.year_ids = year_ids
        self.year_start_ids = year_start_ids
        self.year_end_ids = year_end_ids
        self.directories_set = {self.output_path}

        # Copy the original so dynamic edits don't affect other class instances
        self.file_system = copy.deepcopy(self.FILE_SYSTEM)

    def make_file_system(self) -> None:
        """Makes the file system for the run, creating all necessary folders.

        Builds a set of all directories in the file system at the same time
        from the human-readable file system (FILE_SYSTEM).
        """
        self._make_file_system_recurse(self.output_path, self.file_system)

        # write draw storage metadata
        storage_metadata = ModelStorageMetadata.from_dict(
            {
                "storage_pattern": "FILEPATH",
                "h5_tablename": H5_DRAWS_KEY,
            }
        )
        storage_metadata.to_file(directory=self.output_path)

    def _make_file_system_recurse(self, directory: str, subdirectories: Directory) -> None:
        """Recursive helper function for making run directories.

        Takes a not-yet existing directory and that directory's sub-directory system.
        Creates the directory and returns if there are no subdirectories. Otherwise,
        repeats the process.

        Args:
            directory: a complete file path for a directory to be created.
            subdirectories: A subtree of the file_system to recurse down
        """
        # Create the given directory, set the permissions, and add to the directories set
        os.makedirs(directory, exist_ok=True)
        os.chmod(directory, 0o775)
        self.directories_set.add(directory)

        # Base case: if directory doesn't have any subdirectories (empty dict), stop here
        if not subdirectories:
            return

        # Fill in placeholders (year placeholder, year start placeholders) appropriately:
        if YEARS_PLACEHOLDER in subdirectories:
            placeholder_subdirs = subdirectories.pop(YEARS_PLACEHOLDER)
            for year_id in self.year_ids:
                subdirectories[str(year_id)] = copy.deepcopy(placeholder_subdirs)

        if YEARS_START_PLACEHOLDER in subdirectories:
            placeholder_subdirs = subdirectories.pop(YEARS_START_PLACEHOLDER)
            if self.year_start_ids:
                for year_start_id, year_end_id in zip(self.year_start_ids, self.year_end_ids):
                    subdirectories[f"{year_start_id}_{year_end_id}"] = copy.deepcopy(
                        placeholder_subdirs
                    )

        # For each subdirectory, continue the same process one level down.
        for subdir in subdirectories:
            new_dir_path = os.path.join(directory, subdir)

            # Invariant check: each entry in the file system should be unique
            if new_dir_path in self.directories_set:
                raise RuntimeError(
                    f"More than one directory named '{subdir}' exists in file system. Index "
                    "cannot have entries for both."
                )

            self._make_file_system_recurse(new_dir_path, subdirectories[subdir])

    # HELPER FUNCTIONS

    def get_file_path(self, *directories: Union[str, int]) -> pathlib.Path:
        """Returns the full file path of the desired directory.

        If make_file_system has been run, this function validates that the desired
        directory exists in the file system. This allows FileSystem's functions to be used
        both with and without actually creating the file system.

        Args:
            *directories: 1 or more arguments to describe the directory where each argument is
                a directory name, each deeper than the previous. Ex: "unaggregated",
                "rescaled" refers to FILEPATH

        Raises:
            ValueError: if the directory does not exist (if make_file_system has been run)
        """
        # Convert any ints into strings
        directories = [str(directory) for directory in directories]
        directory_path = os.path.join(self.output_path, *directories)

        # Only check that dir is valid if make_file_system has been run (directories_set > 1)
        if len(self.directories_set) > 1 and directory_path not in self.directories_set:
            raise ValueError(
                f"Directory '{directory_path}' does not exist in the file system."
            )

        return pathlib.Path(directory_path)

    def sink_draws(
        self,
        directory: Union[str, pathlib.Path],
        file_pattern: str,
        draws: pd.DataFrame,
        h5_tablename: str = H5_DEFAULT_KEY,
    ) -> None:
        """Sink draws as H5 files into the directory using the given file pattern.

        Args:
            directory: directory where to sink (save) draws.
            file_pattern: draw file pattern. Can be given fillable fields like {sex_id}
                for sharding. Ex: sinking draws with values 1 and 2 in column 'sex_id' where
                file_pattern is '{sex_id}.h5' will result in saving two files: 1.h5 and 2.h5
                where the former only contains data where sex_id == 1 and the latter where
                sex_id == 2.
            draws: the draws to save
            h5_tablename: h5 tablename key to use for draws. Defaults to H5_DEFAULT_KEY

        Raise:
            ValueError: if the directory does not exist in the file system or the file_pattern
                provided is not an h5 file.
        """
        directory = str(directory)
        if directory not in self.directories_set:
            raise ValueError(f"Directory '{directory}' does not exist in the file system.")

        if not file_pattern.endswith(".h5"):
            raise ValueError(
                f"File pattern '{file_pattern}' is not an h5 file, expected file to end with "
                "'.h5'."
            )

        draw_sink = DrawSink(
            {
                "draw_dir": directory,
                "file_pattern": file_pattern,
                "h5_tablename": h5_tablename,
            }
        )
        draw_sink.push(draws, append=False)

    @staticmethod
    def source_draws(
        directory: Union[str, pathlib.Path],
        file_pattern: str,
        filters: Optional[Dict[str, Union[int, List[int]]]] = None,
        h5_tablename: str = H5_DEFAULT_KEY,
        num_workers: int = 1,
    ) -> pd.DataFrame:
        """Source draws from directory with given file pattern and filters.

        Args:
            directory: directory where to sink (save) draws
            file_pattern: draw file pattern. Can be given fillable fields like {sex_id}
                for reading in sharded files. Ex: sourcing a file pattern like '{sex_id}.h5'
                will result in reading in files in the directory and assuming their particular
                file names are the sex_id for the file.
            filters: Optional dictionary for filtering what data is returned. If provided,
                keys are the columns names and values are the values to filter the data by
            h5_tablename: h5 tablename key to use for draws. Defaults to H5_DEFAULT_KEY
            num_workers: number of workers to use when reading in draws

        Raises:
            ValueError: if the resulting draws dataframe is empty
        """
        draws = DrawSource(
            {
                "draw_dir": directory,
                "file_pattern": file_pattern,
                "h5_tablename": h5_tablename,
                "num_workers": num_workers,
            }
        ).content(filters=filters)

        if draws.empty:
            raise ValueError(
                f"Dataframe from draw files in directory '{directory}' for file_pattern "
                f"{file_pattern} and filters {filters} is empty."
            )

        return draws

    def cache_hdf(
        self,
        data: pd.DataFrame,
        file_path: Union[str, pathlib.Path],
        index_columns: Optional[List[str]] = None,
    ) -> None:
        """Write data to file_path as an HDF file with optional indexed data columns.

        Args:
            data: Data to cache/save
            file_path: Where to save the data
            index_columns: Optional, list of data columns with which the h5 should be indexed

        Raise:
            ValueError: if the file_path provided is not an h5 file.
        """
        file_path = str(file_path)
        if not file_path.endswith(".h5"):
            raise ValueError(
                f"File path '{file_path}' is not an h5 file, expected file to end with "
                "'.h5'."
            )

        data.to_hdf(
            file_path,
            key=H5_DEFAULT_KEY,
            mode="w",
            format="table" if index_columns else "fixed",
            data_columns=index_columns,
        )

    @staticmethod
    def read_cached_hdf(
        file_path: Union[str, pathlib.Path],
        where: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read cached DataFrame from h5 file.

        Arguments:
            file_path: Where the h5 file should be read from.
            where: Optional list of where clauses to use when reading cached h5.
                 E.g. ['sex_id==2']
            columns: columns to return. Defaults to None, meaning all columns

        Raises:
                FileNotFoundError: if file containing cached DataFrame does not exist
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Cached file does not exist: {file_path}")

        return cast(pd.DataFrame, pd.read_hdf(file_path, where=where, columns=columns))

    def save_csv(self, data: pd.DataFrame, file_path: Union[str, pathlib.Path]) -> None:
        """Save data as a csv with 755 permissions.

        Args:
            data: Data to cache/save
            file_path: Where to save the data

        Raise:
            ValueError: if the file_path provided is not a csv file.
        """
        file_path = pathlib.Path(file_path)
        if not file_path.suffix == ".csv":
            raise ValueError(
                f"File path '{file_path}' is not a csv file, expected file to end with "
                "'.csv'."
            )

        data.to_csv(file_path, index=False, encoding="utf8")
        file_path.chmod(0o775)

    @staticmethod
    def read_csv(file_path: Union[str, pathlib.Path]) -> pd.DataFrame:
        """Read in csv at the given file path.

        Note:
            While currently a one-line function, separating the interface gives
            more flexibility in the future in case the logic becomes more complex.

        Args:
            file_path: Where to read the data
        """
        return pd.read_csv(file_path)

    # INPUT_FILES

    def cache_correction_hierarchy(self, correction_hierarchy: pd.DataFrame) -> None:
        """Cache the correction hierarchy, generated from the base cause set.
        """
        file_path = self.get_file_path(INPUT_FILES) / "correction_hierarchy.csv"
        self.save_csv(correction_hierarchy, file_path)

    def cache_input_model_tracking_report(
        self, version_id: int, input_model_df: pd.DataFrame
    ) -> None:
        """Cache the input model tracking report, which lists all input CoD model versions.
        """
        file_path = self.get_file_path(INPUT_FILES) / f"codcorrect_v{version_id}_models.csv"
        input_model_df.to_csv(file_path, index=False, encoding="utf8")

    def cache_spacetime_restrictions(self, restrictions: pd.DataFrame) -> None:
        """Cache spacetime restrictions.

        Args:
            restrictions: dataframe of spacetime restrictions
        """
        file_path = self.get_file_path(INPUT_FILES) / "spacetime_restrictions.h5"
        index_columns = [Columns.CAUSE_ID, Columns.LOCATION_ID, Columns.YEAR_ID]

        self.cache_hdf(restrictions, file_path, index_columns=index_columns)

    def read_spacetime_restrictions(self) -> pd.DataFrame:
        """Read in cached spacetime restrictions."""
        file_path = self.get_file_path(INPUT_FILES) / "spacetime_restrictions.h5"
        columns = [Columns.CAUSE_ID, Columns.LOCATION_ID, Columns.YEAR_ID]

        return self.read_cached_hdf(file_path, columns=columns)

    def cache_envelope_draws(self, envelope_draws: pd.DataFrame) -> None:
        """Cache mortality envelope draws.

        Args:
            envelope_draws: dataframe of envelope draws
        """
        file_path = self.get_file_path(INPUT_FILES) / "envelope_draws.h5"
        index_columns = Columns.DEMOGRAPHIC_INDEX

        self.cache_hdf(envelope_draws, file_path, index_columns=index_columns)

    def read_envelope_draws(
        self, location_id: int, sex_id: int, draw_cols: List[int]
    ) -> pd.DataFrame:
        """Read envelope draws for a given location and sex.

        Args:
            location_id: Location to return draws for
            sex_id: Sex to return draws for
            draw_cols: the draw columns to return. Max is a list of 1000 draws

        """
        file_path = self.get_file_path(INPUT_FILES) / "envelope_draws.h5"
        where = [f"{Columns.LOCATION_ID}=={location_id}", f"{Columns.SEX_ID}=={sex_id}"]
        columns = cast(List[Union[int, str]], Columns.DEMOGRAPHIC_INDEX) + draw_cols

        return self.read_cached_hdf(file_path, where=where, columns=columns)

    def cache_envelope_summary(self, envelope_summary: pd.DataFrame) -> None:
        """Cache mortality envelope summary.

        Args:
            envelope_summary: dataframe of envelope summary
        """
        file_path = self.get_file_path(INPUT_FILES) / "envelope_summary.h5"
        index_columns = Columns.DEMOGRAPHIC_INDEX

        self.cache_hdf(envelope_summary, file_path, index_columns=index_columns)

    def read_envelope_summary(
        self, location_id: int, sex_id: Optional[int] = None, year_id: Optional[int] = None
    ) -> pd.DataFrame:
        """Read envelope summary for a given location and sex.

        Args:
            location_id: Location to return summary for
            sex_id: Sex to return summary for. Optional, if not provided, returns all sexes
            year_id: Year to return summary for. Optional, if not provided, returns all years
        """
        file_path = self.get_file_path(INPUT_FILES) / "envelope_summary.h5"

        # Set filtering based on if given sex_id/year_id
        where = [f"{Columns.LOCATION_ID}=={location_id}"]
        if sex_id:
            where.append(f"{Columns.SEX_ID}=={sex_id}")

        if year_id:
            where.append(f"{Columns.YEAR_ID}=={year_id}")

        columns = Columns.DEMOGRAPHIC_INDEX + [Columns.ENVELOPE]

        return self.read_cached_hdf(file_path, where=where, columns=columns)

    def cache_population(self, population: pd.DataFrame) -> None:
        """Cache population.

        Args:
            population: dataframe of population
        """
        file_path = self.get_file_path(INPUT_FILES) / "pop.h5"
        index_columns = Columns.DEMOGRAPHIC_INDEX

        self.cache_hdf(population, file_path, index_columns=index_columns)

    def read_population(self, location_id: int, year_id: int) -> pd.DataFrame:
        """Read population for a given location and year.

        Args:
            location_id: Location to return population for
            year_id: Year to return population for
        """
        file_path = self.get_file_path(INPUT_FILES) / "pop.h5"
        where = [f"{Columns.LOCATION_ID}=={location_id}", f"{Columns.YEAR_ID}=={year_id}"]
        columns = Columns.DEMOGRAPHIC_INDEX + [Columns.POPULATION]

        return self.read_cached_hdf(file_path, where=where, columns=columns)

    def cache_pred_ex(self, pred_ex: pd.DataFrame) -> None:
        """Cache predicted life expectancy (pred ex).

        Args:
            pred_ex: dataframe of predicted life expectancy
        """
        file_path = self.get_file_path(INPUT_FILES) / "pred_ex.h5"
        index_columns = [Columns.LOCATION_ID, Columns.SEX_ID, Columns.YEAR_ID]

        self.cache_hdf(pred_ex, file_path, index_columns=index_columns)

    def read_pred_ex(self, location_id: int, sex_id: int) -> pd.DataFrame:
        """Read predicted life expectancy (pred ex).

        Args:
            location_id: Location to return pred ex for
            sex_id: Sex to return pred ex for
        """
        file_path = self.get_file_path(INPUT_FILES) / "pred_ex.h5"
        where = [f"{Columns.LOCATION_ID}=={location_id}", f"{Columns.SEX_ID}=={sex_id}"]
        columns = Columns.INDEX + [Columns.PRED_EX]

        return self.read_cached_hdf(file_path, where=where, columns=columns)

    def cache_regional_scalars(self, regional_scalars: pd.DataFrame) -> None:
        """Cache regional scalars.

        Args:
            regional_scalars: dataframe of regional_scalars
        """
        file_path = self.get_file_path(INPUT_FILES) / "regional_scalars.h5"
        index_columns = [Columns.LOCATION_ID, Columns.SEX_ID, Columns.YEAR_ID]

        self.cache_hdf(regional_scalars, file_path, index_columns=index_columns)

    def read_regional_scalars(self) -> pd.DataFrame:
        """Read regional scalars.

        Returns all regional scalars without filtering.
        """
        file_path = self.get_file_path(INPUT_FILES) / "regional_scalars.h5"
        columns = [Columns.YEAR_ID, Columns.LOCATION_ID, Columns.MEAN]

        return self.read_cached_hdf(file_path, columns=columns)

    # COD MODEL DRAWS

    def save_validated_draws(self, draws: pd.DataFrame, model_version_id: int) -> None:
        """Save validated draws for a CoD model as a single h5 file.

        Args:
            model_version_id: ID of the CoD model
            draws: validated draws

        Raises:
            ValueError if draws have more than one model version type
        """
        # Validate model_version_type_id column in draws
        model_version_type_id = draws.model_version_type_id.unique()
        if len(model_version_type_id) != 1:
            raise ValueError(
                "Expected exactly one model_version_type_id in validated draws, got "
                f"{len(model_version_type_id)}: {model_version_type_id}"
            )

        # Set directory based on exempt from scaling (shocks) or not
        model_version_type_id = int(model_version_type_id[0])
        directory = (
            self.get_file_path(UNAGGREGATED, SHOCKS, DEATHS)
            if model_version_type_id in ModelVersionTypeId.EXEMPT_TYPE_IDS
            else self.get_file_path(UNAGGREGATED, UNSCALED, DEATHS)
        )

        self.sink_draws(directory, f"{model_version_id}.h5", draws)

    def read_unscaled_draws(
        self, location_id: int, sex_id: int, num_workers: int = 10
    ) -> pd.DataFrame:
        """Read unscaled CoD model draws, used in scaling.

        Args:
            location_id: Location to return draws for
            sex_id: Sex to return draws for
            num_workers: number of workers to read draws in with, passed in to DrawSource.

        Raises:
            InvalidSpec if cannot find unscaled draws for the location and sex
        """
        directory = self.get_file_path(UNAGGREGATED, UNSCALED, DEATHS)
        file_pattern = "{model_version_id}.h5"
        filters = {Columns.LOCATION_ID: location_id, Columns.SEX_ID: sex_id}

        return self.source_draws(directory, file_pattern, filters, num_workers=num_workers)

    # CORRECTION / SCALING

    def save_pre_scaling_draws(self, draws: pd.DataFrame) -> None:
        """Saves pre-scaling (unscaled) draws for diagnostic purposes.

        Assumes draws have a single location id and sex id.

        Args:
            draws: pre-scaling (unscaled) draws
        """
        directory = self.get_file_path(UNAGGREGATED, UNSCALED, DIAGNOSTICS, DEATHS)
        file_pattern = "unscaled_{location_id}_{sex_id}.h5"

        self.sink_draws(directory, file_pattern, draws)

    def save_rescaled_draws(self, draws: pd.DataFrame) -> None:
        """Saves rescaled (aka corrected, scaled) draws.

        Assumes draws have a single location id and sex id.

        Args:
            draws: rescaled draws
        """
        directory = self.get_file_path(UNAGGREGATED, RESCALED, DEATHS)
        file_pattern = "rescaled_{location_id}_{sex_id}.h5"

        self.sink_draws(directory, file_pattern, draws)

    # CAUSE AGGREGATION

    def read_unaggregated_unscaled_draws(self, location_id: int, sex_id: int) -> pd.DataFrame:
        """Read in unaggregated unscaled (pre-scaling) draws.

        Args:
            location_id: Location to return draws for
            sex_id: Sex to return draws for
        """
        directory = self.get_file_path(UNAGGREGATED, UNSCALED, DIAGNOSTICS, DEATHS)
        file_pattern = f"unscaled_{location_id}_{sex_id}.h5"

        return self.source_draws(directory, file_pattern, num_workers=CAUSE_AGG_WORKERS)

    def read_unaggregated_rescaled_draws(self, location_id: int, sex_id: int) -> pd.DataFrame:
        """Read in unaggregated rescaled draws.

        Args:
            location_id: Location to return draws for
            sex_id: Sex to return draws for
        """
        directory = self.get_file_path(UNAGGREGATED, RESCALED, DEATHS)
        file_pattern = f"rescaled_{location_id}_{sex_id}.h5"

        return self.source_draws(directory, file_pattern, num_workers=CAUSE_AGG_WORKERS)

    def read_unaggregated_shocks_draws(self, location_id: int, sex_id: int) -> pd.DataFrame:
        """Read in unaggregated shocks draws.

        Args:
            location_id: Location to return draws for
            sex_id: Sex to return draws for
        """
        directory = self.get_file_path(UNAGGREGATED, SHOCKS, DEATHS)
        file_pattern = "{model_version_id}.h5"
        filters = {Columns.LOCATION_ID: location_id, Columns.SEX_ID: sex_id}

        return self.source_draws(
            directory, file_pattern, filters=filters, num_workers=CAUSE_AGG_WORKERS
        )

    def save_aggregated_unscaled_draws(self, draws: pd.DataFrame) -> None:
        """Saves aggregated unscaled draws, sharded by location, sex and year.

        Assumes files are unique by location and sex but could have multiple years.

        """
        directory = self.get_file_path(AGGREGATED, UNSCALED, DEATHS)
        file_pattern = "{sex_id}_{location_id}_{year_id}.h5"

        self.sink_draws(directory, file_pattern, draws)

    def save_aggregated_rescaled_draws(self, draws: pd.DataFrame) -> None:
        """Saves aggregated unscaled draws, sharded by location, sex and year.

        Assumes files are unique by location and sex but could have multiple years.
        """
        directory = self.get_file_path(AGGREGATED, RESCALED, DEATHS)
        file_pattern = "{sex_id}_{location_id}_{year_id}.h5"

        self.sink_draws(directory, file_pattern, draws)

    def save_aggregated_shocks_draws(self, draws: pd.DataFrame) -> None:
        """Saves aggregated unscaled draws, sharded by location, sex and year.

        Assumes files are unique by location and sex but could have multiple years.
        """
        directory = self.get_file_path(AGGREGATED, SHOCKS, DEATHS)
        file_pattern = "{sex_id}_{location_id}_{year_id}.h5"

        self.sink_draws(directory, file_pattern, draws)

    def save_diagnostics(
        self, diagnostics: pd.DataFrame, location_id: int, sex_id: int
    ) -> None:
        """Save cause aggregation diagnostics, which have values by cause pre/post scaling.
        """
        file_path = (
            self.get_file_path(DIAGNOSTICS) / f"diagnostics_{sex_id}_{location_id}.csv"
        )

        self.save_csv(diagnostics, file_path)

    # YLLS

    def read_aggregated_rescaled_draws(
        self,
        location_id: int,
        sex_id: Optional[int] = None,
        year_id: Optional[int] = None,
        measure_id: int = gbd_constants.measures.DEATH,
    ) -> pd.DataFrame:
        """Read in aggregated rescaled draws.

        This function can be used both for reading in deaths and YLLs. Returns all years
        if sex_id is provided, and returns all sexes if year_id is given.

        Args:
            location_id: Location to return draws for
            sex_id: Sex to return draws for. Optional, sex_id OR year_id must be given
            year_id: Year to return draws for. Optional, sex_id OR year_id must be given
            measure_id: ID for deaths (1) or YLLs (4). Defaults to deaths
        """
        # Confirm EITHER sex_id OR year_id provided, not both or neither
        if not bool(sex_id) ^ bool(year_id):
            raise ValueError(
                f"Either sex_id (given: {sex_id}) or year_id (given: {year_id}) must be "
                "provided, not both or neither."
            )

        directory = (
            self.get_file_path(AGGREGATED, RESCALED, DEATHS)
            if measure_id == gbd_constants.measures.DEATH
            else self.get_file_path(AGGREGATED, RESCALED, YLLS)
        )

        # Decide file pattern based on if either sex_id or year_id provided
        # If sex_id provided, we'll read in all years. If year_id provided,
        # we'll read in all sexes
        if sex_id:
            file_pattern = f"{sex_id}_{location_id}_{{year_id}}.h5"
            filters = {Columns.LOCATION_ID: location_id, Columns.SEX_ID: sex_id}
        else:
            file_pattern = f"{{sex_id}}_{location_id}_{year_id}.h5"
            filters = {Columns.LOCATION_ID: location_id, Columns.YEAR_ID: year_id}

        return self.source_draws(
            directory, file_pattern, filters=filters, num_workers=CAUSE_AGG_WORKERS
        )

    def read_aggregated_shock_draws(
        self, location_id: int, sex_id: int, measure_id: int = gbd_constants.measures.DEATH
    ) -> pd.DataFrame:
        """Read in aggregated rescaled draws.

        Reads in all years from the run. This function can be used both for reading in deaths
        and YLLs.

        Args:
            location_id: Location to return draws for
            sex_id: Sex to return draws for
            measure_id: ID for deaths (1) or YLLs (4). Defaults to deaths
        """
        directory = (
            self.get_file_path(AGGREGATED, SHOCKS, DEATHS)
            if measure_id == gbd_constants.measures.DEATH
            else self.get_file_path(AGGREGATED, SHOCKS, YLLS)
        )
        file_pattern = f"{sex_id}_{location_id}_{{year_id}}.h5"
        filters = {Columns.LOCATION_ID: location_id, Columns.SEX_ID: sex_id}

        return self.source_draws(
            directory, file_pattern, filters=filters, num_workers=CAUSE_AGG_WORKERS
        )

    def save_all_ylls(
        self,
        non_shock_ylls: pd.DataFrame,
        shock_ylls: pd.DataFrame,
        location_id: int,
        sex_id: int,
    ) -> None:
        """Save YLL draws for non-shocks and shocks (separately).

        Splits out draws by year.
        """
        non_shock_directory = self.get_file_path(AGGREGATED, RESCALED, YLLS)
        shock_directory = self.get_file_path(AGGREGATED, SHOCKS, YLLS)
        file_pattern = f"{sex_id}_{location_id}_{{year_id}}.h5"

        self.sink_draws(non_shock_directory, file_pattern, non_shock_ylls)
        self.sink_draws(shock_directory, file_pattern, shock_ylls)

    # LOCATION AGGREGATION

    def get_location_aggregation_draw_source(
        self, aggregation_type: str, measure_id: int, year_id: int
    ) -> DrawSource:
        """Get draw source for location aggregation, used directly by the aggregation function.

        Args:
            aggregation_type: type of draws to aggregate. 'rescaled', 'unscaled', 'shocks'
            measure_id: ID of the measure of draws to aggregate. 1 (deaths), 4 (YLLs)
            year_id: year to do location aggregation for
        """
        directory = self.get_file_path(AGGREGATED, aggregation_type, measure_id)
        file_pattern = f"{{sex_id}}_{{location_id}}_{year_id}.h5"

        return DrawSource(
            {
                "draw_dir": directory,
                "file_pattern": file_pattern,
                "h5_tablename": H5_DEFAULT_KEY,
            }
        )

    def get_location_aggregation_draw_sink(
        self, aggregation_type: str, measure_id: int, year_id: int
    ) -> DrawSink:
        """Get draw sink for location aggregation, used directly by the aggregation function.

        Args:
            aggregation_type: type of draws aggregated. 'rescaled', 'unscaled', 'shocks'
            measure_id: ID of the measure of draws aggregated. 1 (deaths), 4 (YLLs)
            year_id: year location aggregation was done for
        """
        directory = self.get_file_path(AGGREGATED, aggregation_type, measure_id)
        file_pattern = f"{{sex_id}}_{{location_id}}_{year_id}.h5"

        return DrawSink(
            {
                "draw_dir": directory,
                "file_pattern": file_pattern,
                "h5_tablename": H5_DEFAULT_KEY,
            }
        )

    def delete_existing_location_aggregates(
        self,
        aggregation_type: str,
        measure_id: int,
        location_set_id: int,
        year_id: int,
        release_id: int,
    ) -> None:
        """Deleting any existing location aggregates.

        Assumes files follow pattern: "{sex_id}_{location_id}_{year_id}.h5"

        Args:
            aggregation_type: type of draws aggregated. 'rescaled', 'unscaled', 'shocks'
            measure_id: ID of the measure of draws aggregated. 1 (deaths), 4 (YLLs)
            location_set_id: ID of the location set that location aggregates are being created
                for
            year_id: year that location aggregates are being created for
            release_id: release ID of the run. Determines what location ids are deleted
        """
        directory = self.get_file_path(AGGREGATED, aggregation_type, measure_id)

        # Retrieve all aggregate locations for the given parameters
        loc_metadata = db_queries.get_location_metadata(
            location_set_id=location_set_id, release_id=release_id
        )
        aggregate_location_ids = loc_metadata.loc[
            loc_metadata[Columns.MOST_DETAILED] != 1, Columns.LOCATION_ID
        ].tolist()

        # Loop through all files, leaving most detailed locations and other years alone
        # and deleting aggregates
        all_files = os.listdir(directory)
        for file in all_files:
            _, location_id, file_year_id = file.split("_")
            file_year_id, _ = file_year_id.split(".")
            if int(location_id) in aggregate_location_ids and int(file_year_id) == year_id:
                os.remove(os.path.join(directory, file))

    # DIAGNOSTICS

    def read_aggregated_unscaled_draws(self, location_id: int, sex_id: int) -> pd.DataFrame:
        """Read in aggregated unscaled draws.

        Reads in all years from the run.

        Args:
            location_id: Location to return draws for
            sex_id: Sex to return draws for
        """
        directory = self.get_file_path(AGGREGATED, UNSCALED, DEATHS)
        file_pattern = f"{sex_id}_{location_id}_{{year_id}}.h5"
        filters = {Columns.LOCATION_ID: location_id, Columns.SEX_ID: sex_id}

        return self.source_draws(
            directory, file_pattern, filters=filters, num_workers=CAUSE_AGG_WORKERS
        )

    def read_diagnostics(self, location_id: int, sex_id: int) -> pd.DataFrame:
        """Read diagnostic file created during cause aggregation.

        Args:
            location_id: Location to return diagnostics for
            sex_id: Sex to return diagnostics for
        """
        file_path = (
            self.get_file_path(DIAGNOSTICS) / f"diagnostics_{sex_id}_{location_id}.csv"
        )

        return self.read_csv(file_path)

    # APPEND SHOCKS

    def save_appended_shock_draws(
        self, draws: pd.DataFrame, location_id: int, sex_id: int, measure_id: int
    ) -> None:
        """Save draws after appending shocks (ie. combining shocks and non-shocks).

        Splits out draws by year. Saved in different folder depending on measure.
        H5 tablename key is 'draws' rather than the default for the rest of the
        CodCorrect files as that's what get_draws expects.
        """
        directory = (
            self.get_file_path(DRAWS, DEATHS)
            if measure_id == gbd_constants.measures.DEATH
            else self.get_file_path(DRAWS, YLLS)
        )
        file_pattern = f"{sex_id}_{location_id}_{{year_id}}.h5"

        self.sink_draws(directory, file_pattern, draws, h5_tablename=H5_DRAWS_KEY)

    # SUMMARIZE GBD/COD/PERCENT CHANGE

    def read_final_draws(
        self, location_id: int, year_id: int, measure_id: int
    ) -> pd.DataFrame:
        """Read 'final' CodCorrect draws.

        After appending shocks, the CodCorrect draws are 'final' or 'complete',
        meaning they won't be changed again: we've arrived at the final answer.

        Both sexes are read in, so sex_id is not a necessary parameter.

        Args:
            location_id: Location to return draws for
            year_id: Year to return draws for
            measure_id: Measure to return draws for.
        """
        directory = (
            self.get_file_path(DRAWS, DEATHS)
            if measure_id == gbd_constants.measures.DEATH
            else self.get_file_path(DRAWS, YLLS)
        )
        file_pattern = f"{{sex_id}}_{location_id}_{year_id}.h5"

        return self.source_draws(
            directory, file_pattern, h5_tablename=H5_DRAWS_KEY, num_workers=SUMMARIZE_WORKERS
        )

    def save_summaries(
        self,
        summaries: pd.DataFrame,
        database: str,
        location_id: int,
        year_id: int,
        measure_id: int,
        summary_type: Optional[str] = None,
    ) -> None:
        """Save CSV summaries for CodCorrect final draws.

        Args:
            summaries: summaries dataframe
            database: database summaries were created for. Either 'cod' or 'gbd'
            location_id: ID of the location to save summaries for
            year_id: ID of the year to save summaries for. If percent change (multi year),
                looks like '1990_2000'.
            measure_id: ID of the measure to save summaries for. Either 1 or 4
            summary_type: Type of summary, either 'single' or 'multi' Only relevant
                for GBD uploads. Defaults to None.
        """
        measure_dir = DEATHS if measure_id == gbd_constants.measures.DEATH else YLLS

        # GBD db summaries can be single year or multi-year, both saved in different places
        # COD db summaries only have single year
        if database == DataBases.GBD:
            summary_dir = SINGLE if summary_type == SummaryType.SINGLE else MULTI
            file_path = (
                self.get_file_path(SUMMARIES, GBD, summary_dir, measure_dir, year_id)
                / f"{location_id}.csv"
            )
        else:
            file_path = (
                self.get_file_path(SUMMARIES, COD, measure_dir, year_id)
                / f"{location_id}.csv"
            )
        self.save_csv(summaries, file_path)

    # UPLOAD

    def get_summary_directories(
        self,
        database: str,
        measure_id: Optional[int] = None,
        summary_type: Optional[str] = None,
    ) -> List[str]:
        """Get all directories where summaries are saved for upload to a database.

        For GBD, we potentially have single and multi-year summaries for YLLs and deaths.
        For COD, we only upload single-year summaries for deaths.
        For CodCorrect, there's only a single directory.

        Returns full paths to all requested summary directories.

        Args:
            database: database summaries were created for. Either 'cod', 'gbd', or 'codcorrect'
            measure_id: ID of the measure to save summaries for. Either 1 or 4. Only relevant
                for GBD uploads. Defaults to None.
            summary_type: Type of summary, either 'single' or 'multi' Only relevant
                for GBD uploads. Defaults to None.
        """
        # Each database has their files saved across different directories.
        if database == DataBases.GBD:
            summary_dir = SINGLE if summary_type == SummaryType.SINGLE else MULTI
            measure_dir = DEATHS if measure_id == gbd_constants.measures.DEATH else YLLS

            base_dir = self.get_file_path(SUMMARIES, GBD, summary_dir, measure_dir)
            sub_dirs = sorted(
                list(self.file_system[SUMMARIES][GBD][summary_dir][measure_dir])
            )
            dirs = [str(base_dir / sub_dir) for sub_dir in sub_dirs]
        elif database == DataBases.COD:
            base_dir = self.get_file_path(SUMMARIES, COD, DEATHS)
            sub_dirs = sorted(list(self.file_system[SUMMARIES][COD][DEATHS]))
            dirs = [str(base_dir / sub_dir) for sub_dir in sub_dirs]
        else:
            dirs = [str(self.get_file_path(DIAGNOSTICS))]

        return dirs

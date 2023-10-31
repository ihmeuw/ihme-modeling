"""Files and file path functions/classes."""
import os
import warnings
from typing import Dict

import pandas as pd

from le_decomp.lib.constants import Filepaths

Directory = Dict[str, "Directory"]


def get_new_version_id(parent_dir: str = Filepaths.PARENT_DIR) -> int:
    """Returns a new LE decomp version id.

    Expects LE decomp directories to be live in the output directory,
    titled by version (ie '123', 124', ...). Increments the last version by 1.

    Raises:
        RuntimeError: If the output dir is empty or does not have versioned
            directories as expected
    """
    files = os.listdir(parent_dir)
    if not files:
        raise RuntimeError(
            f"No files found at {parent_dir}. Expected to find LE decomp version "
            "directories here."
        )

    max_version = -1
    for version_dir in files:
        try:
            version_dir = int(version_dir)
            if version_dir > max_version:
                max_version = version_dir
        except (TypeError, ValueError):
            continue

    if max_version == -1:
        raise RuntimeError(f"No versioned LE decomp directories found at {parent_dir}")

    return max_version + 1


def get_output_dir(version_id: int) -> str:
    """Gets the output directory for the LE decomp run.

    Arguments:
        version_id: LE decomp version
    """
    return os.path.join(Filepaths.PARENT_DIR, str(version_id))


class LeDecompFileSystem:
    """Filepath abstraction for LE Decomp.

    LeDecompFileSystem has functions for creating/accessing file paths used in LE Decomp.
    Each folder has its own caching function, ie cache_inputs, cache_results.

    This class's goal is to provide an abstraction for managing the project's
    file system/directories in a human-readable way, which is what the field FILE_SYSTEM
    represents. For example, if we had a directory like:

    root
    | -- draws
    |    -- deaths
    |    -- ylls
    | -- inputs
    | -- logs
    |    -- step1
    |    -- step2
    |    -- step3

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

    When the file system is created (make_file_system), we create an index of all the folders:

    flat_file_system = {
        "output_path": "/root",
        "draws": "/root/draws",
        "deaths": "/root/draws/deaths",
        "ylls": "/root/draws/ylls",
        "inputs": "/root/inputs",
        "logs": "/root/logs",
        "step1": "/root/logs/step1",
        "step2": "/root/logs/step2",
        "step3": "/root/logs/step3",
    }

    This means that EACH FOLDER NAME MUST BE UNIQUE. This will not throw an error, but the
    file index will only contain a value for one.

    Note:
        Admittedly this is overkill for this project, which is so small. I am using this as
        testing grounds for concepts that could be useful in CodCorrect.

    Fields:
        output_path: the root of the file system. Expected parameter to instantiate
        FILE_SYSTEM: a human-readable version of what the project file system structure
            looks like. New folders/edits go here and require a corresponding caching function.
        flat_file_system: an indexed (flat) version of FILE_SYSTEM for quick file path
            retrieval
    """

    FILE_SYSTEM: Directory = {
        Filepaths.INPUTS: {},
        Filepaths.RESULTS: {},
    }

    def __init__(self, output_path: str):
        self.output_path = output_path
        self.flat_file_system = {}
        self.flat_file_system["output_path"] = self.output_path

    def make_file_system(self) -> None:
        """Makes the file system for the run, creating all necessary folders.

        Sets up a flat index of the file system at the same time (flat_file_system)
        from the human-readable file system (FILE_SYSTEM).

        Raises a warning if there are any non-unique directory names as we can't
        properly make an index.
        """
        self._make_file_system_recurse(self.output_path, self.FILE_SYSTEM)

    def _make_file_system_recurse(self, directory: str, subdirectories: Directory) -> None:
        """Recursive helper function for making run directories.

        Takes a not-yet existing directory and that directory's sub-directory system.
        Creates the directory and returns if there are no subdirectories. Otherwise,
        repeats the process.
        """
        os.makedirs(directory, exist_ok=True)
        os.chmod(directory, 0o775)

        if not subdirectories:
            return

        for subdir in subdirectories:
            new_dir_path = os.path.join(directory, subdir)

            if subdir in self.flat_file_system:
                warnings.warn(
                    f"More than one directory named '{subdir}' exists in file system. Index "
                    "cannot have entries for both.",
                    RuntimeWarning,
                )

            self.flat_file_system[subdir] = new_dir_path
            self._make_file_system_recurse(new_dir_path, subdirectories[subdir])

    def cache_inputs(self, inputs_df: pd.DataFrame) -> None:
        """Caches the inputs df."""
        path = os.path.join(self.get_directory_path(Filepaths.INPUTS), "le_decomp_inputs.csv")
        inputs_df.to_csv(path, index=False)
        os.chmod(path, 0o775)

    def cache_results(self, results_df: pd.DataFrame, location_id: int, sex_id: int) -> None:
        """Caches the LE decomp results dataframe, which is location-sex-specific."""
        path = os.path.join(
            self.get_directory_path(Filepaths.RESULTS), f"{location_id}_{sex_id}.csv"
        )
        results_df.to_csv(path, index=False)
        os.chmod(path, 0o775)

    def get_directory_path(self, directory: str) -> str:
        """Returns the full path of the directory.

        Raises:
            ValueError: if the directory does not exist
        """
        if directory not in self.flat_file_system:
            raise ValueError(
                f"Directory {directory} does not exist in the LE Decomp file system"
            )

        return self.flat_file_system[directory]

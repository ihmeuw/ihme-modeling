import re
from pathlib import Path
from typing import Dict

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser


class DataDirManager:
    """Manages directories for pipeline inputs that are saved to a flat file.

    Attributes:
        config_key: Key to use when pulling specific directory.
        p: A parser helper class to use with the config file.
        base: The directory contained versioned data for the input config key.
    """

    def __init__(self, config_key: str):
        """Initializes the class based on a config key to use with the parser and a
        config.ini file which stores the directories."""
        self.config_key = config_key
        self.base = Path(
            filepath_parser(
                ini="pipeline.version_dir_manager",
                section="inpatient",
                section_key=self.config_key,
            )
        )

    def pull_version_dir(self, run_metadata: pd.DataFrame) -> str:
        """Return root of versioned metadata directory."""

        r_m_field = self.config_field_map[self.config_key]
        version_id = int(run_metadata[r_m_field][0])
        version_base = self.version_dir_convention()
        temp = self.base.joinpath(f"{version_base}{version_id}")

        if not temp.is_dir():
            raise RuntimeError(f"Path does not exsist {temp}")

        return str(temp)

    def create_version_dir(self, version_id: int) -> str:
        """Create a new metadata version directory."""

        version_dir_name = self.version_dir_convention()
        temp = self.base.joinpath(f"{version_dir_name}{version_id}")

        # pathlib will throw a FileExistsError if target dir already exists
        temp.mkdir()

        return str(temp)

    def version_dir_convention(self) -> str:
        """Find the naming convention of the versioned directory.

        Don't want to get burned by hardcoding (e.g. age_sex_weights/version_id_1).
        Assume that the subdirectory will always be there.
        """

        # returns the base naming convention (e.g 'version_id_', cf_version_set_id_, etc).
        version_dir_name = {
            re.sub(r"[-\d]|[0-9]+", "", str(e.parts[-1]))
            for e in self.base.glob("*")
            if e.is_dir()
        }

        if not len(version_dir_name) == 1:
            raise RuntimeError("There are multiple naming conventions.")

        return version_dir_name.pop()

    @property
    def config_field_map(self) -> Dict[str, str]:
        """Maps a key in lib/utils/config.ini to a field in run_metadata."""

        return {
            "age_sex_weights": "age_sex_weights_version_id",
            "cf_version_set": "cf_version_set_id",
            "inj_cf_input": "inj_cf_input_version_id",
            "inj_cf_en": "inj_cf_version_id",
        }

    def __repr__(self) -> str:
        return f"Directory: {self.config_key} Path: {self.base}"
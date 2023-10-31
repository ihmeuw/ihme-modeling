from enum import Enum
import os
from pathlib import Path

import pandas as pd
from clinical_info.Upload.tools.io.database import Database


class DataManagerError(Exception):
    pass


class DataManager:
    """ An object to manage IO of data"""

    def __init__(self):
        self._logger = None

    def setup_logger(self, logger):
        self._logger = logger

    def _parse_path(self, path):
        try:
            path = Path(path)
            path = path.expanduser()
        except Exception as e:
            raise DataManagerError(
                "Provided path must be convertable to " "a pathlib.Path"
            )
        return path

    def load_file(self, path):
        path = self._parse_path(path)
        if not path.exists():
            raise DataManagerError(f"Provided path {path} does not exist")

        self._logger.log(f"Loading {path}")
        if path.suffix == ".csv":
            df = pd.read_csv(path)
        elif path.suffix == ".H5":
            df = pd.read_hdf(path)
        elif path.suffix.lower() == ".dta":
            df = pd.read_stata(path)
        else:
            raise DataManagerError(f"Unrecognized file format: {path.suffix}")

        size_est = df.memory_usage().sum() / 1024 / 1024
        self._logger.log(
            f"Loaded {len(df)} rows x {len(df.columns)} columns, " f"~{size_est:.2f}Mb"
        )
        return df

    def upload(self, df, odbc, schema, table):
        db = Database()
        db.load_odbc(odbc)

        self._logger.log(f"Uploading to {db.credentials.host} - {schema}.{table}\n")
        input("Press enter to continue")

        db.write(df, schema, table)

    def save(self, df, path):
        path = self._parse_path(path)
        backup_file_name = "backup.csv"

        # Check if the path exists. If it doesn't, ask to create it.
        # 1) User wants to create the folder: create it and save data
        # 2) User doesn't want to create the folder: raise an error
        if not path.exists():
            response = input("Provided path doesn't exists. Create it?(y/n)\n")
            print(response)
            if response.strip().lower() == "y":
                os.makedirs(path)
                self.save(df, path)
                return
            else:
                raise DataManagerError(f"Provided path doesn't exist: " f"{path}")
        # If the path isn't a folder then raise an error
        if not path.is_dir():
            raise DataManagerError(f"Provided path is not a folder: " f"{path}")

        # Path exists and is a directory but it could already contain a
        # backup.csv so double check. If there is one, confirm the user
        # wants to overwrite it.
        if (path / backup_file_name).exists():
            response = input("A backup already exists. Override?(y/n):\n")
            if not response.strip().lower() == "y":
                return

        self._logger.log(f"Saving to {path / backup_file_name})")
        df.to_csv(path / backup_file_name)
        size_est = df.memory_usage().sum() / 1024 / 1024
        self._logger.log(
            f"Saved {len(df)} rows x {len(df.columns)} columns, " f"~{size_est:.2f}Mb"
        )
        self._logger.save(path)


class Pipeline(Enum):
    INPATIENT = 1
    OUTPATIENT = 2
    CLAIMS = 3


class ClinicalDM(DataManager):
    """ A DataManager customized for the run_id paradigm"""

    def __init__(self, run_id):
        super().__init__()
        self._run_id = run_id

    @property
    def run_id(self):
        return self._run_id

    @property
    def log_folder(self):
        return Path(f"FILEPATH")

    def load_final_estimates(self, pipeline):
        dir_path = Path(f"FILEPATH")
        if pipeline == Pipeline.INPATIENT:
            return self._load_file(dir_path / "inpatient.csv")
        elif pipeline == Pipeline.OUTPATIENT:
            return self._load_file(dir_path / "outpatient.csv")
        elif pipeline == Pipeline.CLAIMS:
            return self._load_file(dir_path / "claims.csv")
        else:
            raise DataManagerError(f"Invalid pipeline provided: {pipeline}")

    def backup_final_estimates(self, df, pipeline):
        dir_path = Path(f"FILEPATH")
        if pipeline == Pipeline.INPATIENT:
            dir_path = dir_path / "inpatient"
        elif pipeline == Pipeline.OUTPATIENT:
            dir_path = dir_path / "outpatient"
        elif pipeline == Pipeline.CLAIMS:
            dir_path = dir_path / "claims"
        else:
            raise DataManagerError(f"Invalid pipeline provided: {pipeline}")

        if not dir_path.exists():
            os.makedirs(dir_path)

        save_path = dir_path / "backup.csv"
        if save_path.exists():
            input(f"{save_path} already exists.\n" f"Press any key to overwrite...\n")
            self._logger.log("overwritting previously saved data")
        self._save(df, save_path)
        self._logger.save(dir_path)


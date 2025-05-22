import logging
import os

from gbd import conn_defs
from gbd import constants as gbd
from ihme_cc_gbd_schema.common import ModelStorageMetadata

from codem.metadata.step_metadata import STEP_IDS
from codem.reference import db_connect

logger = logging.getLogger(__name__)


def setup_dir(directory: str) -> str:
    """Creates model directory if it does not exist."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        os.chmod(directory, 0o775)  # nosec: B103
    return directory


def get_base_dir(model_version_id: int, conn_def: str) -> str:
    """Gets data directory of given model_version_id."""
    dev_prod = "" if conn_def == conn_defs.CODEM else "dev/"
    acause = db_connect.execute_select(
        """
        SELECT acause
        FROM cod.model_version
        JOIN shared.cause USING(cause_id)
        WHERE model_version_id = :model_version_id
        """,
        parameters={"model_version_id": model_version_id},
        conn_def=conn_def,
    )["acause"][0]
    return setup_dir(f"FILEPATH/{acause}/{dev_prod}{model_version_id}")


class ModelPaths:
    """Class for managing CODEm model files and directories."""

    def __init__(self, model_version_id: int, conn_def: str) -> None:
        self.BASE_DIR = get_base_dir(model_version_id=model_version_id, conn_def=conn_def)
        self.INPUT_DIR = setup_dir(os.path.join(self.BASE_DIR, "inputs"))
        self.DIAGNOSTICS_DIR = setup_dir(os.path.join(self.BASE_DIR, "diagnostics"))

        self.DATA_FRAME = self.input_file("cod_data.csv")
        self.COVARIATES = self.input_file("covariates_data.csv")
        self.ALL_DATA = self.input_file("input_database_square.csv")
        self.PRIORS = self.input_file("priors.csv")
        self.KO_DATA = self.input_file("ko_data.csv")

        self.DRAWS = setup_dir(os.path.join(self.BASE_DIR, "draws"))
        self.DRAW_FILE = os.path.join(self.DRAWS, "deaths_{sex}.h5")
        self.STORAGE_PATTERN = os.path.relpath(self.DRAW_FILE, self.BASE_DIR)
        self.DRAW_FILE_KEY = "data"

        self.SUMMARIES = setup_dir(os.path.join(self.BASE_DIR, "summaries"))
        self.SUMMARY_FILE = os.path.join(self.SUMMARIES, "{table}.csv")

        self.COVARIATE_FILES = {
            "ln_rate": self.step_file(
                step_id=STEP_IDS["CovariateSelection"], filename="cv_selected_ln_rate.txt"
            ),
            "lt_cf": self.step_file(
                step_id=STEP_IDS["CovariateSelection"], filename="cv_selected_lt_cf.txt"
            ),
        }

        self.COVARIATE_FILES_NO_SELECT = {
            "ln_rate": self.step_file(
                step_id=STEP_IDS["CovariateSelection"],
                filename="no_covariates_for_ln_rate.txt",
            ),
            "lt_cf": self.step_file(
                step_id=STEP_IDS["CovariateSelection"], filename="no_covariates_for_lt_cf.txt"
            ),
        }

        self.JSON_FILES = {
            "linear": self.step_file(
                step_id=STEP_IDS["LinearModelBuilds"], filename="linear_model_json.txt"
            ),
            "spacetime": self.step_file(
                step_id=STEP_IDS["LinearModelBuilds"], filename="space_time_json.txt"
            ),
        }

        self.JOB_METADATA = os.path.join(self.BASE_DIR, "job_metadata.txt")

    def input_file(self, filename: str) -> str:
        return os.path.join(self.INPUT_DIR, filename)

    def diagnostics_file(self, filename: str) -> str:
        return os.path.join(self.DIAGNOSTICS_DIR, filename)

    def step_dir(self, step_id: int) -> str:
        return setup_dir(os.path.join(self.BASE_DIR, f"step_{str(step_id)}"))

    def step_file(self, step_id, filename: str) -> str:
        path = self.step_dir(step_id)
        return os.path.join(path, filename)

    def output_storage_metadata(self, sex: str) -> None:
        """Outputs draw storage metadata if it does not exist."""
        output_file = os.path.join(self.BASE_DIR, "version_metadata.json")
        if not os.path.exists(output_file):
            storage_metadata = ModelStorageMetadata.from_dict(
                {
                    "storage_pattern": self.STORAGE_PATTERN.format(sex=sex),
                    "h5_tablename": self.DRAW_FILE_KEY,
                }
            )
            storage_metadata.to_file(directory=self.BASE_DIR)


def cleanup_files(model_version_id: int, conn_def: str) -> None:
    """
    Cleans up the files that are made in intermediate steps
    that we don't want to keep for any diagnostic purposes
    later. Will not delete if it's a path.

    :param model_version_id: (int)
    :param conn_def: (str)
    :return:
    """
    files_to_delete = {
        "ReadSpacetimeModels": ["st_models_linear"],
        "ApplySpacetimeSmoothing": ["st_models_spacetime"],
        "ApplyGPSmoothing": ["st_models_gp"],
        "ReadLinearModels": ["linear_models_linear"],
        "SpacetimePV": ["st_models_pv"],
        "LinearPV": ["linear_models_pv"],
        "OptimalPSI": ["ensemble_preds", "all_st_predictions", "all_linear_predictions"],
        "LinearDraws": ["linear_models_draws"],
        "GPRDraws": ["st_models_draws"],
    }
    paths = ModelPaths(model_version_id=model_version_id, conn_def=conn_def)
    for d in files_to_delete:
        for f in files_to_delete[d]:
            file_path = paths.step_file(STEP_IDS[d], f"{f}.pickle")
            if os.path.exists(file_path):
                logger.info(f"Deleting {file_path}")
                os.unlink(file_path)

import logging
import os

from gbd import conn_defs

from hybridizer.reference import db_connect

logger = logging.getLogger(__name__)


def setup_dir(directory: str) -> str:
    """Create directory if it does not exist."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        os.chmod(directory, 0o775)  # nosec: B103
    return directory


def get_base_dir(model_version_id: int, conn_def: str) -> str:
    """Get base directory for given model version ID on file system."""
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

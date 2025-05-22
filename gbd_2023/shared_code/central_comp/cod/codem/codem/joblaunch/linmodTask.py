import logging
import subprocess  # nosec: B404
from pathlib import Path
from typing import List, Optional

import codem

logger = logging.getLogger(__name__)


def get_rscript(scriptname):
    install_dir = Path(codem.__file__).parents[0] / "linmod" / "{}.R".format(scriptname)
    return str(install_dir)


class LinMod(object):
    """Class for running CODEm R scripts."""

    def __init__(
        self,
        scriptname: str,
        model_version_id: int,
        conn_def: str,
        model_dir: str,
        cores: int,
        more_args: Optional[List[str]] = None,
    ) -> None:
        self.scriptname = scriptname
        self.model_version_id = model_version_id
        self.conn_def = conn_def
        self.model_dir = model_dir
        self.cores = cores
        self.more_args = more_args

        logger.info("Submitting {}".format(scriptname))
        self.call = (
            "FILEPATH "
            + get_rscript(self.scriptname)
            + " "
            + str(self.model_version_id)
            + " "
            + self.model_dir
            + " "
            + self.conn_def
            + " "
            + str(self.cores)
        )
        if self.more_args:
            for arg in self.more_args:
                self.call += " " + str(arg)
        logger.info(self.call)

    def run(self) -> int:
        exit_status = subprocess.call(self.call, shell=True)  # nosec: B602
        return exit_status

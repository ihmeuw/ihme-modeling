import logging
import os
import pickle  # nosec: B403

import numexpr as ne
import pandas as pd

from codem.metadata.model_metadata import ModelVersionMetadata
from codem.metadata.step_metadata import STEP_DICTIONARY, STEP_NAMES
from codem.reference.log_config import ModelerAlert
from codem.reference.paths import ModelPaths

pd.set_option("chained", None)
ne.set_num_threads(30)

logger = logging.getLogger(__name__)


class ModelTask:
    """Base class for running a CODEm step."""

    def __init__(
        self,
        model_version_id: int,
        conn_def: str,
        old_covariates_mvid: int,
        debug_mode: bool,
        cores: int,
        step_id: int,
        make_inputs: bool = False,
        make_ko: bool = False,
    ) -> None:
        """
        Base class for a step to do work_exec. Defines the metadata for the
        model versions, paths to save things, and input and output
        directories. Reads in metadata about which things the object
        needs from STEP_DICTIONARY in codem.metadata.step_metadata

        :param model_version_id: (int)
        :param conn_def: (str)
        :param debug_mode: (bool)
        :param cores: (number of cores passed through "fthread"
        :param step_id: (int)
        """
        self.model_version_id = model_version_id
        self.conn_def = conn_def
        self.old_covariates_mvid = old_covariates_mvid
        self.debug_mode = debug_mode
        self.cores = cores

        self.step_id = step_id

        self.make_inputs = make_inputs
        self.make_ko = make_ko

        self.alerts = ModelerAlert(
            model_version_id=self.model_version_id, conn_def=self.conn_def
        )
        self.model_metadata = ModelVersionMetadata(
            model_version_id=self.model_version_id,
            conn_def=self.conn_def,
            debug_mode=self.debug_mode,
            re_initialize=self.make_inputs,
            generate_ko=self.make_ko,
        )

        self.model_paths = ModelPaths(
            model_version_id=self.model_version_id, conn_def=self.conn_def
        )
        self.step_dir = self.model_paths.step_dir(self.step_id)
        self.step_metadata = STEP_DICTIONARY[STEP_NAMES[self.step_id]]

        self.pickled_inputs = {}
        self.pickled_outputs = {}
        for o in self.step_metadata.pickled_outputs:
            self.pickled_outputs[o] = None

        self.read_inputs()

    def read_inputs(self) -> None:
        """Loads input pickle files for step."""
        for s in self.step_metadata.pickled_inputs:
            for f in self.step_metadata.pickled_inputs[s]:
                logger.info(f"Reading {f}")
                file = open(os.path.join(self.model_paths.step_dir(s), f"{f}.pickle"), "rb")
                obj = pickle.load(file)  # nosec: B301
                file.close()
                self.pickled_inputs[f] = obj

    def save_outputs(self) -> None:
        """Saves output pickle files for step."""
        for k, v in self.pickled_outputs.items():
            logger.info(f"Saving {k}")
            file = open(os.path.join(self.step_dir, f"{k}.pickle"), "wb")
            pickle.dump(v, file, protocol=pickle.HIGHEST_PROTOCOL)
            file.close()

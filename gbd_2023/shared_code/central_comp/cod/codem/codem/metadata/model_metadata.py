import logging
import os

import numpy as np
import pandas as pd

from codem.data.knockout import generate_knockouts
from codem.data.parameters import get_model_parameters
from codem.data.query import adjust_input_data, get_codem_input_data
from codem.reference.log_config import ModelerAlert
from codem.reference.paths import ModelPaths
from codem.reference.warning import WarningLog

logger = logging.getLogger(__name__)


class ModelVersionMetadata:
    """Manages loading and saving of a model's input data used throughout workflow."""

    def __init__(
        self,
        model_version_id: int,
        conn_def: str,
        debug_mode: bool = True,
        update: bool = True,
        re_initialize: bool = False,
        generate_ko: bool = False,
    ) -> None:
        """
        Metadata object for codem model version. This object should be initialized
        once at the beginning of a codem job and will run load input data, load covariate data,
        load priors, etc.

        For all subsequent steps, it will just use this object but not have to load
        the input data because it's already saved.

        :param model_version_id: (int) model version from cod.model_version table
        :param conn_def: (str) database connection either 'codem' or 'codem-test'
        :param debug_mode: (bool) do we want to set the seed to be model_version_id?
        :param update: (bool) whether to update pending status in the database
        :param re_initialize: (bool) whether to re-make all of the data
            (should very rarely be done)
        """
        self.model_version_id = model_version_id
        self.conn_def = conn_def
        self.debug_mode = debug_mode
        self.update = update
        self.re_initialize = re_initialize
        self.generate_ko = generate_ko

        self.data_frame = None
        self.covariates = None
        self.all_data = None

        self.cv_names = None
        self.priors = None
        self.ko_data = None
        self.all_ko = None

        self.alerts = ModelerAlert(
            model_version_id=self.model_version_id, conn_def=self.conn_def
        )

        self.model_parameters = get_model_parameters(
            model_version_id=self.model_version_id, conn_def=self.conn_def, update=self.update
        )

        self.model_paths = ModelPaths(
            model_version_id=self.model_version_id, conn_def=self.conn_def
        )
        os.chdir(self.model_paths.BASE_DIR)

        self.warn = WarningLog()

        self.seed = 0 if self.debug_mode else self.model_version_id
        np.random.seed(self.seed)

        if self.re_initialize or not self.check_inputs():
            self.make_inputs()
            self.save_inputs()
        else:
            self.reload_inputs()

        if self.generate_ko:
            self.make_ko()
            self.save_ko()
        else:
            try:
                self.reload_ko()
                self.all_ko = self.ko_data[-1].iloc[:, 0].values
            except IOError as e:
                logger.info(e)

    def make_inputs(self) -> None:
        """Pulls input data and saves them to variables."""
        logger.info(f"Making inputs for model version {self.model_version_id}")
        self.alerts.alert("Querying for input data.")
        df, covs, priors = get_codem_input_data(model_parameters=self.model_parameters)
        self.cv_names = priors.name.values

        self.warn.log_missingness(covs, self.cv_names)
        self.warn.log_bad_cf(data_frame=df, var="cf")

        df, covs, all_df = adjust_input_data(df, covs)

        self.data_frame = df
        self.covariates = covs
        self.all_data = all_df
        self.priors = priors

    def make_ko(self) -> None:
        """Makes knockout data."""
        logger.info(
            f"Generating {self.model_parameters['holdout_number']} knockouts "
            f"for {self.model_version_id}"
        )
        self.alerts.alert("Generating knockouts.")
        self.ko_data = generate_knockouts(
            df=self.data_frame,
            holdouts=self.model_parameters["holdout_number"],
            seed=self.seed,
        )

    def check_inputs(self) -> None:
        """Checks inputs exist for this model version already. If they do, don't recreate."""
        return (
            os.path.exists(self.model_paths.DATA_FRAME)
            and os.path.exists(self.model_paths.COVARIATES)
            and os.path.exists(self.model_paths.PRIORS)
            and os.path.exists(self.model_paths.ALL_DATA)
        )

    def save_inputs(self) -> None:
        """Saves various input data files to csvs."""
        logger.info(f"Saving inputs to {self.model_paths.BASE_DIR}")
        self.data_frame.to_csv(self.model_paths.DATA_FRAME, index=False)
        self.covariates.to_csv(self.model_paths.COVARIATES, index=False)
        self.all_data.to_csv(self.model_paths.ALL_DATA, index=False)
        self.priors.to_csv(self.model_paths.PRIORS, index=False)

    def save_ko(self) -> None:
        """Saves knockout data to data directory."""
        logger.info(f"Saving ko data to {self.model_paths.BASE_DIR}")
        pd.concat(self.ko_data, axis=1).to_csv(self.model_paths.KO_DATA, index=False)

    def reload_inputs(self) -> None:
        """Loads or reloads inputs data from data directory."""
        logger.info(f"Reloading inputs from {self.model_paths.BASE_DIR}")
        self.data_frame = pd.read_csv(self.model_paths.DATA_FRAME)
        self.covariates = pd.read_csv(self.model_paths.COVARIATES)
        self.priors = pd.read_csv(self.model_paths.PRIORS)
        self.cv_names = self.priors.name.values

    def reload_ko(self) -> None:
        """Loads knockout data from data directory."""
        logger.info(f"Reloading ko data from {self.model_paths.BASE_DIR}")
        if os.path.exists(self.model_paths.KO_DATA):
            ko = pd.read_csv(self.model_paths.KO_DATA)
            ko_cols = [
                ["ko%02d_%s" % (i, x) for x in ["train", "test1", "test2"]]
                for i in range(
                    1, int(self.model_parameters["all_data_holdout"].strip("ko")) + 1
                )
            ]
            self.ko_data = [ko[c] for c in ko_cols]
        else:
            raise IOError(
                "You have not yet created the knockouts. "
                "If you don't have a task coming up where you do, your jobs will fail."
            )

import numpy as np
import pandas as pd
import os
import logging

from codem.reference.paths import FilePaths, ModelPaths, setup_dir
from codem.reference.warning import WarningLog
from codem.reference.log_config import ModelerAlert
from codem.data.query import get_codem_input_data, adjust_input_data
from codem.data.parameters import get_model_parameters
from codem.data.knockout import generate_knockouts

logger = logging.getLogger(__name__)


class ModelVersionMetadata:
    def __init__(self, model_version_id, db_connection, debug_mode=True, update=True,
                 re_initialize=False, generate_ko=False):
        """
        Metadata object for codem model version. This object should be initialized
        once at the beginning of a codem job and will run load input data, load covariate data,
        load priors, etc.

        For all subsequent steps, it will just use this object but not have to load
        the input data because it's already saved.

        :param model_version_id: (int) model version from cod.model_version table
        :param db_connection: (str) database connection either 'ADDRESS' or 'ADDRESS'
        :param debug_mode: (bool) do we want to set the seed to be model_version_id?
        :param update: (bool) whether to update pending status in the database
        :param re_initialize: (bool) whether to re-make all of the data (should very rarely be done)
        """
        self.model_version_id = model_version_id
        self.db_connection = db_connection
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

        self.alerts = ModelerAlert(model_version_id=self.model_version_id, db_connection=self.db_connection)

        self.model_parameters = get_model_parameters(model_version_id=self.model_version_id,
                                                     db_connection=self.db_connection,
                                                     update=self.update)
        self.base_dir = FilePaths.BASE_DIR.format(acause=self.model_parameters['acause'],
                                                  model_version_id=self.model_version_id)
        setup_dir(self.base_dir)
        os.chdir(self.base_dir)
        self.model_paths = ModelPaths(model_version_id=model_version_id,
                                      acause=self.model_parameters['acause'])

        self.warn = WarningLog(model_version_id=self.model_version_id,
                               db_connection=self.db_connection)

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
                self.all_ko = self.ko_data[-1].ix[:, 0].values
            except IOError as e:
                logger.info(e)

    def make_inputs(self):
        logger.info(f"Making inputs for model version {self.model_version_id}")
        self.alerts.alert("Querying for input data.")
        df, covs, priors = get_codem_input_data(
            model_parameters=self.model_parameters)
        self.cv_names = priors.name.values

        self.warn.log_missingness(covs, self.cv_names)
        self.warn.log_bad_cf(data_frame=df, var='cf')

        df, covs, all_df = adjust_input_data(df, covs)

        self.data_frame = df
        self.covariates = covs
        self.all_data = all_df
        self.priors = priors

    def make_ko(self):
        logger.info(f"Generating {self.model_parameters['holdout_number']} knockouts for {self.model_version_id}")
        self.alerts.alert("Generating knockouts.")
        self.ko_data = generate_knockouts(df=self.data_frame,
                                          holdouts=self.model_parameters['holdout_number'],
                                          seed=self.seed)

    def check_inputs(self):
        """
        Checks if the inputs exist for this model version already. If they do, don't recreate.
        :return:
        """
        return (
            os.path.exists(self.model_paths.DATA_FRAME) and
            os.path.exists(self.model_paths.COVARIATES) and
            os.path.exists(self.model_paths.PRIORS) and
            os.path.exists(self.model_paths.ALL_DATA)
        )

    def save_inputs(self):
        logger.info(f"Saving inputs to {self.model_paths.BASE_DIR}")
        self.data_frame.to_csv(self.model_paths.DATA_FRAME, index=False)
        self.covariates.to_csv(self.model_paths.COVARIATES, index=False)
        self.all_data.to_csv(self.model_paths.ALL_DATA, index=False)
        self.priors.to_csv(self.model_paths.PRIORS, index=False)

    def save_ko(self):
        logger.info(f"Saving ko data to {self.model_paths.BASE_DIR}")
        pd.concat(self.ko_data, axis=1).to_csv(self.model_paths.KO_DATA, index=False)

    def reload_inputs(self):
        logger.info(f"Reloading inputs from {self.model_paths.BASE_DIR}")
        self.data_frame = pd.read_csv(self.model_paths.DATA_FRAME)
        self.covariates = pd.read_csv(self.model_paths.COVARIATES)
        self.priors = pd.read_csv(self.model_paths.PRIORS)
        self.cv_names = self.priors.name.values

    def reload_ko(self):
        logger.info(f"Reloading ko data from {self.model_paths.BASE_DIR}")
        if os.path.exists(self.model_paths.KO_DATA):
            ko = pd.read_csv(self.model_paths.KO_DATA)
            ko_cols = [['ko%02d_%s' % (i, x) for x in ["train", "test1", "test2"]]
                       for i in range(1, int(self.model_parameters['all_data_holdout'].strip('ko')) + 1)]
            self.ko_data = [ko[c] for c in ko_cols]
        else:
            raise IOError("You have not yet created the knockouts. "
                          "If you don't have a task coming up where you do, your jobs will fail.")

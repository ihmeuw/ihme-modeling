import os
import logging
import sys
import time

from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.joblaunch.linmodTask import LinMod
from codem.reference.log_config import setup_logging

from codem.stgpr.space_time_smoothing import import_json

logger = logging.getLogger(__name__)

MEM_EXIT_STATUS = 137


class LinearModelBuilds(ModelTask):
    def __init__(self, **kwargs):
        """
        Launch the R script for running linear models builds.
        Eventually we want to add some smarter error catching to get the error
        back from the R script.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['LinearModelBuilds'])

    def check_file_output(self):
        return (os.path.isfile(self.model_paths.JSON_FILES['linear'])) and \
               (os.path.isfile(self.model_paths.JSON_FILES['spacetime']))

    def launch_linear_model_builds(self):
        if not self.check_file_output():
            logger.info("Running Linear Model Builds")
            self.alerts.alert("Submitting linear model builds in R.")
            linear_mod = LinMod(
                scriptname='lm_model_prototype',
                model_version_id=self.model_version_id,
                db_connection=self.db_connection,
                model_dir=self.model_paths.BASE_DIR,
                cores=self.cores
            )
            exit_status = linear_mod.run()
            if exit_status:
                logger.error(f"Linear model builds returning an error code of {exit_status}")
                self.alerts.alert("There was an error in linear model builds. Submit a ticket please!")
                sys.exit(MEM_EXIT_STATUS)
            else:
                i = 0
                while not self.check_file_output():
                    time.sleep(1*60)
                    i += 1
                    if i > 10:
                        logger.info("Cannot find linear model files.")
                        self.alerts.alert("Cannot find linear model files. Please submit a ticket!")
                        raise RuntimeError("Cannot find linear model -- file system problem?")
        else:
            logger.info("Skipping linear model builds")
            self.alerts.alert("Skipping: linear model builds has already happened for this model.")
            pass
        if not self.check_file_output():
            logger.info("Linear model builds has failed.")
            self.alerts.alert("Your model has failed to build the linear models in R. Please submit a ticket!")
            sys.exit(MEM_EXIT_STATUS)

    def save_response_list(self):
        logger.info("Generating the response (outcome variable) list that will be used throughout the pipeline.")
        json_dict = import_json(os.path.join(self.step_dir, 'space_time_json.txt'))
        response_list = [x.rsplit("_", 1)[0] for x in sorted(list(json_dict.keys()))]
        self.pickled_outputs['response_list'] = response_list


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['LinearModelBuilds'])

    logger.info("Initiating linear model builds")
    t = LinearModelBuilds(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.launch_linear_model_builds()
    t.save_response_list()
    t.save_outputs()
    logger.info("Finished with Linear Model Builds")
    t.alerts.alert("Finished with linear model builds")

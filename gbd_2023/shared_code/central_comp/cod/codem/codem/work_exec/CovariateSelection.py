import logging
import os
import sys
import time

from codem.joblaunch.args import get_step_args
from codem.joblaunch.linmodTask import LinMod
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

logger = logging.getLogger(__name__)

MEM_EXIT_STATUS = 137


class CovariateSelection(ModelTask):
    def __init__(self, outcome, **kwargs):
        """
        Launch covariate selection through R linear models.

        :param kwargs:
        """
        self.outcome = outcome
        super().__init__(**kwargs, step_id=STEP_IDS["CovariateSelection"])

    def check_file_output(self):
        """
        Check for either the presence of a covariate file or a no covariates selected file.
        :return:
        """
        return os.path.isfile(
            self.model_paths.COVARIATE_FILES[self.outcome]
        ) or os.path.isfile(self.model_paths.COVARIATE_FILES_NO_SELECT[self.outcome])

    def launch_covariate_selection(self):
        if not self.check_file_output():
            logger.info(
                f"Need to run covariate selection for {self.outcome} because there "
                f"are no covariates yet."
            )
            self.alerts.alert(f"Submitting covariate selection for {self.outcome}")
            cv_select = LinMod(
                scriptname="cvSelection",
                model_version_id=self.model_version_id,
                conn_def=self.conn_def,
                model_dir=self.model_paths.BASE_DIR,
                cores=self.cores,
                more_args=[
                    self.outcome,
                    self.model_metadata.model_parameters["model_version_type_id"],
                    self.model_metadata.model_parameters["include_cf"],
                    self.model_metadata.model_parameters["include_rates"],
                ],
            )
            exit_status = cv_select.run()
            if exit_status:
                logger.error(
                    f"Covariate selection for {self.outcome} returning an error code "
                    f"of {exit_status}"
                )
                self.alerts.alert(
                    f"There was an error in covariate selection for {self.outcome}."
                    f"Submit a ticket please!"
                )
                sys.exit(MEM_EXIT_STATUS)
            else:
                logger.info(
                    f"Sleeping for 1 minute while waiting for the {self.outcome} "
                    f"covariate files."
                )
                time.sleep(1 * 60)
                i = 0
                while not self.check_file_output():
                    logger.info(
                        f"Not finding the covariate selection files for {self.outcome}"
                    )
                    logger.info(f"Been waiting for {i*60} seconds for {self.outcome}.")
                    time.sleep(1 * 60)
                    i += 1
                    if i > 25:
                        logger.info(
                            f"Cannot find covariate files for {self.outcome} after 25 minutes."
                        )
                        self.alerts.alert(
                            f"Cannot find covariate files for {self.outcome}. "
                            f"Please submit a ticket!"
                        )
                        raise RuntimeError(
                            f"Cannot find covariates files {self.outcome}--"
                            f"file system problem?"
                        )
        else:
            logger.info(
                f"Covariate selection has already happened for {self.outcome} "
                f"for {self.model_version_id}."
            )
            self.alerts.alert(
                f"Covariate selection has already happened for {self.outcome} "
                f"for {self.model_version_id}."
            )


def main():
    args = get_step_args()
    setup_logging(
        model_version_id=args.model_version_id,
        step_id=str(STEP_IDS["CovariateSelection"]) + f"_{args.outcome}",
        conn_def=args.conn_def,
    )

    logger.info("Initiating Covariate Selection")
    t = CovariateSelection(
        model_version_id=args.model_version_id,
        conn_def=args.conn_def,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores,
        outcome=args.outcome,
    )
    t.launch_covariate_selection()
    logger.info(f"Finished with Covariate Selection for {args.outcome}")
    t.alerts.alert(f"Finished with covariate selection for {args.outcome}")

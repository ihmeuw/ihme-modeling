from datetime import datetime
import logging
import os
import sys
import time

import gbd.constants as GBD

from imported_cases.core import get_spacetime_restrictions
from imported_cases.job_swarm import ImportedCasesJobSwarm
import imported_cases.log_utilities as log_utils


# As per CENCOM 2701: After the correction step in CoDCorrect, append the
# deaths due to opioids under the age of 15 from the raw VR data. Raw deaths
# for this cause and age group can be pulled using get_cod_data. This will be
# similar to how we do imported causes where restrictions are based on
# geography...
ADDITIONAL_RESTRICTIONS = {
    562: 'mental_drug_opioids'
}


def pretty_now():
    return datetime.now().strftime("[%m/%d/%Y %H:%M:%S]")


def setup_folders(outdir, cause_ids):
    """Create output directories to store draws."""
    new_folders = [str(x) for x in cause_ids]

    for folder in new_folders:
        directory = '{d}/{f}'.format(d=outdir, f=folder)
        if not os.path.exists(directory):
            os.makedirs(directory)
    return outdir


if __name__ == '__main__':
    # Manual creation of version for now.
    version_id = 5
    decomp_step = "step4"
    gbd_round_id = GBD.GBD_ROUND_ID

    # Directories
    code_dir = 'FILEPATH'
    outdir = 'FILEPATH'
    logdir = os.path.join(outdir, 'logs')

    log_utils.setup_logging(logdir, 'launch_imported_cases',
                            time.strftime('%m_%d_%Y_%H'))

    logging.info("{} Starting Imported Cases Generator: "
                 "params-- version_id: {}, decomp_step: {} "
                 "gbd_round_id: {}"
                 .format(pretty_now(), version_id, decomp_step, gbd_round_id))

    # Get a list of imported cases cause ids:
    # spacetime-restricted causes + age-restricted causes
    try:
        logging.info("{} Getting cause ids to process".format(pretty_now()))
        st_cause_ids = get_spacetime_restrictions(
            GBD.GBD_ROUND
        ).cause_id.unique().tolist()
        restricted_causes = st_cause_ids + list(ADDITIONAL_RESTRICTIONS.keys())

        logging.info("{} Set up folders".format(pretty_now()))
        parent_dir = setup_folders(outdir, restricted_causes)

        # Instantiate necessary Jobmon classes and setup workflow
        logging.info("{} Submitting Jobs".format(pretty_now()))
        imported_cases_swarm = ImportedCasesJobSwarm(
            code_dir, outdir, version_id, restricted_causes, decomp_step,
            gbd_round_id
        )
        imported_cases_swarm.create_imported_cases_jobs()

        logging.info("{} All jobs submitted".format(pretty_now()))
        success = imported_cases_swarm.run()

        if not success:
            logging.info("Uncaught error in Imported Cases.")
        else:
            logging.info("Job succesfully run!")

    except Exception as e:
        logging.info("Uncaught Exception occurred: {}".format(e))
        sys.exit(1)

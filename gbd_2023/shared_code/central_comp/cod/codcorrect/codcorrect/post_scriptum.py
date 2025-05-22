"""
"Post-scriptum" updates to be run after CoDCorrect runs and
is uploaded. Includes:
    * activating the gbd process version
    * creating and activating the GBD compare version
    * updating the compare version description

Some useful vocab:
    GBD process, referred to here as a process:
        - A way of distinguishing between central machinery types, essentially
          the same concept as gbd_artifact's artifact type.

    GBD process version:
        - Each new run of a process gets its own process version; however,
          process version id is NOT the same as a version of a particular
          machinery.

    GBD compare version:
        - An abstraction to represent a bundle of central machinery runs.
          Together, all of the associated runs can be looked at as a set of
          results from mortality to morbidity, from cause to risk.
"""
import pandas as pd

import gbd_outputs_versions
from gbd.constants import gbd_process_version_status, compare_version_status

from codcorrect.lib import db
from codcorrect.legacy.utils.constants import Status


def post_scriptum_upload(
    version_id: int,
    process_version_id: int,
    cod_output_version_id: int,
    release_id: int,
    test: bool,
) -> int:
    """Handle all the post-run steps.

    Actions:
        * Activate the gbd process version
        * Create and activate the GBD compare version this run will be a part
            of. This the how others can view the results
        * Set CodCorrect version as 'complete' in the DB so CodViz shows results

    Returns:
        The created and activated compare version
    """
    process_version = gbd_outputs_versions.GBDProcessVersion(process_version_id)
    if test:
      process_version.update_status(gbd_process_version_status.INTERNAL_TEST)
    else:
      process_version.update_status(gbd_process_version_status.ACTIVE)

    new_compare_version = gbd_outputs_versions.CompareVersion.add_new_version(
        release_id=release_id
    )

    # Add the CodCorrect GBD process version to the compare version and active
    new_compare_version.add_process_version(process_version_id)
    if test:
      new_compare_version.update_status(compare_version_status.INTERNAL_TEST)
    else:
      new_compare_version.update_status(compare_version_status.ACTIVE)
    
    # Set COD output version
    db.update_status(cod_output_version_id, Status.COMPLETE)

    return new_compare_version.compare_version_id

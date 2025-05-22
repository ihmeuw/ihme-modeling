from typing import List, Optional

from gbd.constants import gbd_process, gbd_process_version_status
from gbd_outputs_versions import (
    CompareVersion,
    DBEnvironment,
    GBDProcessVersion,
    internal_to_process_version,
)

from ihme_cc_sev_calculator.lib import constants, input_utils


def should_update_compare_version(
    compare_version_id: Optional[int],
    paf_version_id: Optional[int],
    como_version_id: Optional[int],
    codcorrect_version_id: Optional[int],
    measures: List[str],
    overwrite_compare_version: bool,
    env: DBEnvironment = DBEnvironment.PROD,
) -> bool:
    """Determines if we should add sev/rrmax process versions to the passed compare version
    (if any), or if we should create a new compare version instead.

    The logic to make this decision is as follows.

    - If a compare version was passed, and we are running rrmax only, the passed compare
    version will be updated if either overwrite_compare_version is True or the passed compare
    version has no pre-existing rrmax version.

    - If a compare version was passed, and we are running both rrmax and sevs, the passed
    compare version will be updated if both of the following hold: (i) its paf, como, and
    codcorrect versions were not overridden in the sev calculator call, and (ii) either
    overwrite_compare_version is True or the passed compare version has no pre-existing rrmax
    version and no pre-existing sev version.

    - In all other cases, a new compare version will be created.
    """
    if compare_version_id is not None:
        cv = CompareVersion(compare_version_id, env=env)
        compare_paf_version_id = input_utils.retrieve_internal_version_id(cv, gbd_process.PAF)
        compare_como_version_id = input_utils.retrieve_internal_version_id(
            cv, gbd_process.EPI
        )
        compare_codcorrect_version_id = input_utils.retrieve_internal_version_id(
            cv, gbd_process.COD
        )
        compare_rrmax_version_id = input_utils.retrieve_internal_version_id(
            cv, gbd_process.RR_MAX
        )
        compare_sev_version_id = input_utils.retrieve_internal_version_id(cv, gbd_process.SEV)
        if (
            constants.SEV in measures
            and compare_paf_version_id == paf_version_id
            and compare_como_version_id == como_version_id
            and compare_codcorrect_version_id == codcorrect_version_id
            and (
                overwrite_compare_version
                or compare_rrmax_version_id is None
                and compare_sev_version_id is None
            )
        ) or (
            constants.SEV not in measures
            and (overwrite_compare_version or compare_rrmax_version_id is None)
        ):
            return True

    return False


def get_validated_process_versions_to_add(
    version_id: int,
    paf_version_id: int,
    como_version_id: int,
    codcorrect_version_id: int,
    measures: List[str],
    updating_cv: bool,
    env: DBEnvironment = DBEnvironment.PROD,
) -> List[int]:
    """Obtains list of process versions to add to the compare version, using the updating_cv
    flag to indicate if we are updating an existing compare version. This function validates
    active or best status for each process version that is relevant to the sev calculator run,
    whether or not that process version already exists in the compare version. Relevant
    process versions are for rr_max, sev, paf aggregator, como, and codcorrect if sev is
    included in the measures, and for rr_max only if sev is not included in the measures.

    Returns:
        List of validated process versions to add to the compare version.

    Raises:
        RuntimeError if a relevant process version has status other than active or best.
    """
    # Record relevant process ids and internal version ids.
    if constants.SEV in measures:
        gbd_process_ids = [
            gbd_process.RR_MAX,
            gbd_process.SEV,
            gbd_process.PAF,
            gbd_process.EPI,
            gbd_process.COD,
        ]
        internal_version_ids = [
            version_id,
            version_id,
            paf_version_id,
            como_version_id,
            codcorrect_version_id,
        ]
    else:
        gbd_process_ids = [gbd_process.RR_MAX]
        internal_version_ids = [version_id]

    # Convert internal versions into GBD process versions for validation and addition to the
    # compare version.
    process_version_ids = [
        internal_to_process_version(internal_version_id, gbd_process_id, env=env)
        for internal_version_id, gbd_process_id in zip(internal_version_ids, gbd_process_ids)
    ]

    # Confirm process versions have best or active status.
    for process_version_id in process_version_ids:
        pv = GBDProcessVersion(process_version_id, env=env)
        if pv.gbd_process_version_status_id not in [
            gbd_process_version_status.BEST,
            gbd_process_version_status.ACTIVE,
        ]:
            raise RuntimeError(
                "The following process version has status "
                f"{pv.gbd_process_version_status_id}: {pv}."
            )

    # Return validated process versions that we need to add to the compare version.
    if updating_cv:
        # Only need sev/rrmax.
        return process_version_ids[:2]
    else:
        # Need all process versions.
        return process_version_ids

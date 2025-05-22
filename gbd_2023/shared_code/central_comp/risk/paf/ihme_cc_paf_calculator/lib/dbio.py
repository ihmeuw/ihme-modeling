from typing import List

import db_queries
import db_tools_core
from gbd import conn_defs
from gbd import constants as gbd_constants

from ihme_cc_paf_calculator.lib import constants, io_utils


def years_from_release(release_id: int) -> List[int]:
    """Get years based on release_id."""
    gbd_team = gbd_constants.gbd_teams.EPI
    demographics = db_queries.get_demographics(release_id=release_id, gbd_team=gbd_team)
    return demographics["year_id"]


def fail_submitted_paf_models(settings: constants.PafCalculatorSettings) -> None:
    """Change submitted PAF model statuses to failed."""
    update_model_statuses(
        model_version_ids=io_utils.get_paf_model_versions_from_settings(settings),
        from_status=constants.EpiModelStatus.SUBMITTED,
        to_status=constants.EpiModelStatus.FAILED,
        test=settings.test,
    )


def submit_failed_paf_models(settings: constants.PafCalculatorSettings) -> None:
    """Change failed PAF model statuses to submitted."""
    update_model_statuses(
        model_version_ids=io_utils.get_paf_model_versions_from_settings(settings),
        from_status=constants.EpiModelStatus.FAILED,
        to_status=constants.EpiModelStatus.SUBMITTED,
        test=settings.test,
    )


def delete_failed_paf_models(model_version_ids: List[int], test: bool) -> None:
    """Change failed PAF model statuses to deleted."""
    update_model_statuses(
        model_version_ids=model_version_ids,
        from_status=constants.EpiModelStatus.FAILED,
        to_status=constants.EpiModelStatus.DELETED,
        test=test,
    )


def update_model_statuses(
    model_version_ids: List[int], from_status: int, to_status: int, test: bool
) -> None:
    """For the given nonempty model_version_ids, change any with status from_status to status
    to_status, using a conn_def corresponding to the given test status.
    """
    with db_tools_core.session_scope(
        constants.PRIVILEGED_TEST_EPI_CONN_DEF if test else constants.PRIVILEGED_EPI_CONN_DEF
    ) as session:
        session.execute(
            constants.UPDATE_MODEL_STATUSES_QUERY,
            params={
                "model_version_ids": model_version_ids,
                "from_status": from_status,
                "to_status": to_status,
            },
        )


def validate_no_deleted_paf_models(settings: constants.PafCalculatorSettings) -> None:
    """Validate that the given settings do not include deleted PAF model version IDs, raising a
    RuntimeError if they do.
    """
    with db_tools_core.session_scope(
        conn_defs.EPI_TEST if settings.test else conn_defs.EPI
    ) as session:
        deleted_model_version_ids = db_tools_core.query_2_df(
            """
            SELECT model_version_id
            FROM epi.model_version
            WHERE model_version_id IN :model_version_ids
            AND model_version_status_id IN :deleted_model_version_status_ids
            """,
            parameters={
                "model_version_ids": io_utils.get_paf_model_versions_from_settings(settings),
                "deleted_model_version_status_ids": [
                    constants.EpiModelStatus.DELETED,
                    constants.EpiModelStatus.SOFT_SHELL_DELETED,
                ],
            },
            session=session,
        ).model_version_id.tolist()

    if deleted_model_version_ids:
        raise RuntimeError(
            "Cannot resume the PAF Calculator for PAF model version ID "
            f"{settings.paf_model_version_id} because the associated PAF model version ID(s) "
            f"{deleted_model_version_ids} are marked as deleted."
        )


def get_deletion_candidates(test: bool) -> List[int]:
    """For the given test status, returns the list of epi model version IDs having a deleted
    status, or having a failed status that was last updated more than constants.STALE_TIME days
    ago. These are candidates for intermediate result deletion in the PAF model cleanup script.
    """
    with db_tools_core.session_scope(
        conn_defs.EPI_TEST if test else conn_defs.EPI
    ) as session:
        return db_tools_core.query_2_df(
            constants.GET_DELETION_CANDIDATES_QUERY,
            parameters={
                "deleted_model_version_status_ids": [
                    constants.EpiModelStatus.DELETED,
                    constants.EpiModelStatus.SOFT_SHELL_DELETED,
                ],
                "failed_model_version_status_id": constants.EpiModelStatus.FAILED,
                "stale_time": constants.STALE_TIME,
            },
            session=session,
        ).model_version_id.tolist()

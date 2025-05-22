"""Monitoring of PAF Calculator runs."""

from typing import List

from sqlalchemy import orm

import db_tools_core
import ihme_cc_risk_utils
from ihme_cc_risk_utils import exceptions as exc
from jobmon.core.constants import WorkflowStatus

from ihme_cc_paf_calculator.lib import constants

# Jobmon terminal statuses that indicate failure
JOBMON_FAILED_STATUSES: List[WorkflowStatus] = [
    WorkflowStatus.FAILED,
    WorkflowStatus.ABORTED,
    WorkflowStatus.HALTED,
]


def get_paf_model_status(model_version_id: int, session: orm.Session) -> int:
    """Get PAF model status. Either success (0), running (1), or failed (2).

    All PAF Calculator models are staged on epi.model_version via save_results,
    so first we check that the given model_version_id is valid. If it is,
    the jobmon workflow status in the jobmon database is considered the source
    of truth for the PAF model status.
    """
    epi_model_metadata = db_tools_core.query_2_df(
        """
        SELECT me.modelable_entity_id, mer.rei_id
        FROM epi.model_version mv
        JOIN modelable_entity.modelable_entity me USING (modelable_entity_id)
        JOIN modelable_entity.modelable_entity_rei mer USING (modelable_entity_id)
        JOIN modelable_entity.model_type mt USING (model_type_id)
        WHERE mv.model_version_id = :model_version_id
        AND mt.model_type_id = 11  # model type: PAF calculator
        """,
        parameters={"model_version_id": model_version_id},
        session=session,
    )

    if epi_model_metadata.empty:
        raise ValueError(
            f"PAF model_version_id {model_version_id} could not be found. Are you sure it "
            "exists?"
        )

    if len(epi_model_metadata) > 1:
        raise RuntimeError(
            f"More than one metadata row found for model_version_id {model_version_id}. "
            "Please submit a help desk ticket."
        )

    epi_model_metadata = epi_model_metadata.loc[0].to_dict()

    # Try to pull jobmon status, catching two known errors the function throws
    try:
        _, jobmon_status = ihme_cc_risk_utils.get_failed_workflow_args(
            constants.JOBMON_WORKFLOW_NAME.format(
                rei_id=epi_model_metadata["rei_id"],
                me_id=epi_model_metadata["modelable_entity_id"],
                mv_id=model_version_id,
            ),
            return_status=True,
        )
    except exc.NoWorkflowFound:
        # Case: an epi model version exists but no jobmon workflow for the PAF Calculator
        # run has been found. Most likely scenario is that the model has just started running.
        # It's possible that the workflow creation failed and the model is actually in a
        # failed state, but we do not handle that possibility here.
        return constants.PafModelStatus.RUNNING
    except exc.WorkflowFinished:
        # Case: the workflow finished successfully
        return constants.PafModelStatus.SUCCESS

    # Check workflow status. If status is one of the known jobmon
    # workflow terminal states, return FAILED. Otherwise, RUNNING.
    # We do not control jobmon workflow reaping, so in a case like the jobmon db
    # goes down in flames and the workflow fails without the db reflecting it as such,
    # we would return RUNNING incorrectly until (and if) the workflow is reaped to failed
    if jobmon_status in JOBMON_FAILED_STATUSES:
        return constants.PafModelStatus.FAILED
    else:
        return constants.PafModelStatus.RUNNING

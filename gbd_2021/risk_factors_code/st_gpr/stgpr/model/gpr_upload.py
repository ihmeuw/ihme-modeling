import sys

from sqlalchemy import orm

import db_stgpr
from db_stgpr.api.enums import ModelStatus
from db_tools import ezfuncs

from stgpr_helpers.api import internal as stgpr_helpers_internal
from stgpr_helpers.api.constants import columns, conn_defs, enums


def _upload_gpr_estimates(
    stgpr_version_id: int,
    model_iteration_id: int,
    parameter_set_number: int,
    session: orm.Session,
) -> None:
    """Uploads GPR estimates to the database."""
    output_path = stgpr_helpers_internal.get_output_path(stgpr_version_id, session)
    file_utility = stgpr_helpers_internal.StgprFileUtility(output_path)

    gpr_df = (
        file_utility.read_gpr_estimates(parameter_set_number)
        .rename(
            columns={
                "gpr_mean": columns.VAL,
                "gpr_lower": columns.LOWER,
                "gpr_upper": columns.UPPER,
            }
        )
        .drop_duplicates()
    )
    stgpr_helpers_internal.load_gpr_estimates(
        stgpr_version_id, gpr_df, model_iteration_id, session
    )


if __name__ == "__main__":
    stgpr_version_id = int(sys.argv[1])
    model_iteration_id = int(sys.argv[2])
    parameter_set_number = int(sys.argv[3])
    last_upload_job = bool(sys.argv[4])
    with ezfuncs.session_scope(conn_defs.STGPR) as scoped_session:
        _upload_gpr_estimates(
            stgpr_version_id, model_iteration_id, parameter_set_number, scoped_session
        )

        # Set model status to next step if this was the last upload job
        if last_upload_job:
            db_stgpr.update_model_status(stgpr_version_id, ModelStatus.rake, scoped_session)

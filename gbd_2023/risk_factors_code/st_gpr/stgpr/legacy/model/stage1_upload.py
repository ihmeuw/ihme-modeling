import sys

import pandas as pd
from sqlalchemy import orm

import db_stgpr
import db_tools_core
import stgpr_helpers
import stgpr_schema
from stgpr_helpers import columns, parameters

from stgpr.lib import expansion, location_aggregation


def _upload_stage_1_estimates(stgpr_version_id: int, session: orm.Session) -> None:
    """Uploads stage 1 estimates or custom stage 1 to the database.

    Also aggregates estimates from country level up to global.
    """
    file_utility = stgpr_helpers.StgprFileUtility(stgpr_version_id)
    params = file_utility.read_parameters()
    # If stage 1 was run (no custom stage 1), upload the stage 1 statistics
    if not params[parameters.CUSTOM_STAGE_1]:
        stage_1_statistics_df = file_utility.read_stage_1_statistics().pipe(_format_statistics)
        stgpr_helpers.load_stage_1_statistics(stgpr_version_id, stage_1_statistics_df, session)

    stage_1_estimates_df = file_utility.read_stage_1_estimates().rename(columns={"stage1": columns.VAL})

    # Only run location aggregation if metric id is non-null
    if params[parameters.METRIC_ID]:
        stage_1_estimates_df = location_aggregation.aggregate_locations(
            stgpr_version_id, stage_1_estimates_df, data_in_model_space=True
        )

    stage_1_estimates_df = expansion.expand_results(data=stage_1_estimates_df, params=params)

    stgpr_helpers.load_stage_1_estimates(stgpr_version_id, stage_1_estimates_df, session)


def _format_statistics(stats_df: pd.DataFrame) -> pd.DataFrame:
    """Transforms stage 1 statistics into the format the database expects.

    Uses regexes to extract covariate name and factor from the "covariate" column.
    """
    return (
        stats_df.assign(
            **{
                columns.FACTOR: lambda df: df[columns.COVARIATE].str.extract(
                    r"as\.factor\(.*\)(.*)", expand=False
                ),
                columns.COVARIATE: lambda df: df[columns.COVARIATE]
                .str.extract(r"as\.factor\((.*)\).*", expand=False)
                .fillna(stats_df[columns.COVARIATE]),
            }
        )
        .rename(
            columns={
                "betas": columns.BETA,
                "se": columns.STANDARD_ERROR,
                "zval": columns.Z_VALUE,
                "pval": columns.P_VALUE,
            }
        )
        .drop(columns=["model"])
    )


if __name__ == "__main__":
    stgpr_version_id = int(sys.argv[1])
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        # Extend length to provide sufficient time for location aggregation and expansion
        scoped_session.execute("SET wait_timeout=600")
        _upload_stage_1_estimates(stgpr_version_id, scoped_session)

        db_stgpr.update_model_status(
            stgpr_version_id, stgpr_schema.ModelStatus.spacetime, scoped_session
        )

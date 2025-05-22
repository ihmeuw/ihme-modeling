from typing import List, Optional

import pandas as pd
from sqlalchemy import text

import db_queries
import db_tools_core
from gbd import conn_defs
from gbd import constants as gbd_constants

from ihme_cc_paf_aggregator.lib import constants, mediation

DRAW_TYPE_METADATA_TYPE_ID = 17


def get_expected_paf_causes(release_id: int) -> List[int]:
    """Causes we should expect PAFs for."""
    cause_metadata = db_queries.get_cause_metadata(
        cause_set_id=constants.COMPUTATION_CAUSE_SET_ID, release_id=release_id
    )
    # Edit hierarchy to set certain causes that are a part of a risk-cause pair
    # but not a most detailed cause for the purpose of directly calculating the
    # PAF for that cause rather than aggregating from child causes.
    # Currently: All-cause for Low educational attainment
    cause_metadata.loc[
        cause_metadata[constants.CAUSE_ID].isin(constants.AGGREGATE_OUTCOMES),
        constants.MOST_DETAILED,
    ] = 1
    all_paf_causes = cause_metadata.query("most_detailed == 1")[constants.CAUSE_ID].tolist()
    return all_paf_causes


def years_from_release(release_id: int) -> List[int]:
    """Get years based on release_id."""
    gbd_team = gbd_constants.gbd_teams.EPI_AR
    if release_id == gbd_constants.release.FHS_2100:
        # we currently have one non-gbd use for the Aggregator with different demographics
        gbd_team = gbd_constants.gbd_teams.FHS

    demographics = db_queries.get_demographics(release_id=release_id, gbd_team=gbd_team)
    return demographics["year_id"]


def get_child_ckd_causes(release_id: int) -> List[int]:
    """Child CKD causes for mediation matrix expansion."""
    cause_metadata = db_queries.get_cause_metadata(
        cause_set_id=constants.COMPUTATION_CAUSE_SET_ID, release_id=release_id
    )
    child_ckd_causes = cause_metadata.query(f"parent_id == {constants.CKD_PARENT_CAUSE_ID}")[
        constants.CAUSE_ID
    ].tolist()
    return child_ckd_causes


def get_detailed_locations(location_set_id: int, release_id: int) -> List[int]:
    """Returns a list of most detailed locations to perform PAF aggregation."""
    return (
        db_queries.get_location_metadata(
            location_set_id=location_set_id, release_id=release_id
        )
        .query("most_detailed == 1")
        .location_id.tolist()
    )


def get_best_input_models(
    release_id: int,
    mediation_matrix: pd.DataFrame,
    input_rei_set_id: List[int],
    rei_id: Optional[List[int]] = None,
) -> pd.DataFrame:
    """Gets best PAF model versions to use in PAF aggregation for a release. This
    includes both 'paf' and 'paf_unmediated' draw types. Unmediated PAF models will
    only be used in aggregation if they are specified by the mediation matrix.

    Arguments:
        release_id: the release ID to query
        mediation_matrix: The mediation matrix, used to determine the unmediated PAF
            models required
        input_rei_set_id: A list of rei set IDs used to subset the PAFs in the release.
        rei_id: An optional list of rei IDs used to subset the PAFs in the release.
            Overrides input_rei_set_id

    Returns:
        df: A DataFrame with at least columns 'rei_id', 'model_version_id', 'draw_type'
    """
    if not rei_id and not input_rei_set_id:
        raise ValueError("One of rei_id or input_rei_set_id must be provided.")
    elif not rei_id:
        rei_meta = pd.concat(
            [
                db_queries.get_rei_metadata(rei_set_id=id, release_id=release_id)
                for id in input_rei_set_id
            ]
        )
        rei_id = rei_meta[constants.REI_ID].unique().tolist()

    df = db_queries.get_best_model_versions(
        entity="rei", ids=rei_id, release_id=release_id, source=["paf", "paf_unmediated"]
    )

    # drop any best unmediated PAF models if they aren't required by the matrix
    risks_with_unmediated_pafs = mediation.get_risks_with_unmediated_pafs(mediation_matrix)
    unneeded_mask = (df[constants.DRAW_TYPE] == "paf_unmediated") & (
        ~df[constants.REI_ID].isin(risks_with_unmediated_pafs)
    )

    return df[~unneeded_mask]


def get_relative_risk_types(model_version_ids: List[int]) -> pd.DataFrame:
    """Generates a dataframe of relative risk types.

    This information has been historically available for inspection from a PAF
    Aggregation run.
    """
    query = """
    SELECT DISTINCT rei_id, rei_name, cause_id, cause_name, relative_risk_type_name,
                    input_model_version_id AS model_version_id, model_version_status,
                    best_start, best_end
    FROM epi.paf_model_version pmv
    JOIN epi.model_version mv ON pmv.input_model_version_id = mv.model_version_id
    JOIN epi.model_version_status USING (model_version_status_id)
    JOIN epi.relative_risk_metadata USING (model_version_id)
    JOIN epi.relative_risk_type USING (relative_risk_type_id)
    JOIN shared.rei USING (rei_id)
    JOIN shared.cause USING (cause_id)
    WHERE paf_model_version_id IN :model_version_ids
    ORDER BY rei_id, cause_id
    """
    with db_tools_core.session_scope(conn_defs.EPI) as session:
        df = db_tools_core.query_2_df(
            query=text(query),
            session=session,
            parameters={"model_version_ids": model_version_ids},
        )

    return df

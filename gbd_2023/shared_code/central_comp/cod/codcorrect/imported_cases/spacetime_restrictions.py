"""Functions for pulling spacetime restrictions."""

import pandas as pd

from db_tools import ezfuncs


def get_all_spacetime_restrictions(release_id: int) -> pd.DataFrame:
    """Fetch all spacetime restrictions from the codcorrect database.

    Args:
        release_id: release ID for this codcorrect run

    Returns:
        DataFrame containing cause_id (int), location_id (int), and
            year_id (int)

    Raises:
        RuntimeError: if no restrictions can be found
    """
    restrictions = ezfuncs.query(
        """
        SELECT
            rv.cause_id,
            r.location_id,
            r.year_id
        FROM
            codcorrect.spacetime_restriction r
        JOIN
            codcorrect.spacetime_restriction_version rv
                USING (restriction_version_id)
        WHERE
            rv.is_best = 1 AND
            rv.release_id = :release_id
        """,
        conn_def="codcorrect",
        parameters={"release_id": release_id},
    )

    if restrictions.empty:
        raise RuntimeError(
            f"No spacetime restrictions could be found for release {release_id}.\n"
        )

    return restrictions

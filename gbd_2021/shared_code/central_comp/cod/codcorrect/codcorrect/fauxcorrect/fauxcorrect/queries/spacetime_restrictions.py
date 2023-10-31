"""
Functions related to spacetime restrictions in CodCorrect.

What are spacetime restrictions?
    There are some causes that we know cannot be in certain locations
    or years. For instance, there can be no cases of ebola before 2014,
    because that was when the disease first appeared. Another example is
    dengue, a disease that is endemic only in tropical locations like
    parts of South America and West and Central Africa because the disease
    cannot survive colder temperatures.

    It can be difficult to get epidemiological models to reflect these hard,
    a-priori truths, so sometimes the best option is to 0 out any "cases" of
    disease for locations and years where we know this is impossible.

    Spacetime restrictions are exactly that: they tell us where and when
    deaths from a disease MUST be 0. For example, a restriction on cause 350
    for 2000 in the US tells us there cannot be any deaths from this cause
    from 2000 in the US.
"""
from datetime import datetime
import getpass
import os
import tempfile
from typing import Dict, List
import warnings

import pandas as pd

from db_tools import loaders
from db_tools.ezfuncs import get_session, query
from db_tools.query_tools import exec_query
from gbd.decomp_step import decomp_step_id_from_decomp_step
from gbd.gbd_round import gbd_round_from_gbd_round_id

from fauxcorrect.queries.queries import SpacetimeRestrictions
from fauxcorrect.utils.constants import (
    Columns, ConnectionDefinitions, DataBases, FilePaths
)


def get_all_spacetime_restrictions(
        gbd_round_id: int,
        decomp_step: str
) -> pd.DataFrame:
    """
    Fetch all spacetime restrictions from the codcorrect database.

    Arguments:
        gbd_round_id: GBD round ID for this codcorrect run
        decomp_step: decomp step ^

    Returns:
        DataFrame containing cause_id (int), location_id (int), and
            year_id (int)
    Raises:
        RuntimeError: if no restrictions can be found
    """
    restrictions = query(
        SpacetimeRestrictions.GET_ALL,
        conn_def=ConnectionDefinitions.CODCORRECT,
        parameters={
            Columns.GBD_ROUND_ID: gbd_round_id,
            Columns.DECOMP_STEP_ID: decomp_step_id_from_decomp_step(
                decomp_step, gbd_round_id
            )
        }
    )

    if restrictions.empty:
        raise RuntimeError(
            f"No spacetime restrictions could be found for GBD round "
            f"{gbd_round_id}, {decomp_step}.\n"
            f"Query:\n{SpacetimeRestrictions.GET_ALL}")

    return restrictions


def update_spacetime_restrictions(
        all_restrictions: pd.DataFrame,
        gbd_round_id: int,
        decomp_step: str,
        mark_best: bool = True
) -> None:
    """Full integration function to update spacetime restrictions.

    Handles the logic for creating spacetime restriction versions,
    uploading the restrictions with the proper restriction version
    and marking the restriction versions best (if desired).

    Arguments:
        all_restrictions: a pandas data frame of all the spacetime restrictions
            to upload. Can include any number of causes
        gbd_round_id: GBD round ID
        decomp_step: decomp step
        mark_best: True iff the created restriction versions should be marked best.
            Defaults to True.
    """
    version_map = _add_all_spacetime_restriction_versions(
        all_restrictions.cause_id.unique().tolist(),
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step
    )

    _upload_all_spacetime_restrictions(
        all_restrictions, restriction_version_map=version_map
    )

    if mark_best:
        for version_id in version_map.values():
            set_spacetime_restriction_version_best(version_id)


def add_spacetime_restriction_version(
        cause_id: int,
        gbd_round_id: int,
        decomp_step: str
) -> int:
    """
    Inserts a new spacetime restriction version in
    codcorrect.spacetime_restriction_version for the cause
    in the given GBD round and decomp step. Does not add any restriction
    data to the version.

    Note:
        The restriction version is not automatically marked best;
        that should be done after the restrictions are uploaded

        Also, there's not any guards for this so make sure your
        inputs are correct.

    Returns:
        The spacetime restriction version id
    """
    username = getpass.getuser()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    exec_query(
        SpacetimeRestrictions.ADD_SPACETIME_RESTRICTION_VERSION,
        parameters={
            "cause_id": cause_id,
            "gbd_round": gbd_round_from_gbd_round_id(gbd_round_id),
            "gbd_round_id": gbd_round_id,
            "decomp_step_id": decomp_step_id_from_decomp_step(
                decomp_step, gbd_round_id
            ),
            "inserted_by": username,
            "date_inserted": timestamp
        },
        session=get_session(ConnectionDefinitions.CODCORRECT),
        close=True
    )

    return query(
        SpacetimeRestrictions.GET_LATEST_RESTRICTION_VERSION_ID,
        conn_def=ConnectionDefinitions.CODCORRECT
    ).restriction_version_id.iat[0]


def upload_spacetime_restrictions(restrictions: pd.DataFrame) -> None:
    """
    Takes the given dataset and uploads to codcorrect.spacetime_restriction.

    Validates:
        * Three columns exist: restriction_version_id, location_id, year_id
        * restriction_version_id is valid
    """
    for col in Columns.SPACETIME_RESTRICTIONS:
        if col not in restrictions:
            raise RuntimeError(f"restrictions df is missing column: {col}")

    restrictions = restrictions[Columns.SPACETIME_RESTRICTIONS]
    restrictions = restrictions.drop_duplicates()

    validate_spacetime_restriction_version_id(
        restrictions.restriction_version_id.unique().tolist()
    )

    # upload
    infiler = loaders.Infiles(
        table="spacetime_restriction",
        schema=DataBases.CODCORRECT,
        session=get_session(ConnectionDefinitions.CODCORRECT)
    )

    # the infiler need a directory to read files in from, so
    # we have to translate our dataset from in-memory to in a temp dir
    restrictions.to_csv(
        os.path.join(FilePaths.TEMP_DIR, "spacetime_restrictions/tmpfile.csv"), index=False
    )

    infiler.indir(
        path=os.path.join(FilePaths.TEMP_DIR, FilePaths.TEMP_DIR, "spacetime_restrictions"),
        with_replace=False,
        partial_commit=True,
        commit=True
    )

    # Clean up
    os.remove(os.path.join(FilePaths.TEMP_DIR, "spacetime_restrictions/tmpfile.csv"))


def set_spacetime_restriction_version_best(
        restriction_version_id: int
) -> None:
    """
    Sets the given restriction version id best. If there's already a best
    for the cause-round-step that the restriction version represents, that
    version is set to is_best=0.

    Warns:
        If another version must be unmarked best to mark this version best

    Raises:
        ValueError if restriction_version_id doesn't exist; RuntimeError if
            multiple restriction versions are currently marked best for the
            cause/round/step that the given version represents.
    """
    validate_spacetime_restriction_version_id([restriction_version_id])

    # get cause/round/step associated with version
    version_metadata = query(
        SpacetimeRestrictions.GET_RESTRICTION_VERSION_METADATA,
        parameters={
            "restriction_version_id": restriction_version_id
        },
        conn_def=ConnectionDefinitions.CODCORRECT
    )

    cause_id, gbd_round_id, decomp_step_id = version_metadata.loc[0]

    # check if there's already a best for this cause/round/step combo
    current_best = query(
        SpacetimeRestrictions.GET_CURRENT_BEST_VERSION_ID,
        parameters={
            "cause_id": cause_id,
            "gbd_round_id": gbd_round_id,
            "decomp_step_id": decomp_step_id
        },
        conn_def=ConnectionDefinitions.CODCORRECT
    ).restriction_version_id

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # If there is already a best, unmark it best
    if not current_best.empty:
        if len(current_best) > 1:
            raise RuntimeError(
                "More than one best restriction version found for cause id "
                f"{cause_id}, GBD round {gbd_round_id}, decomp step id "
                f"{decomp_step_id}: {current_best.tolist()}")

        current_best = current_best.iat[0]

        warnings.warn(f"Unmarking restriction version {current_best} best.")
        exec_query(
            SpacetimeRestrictions.UNMARK_BEST,
            parameters={
                "restriction_version_id": current_best,
                "best_end": timestamp
            },
            session=get_session(ConnectionDefinitions.CODCORRECT),
            close=True
        )

    # mark this version best
    exec_query(
        SpacetimeRestrictions.MARK_BEST,
        parameters={
            "restriction_version_id": restriction_version_id,
            "best_start": timestamp
        },
        session=get_session(ConnectionDefinitions.CODCORRECT),
        close=True
    )


def validate_spacetime_restriction_version_id(
        restriction_version_ids: List[int]
) -> None:
    """
    Validates that all restriction versions in the list
    are valid, ie that they exist in the database.

    Raises:
        ValueError: if any of the versions are not valid
    """
    matched_versions = query(
        SpacetimeRestrictions.CHECK_RESTRICTION_VERSION_ID,
        parameters={
            "restriction_version_id": restriction_version_ids
        },
        conn_def=ConnectionDefinitions.CODCORRECT
    ).restriction_version_id

    missing_versions = set(restriction_version_ids) - set(matched_versions)
    if missing_versions:
        raise ValueError(
            "The following restriction version id(s) do not exist: "
            f"{list(missing_versions)}")


def _add_all_spacetime_restriction_versions(
        cause_ids: List[int],
        gbd_round_id: int,
        decomp_step: str
) -> Dict[int, int]:
    """
    Adds a restriction version id for each cause id in the list.

    Wrapper for add_spacetime_restriction_version. Returns a dictionary
    mapping cause id to the corresponding restriction version id created.
    """
    version_map = {}
    for cause_id in cause_ids:
        restriction_version_id = add_spacetime_restriction_version(
            cause_id=cause_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step
        )
        version_map[cause_id] = restriction_version_id

    return version_map


def _upload_all_spacetime_restrictions(
        restrictions: pd.DataFrame,
        restriction_version_map: Dict[int, int]
) -> None:
    """
    Uploads the restrictions to codcorrect.spacetime_restrictions.

    Wrapper for upload_spacetime_restrictions. Uploads all restrictions
    at once rather than cycling through one cause at a time.

    Args:
        restrictions: dataframe with restrictions to upload with
            three columns: cause_id, location_id, year_id.
        restriction_version_map: dictionary with mapping from
            cause_id -> restriction version id. This will be used
            to replace cause_id in the restrictions dataframe with
            the restrictions version id for the upload.
    """
    restrictions["restriction_version_id"] = restrictions.cause_id.map(
        restriction_version_map
    )

    if restrictions.restriction_version_id.isna().any():
        raise RuntimeError(
            "Restriction version is NA for "
            f"{len(restrictions[restrictions.restriction_version_id.isna()])} rows"
        )

    upload_spacetime_restrictions(restrictions)

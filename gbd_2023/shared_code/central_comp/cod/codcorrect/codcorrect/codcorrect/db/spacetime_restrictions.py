"""Functions related to spacetime restrictions in CodCorrect.

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
"""

import datetime
import getpass
import os
import warnings
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import orm

import db_tools_core
from db_tools import loaders

from codcorrect.legacy.utils.constants import (
    Columns,
    ConnectionDefinitions,
    DataBases,
    FilePaths,
)
from codcorrect.lib.db.queries import SpacetimeRestrictions


def get_all_spacetime_restrictions(
    release_id: int, session: Optional[orm.Session] = None
) -> pd.DataFrame:
    """Fetch all spacetime restrictions from the codcorrect database.

    Arguments:
        release_id: release ID for this codcorrect run
        session: session with the CodCorrect database

    Returns:
        DataFrame containing cause_id (int), location_id (int), and
            year_id (int)
    Raises:
        RuntimeError: if no restrictions can be found
    """
    with db_tools_core.session_scope(
        ConnectionDefinitions.CODCORRECT, session=session
    ) as scoped_session:
        restrictions = db_tools_core.query_2_df(
            SpacetimeRestrictions.GET_ALL,
            session=scoped_session,
            parameters={Columns.RELEASE_ID: release_id},
        )

    if restrictions.empty:
        raise RuntimeError(
            f"No spacetime restrictions could be found for release ID "
            f"{release_id}.\nQuery:\n{SpacetimeRestrictions.GET_ALL}"
        )

    return restrictions


def update_spacetime_restrictions(
    all_restrictions: pd.DataFrame, release_id: int, mark_best: bool = True
) -> None:
    """Full integration function to update spacetime restrictions.

    Handles the logic for creating spacetime restriction versions,
    uploading the restrictions with the proper restriction version
    and marking the restriction versions best (if desired).

    Arguments:
        all_restrictions: a pandas data frame of all the spacetime restrictions
            to upload. Can include any number of causes
        release_id: release ID
        mark_best: True iff the created restriction versions should be marked best.
            Defaults to True.
    """
    with db_tools_core.session_scope(ConnectionDefinitions.CODCORRECT) as scoped_session:
        version_map = _add_all_spacetime_restriction_versions(
            all_restrictions.cause_id.unique().tolist(),
            release_id=release_id,
            session=scoped_session,
        )

        _upload_all_spacetime_restrictions(
            all_restrictions, restriction_version_map=version_map, session=scoped_session
        )

        if mark_best:
            for version_id in version_map.values():
                set_spacetime_restriction_version_best(version_id, session=scoped_session)


def add_spacetime_restriction_version(
    cause_id: int, session: orm.Session, release_id: int
) -> int:
    """Inserts a new spacetime restriction version in
    codcorrect.spacetime_restriction_version for the cause
    in the given release.
    Does not add any restriction data to the version.

    Note:
        The restriction version is not automatically marked best;
        that should be done after the restrictions are uploaded

        Also, there's not any guards for this so make sure your
        inputs are correct.

    Returns:
        The spacetime restriction version id
    """
    username = getpass.getuser()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    session.execute(
        SpacetimeRestrictions.ADD_SPACETIME_RESTRICTION_VERSION,
        params={
            "cause_id": cause_id,
            "release_id": release_id,
            "inserted_by": username,
            "date_inserted": timestamp,
        },
    )

    return int(
        session.execute(SpacetimeRestrictions.GET_LATEST_RESTRICTION_VERSION_ID).scalar()
    )


def upload_spacetime_restrictions(restrictions: pd.DataFrame, session: orm.Session) -> None:
    """Takes the given dataset and uploads to codcorrect.spacetime_restriction.

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
        restrictions.restriction_version_id.unique().tolist(), session=session
    )

    # upload
    infiler = loaders.Infiles(
        table="spacetime_restriction",
        schema=DataBases.CODCORRECT,
        session=session,
    )

    # the infiler need a directory to read files in from, so
    # we have to translate our dataset from in-memory to in a temp dir
    restrictions.to_csv(
        os.path.join(FilePaths.TEMP_DIR, "FILEPATH"), index=False
    )

    infiler.indir(
        path=os.path.join(FilePaths.TEMP_DIR, FilePaths.TEMP_DIR, "spacetime_restrictions"),
        with_replace=False,
        partial_commit=True,
        commit=True,
    )

    # Clean up
    os.remove(os.path.join(FilePaths.TEMP_DIR, "FILEPATH"))


def set_spacetime_restriction_version_best(
    restriction_version_id: int, session: orm.Session
) -> None:
    """Sets the given restriction version id best. If there's already a best
    for the cause-round-step that the restriction version represents, that
    version is set to is_best=0.

    Warns:
        If another version must be unmarked best to mark this version best

    Raises:
        ValueError if restriction_version_id doesn't exist; RuntimeError if
            multiple restriction versions are currently marked best for the
            cause/round/step that the given version represents.
    """
    validate_spacetime_restriction_version_id([restriction_version_id], session=session)

    # get cause/round/step associated with version
    version_metadata = db_tools_core.query_2_df(
        SpacetimeRestrictions.GET_RESTRICTION_VERSION_METADATA,
        session=session,
        parameters={"restriction_version_id": restriction_version_id},
    )

    cause_id, release_id = version_metadata.loc[0]

    # check if there's already a best for this cause/release combo
    current_best = db_tools_core.query_2_df(
        SpacetimeRestrictions.GET_CURRENT_BEST_VERSION_ID,
        session=session,
        parameters={"cause_id": cause_id, "release_id": release_id},
    ).restriction_version_id

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # If there is already a best, unmark it best
    if not current_best.empty:
        if len(current_best) > 1:
            raise RuntimeError(
                "More than one best restriction version found for cause id "
                f"{cause_id}, release ID {release_id}: {current_best.tolist()}"
            )

        current_best = current_best.iat[0]

        warnings.warn(f"Unmarking restriction version {current_best} best.")
        session.execute(
            SpacetimeRestrictions.UNMARK_BEST,
            params={"restriction_version_id": current_best, "best_end": timestamp},
        )

    # mark this version best
    session.execute(
        SpacetimeRestrictions.MARK_BEST,
        params={"restriction_version_id": restriction_version_id, "best_start": timestamp},
    )


def validate_spacetime_restriction_version_id(
    restriction_version_ids: List[int], session: orm.Session
) -> None:
    """Validates that all restriction versions in the list
    are valid, ie that they exist in the database.

    Raises:
        ValueError: if any of the versions are not valid
    """
    matched_versions = db_tools_core.query_2_df(
        SpacetimeRestrictions.CHECK_RESTRICTION_VERSION_ID,
        session=session,
        parameters={"restriction_version_id": restriction_version_ids},
    ).restriction_version_id

    missing_versions = set(restriction_version_ids) - set(matched_versions)
    if missing_versions:
        raise ValueError(
            "The following restriction version id(s) do not exist: "
            f"{list(missing_versions)}"
        )


def _add_all_spacetime_restriction_versions(
    cause_ids: List[int], release_id: int, session: orm.Session
) -> Dict[int, int]:
    """Adds a restriction version id for each cause id in the list.

    Wrapper for add_spacetime_restriction_version. Returns a dictionary
    mapping cause id to the corresponding restriction version id created.
    """
    version_map = {}
    for cause_id in cause_ids:
        restriction_version_id = add_spacetime_restriction_version(
            cause_id=cause_id, release_id=release_id, session=session
        )
        version_map[cause_id] = restriction_version_id

    return version_map


def _upload_all_spacetime_restrictions(
    restrictions: pd.DataFrame, restriction_version_map: Dict[int, int], session: orm.Session
) -> None:
    """Uploads the restrictions to codcorrect.spacetime_restrictions.

    Wrapper for upload_spacetime_restrictions. Uploads all restrictions
    at once rather than cycling through one cause at a time.

    Args:
        restrictions: dataframe with restrictions to upload with
            three columns: cause_id, location_id, year_id.
        restriction_version_map: dictionary with mapping from
            cause_id -> restriction version id. This will be used
            to replace cause_id in the restrictions dataframe with
            the restrictions version id for the upload.
        session: session with the CodCorrect database
    """
    restrictions["restriction_version_id"] = restrictions.cause_id.map(
        restriction_version_map
    )

    if restrictions.restriction_version_id.isna().any():
        raise RuntimeError(
            "Restriction version is NA for "
            f"{len(restrictions[restrictions.restriction_version_id.isna()])} rows"
        )

    upload_spacetime_restrictions(restrictions, session=session)

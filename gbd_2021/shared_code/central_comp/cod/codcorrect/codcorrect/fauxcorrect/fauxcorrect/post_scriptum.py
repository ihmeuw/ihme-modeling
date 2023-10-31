"""
"Post-scriptum" updates to be run after CoD/FauxCorrect runs and
is uploaded. Includes:
    * activating the gbd process version
    * creating and activating the GBD compare version
    * updating the compare version description

All of the action takes place in the gbd database:
    modeling-gbd-db.ihme.washington.edu

Some useful vocab:
    GBD process, referred to here as a process:
        - A way of distinguishing between central machinery types, essentially
          the same concept as gbd_artifact's artifact type. Suspicious...
          BUT processes have ids associated with them as well as human-readable
          names. Ex: CodCorrect (process id 3) and FauxCorrect (process id 35)
          are separate GBD processes

    GBD process version:
        - Each new run of a process gets its own process version; however,
          process version id is NOT the same as a version of a particular
          machinery. Confusing, I know. The difference is that every time
          that ANY process is run, the process version id is increased by 1,
          no matter which process is running, whereas versions of particular
          machinery are increased only by runs of their own
        - This is mostly confusing because GBD process versions are
          redundant IMO, but here's an example:
            * v110 of CodCorrect runs and is assigned a process version of 12345
            * Now let's say HALE, COMO, and something else run before the next
              CodCorrect...
            * v111 of CodCorrect is then run and is assigned a process version
              of 12349
            * The process version (12349) has increased by 4 now because it's
              actually dependant on all processes! CodCorrect's own versioning,
              however, is self-contained and only increased by 1

    GBD compare version:
        - An abstraction to represent a bundle of central machinery runs.
          Together, all of the associated runs can be looked at as a set of
          results from mortality to morbidity, from cause to risk. The
          following compare version is representative of what this script
          will typically create:
            Burdenator: 127
            COMO: 461
            CoDCorrect: 114
            DALYnator: 43,
            HALE: 14714
            PAFs: 282
            SEV: 281

    The bleeding edge:
        - The most latest and greatest of all the processes make up this
          to ensure that the most recent developments in the worlds of epi,
          cod, etc are carried through
"""
import pandas as pd

from gbd_outputs_versions import compare_version
from gbd_outputs_versions import convenience
from db_tools.ezfuncs import get_session
from db_tools.query_tools import exec_query

from fauxcorrect.queries.queries import GbdDatabase
from fauxcorrect.utils import helpers
from fauxcorrect.utils.constants import GBD


def post_scriptum_upload(
        process_version_id: int,
        machine_process: str,
        gbd_round_id: int,
        decomp_step: str
) -> int:
    """
    Handle all the post-run bureaucracy, marking things active and creating
    versions and descriptions as necessary.

    Actions:
        * Activate the gbd process version
        * Create and activate the GBD compare version this run will be a part
            of. This the how others can view the results, so it's critical
        * Update the compare version description. Generally a Cod/FauxCorrect
            description consists of all versions of central machinery that
            go into it. An example of compare version 7230:
                "Burdenator: 127, COMO: 461, CoDCorrect: 114, DALYnator: 43,
                HALE: 14714, PAFs: 282, SEV: 281"

    Returns:
        The freshly created and activated compare version
    """
    running_process_id = helpers.get_gbd_process_id_from_name(machine_process)
    other_process_id = helpers.get_gbd_process_id_from_name(
        machine_process, return_opposite=True)

    activate_process_version(process_version_id)

    new_compare_version = compare_version.CompareVersion.add_new_version(
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        compare_version_description="Temp to be updated")

    edge_df = convenience.bleeding_edge(
        new_compare_version.compare_version_id)
    edge_df = edge_df.loc[edge_df.gbd_process_id != other_process_id]

    description = create_compare_version_description(
        process_version_id=process_version_id,
        machine_process=machine_process,
        machine_process_id=running_process_id,
        bleeding_edge_df=edge_df)

    # Add gbd_process_version_ids to compare version,
    # active the compare version + its description
    new_compare_version.add_process_version(
        edge_df.gbd_process_version_id.tolist())
    activate_compare_version(new_compare_version.compare_version_id)
    update_compare_version_description(
        description, new_compare_version.compare_version_id)

    return new_compare_version.compare_version_id


def activate_process_version(
        process_version_id: int,
        conn_def: str = "gbd"
) -> None:
    """
    Activates the given process version.

    This means flipping the status id from -1 to 2.

    Args:
        process_version_id: the GBD process version ID of the Cod/FauxCorrect
            run. This is created at the start of the run
        conn_def: a connection definition to connect to a database. Defaults
            to 'gbd'.
    """
    exec_query(
        GbdDatabase.ACTIVATE_PROCESS_VERSION,
        parameters={
            "process_version_id": process_version_id
        },
        session=get_session(conn_def),
        close=True)


def activate_compare_version(
        compare_version_id: int,
        conn_def: str = "gbd"
) -> None:
    """
    Activates the given compare version.

    This means flipping the status id from -1 to 2.

    Note:
        Compare versions represent a bundle of central machinery runs;
        ie. CodCorrect v111, Burdenator v33, DALYnator v222, etc

    Args:
        compare_version_id: the GBD compare version ID of the
            Cod/FauxCorrect run. This is created after a run finishes.
        conn_def: a connection definition to connect to a database. Defaults
            to 'gbd'
    """
    exec_query(
        GbdDatabase.ACTIVATE_COMPARE_VERSION,
        parameters={
            "compare_version_id": compare_version_id
        },
        session=get_session(conn_def),
        close=True)


def update_compare_version_description(
        description: str,
        compare_version_id: int,
        conn_def: str = "gbd"
) -> None:
    """
    Compare versions are borne into this world naked; it is our duty
    to clothe them.

    Here we slap an informative description onto the compare version
    we just made for the new CoD/FauxCorrect run.
    """
    exec_query(
        GbdDatabase.UPDATE_COMPARE_VERSION_DESCRIPTION,
        parameters={
            "description": description,
            "compare_version_id": compare_version_id
        },
        session=get_session(conn_def),
        close=True)


def create_compare_version_description(
        process_version_id: int,
        machine_process: str,
        machine_process_id: int,
        bleeding_edge_df: pd.DataFrame
) -> str:
    """
    Returns a formatted string description of the compare version.

    Note:
        Interal versions (a la CodCorrect v103) are parsed from their
        process version string, NOT any particular database field. For
        processes that don't have internal versoning, their process
        version id is used.

    Example:
        "Burdenator: 127, COMO: 123, CoDCorrect: 116,
        DALYnator: 43, HALE: 14642, PAFs: 282, SEV: 281, MMR 14701"

    Args:
        process_version_id: id to differentiate a version of a process out of,
            ironically, all processes
        machine_process: str, name of the process. Ex: "codcorrect",
            "fauxcorrect"
        machine_process_id: id of the process. Ex: 3 for codcorrect, 35
            for fauxcorrect
        bleeding_edge_df: dataframe representing the bleeding edge of
            relevant central machinery runs
    """
    cv_strs = []
    for _, row in bleeding_edge_df.iterrows():
        if row.gbd_process_id == machine_process_id:
            if row.gbd_process_version_id != process_version_id:
                raise RuntimeError(
                    f"For {machine_process} (process id {machine_process_id}),"
                    " bleeding edge process version id "
                    f"{row.gbd_process_version_id} does not match the running "
                    f"process version id {process_version_id}.")

        note = (
            row.gbd_process_version_note
            if row.gbd_process_id not in
            GBD.Process.Id.PROCESS_HAS_NO_INTERNAL_VERSIONING
            else str(row.gbd_process_version_id)
        )

        cv_strs.append(_get_process_description_from_id(
            row.gbd_process_id, note))

    cv_strs = sorted(cv_strs)
    return ", ".join(cv_strs)


def _get_process_description_from_id(process_id: int, note: str) -> str:
    """
    Return a properly formatted description for the given process id
    to be tacked on with ALL processes in the new
    compare version created for this CoD/FauxCorrect run.

    Args:
        process_id: id of the process. Ex: 3 for codcorrect, 35
            for fauxcorrect
        note: the description of the process version run. Commonly,
            'COMO: v123', which is converted into '123'
    """
    process_to_processor = {
        GBD.Process.Id.COMO:  'COMO: {}'.format(note.split(' v')[-1]),
        GBD.Process.Id.CODCORRECT:  'CoDCorrect: {}'.format(
            note.split(', ')[0].split('v')[-1]),
        GBD.Process.Id.BURDENATOR:  'Burdenator: {}'.format(
            note.split(', ')[0].split('v')[-1]),
        GBD.Process.Id.PAFS:  'PAFs: {}'.format(
            note.split(', ')[-1].split()[-1]),
        GBD.Process.Id.DALYNATOR:  'DALYnator: {}'.format(
            note.split(', ')[0].split('v')[-1]),
        GBD.Process.Id.SEV: 'SEV: {}'.format(
            note.split(', ')[0].split('v')[-1]),
        GBD.Process.Id.HALE: 'HALE: {}'.format(note),
        GBD.Process.Id.MMR: 'MMR: {}'.format(note),
        GBD.Process.Id.FAUXCORRECT: 'FauxCorrect: {}'.format(
            note.split(', ')[0].split('v')[-1])
    }
    return process_to_processor[process_id]

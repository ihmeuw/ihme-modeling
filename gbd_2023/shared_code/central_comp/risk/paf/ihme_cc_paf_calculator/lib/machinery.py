"""Logic related to central machinery (COMO, CodCorrect) used within the PAF Calculator."""

from typing import Dict, Optional

from gbd.constants import gbd_process, release

# Map of GBD release -> default machinery versions dictionary (GBD process ID ->
# internal version). Declares default versions to use for CoDCorrect/COMO
# if the user does not specify one. Note: final runs for GBD 2021 used CoDCorrect
# v363 and COMO v1082, but the versions were updated later after v363 and v1082
# were deleted.
DEFAULT_MACHINERY_VERSIONS_MAP: Dict[int, Dict[int, Optional[int]]] = {
    release.GBD_2021_R1: {gbd_process.COD: 393, gbd_process.EPI: 1471},
    release.USRE: {  # USRE does not use GBD central machinery. They create their own burden
        gbd_process.COD: None,
        gbd_process.EPI: None,
    },
    release.GBD_2023: {gbd_process.COD: 422, gbd_process.EPI: 1531},
    release.US_AVERTABLE_BURDEN_2021: {gbd_process.COD: 403, gbd_process.EPI: 1480},
}

# Map of GBD release -> arbitrary year_id to use for splitting parent cause PAFs to
# child causes. Should align with the year the SEV Calculator uses for its analogous
# process of aggregating child cause PAFs to parent causes.
SUBCAUSE_SPLITTING_ARBITRARY_YEAR_MAP: Dict[int, int] = {
    release.GBD_2021_R1: 2020,
    release.USRE: 2020,  # Set to match GBD 2021. May need to be revisited
    release.GBD_2023: 2023,
    release.US_AVERTABLE_BURDEN_2021: 2020,
}


def release_has_machinery(release_id: int, gbd_process_id: int) -> bool:
    """Helper to determine if the given release has results for the given machinery available,
    from the perspective of the PAF Calculator.
    """
    return (
        release_id in DEFAULT_MACHINERY_VERSIONS_MAP
        and gbd_process_id in DEFAULT_MACHINERY_VERSIONS_MAP[release_id]
        and DEFAULT_MACHINERY_VERSIONS_MAP[release_id][gbd_process_id] is not None
    )


def get_default_machinery_version_id(gbd_process_id: int, release_id: int) -> Optional[int]:
    """Get default internal version ID for given GBD process and release.

    Ex: given GBD process ID 3 (CodCorrect) and release 9 (GBD 2021), returns 363, the
    CodCorrect version used for PAF models for GBD 2021.

    All supported releases are expected to have an entry in DEFAULT_MACHINERY_VERSIONS_MAP.
    """
    if release_id not in DEFAULT_MACHINERY_VERSIONS_MAP:
        raise RuntimeError(
            "Internal error: no default central machinery versions set for release ID "
            f"{release_id}."
        )

    return DEFAULT_MACHINERY_VERSIONS_MAP[release_id][gbd_process_id]


def get_arbitrary_year_id_for_subcause_splitting(release_id: int) -> int:
    """Get arbitrary year_id to pull burden for "subcause splitting.

    Splitting PAFs from parent cause to child causes assumes the ratio of child burden
    does not vary much by year. Therefore, in theory, any year could be uses. Research
    management sets what the arbitrary year is for each release.

    All supported releases are expected to have an entry in
    SUBCAUSE_SPLITTING_ARBITRARY_YEAR_MAP.
    """
    if release_id not in SUBCAUSE_SPLITTING_ARBITRARY_YEAR_MAP:
        raise RuntimeError(
            "Internal error: subcause splitting not currently supported for release "
            f"ID {release_id}."
        )

    return SUBCAUSE_SPLITTING_ARBITRARY_YEAR_MAP[release_id]

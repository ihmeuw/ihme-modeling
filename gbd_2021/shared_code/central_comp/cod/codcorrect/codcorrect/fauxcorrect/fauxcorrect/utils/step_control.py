"""
Maintaining backwards compatibility with previous GBD rounds
by keeping track of GBD round-specific constants, arguments, and more.

Abstractions:
    GBD dictionary: a dictionary whose keys are GBD round ids. The values can
        be anything. Very useful for easily swapping values/functions/methods
        in and out based on the GBD round.

        Example: _DECOMP_EXEMPT

    Decomp step dictionary: These are GBD round-specific because decomp steps
        vary by GBD round. So within a GBD dictionary, if values/actions vary
        based on the decomp step, we have values that are inner dictionaries
        with decomp steps as keys.

        Example: _NEW_CAUSES_ENTER_DECOMP

Note:
    As written, GBD and decomp step dictionaries need to be exhaustive lists.
    Meaning that ALL possible GBD rounds and decomp steps need to be enumerated
    or else errors will be hit. There's no imputation based on which keys
    are available like what happens in gbd_artifacts with sparse GBD
    dictionaries.
"""
import functools
from typing import Any, Dict, List, Optional

from gbd.constants import decomp_step as ds
from gbd.decomp_step import decomp_step_id_from_decomp_step
from rules.enums import ResearchAreas, Rules, Tools
from rules import RulesManager

from fauxcorrect.utils.constants import LocationSetId


# BASE LOCATION SETS

# The "base" location set is the location set *Correct is initially run on.
# All other location sets in a run are aggregates that built off the base.
_DEFAULT_BASE_LOCATION_SET: int = LocationSetId.STANDARD
_BASE_LOCATION_SET: Dict[int, int] = {
    6: LocationSetId.OUTPUTS
}


def get_base_location_set_id(gbd_round_id: int) -> int:
    """
    Returns the base location set given the GBD round.

    The "base" location set is the location set *Correct is initially run on.
    All other location sets in a run are aggregates that built off the base.
    """
    return _read_gbd_round_dictionary(
        gbd_round_id, _BASE_LOCATION_SET, "BASE_LOCATION_SET",
        default=_DEFAULT_BASE_LOCATION_SET)


# These are causes modeled as fatal in the previous round
# that are no longer being modeled as fatal (yll only) or are being dropped.
_CAUSES_DROPPED_FROM_CODCORRECT: Dict[int, List[int]] = {
    6: [
        333,
        334,
        335,
        336,
        337,
        605
    ],
    7: [
        462,
        347,
        574
    ]
}


def get_decomp_exempt_cause_ids(
        gbd_round_id: int,
        decomp_step: str
) -> List[int]:
    """
    Returns a list of decomp exempt causes (that aren't new causes)
    for the given GBD round.
    """
    return _get_step_rules(
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step
    )[Rules.EXEMPT_CAUSES_IN_ROUND] or []

def get_new_cause_ids(
        gbd_round_id: int,
        decomp_step: str,
) -> List[int]:
    """
    Returns a list of new causes for the given GBD round.
    These are technically "decomp exempt", but only because
    they are new to the round, so I prefer to differenciate
    between them and true decomp exempt causes.

    Returns an empty list if new causes aren't a part of decomp
    for the given decomp step.
    """
    return _get_step_rules(
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step
    )[Rules.CAUSES_NEW_TO_ROUND] or []


def get_iterative_cause_ids(gbd_round_id: int, decomp_step: str) -> List[int]:
    """
    Returns a list of all causes, including decomp-exempt and
    new causes, that have best models in iterative rather
    than in the particular decomp step being run.

    New causes will only be returned if the given decomp step
    allows for new causes to be a part of decomp.
    """
    return (
        get_decomp_exempt_cause_ids(gbd_round_id, decomp_step) +
        get_new_cause_ids(gbd_round_id, decomp_step)
    )


def get_cause_ids_no_longer_fatal(gbd_round_id: int) -> List[int]:
    """
    Returns a list of cause ids that were in CodCorrect the previous round
    but no longer are either because the causes are now yll-only or
    were dropped altogether.

    Only used for best model tracking currently.
    """
    return _read_gbd_round_dictionary(
        gbd_round_id, _CAUSES_DROPPED_FROM_CODCORRECT,
        "CAUSES_DROPPED_FROM_CODCORRECT")


# Maps gbd round id to the final modeling step, meaning the final
# step for modelers
_FINAL_MODELING_STEP: Dict[int, Dict[int, int]] = {
    5: ds.ITERATIVE,
    6: ds.FOUR,
    7: ds.THREE
}


def get_final_modeling_step(gbd_round_id: int) -> str:
    """
    Returns the decomp step of the final modeling step
    of the GBD round.
    """
    return _read_gbd_round_dictionary(
        gbd_round_id, _FINAL_MODELING_STEP,
        "FINAL_MODELING_STEP")


# HELPERS

def _read_gbd_round_dictionary(
        gbd_round_id: int,
        dictionary: Dict[int, Any],
        dictionary_name: str,
        default: Optional[Any] = None
)-> Any:
    """
    Simple wrapper for reading from dictionaries to get a slightly
    more informative error message in the case of a key error. GBD
    dictionaries are defined as any dictionary whose key is a
    GBD round id mapping to values that can hold any value.

    Args:
        gbd_round_id: GBD round id
        dictionary: a GBD dictionary (keys are gbd round ids)
        dictionary_name: the name of the dictionary. Only used in
            the error message in the case that gbd_round_id is not
            a valid key

    Returns:
        the value inside the dictionary for the given gbd round id

    Raises:
        ValueError: the gbd_round_id is not in the dictionary
    """
    if gbd_round_id not in dictionary:
        if default is not None:
            return default
        else:
            raise ValueError(
                f"GBD round {gbd_round_id} is not in the list of acceptable "
                f"GBD round ids ({list(dictionary.keys())}) for dictionary "
                f"'{dictionary_name}'")

    return dictionary[gbd_round_id]


def _read_gbd_round_to_decomp_step_dictionary(
        gbd_round_id: int,
        decomp_step: str,
        dictionary: Dict[int, Any],
        dictionary_name: str
) -> Any:
    """
    Wraps _read_gbd_round_dictionary; for use with GBD dictionaries
    that map to decomp step dictionarys.

    Ex:
        GBD dict = {
            gbd round 6: {
                step 1: item/action 1,
                step 2: item/action 2
            },
            etc...
        }

    Args:
        gbd_round_id: GBD round id
        decomp_step: decomp step as a string
        dictionary: a GBD dictionary (keys are gbd round ids)
        dictionary_name: the name of the dictionary. Only used in
            the error message in the case that gbd_round_id is not
            a valid key

    Returns:
        the value inside the dictionary for the given gbd round id

    Raises:
        ValueError: the gbd_round_id is not in the dictionary,
            if the decomp step is not in the inner decomp step dictionary
    """
    step_dictionary = _read_gbd_round_dictionary(
        gbd_round_id, dictionary, dictionary_name)

    if decomp_step not in step_dictionary:
        raise ValueError(
            f"Decomp step '{decomp_step}' is not in the list of acceptable "
            f"decomp steps ({list(step_dictionary.keys())}) for dictionary "
            f"'dictionary_name'")

    return step_dictionary[decomp_step]


@functools.lru_cache(maxsize=32)
def _get_step_rules(
        gbd_round_id: int,
        decomp_step: str
) -> Dict[str, Optional[List[int]]]:
    """
    Retrieve all rules needed for CodCorrect in a given GBD round and step.

    Current relevant rules:
        * CAUSES_NEW_TO_ROUND - these causes' models will exist in iterative
        * EXEMPT_CAUSES_IN_ROUND - ^

    Returns:
        dictionary of rule results
    """
    manager = RulesManager.RulesManager(
        ResearchAreas.COD,
        Tools.CODEM,
        decomp_step_id=decomp_step_id_from_decomp_step(
            step=decomp_step, gbd_round_id=gbd_round_id
        )
    )
    rules_list = [
        Rules.CAUSES_NEW_TO_ROUND,
        Rules.EXEMPT_CAUSES_IN_ROUND
    ]
    return manager.get_rule_values(rules=rules_list, as_dict=True)

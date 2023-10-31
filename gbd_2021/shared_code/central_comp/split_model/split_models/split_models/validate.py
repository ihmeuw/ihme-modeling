from typing import Dict, List, Union

import pandas as pd

from db_tools import ezfuncs
from gbd import decomp_step as gbd_decomp_step
from rules.RulesManager import RulesManager
from rules.enums import ResearchAreas, Rules, Tools

from split_models.exceptions import (
    IllegalSplitCoDArgument, IllegalSplitEpiArgument
)


def is_valid_id(cause_id, valid_ids):
    return cause_id in valid_ids


def validate_ids(cause_id, valid_ids, usage):
    if isinstance(cause_id, list):
        if not all(is_valid_id(c_id, valid_ids) for c_id in cause_id) or (
                len(cause_id) == 0):
            bad_ids = [
                c_id for c_id in cause_id if not is_valid_id(c_id, valid_ids)
            ]
            raise IllegalSplitCoDArgument(
                "Invalid id(s) found in {usage}. Invalid ids: {bad_ids}"
                .format(usage=usage, bad_ids=', '.join(map(str, bad_ids)))
            )
    else:
        if not is_valid_id(cause_id, valid_ids):
            raise IllegalSplitCoDArgument(
                "{usage}: {c_id} is not valid."
                .format(c_id=cause_id, usage=usage)
            )


def validate_decomp_step_input(decomp_step, gbd_round_id):
    """
    Validates the decomp_step against valid values, distinguished by gbd_round.

    Arguments:
        decomp_step (str)
        gbd_round_id (int)

    Raises:
        ValueError if the decomp_step is invalid.

    NOTE: We use research area "EPI" because all calls to split models (both
    cod and epi) involve proportion modelable entities.
    """
    if gbd_round_id < 6:
        gbd_decomp_step.validate_decomp_step(step=decomp_step, gbd_round_id=gbd_round_id)
    else:
        rules_manager = RulesManager(
            research_area=ResearchAreas.EPI,
            tool=Tools.SHARED_FUNCTIONS,
            decomp_step=decomp_step,
            gbd_round_id=gbd_round_id
        )
        if not rules_manager.get_rule_value(Rules.STEP_VIEWABLE):
            raise ValueError(
                f"decomp_step {decomp_step} is not current valid for "
                f"gbd_round_id {gbd_round_id} at this time."
            )


def validate_measure_id(measure_id):
    if not isinstance(measure_id, int):
        raise IllegalSplitEpiArgument(
            "Measure_id must be an integer. Received type: {t}"
            .format(t=type(measure_id))
        )


def validate_meids(meids, id_type):
    if isinstance(meids, list):
        if not all(isinstance(meid, int) for meid in meids) or (
                len(meids) == 0
        ):
            raise IllegalSplitEpiArgument(
                "{id_type} modelable_entity_id must all be integers. Received: {bad}"
                .format(id_type=id_type, bad=meids)
            )
    else:
        raise IllegalSplitEpiArgument(
            "{id_type} meids must be a list of integers. Received: {bad}"
            .format(id_type=id_type, bad=meids)
        )


def validate_requested_measures(requested_measures, existing_measures):
    """Ensure that we don't pass a requested split measure that doesn't exist
    in the source draws."""
    existing_measures = [int(meas) for meas in existing_measures]
    requested_measures = [int(meas) for meas in requested_measures]
    for measure in requested_measures:
        if measure not in existing_measures:
            raise ValueError(
                "Requested measure_id, {measure}, does not exist in source "
                "draws. Available measures are: {available}".format(
                    measure=measure,
                    available=', '.join(
                        [str(meas) for meas in existing_measures]
                    )
                )
            )


def validate_source_meid(meid):
    if not isinstance(meid, int):
        raise IllegalSplitEpiArgument(
            "Source modelable_entity_id must be an integer. Received type: {t}"
            .format(t=type(meid))
        )


def validate_split_measure_ids(measure_id):
    if isinstance(measure_id, list):
        if not all(isinstance(m_id, int) for m_id in measure_id) or (
                len(measure_id) == 0
        ):
            raise IllegalSplitEpiArgument(
                "Split measure_ids must all be integers. Received: {bad}"
                .format(bad=measure_id)
            )
    else:
        validate_measure_id(measure_id)


def validate_meid_decomp_step_mapping(
    meids: List[int],
    gbd_round_id: int,
    meid_decomp_steps: Union[List[str], str],
    source: str
) -> Dict[int, str]:
    """Validates given meids are mapped to valid decomp_steps.

    Returns:
        dictionary mapping modelable_entity_ids to the decomp_steps we
        want to pass to chronos.interpolate.

    Arguments:
        meids: List[int]: target propotion me_ids to interpolate
        gbd_round_id: int: argument from split_cod_model
        meid_decomp_steps: Union[List[str], str]:
            decomp_step(s) describing the location of given meids.
            If str, will be constructed as follows:
                meid_decomp_steps = [meid_decomp_steps for _ in meids]
        source: str: Either 'cod' or 'epi'. Determines which error
            to raise.

    Raises:
        ValueError: if meid_decomp_steps a list and not of length len(meids)
        IllegalSplitCoDArgument / IllegalSplitEpiArgument: if me_id doesn't
            have a best model for its given decomp_step.
    """
    if isinstance(meid_decomp_steps, str):
        meid_decomp_steps = [meid_decomp_steps for _ in meids]

    meid_decomp_step_mapping = dict(zip(meids, meid_decomp_steps))
    models: pd.DataFrame = ezfuncs.query(
        (
            "SELECT modelable_entity_id, decomp_step_id "
            "FROM epi.model_version "
            "WHERE modelable_entity_id IN :meids "
            "AND model_version_status_id = 1"
        ),
        parameters={'meids': meids},
        conn_def="CONN_DEF"
    ).set_index('modelable_entity_id')

    for me_id in meid_decomp_step_mapping:
        decomp_step_id = gbd_decomp_step.decomp_step_id_from_decomp_step(  # noqa: F841
            meid_decomp_step_mapping[me_id], gbd_round_id
        )
        best_decomp_models = models.query(
            'decomp_step_id == @decomp_step_id and modelable_entity_id == @me_id'
        )
        if best_decomp_models.empty:
            relevantException = (
                IllegalSplitCoDArgument if source == 'cod' else IllegalSplitEpiArgument
            )
            raise relevantException(
                f'modelable_entity_id {me_id} does not have a best model in '
                f'decomp_step {meid_decomp_step_mapping[me_id]}, gbd_round_id '
                f'{gbd_round_id}'
            )

    return meid_decomp_step_mapping


def validate_target_meid_decomp_steps(
    target_meid_decomp_step: Union[List[str], str],
    gbd_round_id: int,
    source: str
) -> None:
    """Validate a (list of) proportion's me_id_decomp_step(s)."""
    if isinstance(target_meid_decomp_step, str):
        validate_decomp_step_input(target_meid_decomp_step, gbd_round_id)
    elif isinstance(target_meid_decomp_step, list):
        for decomp_step in set(target_meid_decomp_step):
            validate_decomp_step_input(decomp_step, gbd_round_id)
    else:
        relevantException = (
            IllegalSplitCoDArgument if source == 'cod' else IllegalSplitEpiArgument
        )
        raise relevantException(
            f'target_meid_decomp_step must be either a list of strings, or a string. '
            f'recieved target_meid_decomp_step of type {type(target_meid_decomp_step)}.'
        )

from gbd.decomp_step import validate_decomp_step
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
        validate_decomp_step(step=decomp_step, gbd_round_id=gbd_round_id)
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

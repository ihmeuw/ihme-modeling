from fauxcorrect.parameters.master import MachineParameters
from fauxcorrect.utils.constants import Columns

def validate_cause_hierarchies(version: MachineParameters
) -> None:
    """Checks that causes which are not most detailed are
    exclusive to only one of the aggregation cause hierarchies.

    Arguments:
        version (MachineParameters): master parameters for
            the given machine run.
    Raises:
        ValueError: If an aggregate cause is found in more
        than one of the cause hierarchies passed into the
        master parameter set.
    """
    super_set = set()
    for cause_set_id in version.CAUSE_AGGREGATION_SET_IDS:
        cause_hierarchy = version._cause_parameters[cause_set_id].hierarchy
        aggregate_causes = set(
            cause_hierarchy.loc[
            cause_hierarchy[Columns.MOST_DETAILED] != 1]['cause_id'].tolist())
        overlap = super_set.intersection(aggregate_causes)
        if overlap:
            other_cause_sets = [cs for cs in version.CAUSE_AGGREGATION_SET_IDS
                                if cs != cause_set_id]
            raise ValueError(f"Cause_set_id {cause_set_id} contains aggregate causes {overlap}"" "
                             f"which is also present in one or more of "
                             f"cause_sets {other_cause_sets}")
        super_set = super_set.union(aggregate_causes)

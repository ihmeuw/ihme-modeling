from sqlalchemy import orm

from db_queries.api.internal import get_modelable_entity_metadata
from gbd import constants as gbd_constants


def is_pregnancy_modelable_entity(modelable_entity_id: int, session: orm.Session) -> bool:
    """Pulls metadata for a modelable entity ID and returns whether it is a pregnancy ME.

    Args:
        modelable_entity_id: ID of the modelable entity
        session: active session with a modelable entity database clone

    Returns:
        bool, whether ME is a pregnancy ME
    """
    me_metadata = get_modelable_entity_metadata(
        modelable_entity_id=modelable_entity_id, session=session
    )
    population_group_id = me_metadata.get(gbd_constants.me_metadata_type.POPULATION_GROUP_ID)

    return (
        int(population_group_id) == gbd_constants.population_group.PREGNANT
        if population_group_id
        else False
    )

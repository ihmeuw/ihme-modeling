from codem.reference import paths
from db_tools import ezfuncs

from hybridizer.hybridizer import Hybridizer
from hybridizer.metadata import hybrid_metadata


def test_metadata(user, global_model_version_id,
                  datarich_model_version_id, conn_def):
    model = hybrid_metadata(user, global_model_version_id,
                            datarich_model_version_id,
                            conn_def=conn_def)
    assert type(model) is int
    with ezfuncs.session_scope(conn_def) as session:
        session.execute(
            """
            UPDATE cod.model_version
            SET status = 3
            WHERE model_version_id = :model_version_id
            """,
            params={'model_version_id': model}
        )


def test_hybridizer_init(model_version_id, global_model_version_id,
                         datarich_model_version_id, conn_def):
    base_dir = paths.get_base_dir(model_version_id=model_version_id,
                                  conn_def=conn_def)
    h = Hybridizer(model_version_id,
                   global_model=global_model_version_id,
                   datarich_model=datarich_model_version_id,
                   conn_def=conn_def,
                   base_dir=base_dir)
    assert h.cause_id == 468
    assert h.gbd_round_id == 6



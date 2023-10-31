from hybridizer.emails import get_modeler


def test_modeler(gbd_round_id, conn_def):
    for acause in ['maternal', 'cvd_ihd']:
        modeler = get_modeler(acause, gbd_round_id,
                              conn_def)
        assert type(modeler) is list


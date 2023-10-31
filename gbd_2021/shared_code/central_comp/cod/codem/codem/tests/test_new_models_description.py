import datetime as date
import pytest

from codem.data.parameters import get_model_parameters
from codem.joblaunch.run_utils import new_models, change_model_status


# @pytest.fixture
# def params(model_version_id, db_connection):
#     return get_model_parameters(model_version_id, db_connection)


model_1 = 621839
model_2 = 625850
conn_def = 'ADDRESS'
model_params_1 = get_model_parameters(model_1, conn_def)
model_params_2 = get_model_parameters(model_2, conn_def)


def test_wrong_type_param():
    with pytest.raises(TypeError, match="use_new_desc parameter must be "
                                        "either True or False"):
        models = new_models([model_2],
                            model_params_2['gbd_round_id'],
                            model_params_2['decomp_step_id'],
                            conn_def,
                            use_new_desc="False")


def test_new_desc_success(db_connection):
    models = new_models([model_1, model_2],
                        model_params_1['gbd_round_id'],
                        model_params_1['decomp_step_id'],
                        conn_def,
                        use_new_desc=False)
    new_model_params_1 = get_model_parameters(models[0], conn_def)
    new_model_params_2 = get_model_parameters(models[1], conn_def)
    try:
        assert(new_model_params_1['description'] ==
               model_params_1['description'] +
               ', relaunch of model on ' + str(date.date.today()))
        assert(new_model_params_2['description'] ==
               model_params_2['description'] +
               ', relaunch of model on ' + str(date.date.today()))
    finally:
        for model in models:
            change_model_status(model,
                                status=3,
                                db_connection=db_connection)


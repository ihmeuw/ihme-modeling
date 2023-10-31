import pytest
import pandas as pd
import numpy as np

from gbd import constants as gbd

from codem.data.demographics import get_mortality_data
from codem.data.query import get_cod_data, exists_in_outlier_history
from codem.data.parameters import get_model_parameters
from codem.data.query import get_codem_data
from codem.data.query import exclude_regions
from codem.joblaunch.run_utils import excluded_location_string
from codem.data.parameters import get_best_model_version
from codem.data.parameters import get_outlier_model_version_id
from codem.data.parameters import check_if_new_cause
from codem.data.parameters import get_outlier_decomp_step_id
from codem.data.query import get_codem_input_data
from codem.joblaunch.run_utils import new_models


@pytest.fixture
def params(model_version_id, db_connection):
    return get_model_parameters(model_version_id, db_connection)


def test_cod_data(params, model_version_id, db_connection):
    df = get_cod_data(
        cause_id=params['cause_id'],
        sex=params['sex_id'],
        start_year=2010,
        start_age=params['age_start'],
        end_age=params['age_end'],
        location_set_version_id=params['location_set_version_id'],
        db_connection=db_connection,
        model_version_id=model_version_id,
        gbd_round_id=params['gbd_round_id'],
        outlier_decomp_step_id=params['outlier_decomp_step_id'],
        outlier_model_version_id=[model_version_id],
        refresh_id=params['refresh_id'])
    assert df.refresh_id.unique() == [params['refresh_id']]
    assert len(df) > 0
    assert 'gc_var_lt_cf' in df.columns
    assert 'gc_var_ln_rate' in df.columns
    assert df.gc_var_lt_cf.max() > 0
    assert df.gc_var_ln_rate.max() > 0
    assert len(df.sex.unique()) == 1
    assert not df.duplicated(subset=['age', 'location_id', 'year',
                                     'source_type']).any()

@pytest.mark.filterwarnings("ignore::UserWarning",
                            match="Both covariate_id and model_version_id")
def test_same_data():
    params = get_model_parameters(model_version_id=632843, update=False,
                                  db_connection='ADDRESS')
    assert params['outlier_model_version_id'] == [632843]
    assert params['outlier_decomp_step_id'] == 4
    df = get_codem_input_data(model_parameters=params)[0]

    assert np.isclose(df.loc[(df.location_id == 101) & (df.age == 3) & (
            df.year == 2010
    )]['cf'].values[0], 0.02743755)
    assert np.isclose(df.loc[(df.location_id == 101) & (df.age == 3) & (
            df.year == 2010
    )]['sample_size'].values[0], 161.36804)
    assert len(df) == 79228
    assert len(df.loc[~df.cf.isnull()]) == 30794


def test_cod_data_fail(params, model_version_id, db_connection):
    with(pytest.raises(TypeError)):
        get_cod_data(
            cause_id=params['cause_id'],
            sex=params['sex_id'],
            start_year=params['start_year'],
            start_age=params['age_start'],
            end_age=params['age_end'],
            location_set_version_id=params['location_set_version_id'],
            db_connection=db_connection,
            model_version_id=model_version_id,
            gbd_round_id=params['gbd_round_id'],
            outlier_model_version_id=model_version_id,
            outlier_decomp_step_id=params['outlier_decomp_step_id']
        )


def test_params(params):
    assert params['decomp_step_id'] == 4
    assert params['outlier_decomp_step_id'] == 4
    assert params['model_version_id'] == params['outlier_model_version_id']
    assert params['refresh_id'] == 36
    assert params['cause_id'] == 686
    assert params['gbd_round_id'] == 6
    assert params['gbd_round'] == 2019
    assert params['location_set_version_id'] == 517
    assert params['sex_id'] == gbd.sex.FEMALE
    assert params['age_start'] == 3
    assert params['age_end'] == 4
    assert params['run_covariate_selection'] == 0
    assert params['holdout_number'] == 20
    assert params['holdout_ensemble_prop'] == 0.15
    assert params['holdout_submodel_prop'] == 0.15
    assert params['linear_floor_rate'] == 0.0001
    assert params['time_weight_method'] == "tricubic"
    assert params['lambda_time_smooth'] == 0.5
    assert params['lambda_time_smooth_nodata'] == 2
    assert params['zeta_space_smooth'] == 0.99
    assert params['zeta_space_smooth_nodata'] == 0.7
    assert params['omega_age_smooth'] == 1.0
    assert params['gpr_year_corr'] == 10
    assert params['psi_weight_max'] == 5.0
    assert params['psi_weight_min'] == 1
    assert params['psi_weight_int'] == 0.5
    assert params['rmse_window'] == 5
    assert params['envelope_proc_version_id'] == 11280
    assert params['population_proc_version_id'] == 11276
    assert params['amplitude_scalar'] == 1


def test_mortality_query_2(params, db_connection):
    df = get_mortality_data(sex=params['sex_id'], start_year=2017,
                            start_age=params['age_start'],
                            end_age=params['age_end'],
                            location_set_version_id=(
                                params['location_set_version_id']
                            ),
                            gbd_round=params['gbd_round'],
                            db_connection=db_connection,
                            decomp_step_id=4,
                            gbd_round_id=params['gbd_round_id'],
                            env_run_id=params['env_run_id'],
                            pop_run_id=params['pop_run_id'],
                            standard_location_set_version_id=(
                                params['standard_location_set_version_id'])
                            )
    assert df.weight.max() == 1
    assert df.weight.min() > 0
    assert df.weight.min() < 1
    assert df.loc[df.location_id == 101]['weight'].min() == 1.0
    assert df.loc[df.location_id == 101]['weight'].max() == 1.0
    assert df.loc[df.location_id == 4726]['weight'].max() < 1.0

    andhra_pradesh = df.loc[df.location_id.isin([43908, 43872])].copy()
    andhra_pradesh = andhra_pradesh.groupby(['age', 'year', 'sex'])['weight'].sum()
    assert all(andhra_pradesh == 2)

    assert all([x in df.columns for x in ['location_id', 'age', 'year',
                'sex', 'envelope', 'pop', 'weight']])
    assert np.isclose(df.loc[(df.location_id == 101) & (df.year == 2017) &
                  (df.sex == gbd.sex.FEMALE) & (df.age == 3), 'pop'], 10549.90406, atol=0.000001)
    assert np.isclose(df.loc[(df.location_id == 101) & (df.year == 2017) &
                  (df.sex == gbd.sex.FEMALE) & (df.age == 3), 'envelope'], 86.403868, atol=0.000001)

    assert (df['year'].unique().tolist() == (
        list(range(2017, params['gbd_round'] + 1)))
    )


def test_mortality_query_3(params, db_connection):
    df = get_mortality_data(sex=params['sex_id'], start_year=2017,
                            start_age=params['age_start'],
                            end_age=params['age_end'],
                            location_set_version_id=params['location_set_version_id'],
                            gbd_round=params['gbd_round'], db_connection=db_connection,
                            decomp_step_id=4,
                            gbd_round_id=params['gbd_round_id'],
                            env_run_id=params['env_run_id'],
                            pop_run_id=params['pop_run_id'],
                            standard_location_set_version_id=458)
    assert df.weight.max() == 1
    assert df.weight.min() > 0
    assert df.weight.min() < 1
    assert df.loc[df.location_id == 101]['weight'].min() == 1.0
    assert df.loc[df.location_id == 101]['weight'].max() == 1.0
    assert df.loc[df.location_id == 4726]['weight'].max() < 1.0

    andhra_pradesh = df.loc[df.location_id.isin([43908, 43872])].copy()
    andhra_pradesh = andhra_pradesh.groupby(['age', 'year', 'sex'])['weight'].sum()
    assert np.isclose(andhra_pradesh, 1).all()


def test_exclude_locations_from_df():
    df = pd.DataFrame({'level_3': [6, 7, 101, 102, 51],
                       'level_4': [None, None, None, None, None],
                       'location_id': [1, 1, 1, 1, 1],
                       'level_1': [1, 1, 1, 1, 1],
                       'level_2': [1, 1, 1, 1, 1]})
    df = exclude_regions(df,
                         regions_exclude=excluded_location_string(
                             refresh_id=29, db_connection='ADDRESS')
                         )
    assert 51 in df.level_3.tolist()
    assert 6 not in df.level_3.tolist()


def test_codem_data(params, db_connection):
    df = get_codem_data(
        cause_id=params['cause_id'],
        sex=params['sex_id'],
        start_year=params['start_year'],
        start_age=params['age_start'], end_age=params['age_end'],
        location_set_version_id=params['location_set_version_id'],
        regions_exclude=params['locations_exclude'],
        decomp_step_id=params['decomp_step_id'],
        outlier_decomp_step_id=params['outlier_decomp_step_id'],
        refresh_id=params['refresh_id'],
        db_connection=db_connection,
        model_version_id=params['model_version_id'],
        gbd_round_id=params['gbd_round_id'],
        env_run_id=params['env_run_id'],
        pop_run_id=params['pop_run_id'],
        gbd_round=params['gbd_round'],
        outlier_model_version_id=params['outlier_model_version_id'],
        standard_location_set_version_id=params['standard_location_set_version_id']
    )
    assert not df.empty


def test_best_model_version():
    model_version = get_best_model_version(
        cause_id=980, sex_id=gbd.sex.FEMALE,
        gbd_round_id=6, decomp_step_id=4, model_version_type=2,
        db_connection='ADDRESS')
    assert model_version == [631070]


def test_outlier_model_version_id():
    model_version = get_outlier_model_version_id(
        model_version_id=0,
        decomp_step=gbd.decomp_step.FOUR,
        outlier_decomp_step_id=4,
        gbd_round_id=6,
        cause_id=980,
        sex_id=gbd.sex.FEMALE,
        model_version_type=2,
        db_connection='ADDRESS'
    )
    assert [0] == model_version
    model_version = get_outlier_model_version_id(
        model_version_id=0,
        decomp_step=gbd.decomp_step.THREE,
        outlier_decomp_step_id=4,
        gbd_round_id=6,
        cause_id=980, sex_id=gbd.sex.FEMALE,
        model_version_type=2,
        db_connection='ADDRESS'
    )
    assert [631070] == model_version


def test_iterative_outlier_model_version_id():
    model_version = get_outlier_model_version_id(
        model_version_id=0,
        decomp_step=gbd.decomp_step.ITERATIVE,
        outlier_decomp_step_id=1,
        gbd_round_id=6,
        cause_id=980,
        sex_id=gbd.sex.FEMALE,
        model_version_type=2,
        db_connection='ADDRESS'
    )
    assert [0] == model_version


def tests_outlier_history():
    assert not exists_in_outlier_history(0, 'ADDRESS')
    assert exists_in_outlier_history(608057, 'ADDRESS')


def check_new_cause_for_methods_changes():
    assert not check_if_new_cause(366, 6, 'ADDRESS')
    assert check_if_new_cause(401, 6, 'ADDRESS')


def check_outlier_decomp_step_id():
    assert get_outlier_decomp_step_id(
        decomp_step_id=3,
        decomp_step=gbd.decomp_step.THREE,
        iterative_data_decomp_step_id=None,
        db_connection='ADDRESS'
    ) == 2
    assert get_outlier_decomp_step_id(
        decomp_step_id=7,
        decomp_step=gbd.decomp_step.ITERATIVE,
        iterative_data_decomp_step_id=3,
        db_connection='ADDRESS'
    ) == 2
    assert get_outlier_decomp_step_id(
        decomp_step_id=2,
        decomp_step=gbd.decomp_step.TWO,
        iterative_data_decomp_step_id=None,
        db_connection='ADDRESS'
    ) == 2


def test_data_variance(model_version_id, db_connection):
    pass

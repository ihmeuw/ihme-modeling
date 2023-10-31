import pytest
import numpy as np
import pandas as pd

from gbd import constants as gbd

from codem.joblaunch.run_utils import new_models, set_rerun_models
from codem.joblaunch.run_utils import new_covariate_df, get_old_covariates
from codem.devQueries.best_query import get_feeders as best_query
from codem.joblaunch.batch_launch import (check_model_attribute,
                                          check_sex_restrictions)
from codem.joblaunch.run_utils import (change_model_status,
                                       get_excluded_locations,
                                       get_refresh_id)


def test_refresh_id():
    assert get_refresh_id(decomp_step_id=1, db_connection='ADDRESS') == 29


def test_locations_exclude():
    df = pd.DataFrame({'model_version_id': [517460, 555926],
                       'model_version_type_id': [1, 2],
                       'locations_exclude': ['', '']})
    df = get_excluded_locations(df, refresh_id=33,
                                db_connection='ADDRESS')
    assert df.iloc[0] == ''
    assert df.iloc[1] == '6 7 10 11 12 13 14 15 16 17 ' \
                         '18 19 20 22 23 24 25 26 27 28 ' \
                         '29 30 34 38 39 43 44 49 50 53 ' \
                         '54 66 68 74 77 111 114 121 123 ' \
                         '127 129 131 136 139 140 141 ' \
                         '142 143 144 146 147 148 149 ' \
                         '150 151 152 153 154 155 156 ' \
                         '157 160 161 162 163 164 165 ' \
                         '168 169 170 171 172 173 175 ' \
                         '176 177 178 179 180 181 182 ' \
                         '184 185 186 187 189 190 191 ' \
                         '193 194 195 196 197 198 200 ' \
                         '201 202 203 204 205 206 207 ' \
                         '208 209 210 211 212 213 214 ' \
                         '215 216 217 218 298 320 349 ' \
                         '351 367 369 374 376 380 396 ' \
                         '413 416 435 522'


def test_locations_exclude_wrong_refresh_still_same_locations():
    df = pd.DataFrame({'model_version_id': [517460, 555926],
                       'model_version_type_id': [1, 2],
                       'locations_exclude': ['', '']})
    df = get_excluded_locations(df, refresh_id=32,
                                db_connection='ADDRESS')
    assert df.iloc[0] == ''
    assert df.iloc[1] == '6 7 10 11 12 13 14 15 16 17 ' \
                         '18 19 20 22 23 24 25 26 27 28 ' \
                         '29 30 34 38 39 43 44 49 50 53 ' \
                         '54 66 68 74 77 111 114 121 123 ' \
                         '127 129 131 136 139 140 141 ' \
                         '142 143 144 146 147 148 149 ' \
                         '150 151 152 153 154 155 156 ' \
                         '157 160 161 162 163 164 165 ' \
                         '168 169 170 171 172 173 175 ' \
                         '176 177 178 179 180 181 182 ' \
                         '184 185 186 187 189 190 191 ' \
                         '193 194 195 196 197 198 200 ' \
                         '201 202 203 204 205 206 207 ' \
                         '208 209 210 211 212 213 214 ' \
                         '215 216 217 218 298 320 349 ' \
                         '351 367 369 374 376 380 396 ' \
                         '413 416 435 522'


def test_set_rerun_models(model_version_id, gbd_round_id, db_connection,
                          description):
    model = new_models([model_version_id],
                       gbd_round_id=gbd_round_id,
                       db_connection=db_connection,
                       desc=description,
                       run_covariate_selection=1,
                       decomp_step_id=4)
    try:
        assert len(model) == 1
        assert type(model[0]) is int
    finally:
        change_model_status(model_version_id=model[0],
                            status=3,
                            db_connection=db_connection)


def test_model_query(db_connection):
    df = best_query(gbd_round_id=5, decomp_step_id=9,
                    cause_id=686,
                    db_connection=db_connection)
    assert len(df) == 4
    assert len(df.model_version_type_id.unique()) == 2
    assert len(df.sex_id.unique()) == gbd.sex.FEMALE
    assert 484382 in df.child_id.tolist()


def test_single_query(db_connection):
    df = best_query(gbd_round_id=5, decomp_step_id=9,
                    cause_id=686,
                    sex_id=gbd.sex.MALE, age_range=[3, 4],
                    model_version_type_id=1,
                    db_connection=db_connection)
    assert len(df) == 1
    assert type(df['child_id'].iloc[0]) is np.int64


def test_model_check_fail(model_version_id, db_connection):
    with pytest.raises(Exception):
        check_model_attribute(model_version_id, 'cause_id', 689,
                              db_connection=db_connection)


def test_model_check_succeed(model_version_id, db_connection):
    check_model_attribute(model_version_id, 'cause_id', 686,
                          db_connection=db_connection)


def test_sex_restrictions():
    assert check_sex_restrictions(603, 2, 4, 6) is True
    assert check_sex_restrictions(603, 1, 4, 6) is False


def test_add_covariates(db_connection, description):
    model_version_id = 620906
    model = set_rerun_models(model_version_id, gbd_round_id=6, decomp_step_id=4,
                             db_connection=db_connection,
                             desc=description)
    try:
        assert len(model) == 1
        assert type(model[0]) is int
    finally:
        change_model_status(model_version_id=model[0],
                            status=3,
                            db_connection=db_connection)
    # We don't need the model to be status = 0 in order to upload the covariates.
    old = get_old_covariates(model_version_id, db_connection,
                             gbd_round_id=6, decomp_step_id=4)
    add_covariates = {581: {'direction': 1}, 1099: {'direction': 1}}
    df = new_covariate_df(old_model_version_id=model_version_id,
                          new_model_version_id=model[0],
                          db_connection=db_connection,
                          gbd_round_id=6,
                          decomp_step_id=4)
    df_add = new_covariate_df(old_model_version_id=model_version_id,
                              new_model_version_id=model[0],
                              db_connection=db_connection,
                              gbd_round_id=6,
                              decomp_step_id=4,
                              add_covs=add_covariates)
    df = df.sort_values(['covariate_model_version_id'])
    df_add = df_add.sort_values(['covariate_model_version_id'])
    assert not df_add['lag'].notnull().all()
    assert not df_add['selected'].notnull().all()
    assert not df_add['offset'].notnull().all()
    assert (df_add['p_value'] == 0.05).all()
    assert len(df) == len(old)
    assert len(df) + 1 == len(df_add)

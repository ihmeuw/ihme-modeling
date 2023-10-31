# -*- coding: utf-8 -*-
"""
    test_utils
    ~~~~~~~~~~
    Test `utils` module of `sfma` package.
"""
import numpy as np
import pandas as pd
import pytest
from mrtool import utils


@pytest.mark.parametrize('df', [pd.DataFrame({'alpha': np.ones(5),
                                              'beta': np.zeros(5)})])
@pytest.mark.parametrize(('cols', 'col_shape'),
                         [('alpha', (5,)),
                          ('beta', (5,)),
                          (['alpha'], (5, 1)),
                          (['beta'], (5, 1)),
                          (['alpha', 'beta'], (5, 2)),
                          (None, (5, 0))])
def test_get_cols(df, cols, col_shape):
    col = utils.get_cols(df, cols)
    assert col.shape == col_shape


@pytest.mark.parametrize(('cols', 'ok'),
                         [('col0', True),
                          (['col0', 'col1'], True),
                          ([], True),
                          (None, True),
                          (1, False)])
def test_is_cols(cols, ok):
    assert ok == utils.is_cols(cols)


@pytest.mark.parametrize('cols', [None, 'col0', ['col0', 'col1']])
@pytest.mark.parametrize('default', [None, 'col0', ['col0', 'col1']])
def test_input_cols_default(cols, default):
    result_cols = utils.input_cols(cols, default=default)
    if cols is None:
        assert result_cols == [] if default is None else default
    else:
        assert result_cols == cols


@pytest.mark.parametrize('cols', [None, 'col0', ['col0', 'col1']])
@pytest.mark.parametrize('full_cols', [None, ['col2']])
def test_input_cols_append_to(cols, full_cols):
    cols = utils.input_cols(cols, append_to=full_cols)
    if full_cols is not None and cols:
        assert 'col0' in full_cols and 'col2' in full_cols
        if isinstance(cols, list):
            assert 'col1' in full_cols


def test_sizes_to_indices(sizes, indices):
    my_indices = utils.sizes_to_indices(sizes)
    assert all([np.allclose(my_indices[i], indices[i])
                for i in range(len(sizes))])


@pytest.mark.parametrize('sizes', [np.array([1, 2, 3])])
@pytest.mark.parametrize('indices', [[np.arange(0, 1),
                                      np.arange(1, 3),
                                      np.arange(3, 6)]])
def test_sizes_to_indices(sizes, indices):
    my_indices = utils.sizes_to_indices(sizes)
    assert all([np.allclose(my_indices[i], indices[i])
                for i in range(len(sizes))])


@pytest.mark.parametrize(('prior', 'result'),
                         [(np.array([0.0, 1.0]), True),
                          (np.array([[0.0]*2, [1.0]*2]), True),
                          (np.array([0.0, -1.0]), False),
                          (np.array([[0.0]*2, [-1.0]*2]), False),
                          (None, True),
                          ('gaussian_prior', False)])
def test_is_gaussian_prior(prior, result):
    assert utils.is_gaussian_prior(prior) == result


@pytest.mark.parametrize(('prior', 'result'),
                         [(np.array([0.0, 1.0]), True),
                          (np.array([[0.0]*2, [1.0]*2]), True),
                          (np.array([0.0, -1.0]), False),
                          (np.array([[0.0]*2, [-1.0]*2]), False),
                          (None, True),
                          ('uniform_prior', False)])
def test_is_uniform_prior(prior, result):
    assert utils.is_uniform_prior(prior) == result


@pytest.mark.parametrize('obj', [1, 1.0, 'a', True, [1], [1.0], ['a'], [True]])
def test_to_list(obj):
    obj_list = utils.to_list(obj)
    if isinstance(obj, list):
        assert obj_list is obj
    else:
        assert isinstance(obj_list, list)

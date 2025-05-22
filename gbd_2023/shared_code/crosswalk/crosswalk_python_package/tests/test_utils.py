# -*- coding: utf-8 -*-
"""
    test_utils
    ~~~~~~~~~~
    Test `utils` module of `crosswalk` package.
"""
import numpy as np
import pytest
import crosswalk.utils as utils


@pytest.mark.parametrize("x",
                         [[1]*3,
                          np.ones(3),
                          np.arange(3),
                          np.zeros(3) + 0j,
                          np.array([np.nan, 0.0, 0.0]),
                          np.array([np.inf, 0.0, 0.0]),
                          np.array(['a', 'b', 'c'])])
@pytest.mark.parametrize("shape", [None, (3,), (4,)])
def test_is_numerical_array(x, shape):
    ok = utils.is_numerical_array(x, shape=shape)
    if (
            not isinstance(x, np.ndarray) or
            not np.issubdtype(x.dtype, np.number) or
            np.isnan(x).any() or
            np.isinf(x).any() or
            (shape is not None and shape != (3,))
    ):
        assert not ok
    else:
        assert ok


@pytest.mark.parametrize("sizes", [np.array([1, 2, 3])])
@pytest.mark.parametrize("indices", [[range(0, 1), range(1, 3), range(3, 6)]])
def test_sizes_to_indices(sizes, indices):
    my_indices = utils.sizes_to_indices(sizes)
    assert all([my_indices[i] == indices[i] for i in range(len(sizes))])


@pytest.mark.parametrize("x", [np.array([1, 1, 2, 2, 3])])
@pytest.mark.parametrize("num_x", [3])
@pytest.mark.parametrize("x_sizes", [np.array([2, 2, 1])])
@pytest.mark.parametrize("unique_x", [np.array([1, 2, 3])])
def test_array_structure(x, num_x, x_sizes, unique_x):
    my_num_x, my_x_sizes, my_unique_x = utils.array_structure(x)

    assert my_num_x == num_x
    assert (my_x_sizes == x_sizes).all()
    assert (my_unique_x == unique_x).all()


@pytest.mark.parametrize("input", [None, np.ones(1)])
@pytest.mark.parametrize("default", [np.zeros(1)])
def test_default_input(input, default):
    my_input = utils.default_input(input, default=default)
    if input is None:
        assert (my_input == 0.0).all()
    else:
        assert (my_input == 1.0).all()


@pytest.mark.parametrize("log_mean", [np.random.randn(5)])
@pytest.mark.parametrize("log_sd", [np.random.rand(5)])
def test_log_linear(log_mean, log_sd):
    linear_mean, linear_sd = utils.log_to_linear(log_mean, log_sd)
    my_log_mean, my_log_sd = utils.linear_to_log(linear_mean, linear_sd)

    assert np.allclose(log_mean, my_log_mean)
    assert np.allclose(log_sd, my_log_sd)


@pytest.mark.parametrize("logit_mean", [np.random.randn(5)])
@pytest.mark.parametrize("logit_sd", [np.random.rand(5)])
def test_logit_linear(logit_mean, logit_sd):
    linear_mean, linear_sd = utils.logit_to_linear(logit_mean, logit_sd)
    my_logit_mean, my_logit_sd = utils.linear_to_logit(linear_mean, linear_sd)

    assert np.allclose(logit_mean, my_logit_mean)
    assert np.allclose(logit_sd, my_logit_sd)

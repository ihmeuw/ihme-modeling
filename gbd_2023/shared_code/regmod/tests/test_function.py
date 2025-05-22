"""
Test function module
"""
import numpy as np
import pytest
from regmod.function import fun_dict


def ad_dfun(fun, x, eps=1e-16):
    return fun(x + eps*1j).imag/eps


@pytest.mark.parametrize("x", 0.1 + 0.8*np.random.rand(3))
@pytest.mark.parametrize("smooth_fun", fun_dict.values())
def test_smooth_fun(x, smooth_fun):
    assert np.isclose(smooth_fun.dfun(x), ad_dfun(smooth_fun.fun, x))
    assert np.isclose(smooth_fun.d2fun(x), ad_dfun(smooth_fun.dfun, x))
    assert np.isclose(x, smooth_fun.inv_fun(smooth_fun.fun(x)))

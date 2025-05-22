import pytest
from numpy.testing import assert_approx_equal

import pydisagg.transformations as tr

transformations = [
    tr.Log(),
    tr.LogModifiedOdds(1),
    tr.LogModifiedOdds(4),
    tr.LogOdds(),
]

x_values = [0.02, 0.01, 0.5, 0.98]


@pytest.mark.parametrize("T", transformations)
@pytest.mark.parametrize("x", x_values)
def test_inverse_consistency(T, x):
    assert_approx_equal(x, T.inverse(T(x)))


@pytest.mark.parametrize("T", transformations)
@pytest.mark.parametrize("x", x_values)
def test_approximate_derivative(T, x, h=0.001):
    """
    Sanity check that the derivatives are are close to correct with a
    finite difference. It would have been a good idea to just do
    everything with Jax from the start.
    """
    assert_approx_equal(T.diff(x), (T(x + h) - T(x - h)) / (2 * h), 2)

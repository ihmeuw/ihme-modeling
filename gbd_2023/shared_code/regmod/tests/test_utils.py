"""
Test utility module
"""
import numpy as np
import pytest
from xspline import XSpline
import regmod.utils as utils


@pytest.mark.parametrize("vec", [[1, 2, 3], (1, 2, 3)])
@pytest.mark.parametrize("size", [2, 3])
def test_check_size(vec, size):
    if len(vec) != size:
        with pytest.raises(AssertionError):
            utils.check_size(vec, size)
    else:
        utils.check_size(vec, size)


@pytest.mark.parametrize("vec", [1, [1, 2, 3], None])
@pytest.mark.parametrize("size", [3])
@pytest.mark.parametrize("default_value", [1.0])
def test_default_vec_factory(vec, size, default_value):
    vec = utils.default_vec_factory(vec, size, default_value)
    assert len(vec) == size


@pytest.mark.parametrize("knots", [np.linspace(0.0, 1.0, 5)])
@pytest.mark.parametrize("degree", [3])
@pytest.mark.parametrize("l_linear", [True, False])
@pytest.mark.parametrize("r_linear", [True, False])
@pytest.mark.parametrize("knots_type", ["rel_domain", "rel_freq", "abs"])
def test_spline_specs(knots, degree, l_linear, r_linear, knots_type):
    spline_specs = utils.SplineSpecs(knots,
                                     degree,
                                     l_linear,
                                     r_linear,
                                     knots_type=knots_type)

    vec = np.random.randn(100)
    spline = spline_specs.create_spline(vec)
    assert isinstance(spline, XSpline)
    assert spline.num_spline_bases == spline_specs.num_spline_bases
    if knots_type.startswith("rel"):
        assert np.isclose(spline.knots[0], vec.min())
        assert np.isclose(spline.knots[-1], vec.max())
    else:
        assert np.allclose(spline.knots, spline_specs.knots)


@pytest.mark.parametrize("sizes", [[1, 2, 3]])
def test_sizes_to_slices(sizes):
    slices = utils.sizes_to_slices(sizes)
    assert slices[0] == slice(0, 1)
    assert slices[1] == slice(1, 3)
    assert slices[2] == slice(3, 6)

r"""Das Gupta describes a technique for decomposition of additive functions.

Given a mapping :math:`\phi(\vec{x}) = \phi(\sum_c x_c)` and two vectors
:math:`\vec{x}` and :math:`\vec{y}`, Das Gupta defines the contribution to the
difference :math:`\delta:=\phi(\vec{y}) - \phi(\vec{x})` due to the change
from :math:`x_c \mapsto y_c` to be:

.. math::
    \delta_{c} = \delta \cdot \frac{y_c - x_c}{\sum_c y_c - \sum_c x_c}

It is important to note that if :math:`y_c - x_c = 0` then there won't be any
contribution :math:`\delta_c`, which is expected. However, if :math:`\sum_c y_c
- \sum_c x_c = 0`, then that does not necessarily mean that :math:`\delta_c=0`;
  it means that this method cannot be used to determine the decomposition.

How does this relate to life expectancy decompositions? If :math:`\vec{u}` is
cause specific mortality, and :math:`\phi` is taken to be life expectancy
function, then :math:`\phi(\vec{u}) = \phi(\sum_c u_c)`, since :math:`\sum_c
u_c` is all cause mortality.


Instability in calculation
--------------------------

Due to the denominator in Das Gupta's equation
(:math:`\sum_c y_c - \sum_cx_c`), there is an instability in the equation when
the difference is too small. There are two cases where the difference is too
small:

1. Dividing by zero
2. The difference in age-sex-location total mortality rates between two
   different populations are smaller than the one person if measured in number
   space (e.g. 1e-6, 1e-7).

In the first case, using Das Gupta will result in infinity or negative
infinity. In the second case, using Das Gupta might result in totally
unreasonable decomposition components.

Our solution is to revert the calculation to a very simple calculation:
:math:`\delta \frac{x_c}{\sum_c x_c}`. There are *two* populations to consider
(x and y), so which one do we choose to use in this calculation? I guess we
*could* do it twice and take the mean. That's not what we do. Our use-case in
life expectancy decompositions is to decompose life expectancy between a past
year and a forecasted year. Therefore, we use the past data in the defaulting
calculation so that the default behavior doesn't change as the forecasts
change.
"""
import xarray as xr


EPSILON = 1e-5


def single_additive(delta, c_1, c_2, summed_1, summed_2):
    r"""Compute the contribution for a single additive component `c`.

    Args:
        delta:
            The difference being decomposed, :math:`\phi(y) - \phi(x)`
        c_1:
            The additive component `c` for `x`, :math:`x_c`
        c_2:
            The additive component `c` for `y`, :math:`y_c`
        summed_1:
            The sum of all of the components of `x`, :math:`\sum_c x_c`
        summed_2:
            The sum of all of the components of `y`, :math:`\sum_c y_c`

    Returns:
        The contribution of the additive component `c` to `delta`,
        :math:`\delta_c`
    """
    diff = summed_2 - summed_1
    normal_result = delta * (c_2 - c_1) / diff
    too_small = xr.ufuncs.fabs(diff) < EPSILON
    if not too_small.any():
        return normal_result
    else:
        default_result = delta * c_1 / summed_1
        return default_result.where(too_small).combine_first(normal_result)


def array_additive(delta, vec_1, vec_2, summed_1=None,
                   summed_2=None, decomp_dim=None):
    r"""Vectorized Das Gupta additive decomposition.

    Similar to `single_additive`, except the `vec_i` arguments are
    vectorized with respect to the decomposition dimension, which means the
    contributions to the decomposition for each additive component can be
    computed at the same time.

    If the sums are precomputed, they can be passed in. Otherwise, the vectors
    can be summed across the decomposition dimension to compute the sum.

    Args:
        delta (scalar | xr.DataArray):
            The difference being decomposed :math:`\phi(y) - \phi(x)`. This is
            a scalar, or a data array. If it is a data array, it needs to have
            the same dimensions as all the arguments except the decomp
            dimension.
        vec_1 (xr.DataArray):
            The vector `x`. This must have the decomp dimension.
        vec_2 (xr.DataArray):
            The vector `y`. This must have the decomp dimension.
        summed_1 (xr.DataArray):
            The value `x` summed across the decomp dimension.
        summed_2 (xr.DataArray):
            The value `y` summed across the decomp dimension.
        decomp_dim (str):
            The name of the dimension that represents how `delta` is being
            decomposed. `delta` must not have this dimension, and all other
            data arrays must have this dimension.

    Returns:
        xr.DataArray:
            :math:`\delta_c` for all `c`.
    """
    summed_1 = summed_1 or vec_1.sum(decomp_dim)
    summed_2 = summed_2 or vec_2.sum(decomp_dim)
    summed_diff = summed_2 - summed_1
    vec_diff = vec_2 - vec_1

    return delta * vec_diff / summed_diff

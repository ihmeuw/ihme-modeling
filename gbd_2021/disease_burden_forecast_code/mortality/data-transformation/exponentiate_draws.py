import numpy as np
import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants
from tiny_structured_logger.lib.fhs_logging import get_logger

logger = get_logger()


def bias_exp_new(darray: xr.DataArray) -> xr.DataArray:
    r"""Exponentiate a distribution, preserving the mean in a certain way (see below).

    Exponentiate a distribution (draws) and adjust the results such that the
    mean of the exponentiated distribution is equal to the exponentiated
    expected value of the log distribution.

    Requires that input dataarray has draw dimension.

    The idea is that when

    .. math::
        g(x)=e^x,

    then

    .. math::
        E[g(X)]!=g(E[X]).

    Therefore we make factor :math:`c` such that

    .. math::
        c=g(E[X])/E[g(X)].

    Then we have

    .. math::
        \begin{equation}
        \begin{split}
            E[g(X) \ * \ c] &= cE[g(X)] \\
                            &= \frac{g(E[X])}{E[g(X)]}E[g(X)] \\
                            &= g(E[X])
        \end{split}
        \end{equation}

    Args:
        darray (xr.DataArray):
            Data array to exponentiate with bias correction. Must have draw
            dimension.

    Returns:
        xr.DataArray:
            Exponentiated results with adjusted distribution.

    Raises:
        ValueError: in the absence of a "draw" dimension.
    """
    if DimensionConstants.DRAW not in darray.dims:
        err_msg = "There is no draw dimension to use for bias_exp_new!"
        logger.error(err_msg)
        raise ValueError(err_msg)

    factor = np.exp(darray.mean(DimensionConstants.DRAW)) / np.exp(darray).mean(
        DimensionConstants.DRAW
    )

    return np.exp(darray) * factor

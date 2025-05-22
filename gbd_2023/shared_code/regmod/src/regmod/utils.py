"""
Utility classes and functions
"""

from dataclasses import dataclass

import numpy as np
from xspline import XSpline
from regmod._typing import Any, NDArray


def default_vec_factory(
    vec: Any, size: int, default_value: float, vec_name: str = "vec"
) -> NDArray:
    """Validate or create the vector.

    Parameters
    ----------
    vec : Any
        Vector to be validated. When it is `None`, it will use the
        `default_value` and `size` to create the vector. When it is a
        scalar, it will use `size` to create the vector. When it is a vector,
        it will be checked if have correct size.
    size : int
        Size of the vector.
    default_value : float
        Default value of the vector.
    vec_name : str, default='vec'
        Name of the vector, used for print error purpose. Default to `'vec'`.

    Returns
    -------
    NDArray
        Created or validated vector.

    Notes
    -----
    This function should be replaced in the next version.

    """

    if vec is None or np.isscalar(vec):
        return np.repeat(default_value if vec is None else vec, size)
    vec = np.asarray(vec)
    check_size(vec, size, vec_name=vec_name)

    return vec


def check_size(vec: NDArray, size: int, vec_name: str = "vec") -> None:
    """Check the size of the vector.

    Parameters
    ----------
    vec : NDArray
        Vector to be validated.
    size : int
        The assumption size of the vector.
    vec_name : str, default='vec'
        Name of the vector, used for print error purpose. Default to `'vec'`.

    Raises
    ------
    AssertionError
        Raised when the size of the vector not match with the given size.

    Notes
    -----
    This function should be replaced in the next version.

    """
    assert len(vec) == size, f"{vec_name} must length {size}."


@dataclass
class SplineSpecs:
    """Spline parameters used to create spline.

    Attributes
    ----------
    knots : NDArray
        Knots placement of the spline. Depends on `knots_type` this will be
        used differently.
    degree : int, default=3
        Degree of the spline. Default to be 3.
    l_linear : bool, default=False
        If `True`, spline will use left linear tail. Default to be `False`.
    r_linear : bool, default=False
        If `True`, spline will use right linear tail. Default to be `False`.
    include_first_basis : bool, default=False
        If `True`, spline will include the first basis of the spline. Default
        to be `False`.
    knots_type : {'abs', 'rel_domain', 'rel_freq'}, default='abs'
        Type of the spline knots. Can only be choosen from three options,
        `'abs'`, `'rel_domian'` and `'rel_freq'`. When it is `'abs'`
        which standards for absolute, the knots will be used as it is. When it
        is `rel_domain` which standards for relative domain, the knots
        requires to be between 0 and 1, and will be interpreted as the
        proportion of the domain. And when it is `rel_freq` which standards
        for relative frequency, it will be interpreted as the frequency of the
        data and required to be between 0 and 1.
    num_spline_bases : int
        Number of the spline bases

    Methods
    -------
    create_spline(vec)
        Create the spline from given vector as the data.

    """

    knots: NDArray
    degree: int = 3
    l_linear: bool = False
    r_linear: bool = False
    include_first_basis: bool = False
    knots_type: str = "abs"

    def __post_init__(self):
        assert self.knots_type in [
            "abs",
            "rel_domain",
            "rel_freq",
        ], "Knots type must be one of 'abs', 'rel_domain' or 'rel_freq'."

    @property
    def num_spline_bases(self) -> int:
        """Number of the spline bases."""
        inner_knots = self.knots[
            int(self.l_linear) : len(self.knots) - int(self.r_linear)
        ]
        return len(inner_knots) - 2 + self.degree + int(self.include_first_basis)

    def create_spline(self, vec: NDArray | None = None) -> XSpline:
        """Create spline from the given vector.

        Parameters
        ----------
        vec : NDArray, optional
            Given vector as the data. Default to `None`. When it is `None`
            it requires `knots_type` to be `abs`.

        Raises
        ------
        AssertionError
            Raised when `vec` is `None` and `knots_type` is not `abs`.

        Returns
        -------
        XSpline
            Spline object.

        """
        if self.knots_type == "abs":
            knots = self.knots
        else:
            assert (
                vec is not None
            ), "Using relative knots, must provide a vector to finalize knots."
            if self.knots_type == "rel_domain":
                lb = np.min(vec)
                ub = np.max(vec)
                knots = lb + self.knots * (ub - lb)
            else:
                knots = np.quantile(vec, self.knots)

        return XSpline(
            knots,
            self.degree,
            l_linear=self.l_linear,
            r_linear=self.r_linear,
            include_first_basis=self.include_first_basis,
        )


def sizes_to_slices(sizes: list[int]) -> list[slice]:
    """Convert a list of sizes to a list of slices.

    Parameters
    ----------
    sizes : list[int]
        A list of positive integers representing sizes of the groups.

    Raises
    ------
    ValueError
        Raised when `sizes` contains non-positive numbers.

    Returns
    -------
    list[slice]
        A list of slices converted from sizes.

    Examples
    --------
    >>> sizes = [1, 2, 3]
    >>> slices = sizes_to_slices(sizes)
    >>> slices
    [slice(0, 1, None), slice(1, 3, None), slice(3, 6, None)]
    >>> x = list(range(sum(sizes)))
    >>> x
    [0, 1, 2, 3, 4, 5]
    >>> [x[s] for s in slices]
    [[0], [1, 2], [3, 4, 5]]

    """
    sizes = np.asarray(sizes).astype(int)
    if any(sizes < 0):
        raise ValueError("Size must be non-negative.")
    ends = np.cumsum(sizes)
    starts = np.insert(ends, 0, 0)[:-1]
    return [slice(*pair) for pair in zip(starts, ends)]

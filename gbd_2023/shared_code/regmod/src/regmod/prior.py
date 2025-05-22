"""
Prior module
"""

from dataclasses import dataclass, field

import numpy as np
from xspline import XSpline
from regmod._typing import Any, Iterable, NDArray


@dataclass
class Prior:
    """Prior information for the variables, it is used to construct the
    likelihood and solve the optimization problem.

    Parameters
    ----------
    size : int, optional
        Size of variable. Default is `None`. When it is `None`, size is inferred
        from the vector information of the prior.

    Attributes
    ----------
    size : int
        Size of variable.

    Methods
    -------
    process_size(vecs)
        Infer and validate size from given vector information.

    Notes
    -----
    We should figure out a better structure for linear and spline prior, so that
    the extensions will be easier.

    """

    size: int = None

    def process_size(self, vecs: list[Any]):
        """Infer and validate size from given vector information.

        Parameters
        ----------
        vecs : list[Any]
            Vector information of the prior. For Gaussian prior it will be mean
            and standard deviation. For Uniform prior it will be lower and upper
            bounds.

        Raises
        ------
        ValueError
            Raised when size is not positive or integer.

        """
        if self.size is None:
            sizes = [len(vec) for vec in vecs if isinstance(vec, Iterable)]
            sizes.append(1)
            self.size = max(sizes)

        if self.size <= 0 or not isinstance(self.size, int):
            raise ValueError("Size of the prior must be a positive integer.")


@dataclass
class GaussianPrior(Prior):
    """Gaussian prior information.

    Parameters
    ----------
    size : Optional[int], optional
        Size of variable. Default is `None`. When it is `None`, size is inferred
        from the vector information of the prior.
    mean : float | NDArray, default=0
        Mean of the Gaussian prior. Default is 0. If it is a scalar, it will be
        extended to an array with `self.size`.
    sd : float | NDArray, default=np.inf
        Standard deviation of the Gaussian prior. Default is `np.inf`. If it is
        a scalar, it will be extended to an array with `self.size`.

    Attributes
    ----------
    size : int
        Size of the variable.
    mean : NDArray
        Mean of the Gaussian prior.
    sd : NDArray
        Standard deviation of the Gaussian prior.

    Raises
    ------
    ValueError
        Raised when size of mean vector doesn't match.
    ValueError
        Raised when size of the standard deviation vector doesn't match.
    ValueError
        Raised when any value in standard deviation vector is non-positive.

    """

    mean: NDArray = field(default=0.0, repr=False)
    sd: NDArray = field(default=np.inf, repr=False)

    def __post_init__(self):
        self.process_size([self.mean, self.sd])
        if np.isscalar(self.mean):
            self.mean = np.repeat(self.mean, self.size)
        if np.isscalar(self.sd):
            self.sd = np.repeat(self.sd, self.size)
        self.mean = np.asarray(self.mean)
        self.sd = np.asarray(self.sd)
        if self.mean.size != self.size:
            raise ValueError("Mean vector size does not match.")
        if self.sd.size != self.size:
            raise ValueError("Standard deviation vector size does not match.")
        if any(self.sd <= 0.0):
            raise ValueError("Standard deviation must be all positive.")


@dataclass
class UniformPrior(Prior):
    """Uniform prior information.

    Parameters
    ----------
    size : int, optional
        Size of variable. Default is `None`. When it is `None`, size is inferred
        from the vector information of the prior.
    lb : float | NDArray, default=-np.inf
        Lower bound of Uniform prior. Default is `-np.inf`. If it is a scalar,
        it will be extended to an array with `self.size`.
    ub : float | NDArray, default=np.inf
        Upper bound of the Uniform prior. Default is `np.inf`. If it is a
        scalar,it will be extended to an array with `self.size`.

    Attributes
    ----------
    size : int
        Size of the variable.
    lb : NDArray
        Lower bound of Uniform prior.
    ub : NDArray
        Upper bound of Uniform prior.

    Raises
    ------
    ValueError
        Raised when size of the lower bound vector doesn't match.
    ValueError
        Raised when size of the upper bound vector doesn't match.
    ValueError
        Raised if lower bound is greater than upper bound.

    """

    lb: NDArray = field(default=-np.inf, repr=False)
    ub: NDArray = field(default=np.inf, repr=False)

    def __post_init__(self):
        self.process_size([self.lb, self.ub])
        if np.isscalar(self.lb):
            self.lb = np.repeat(self.lb, self.size)
        if np.isscalar(self.ub):
            self.ub = np.repeat(self.ub, self.size)
        self.lb = np.asarray(self.lb)
        self.ub = np.asarray(self.ub)
        if self.lb.size != self.size:
            raise ValueError("Lower bound vector size does not match.")
        if self.ub.size != self.size:
            raise ValueError("Upper bound vector size does not match.")
        if any(self.lb > self.ub):
            ValueError("Lower bounds must be less than or equal to upper bounds.")


@dataclass
class LinearPrior:
    """Linear prior information.

    Parameters
    ----------
    mat : NDArray, optional
        Linear mapping for the prior. Default is an empty matrix.
    size : int, optional
        Size of the prior. Default is `None`. If it is `None`, the size will
        be inferred as the number of rows of `mat`.

    Attributes
    ----------
    mat : NDArray
        Linear mapping for the prior.
    size : int
        Size of the prior.

    Methods
    -------
    is_empty()
        Indicate if the prior is empty.

    """

    mat: NDArray = field(default_factory=lambda: np.empty(shape=(0, 1)), repr=False)
    size: int = None

    def __post_init__(self):
        if self.size is None:
            self.size = self.mat.shape[0]
        else:
            assert self.size == self.mat.shape[0], "`mat` and `size` not match."

    def is_empty(self) -> bool:
        """Indicate if the prior is empty.

        Returns
        -------
        bool
            Return `True` if `self.mat` is empty.
        """
        return self.mat.size == 0.0


@dataclass
class SplinePrior(LinearPrior):
    """Spline prior information.

    Parameters
    ----------
    size : int, default=100
        Size of the spline prior. Default is 100. It determines the number
        of sample points in the specified domain.
    order : int, default=0
        Order of the spline derivative. Default is 0.
    domain_lb : float, default=0.0
        Lower bounds of the domain. Default is 0.0.
    domain_ub : float, default=1.0
        Upper bounds of the domain. Default is 1.0.
    domain_type : {'rel', 'abs'}, default='rel'
        Type of the domain. Default is `'rel'`. It can only be `'abs'` or
        `'rel'`. When it is `'abs'`, `domain_lb` and `domain_ub` are interpreted
        as the absolute position of the domain. When it is `'rel'`, lower and
        upper bounds are treated as the percentage of the domain.

    Attributes
    ----------
    size : int
        Size of the spline prior. It determines the number of sample points in
        the specified domain.
    order : int
        Order of the spline derivative.
    domain_lb : float
        Lower bounds of the domain.
    domain_ub : float
        Upper bounds of the domain.
    domain_type : {'abs', 'rel'}
        Type of the domain. When it is `'abs'`, `domain_lb` and `domain_ub` are
        interpreted as the absolute position of the domain. When it is `'rel'`,
        lower and upper bounds are treated as the percentage of the domain.

    Methods
    -------
    attach_spline(spline)
        Attach the spline to process the domain.

    """

    size: int = 100
    order: int = 0
    domain_lb: float = field(default=0.0, repr=False)
    domain_ub: float = field(default=1.0, repr=False)
    domain_type: str = field(default="rel", repr=False)

    def __post_init__(self):
        assert (
            self.domain_lb <= self.domain_ub
        ), "Domain lower bound must be less or equal than upper bound."
        assert self.domain_type in ["rel", "abs"], "Domain type must be 'rel' or 'abs'."
        if self.domain_type == "rel":
            assert (
                self.domain_lb >= 0.0 and self.domain_ub <= 1.0
            ), "Using relative domain, bounds must be numbers between 0 and 1."

    def attach_spline(self, spline: XSpline):
        """Attach the spline to process the domain.

        Parameters
        ----------
        spline : XSpline
            Spline used to create the linear mapping for the prior.

        """
        knots_lb = spline.knots[0]
        knots_ub = spline.knots[-1]
        if self.domain_type == "rel":
            points_lb = knots_lb + (knots_ub - knots_lb) * self.domain_lb
            points_ub = knots_lb + (knots_ub - knots_lb) * self.domain_ub
        else:
            points_lb = self.domain_lb
            points_ub = self.domain_ub
        points = np.linspace(points_lb, points_ub, self.size)
        self.mat = spline.design_dmat(
            points, order=self.order, l_extra=True, r_extra=True
        )
        super().__post_init__()


@dataclass
class LinearGaussianPrior(LinearPrior, GaussianPrior):
    """Linear Gaussian prior."""

    def __post_init__(self):
        LinearPrior.__post_init__(self)
        GaussianPrior.__post_init__(self)


@dataclass
class LinearUniformPrior(LinearPrior, UniformPrior):
    """Linear Uniform prior."""

    def __post_init__(self):
        LinearPrior.__post_init__(self)
        UniformPrior.__post_init__(self)


@dataclass
class SplineGaussianPrior(SplinePrior, GaussianPrior):
    """Spline Gaussian prior."""

    def __post_init__(self):
        SplinePrior.__post_init__(self)
        GaussianPrior.__post_init__(self)


@dataclass
class SplineUniformPrior(SplinePrior, UniformPrior):
    """Spline Uniform Prior."""

    def __post_init__(self):
        SplinePrior.__post_init__(self)
        UniformPrior.__post_init__(self)

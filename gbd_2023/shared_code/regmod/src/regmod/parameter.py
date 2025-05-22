"""
Parameter module
"""

from dataclasses import dataclass, field

import numpy as np
from scipy.linalg import block_diag

from regmod.data import Data
from regmod.function import SmoothFunction, fun_dict
from regmod.prior import LinearGaussianPrior, LinearUniformPrior
from regmod.variable import SplineVariable, Variable
from regmod._typing import NDArray


@dataclass
class Parameter:
    """Parameter class is used for storing variable information including how
    variable parametrized the parameter and then in turn used in the likelihood
    building.

    Parameters
    ----------
    name
        Name of the parameter.
    variables
        A list of variables that parametrized the parameter.
    inv_link
        Inverse link funcion to link variables to parameter.
    offset
        If `offset=None` parameter will not use offset, when it is a string
        it will look for the corresponding column in the data frame as the
        offset.
    linear_gpriors
        A list of linear Gaussian priors. Default to an empty list.
    linear_upriors
        A list of linear Uniform priors. Default to an empty list.

    """

    name: str
    variables: list[Variable] = field(default_factory=list, repr=False)
    inv_link: str | SmoothFunction = field(default="identity", repr=False)
    offset: str | None = field(default=None, repr=False)
    linear_gpriors: list[LinearGaussianPrior] = field(default_factory=list, repr=False)
    linear_upriors: list[LinearUniformPrior] = field(default_factory=list, repr=False)

    def __post_init__(self):
        if isinstance(self.inv_link, str):
            self.inv_link = fun_dict[self.inv_link]
        assert isinstance(
            self.inv_link, SmoothFunction
        ), "inv_link has to be an instance of SmoothFunction."
        assert all(
            [isinstance(prior, LinearGaussianPrior) for prior in self.linear_gpriors]
        ), "linear_gpriors has to be a list of LinearGaussianPrior."
        assert all(
            [isinstance(prior, LinearUniformPrior) for prior in self.linear_upriors]
        ), "linear_upriors has to be a list of LinearUniformPrior."
        assert all(
            [prior.mat.shape[1] == self.size for prior in self.linear_gpriors]
        ), "linear_gpriors size not match."
        assert all(
            [prior.mat.shape[1] == self.size for prior in self.linear_upriors]
        ), "linear_upriors size not match."
        assert all(
            [isinstance(var, Variable) for var in self.variables]
        ), "variables has to be a list of Variable."
        if len(self.variables) == 0 and self.offset is None:
            raise ValueError(
                "Please provide at least one variable when "
                "parameter does not have an offset"
            )

    @property
    def size(self) -> int:
        """Size of the parameter."""
        return sum([var.size for var in self.variables])

    def check_data(self, data: Data):
        """Attach data to all variables.

        Parameters
        ----------
        data : Data
            Data object.

        """
        for var in self.variables:
            var.check_data(data)

    def get_mat(self, data: Data) -> NDArray:
        """Get the design matrix.

        Parameters
        ----------
        data : Data
            Data object.

        Returns
        -------
        NDArray
            The design matrix.

        """
        if len(self.variables) == 0:
            return np.empty(shape=(data.df.shape[0], 0))
        return np.hstack([var.get_mat(data) for var in self.variables])

    def get_uvec(self) -> NDArray:
        """Get the direct Uniform prior.

        Returns
        -------
        NDArray
            Uniform prior information array.
        """
        if len(self.variables) == 0:
            return np.empty(shape=(2, 0))
        return np.hstack([var.get_uvec() for var in self.variables])

    def get_gvec(self) -> NDArray:
        """Get the direct Gaussian prior.

        Returns
        -------
        NDArray
            Gaussian prior information array.

        """
        if len(self.variables) == 0:
            return np.empty(shape=(2, 0))
        return np.hstack([var.get_gvec() for var in self.variables])

    def get_linear_uvec(self) -> NDArray:
        """Get the linear Uniform prior vector.

        Returns
        -------
        NDArray
            Uniform prior information array.

        """
        if len(self.variables) == 0:
            return np.empty(shape=(2, 0))
        uvec = np.hstack(
            [
                (
                    var.get_linear_uvec()
                    if isinstance(var, SplineVariable)
                    else np.empty((2, 0))
                )
                for var in self.variables
            ]
        )
        if len(self.linear_upriors) > 0:
            uvec = np.hstack(
                [uvec]
                + [np.vstack([prior.lb, prior.ub]) for prior in self.linear_upriors]
            )
        return uvec

    def get_linear_gvec(self) -> NDArray:
        """Get the linear Gaussian prior vector.

        Returns
        -------
        NDArray
            Gaussian prior information array.

        """
        if len(self.variables) == 0:
            return np.empty(shape=(2, 0))
        gvec = np.hstack(
            [
                (
                    var.get_linear_gvec()
                    if isinstance(var, SplineVariable)
                    else np.empty((2, 0))
                )
                for var in self.variables
            ]
        )
        if len(self.linear_gpriors) > 0:
            gvec = np.hstack(
                [gvec]
                + [np.vstack([prior.mean, prior.sd]) for prior in self.linear_gpriors]
            )
        return gvec

    def get_linear_umat(self) -> NDArray:
        """Get the linear Uniform prior matrix.

        Returns
        -------
        NDArray
            Uniform prior design matrix.

        """
        if len(self.variables) == 0:
            return np.empty(shape=(0, 0))
        umat = block_diag(
            *[
                (
                    var.get_linear_umat()
                    if isinstance(var, SplineVariable)
                    else np.empty((0, 1))
                )
                for var in self.variables
            ]
        )
        if len(self.linear_upriors) > 0:
            umat = np.vstack([umat] + [prior.mat for prior in self.linear_upriors])
        return umat

    def get_linear_gmat(self) -> NDArray:
        """Get the linear Gaussian prior matrix.

        Returns
        -------
        NDArray
            Gaussian prior design matrix.

        """
        if len(self.variables) == 0:
            return np.empty(shape=(0, 0))
        gmat = block_diag(
            *[
                (
                    var.get_linear_gmat()
                    if isinstance(var, SplineVariable)
                    else np.empty((0, 1))
                )
                for var in self.variables
            ]
        )
        if len(self.linear_gpriors) > 0:
            gmat = np.vstack([gmat] + [prior.mat for prior in self.linear_gpriors])
        return gmat

    def get_lin_param(
        self,
        coefs: NDArray,
        data: Data,
        mat: NDArray = None,
        return_mat: bool = False,
    ) -> NDArray | tuple[NDArray, NDArray]:
        """Get the parameter before apply the link function.

        Parameters
        ----------
        coefs : NDArray
            Coefficients for the design matrix.
        data : Data
            Data object.
        mat : NDArray, optional
            Alternative design matrix, by default None.
        return_mat : bool, optional
            If `True`, return the created design matrix, by default False.

        Returns
        -------
        NDArray | tuple[NDArray, NDArray]
            Linear parameter vector, or when `return_mat=True` also returns the
            design matrix.

        """
        if len(self.variables) == 0:
            return data.df[self.offset].to_numpy()
        if mat is None:
            mat = self.get_mat(data)
        lin_param = mat.dot(coefs)
        if self.offset is not None:
            lin_param += data.df[self.offset].to_numpy()
        if return_mat:
            return lin_param, mat
        return lin_param

    def get_param(self, coefs: NDArray, data: Data, mat: NDArray = None) -> NDArray:
        """Get the parameter.

        Parameters
        ----------
        coefs : NDArray
            Coefficients for the design matrix.
        data : Data
            Data object.
        mat : NDArray, optional
            Alternative design matrix, by default None.

        Returns
        -------
        NDArray
            Returns the parameter.

        """
        lin_param = self.get_lin_param(coefs, data, mat)
        return self.inv_link.fun(lin_param)

    def get_dparam(self, coefs: NDArray, data: Data, mat: NDArray = None) -> NDArray:
        """Get the derivative of the parameter.

        Parameters
        ----------
        coefs : NDArray
            Coefficients for the design matrix.
        data : Data
            Data object.
        mat : NDArray, optional
            Alternative design matrix, by default None.

        Returns
        -------
        NDArray
            Returns the derivative of the parameter.

        """
        if len(self.variables) == 0:
            return np.empty((data.df.shape[0], 0))
        lin_param, mat = self.get_lin_param(coefs, data, mat, return_mat=True)
        return self.inv_link.dfun(lin_param)[:, None] * mat

    def get_d2param(self, coefs: NDArray, data: Data, mat: NDArray = None) -> NDArray:
        """Get the second order derivative of the parameter.

        Parameters
        ----------
        coefs : NDArray
            Coefficients for the design matrix.
        data : Data
            Data object.
        mat : NDArray, optional
            Alternative design matrix, by default None.

        Returns
        -------
        NDArray
            Returns the second order derivative of the parameter.

        """
        if len(self.variables) == 0:
            return np.empty((data.df.shape[0], 0, 0))
        lin_param, mat = self.get_lin_param(coefs, data, mat, return_mat=True)
        return self.inv_link.d2fun(lin_param)[:, None, None] * (
            mat[..., None] * mat[:, None, :]
        )

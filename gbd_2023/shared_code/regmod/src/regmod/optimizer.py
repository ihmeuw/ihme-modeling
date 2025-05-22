"""
Optimizer module
"""

import numpy as np
from msca.optim.prox import proj_capped_simplex
from msca.optim.solver import IPSolver, NTSolver
from scipy.optimize import LinearConstraint, minimize
from regmod._typing import Callable, NDArray


def scipy_optimize(
    model: "Model", x0: NDArray | None = None, options: dict | None = None
) -> NDArray:
    """Scipy trust-region optimizer.

    Parameters
    ----------
    model : Model
        Instance of `regmod.models.Model` class.
    x0 : NDArray, optional
        Initial guess for the variable, by default None. If `None` use zero
        vector as the initial guess.
    options : dict, optional
        Scipy solver options, by default None.

    Returns
    -------
    NDArray
        Optimal solution.

    """
    x0 = np.zeros(model.size) if x0 is None else x0
    bounds = model.uvec.T
    constraints = (
        [
            LinearConstraint(
                model.linear_umat, model.linear_uvec[0], model.linear_uvec[1]
            )
        ]
        if model.linear_uvec.size > 0
        else []
    )

    result = minimize(
        model.objective,
        x0,
        method="trust-constr",
        jac=model.gradient,
        hess=model.hessian,
        constraints=constraints,
        bounds=bounds,
        options=options,
    )

    model.opt_result = result
    model.opt_coefs = result.x.copy()
    model.opt_vcov = model.get_vcov(model.opt_coefs)
    return result.x


def msca_optimize(
    model: "Model", x0: NDArray | None = None, options: dict | None = None
) -> NDArray:
    x0 = np.zeros(model.size) if x0 is None else x0
    options = options or {}

    if model.cmat.size == 0:
        solver = NTSolver(model.objective, model.gradient, model.hessian)
    else:
        solver = IPSolver(
            model.objective, model.gradient, model.hessian, model.cmat, model.cvec
        )
    result = solver.minimize(x0=x0, **options)
    model.opt_result = result
    model.opt_coefs = result.x.copy()
    model.opt_vcov = model.get_vcov(model.opt_coefs)
    return result.x


def set_trim_weights(model: "Model", index: NDArray, mask: float):
    """Set trimming weights to model object.

    Parameters
    ----------
    model : Model
        Instance of `regmod.models.Model` class.
    index : NDArray
        Index where the weights need to be set.
    mask : float
        Value of the weights to set.

    """
    weights = np.ones(model.data.num_obs)
    weights[index] = mask
    model.data.trim_weights = weights


def trimming(optimize: Callable) -> Callable:
    """Constructor of trimming solver.

    Parameters
    ----------
    optimize : Callable
        Optimization solver.

    Returns
    -------
    Callable
        Trimming optimization solver.

    """

    def optimize_with_trimming(
        model: "Model",
        x0: NDArray = None,
        options: dict = None,
        trim_steps: int = 3,
        inlier_pct: float = 0.95,
    ) -> NDArray:
        if trim_steps < 2:
            raise ValueError("At least two trimming steps.")
        if inlier_pct < 0.0 or inlier_pct > 1.0:
            raise ValueError("inlier_pct has to be between 0 and 1.")
        coefs = optimize(model, x0, options)
        if inlier_pct < 1.0:
            bounds = (0.5 - 0.5 * inlier_pct, 0.5 + 0.5 * inlier_pct)
            index = model.detect_outliers(coefs, bounds)
            if index.sum() > 0:
                masks = np.append(np.linspace(1.0, 0.0, trim_steps)[1:], 0.0)
                for mask in masks:
                    set_trim_weights(model, index, mask)
                    coefs = optimize(model, coefs, options)
                    index = model.detect_outliers(coefs, bounds)
        return coefs

    return optimize_with_trimming


def original_trimming(optimize: Callable) -> Callable:
    def optimize_with_trimming(
        model: "Model",
        x0: NDArray = None,
        options: dict | None = None,
        trim_steps: int = 10,
        step_size: float = 10.0,
        inlier_pct: float = 0.95,
    ) -> NDArray:
        if trim_steps < 2:
            raise ValueError("At least two trimming steps.")
        if inlier_pct < 0.0 or inlier_pct > 1.0:
            raise ValueError("inlier_pct has to be between 0 and 1.")
        coefs = optimize(model, x0, options)
        if inlier_pct < 1.0:
            num_inliers = int(inlier_pct * model.data.obs.size)
            counter = 0
            success = False
            while (counter < trim_steps) and (not success):
                counter += 1
                nll_terms = model.get_nll_terms(coefs)
                model.data.trim_weights = proj_capped_simplex(
                    model.data.trim_weights - step_size * nll_terms, num_inliers
                )
                coefs = optimize(model, x0, options)
                success = all(
                    np.isclose(model.data.trim_weights, 0.0)
                    | np.isclose(model.data.trim_weights, 1.0)
                )
            if not success:
                sort_indices = np.argsort(model.data.trim_weights)
                model.data.trim_weights[sort_indices[-num_inliers:]] = 1.0
                model.data.trim_weights[sort_indices[:-num_inliers]] = 0.0
        return coefs

    return optimize_with_trimming

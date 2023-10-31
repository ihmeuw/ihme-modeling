"""
    other_sampling
    ~~~~~~~~~~~~~~
"""
from warnings import warn
from typing import Union
from dataclasses import dataclass
import numpy as np
from .model import MRBRT
from .cov_model import LinearCovModel


try:
    from limetr.utils import VarMat
except:
    class VarMat:
        pass


@dataclass
class SimpleLMESpecs:
    obs: np.ndarray
    obs_se: np.ndarray
    study_sizes: np.ndarray
    fe_mat: np.ndarray
    re_mat: np.ndarray
    beta_soln: np.ndarray
    gamma_soln: np.ndarray
    fe_gprior: Union[np.ndarray, None] = None
    trimming_weights: np.ndarray = None

    def __post_init__(self):
        self.num_obs = len(self.obs)
        self.num_x_vars = self.fe_mat.shape[1]
        self.num_z_vars = self.re_mat.shape[1]

        if self.fe_gprior is not None and np.isinf(self.fe_gprior[1]).all():
            self.fe_gprior = None

        if self.trimming_weights is None:
            self.trimming_weights = np.ones(self.num_obs)


def is_simple_linear_mixed_effects_model(model: MRBRT) -> bool:
    """Test if a model is simple linear mixed effects model, where
    * covmodel is linear
    * no constraints
    * no uniform prior for fixed effects.

    Args:
        model (MRBRT): Model to be tested.

    Returns:
        bool:
            True if model is linear mixed effects model.
    """
    ok = all([isinstance(cov_model, LinearCovModel)
              for cov_model in model.cov_models])

    uprior = model.create_uprior()
    fe_uprior = uprior[:, :model.num_x_vars]
    ok = ok and np.isneginf(fe_uprior[0]).all() and np.isposinf(fe_uprior[1]).all()

    lprior = model.create_lprior()
    fe_lprior = lprior[:, :model.num_x_vars]
    ok = ok and np.isinf(fe_lprior[1]).all()

    ok = ok and (model.num_constraints == 0)
    return ok


def extract_simple_lme_specs(model: MRBRT) -> SimpleLMESpecs:
    """Extract the simple mixed effects model specs.

    Args:
        model (MRBRT): Simple mixed effects model

    Returns:
        SimpleLMESpecs:
            Data object contains information of the simple linear mixed effects model.
    """
    if not is_simple_linear_mixed_effects_model(model):
        warn("Model is not a simple mixed effects model. Uncertainty might not be accurate.")

    x_fun, x_jac_fun = model.create_x_fun()
    x_mat = x_jac_fun(model.beta_soln)
    z_mat = model.create_z_mat()
    gprior = model.create_gprior()

    beta_soln = model.lt.beta.copy()
    gamma_soln = model.lt.gamma.copy()
    w_soln = model.lt.w.copy()

    return SimpleLMESpecs(
        obs=model.data.obs,
        obs_se=model.data.obs_se,
        study_sizes=model.data.study_sizes,
        fe_mat=x_mat,
        re_mat=z_mat,
        beta_soln=beta_soln,
        gamma_soln=gamma_soln,
        fe_gprior=gprior[:, :model.num_x_vars],
        trimming_weights=w_soln
    )


def extract_simple_lme_hessian(model_specs: SimpleLMESpecs) -> np.ndarray:
    """Extract the Hessian matrix from the simple linear mixed effects model.

    Args:
        model_specs (SimpleLMESpecs): Model specifications.

    Returns:
        np.ndarray: Hessian matrix.
    """
    sqrt_weights = np.sqrt(model_specs.trimming_weights)
    x = model_specs.fe_mat*sqrt_weights[:, None]
    z = model_specs.re_mat*sqrt_weights[:, None]
    d = model_specs.obs_se**(2*model_specs.trimming_weights)
    v = VarMat(d, z, model_specs.gamma_soln, model_specs.study_sizes)

    hessian = x.T.dot(v.invDot(x))
    if model_specs.fe_gprior is not None:
        hessian += np.diag(1.0/model_specs.fe_gprior[1]**2)

    return hessian


def sample_simple_lme_beta(sample_size: int, model: MRBRT) -> np.ndarray:
    """Simple beta from simple linear mixed effects model.

    Args:
        sample_size (int): Sample size.
        model (MRBRT): Simple linear mixed effects model.

    Return:
        np.ndarray:
            Beta samples from the linear mixed effects model.
    """
    # extract information
    model_specs = extract_simple_lme_specs(model)

    # compute the mean anc variance matrix for sampling
    beta_mean = model_specs.beta_soln
    beta_var = np.linalg.inv(extract_simple_lme_hessian(model_specs))

    # sample the solutions
    beta_samples = np.random.multivariate_normal(
        beta_mean,
        beta_var,
        size=sample_size
    )

    return beta_samples

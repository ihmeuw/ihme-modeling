"""
Test Tobit Model
"""
import numpy as np
import pandas as pd
import pytest

from regmod.data import Data
from regmod.models import TobitModel
from regmod.variable import Variable


@pytest.fixture
def df():
    n = 100
    x = np.random.normal(size=n)
    y = np.random.normal(size=n)
    return pd.DataFrame({"x": x, "y": y, "z": np.where(y > 0, y, 0)})


@pytest.fixture
def data(df):
    return Data(col_obs="z", col_covs=["x"], df=df)


@pytest.fixture
def param_specs():
    specs = {
        "mu": {"variables": [Variable("x"), Variable("intercept")]},
        "sigma": {"variables": [Variable("intercept")]},
    }
    return specs


@pytest.fixture
def model(data, param_specs):
    return TobitModel(data=data, param_specs=param_specs)


def test_jax_inv_link(data, param_specs):
    """User-supplied inv_link functions replaced with JAX versions."""
    param_specs["mu"]["inv_link"] = "identity"
    param_specs["sigma"]["inv_link"] = "exp"
    model = TobitModel(data=data, param_specs=param_specs)
    assert model.params[0].inv_link.name == "identity_jax"
    assert model.params[1].inv_link.name == "exp_jax"


def test_neg_obs(df, param_specs):
    """ValueError if data contains negative observations."""
    with pytest.raises(ValueError, match="requires non-negative observations"):
        TobitModel(
            data=Data(col_obs="y", col_covs=["x"], df=df), param_specs=param_specs
        )


def test_vcov_output(model):
    """New get_vcov method matches old version."""
    model.fit()
    coefs = model.opt_coefs

    # Old version
    H = model.hessian(coefs)
    eig_vals, eig_vecs = np.linalg.eigh(H)
    inv_H = (eig_vecs / eig_vals).dot(eig_vecs.T)
    J = model.jacobian2(coefs)
    vcov_old = inv_H.dot(J)
    vcov_old = inv_H.dot(vcov_old.T)

    # New version
    vcov_new = model.get_vcov(coefs)

    assert np.allclose(vcov_old, vcov_new)


def test_pred_values(model):
    """Predicted mu_censored >= 0 and sigma > 0."""
    model.fit()
    df_pred = model.predict()
    assert np.all(df_pred["mu_censored"] >= 0)
    assert np.all(df_pred["sigma"] > 0)


def test_model_no_variables():
    num_obs = 5
    df = pd.DataFrame(
        {
            "obs": np.random.rand(num_obs) * 10,
            "offset": np.ones(num_obs),
        }
    )
    data = Data(
        col_obs="obs",
        col_offset="offset",
        df=df,
    )
    model = TobitModel(
        data, param_specs={"mu": {"offset": "offset"}, "sigma": {"offset": "offset"}}
    )
    coefs = np.array([])
    grad = model.gradient(coefs)
    hessian = model.hessian(coefs)
    assert grad.size == 0
    assert hessian.size == 0

    model.fit()
    assert model.opt_result == "no parameter to fit"


def test_model_one_variable():
    num_obs = 5
    df = pd.DataFrame(
        {
            "obs": np.random.rand(num_obs) * 10,
            "offset": np.ones(num_obs),
        }
    )
    data = Data(
        col_obs="obs",
        col_offset="offset",
        df=df,
    )
    model = TobitModel(
        data,
        param_specs={
            "sigma": {"offset": "offset"},
            "mu": {"variables": [Variable("intercept")]},
        },
    )
    model.fit()
    assert model.opt_coefs.size == 1

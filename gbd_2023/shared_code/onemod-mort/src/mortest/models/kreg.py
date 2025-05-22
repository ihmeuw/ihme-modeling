import jax.numpy as jnp
import numpy as np
import pandas as pd
from kreg.kernel.component import KernelComponent
from kreg.kernel.factory import (
    build_matern_three_half_kfunc,
    build_shifted_scaled_linear_kfunc,
    vectorize_kfunc,
)
from kreg.kernel.kron_kernel import KroneckerKernel
from kreg.likelihood import BinomialLikelihood
from kreg.model import KernelRegModel
from scipy.special import expit, logit


def transform_age(x, a=0.5):
    return np.log(np.exp(a * x) - 1) / a


def transform_year(
    x: pd.Series,
    start: float = 2020.0,
    a: float = 0.7,
    power: int = 2,
    mod: str = "extend",
):
    if mod == "extend":
        return x.where(x <= start, x + a * (x - start) ** power)
    elif mod == "shrink":
        return x.where(x <= start, start + a * (x - start) ** power)
    else:
        raise ValueError("Invalid mod")


def build_pred_kreg(
    data: pd.DataFrame, config: dict
) -> tuple[KernelRegModel, pd.DataFrame]:
    data[["obs", "weights"]] = data[["obs", "weights"]].fillna(0)
    data["offset"] = logit(data["pred_spxmod"])
    # transform age
    data["transformed_age"] = transform_age(
        data["age_mid"], config["age_scale"]
    )
    # transform year
    data["transformed_year"] = transform_year(data["year_id"])
    if config["shrink_under5_year"]:
        index = data.eval("age_mid <= 5.0")
        data.loc[index, "transformed_year"] = transform_year(
            data.loc[index, "year_id"],
            a=0.01,
            power=1,
            start=2015,
            mod="shrink",
        )

    # increase credibility of covid years
    index = data.eval("year_id.isin([2020, 2021])")
    data.loc[index, "weights"] *= 10

    kfunc_age = build_matern_three_half_kfunc(rho=config["gamma_age"])

    linear_year = build_shifted_scaled_linear_kfunc(
        data["year_id"].mean(), data["year_id"].std()
    )
    rbf_year = build_matern_three_half_kfunc(rho=config["gamma_year"])
    rbf_year_short = build_matern_three_half_kfunc(rho=config["gamma_year"] / 2)

    def kfunc_year(x, y):
        return (
            rbf_year(x, y)
            + config["kernel_year_short"] * rbf_year_short(x, y)
            + config["kernel_year_const"]
            + config["kernel_year_linear"] * linear_year(x, y)
        )

    def kfunc_continuous(x, y):
        return (
            kfunc_year(x[0], y[0]) * kfunc_age(x[1], y[1])
            # + 5.0
            # + 0.2 * linear_year(x[0], y[0])
        )

    # build kernel component
    kernel_components = [
        KernelComponent(
            ["transformed_year", "transformed_age"],
            vectorize_kfunc(kfunc_continuous),
        )
    ]

    kernel = KroneckerKernel(kernel_components, nugget=config["nugget"])

    likelihood = BinomialLikelihood("obs", "weights", "offset")
    model = KernelRegModel(
        kernel, likelihood, lam=config["lam"], lam_ridge=config["lam_ridge"]
    )
    y_opt = jnp.zeros(len(data))
    y_opt, hist = model.fit(
        data,
        x0=y_opt,
        gtol=2e-4,
        max_iter=100,
        use_direct=True,
        disable_tqdm=False,
    )
    data.sort_values(model.kernel.names, ignore_index=True, inplace=True)
    data["pred_kreg_raw"] = y_opt
    data["pred_kreg"] = expit(y_opt + data["offset"])

    # create uncertainty
    vcov = get_vcov(
        model, data, y_opt, config["anchor"], config["lam_uncertainty"]
    )
    data["pred_kreg_raw_sd"] = jnp.sqrt(jnp.diag(vcov))

    if config["modify_recent_years"]:
        data = modify_recent_years(data)

    return model, data


def get_vcov(
    model: KernelRegModel,
    data: pd.DataFrame,
    y_opt: np.ndarray,
    anchor: float,
    lam_uncertainty: float,
    alpha: np.ndarray | None = None,
) -> np.ndarray:
    data.sort_values(model.kernel.names, ignore_index=True)
    model.likelihood.attach(data)
    model.kernel.attach(data)
    hessian = np.asarray(model.hessian_matrix(y_opt))
    model.likelihood.detach()
    model.kernel.clear_matrices()
    eigvals, eigvecs = np.linalg.eigh(hessian)
    vcov = (
        eigvecs
        * ((1.0 + lam_uncertainty) / (eigvals + anchor * lam_uncertainty))
    ).dot(eigvecs.T)
    if alpha is not None:
        vcov *= np.outer(alpha, alpha)
    return vcov


def modify_recent_years(data: pd.DataFrame) -> pd.DataFrame:
    data = data.merge(
        data.query("year_id.isin([2018, 2019])")
        .groupby("age_group_id")["pred_kreg_raw"]
        .mean()
        .rename("toremove")
        .reset_index(),
        on="age_group_id",
        how="left",
    )
    index = data.eval("year_id >= 2022 & weights == 0")
    data.loc[index, "pred_kreg_raw"] = data.loc[index, "toremove"]
    data["pred_kreg"] = expit(data["pred_kreg_raw"] + data["offset"])
    data.drop(columns="toremove", inplace=True)
    return data

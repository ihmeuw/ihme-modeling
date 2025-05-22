from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bopforge.utils import get_beta_info, get_gamma_info, get_signal
from matplotlib.pyplot import Axes, Figure
from mrtool import MRBRT, CovFinder, LinearCovModel, LogCovModel, MRBeRT, MRData
from mrtool.core.utils import sample_knots
from pandas import DataFrame
from scipy.stats import norm


def get_signal_model(settings: dict, df: DataFrame) -> MRBeRT:
    """Create signal model for the risk curve.

    Parameters
    ----------
    settings
        Dictionary contains all the settings.
    df
        Data frame contains the training data.

    Returns
    -------
    MRBeRT
        Ensemble version of the mrbrt model.

    """
    data = MRData()
    data.load_df(
        df,
        col_obs="ln_rr",
        col_obs_se="ln_rr_se",
        col_covs=[
            "ref_risk_lower",
            "ref_risk_upper",
            "alt_risk_lower",
            "alt_risk_upper",
        ],
        col_study_id="study_id",
        col_data_id="seq",
    )
    settings["cov_model"] = {
        **dict(
            use_re=False,
            use_spline=True,
            prior_spline_funval_uniform=[-0.999999, 19],
            prior_spline_num_constraint_points=100,
        ),
        **settings["cov_model"],
    }
    cov_model = LogCovModel(
        alt_cov=["alt_risk_lower", "alt_risk_upper"],
        ref_cov=["ref_risk_lower", "ref_risk_upper"],
        **settings["cov_model"],
    )

    for arg in ["knot_bounds", "min_dist"]:
        if not np.isscalar(settings["knots_samples"][arg]):
            settings["knots_samples"][arg] = np.asarray(settings["knots_samples"][arg])
    settings["knots_samples"] = {
        **dict(
            num_knots=len(settings["cov_model"]["spline_knots"]) - 2,
        ),
        **settings["knots_samples"],
    }
    knots_samples = sample_knots(**settings["knots_samples"])
    # TODO: temporary fix
    knots_samples[:, 0] = 0.0
    knots_samples[:, -1] = 1.0

    signal_model = MRBeRT(
        data,
        ensemble_cov_model=cov_model,
        ensemble_knots=knots_samples,
        **settings["signal_model"],
    )

    return signal_model


def convert_bc_to_em(df: DataFrame, signal_model: MRBeRT) -> DataFrame:
    """Convert bias covariate to effect modifier and add one column to indicate
    if data points are inliers or outliers.

    Parameters
    ----------
    df
        Data frame contains the training data.
    signal_model
        Fitted signal model.

    Returns
    -------
    DataFrame
        DataFrame with additional columns for effect modifiers and oulier
        indicator.

    """
    data = signal_model.data
    signal = signal_model.predict(data)
    risk_exposures = df[
        [
            "ref_risk_lower",
            "ref_risk_upper",
            "alt_risk_lower",
            "alt_risk_upper",
        ]
    ].values
    re_signal = signal_model.predict(
        MRData(
            covs={
                "ref_risk_lower": np.repeat(risk_exposures.min(), data.num_obs),
                "ref_risk_upper": np.repeat(risk_exposures.min(), data.num_obs),
                "alt_risk_lower": data.covs["alt_risk_lower"],
                "alt_risk_upper": data.covs["alt_risk_upper"],
            }
        )
    )
    is_outlier = (signal_model.w_soln < 0.1).astype(int)
    df = df.merge(
        pd.DataFrame(
            {
                "seq": data.data_id,
                "signal": signal,
                "re_signal": re_signal,
                "is_outlier": is_outlier,
            }
        ),
        how="outer",
        on="seq",
    )
    for col in df.columns:
        if col.startswith("cov_"):
            df["em" + col[3:]] = df[col] * df["signal"]

    return df


def get_signal_model_summary(name: str, all_settings: dict, df: DataFrame) -> dict:
    """Create signal model summary.

    Parameters
    ----------
    name
        Name of the pair.
    all_settings
        All the settings for the pipeline.
    df
        Data frame that contains the training dataset.

    Returns
    -------
    dict
        Summary dictionary from the signal model.

    """
    risk_exposures = df[
        [
            "ref_risk_lower",
            "ref_risk_upper",
            "alt_risk_lower",
            "alt_risk_upper",
        ]
    ].values

    summary = {
        "name": name,
        "risk_type": str(df.risk_type.values[0]),
        "risk_unit": str(df.risk_unit.values[0]),
    }
    summary["risk_bounds"] = [float(risk_exposures.min()), float(risk_exposures.max())]
    risk_mean = np.vstack(
        [risk_exposures[:, [0, 1]].mean(axis=1), risk_exposures[:, [2, 3]].mean(axis=1)]
    )
    risk_mean.sort(axis=0)
    summary["risk_score_bounds"] = [
        float(np.quantile(risk_mean[0], 0.15)),
        float(np.quantile(risk_mean[1], 0.85)),
    ]
    summary["normalize_to_tmrel"] = all_settings["complete_summary"]["score"][
        "normalize_to_tmrel"
    ]

    return summary


def get_cov_finder_linear_model(df: DataFrame) -> MRBRT:
    """Greate the linear model for the CovFinder to determine the strength of
    the prior on the bias covariates.

    Parameters
    ----------
    df
        Data frame that contains the training data, but without the outlier.

    Returns
    -------
    MRBRT
        The linear model.

    """
    col_covs = ["signal", "re_signal"] + [
        col for col in df.columns if col.startswith("em_")
    ]
    data = MRData()
    data.load_df(
        df,
        col_obs="ln_rr",
        col_obs_se="ln_rr_se",
        col_covs=col_covs,
        col_study_id="study_id",
        col_data_id="seq",
    )
    cov_models = [
        LinearCovModel("signal", use_re=False),
        LinearCovModel("re_signal", use_re=True, prior_beta_uniform=[0.0, 0.0]),
    ]
    cov_finder_linear_model = MRBRT(data, cov_models)

    return cov_finder_linear_model


def get_cov_finder(settings: dict, cov_finder_linear_model: MRBRT) -> CovFinder:
    """Create the instance of CovFinder class.

    Parameters
    ----------
    settings
        Settings for bias covariate selection.
    cov_finder_linear_model
        Fitted cov finder linear model.

    Returns
    -------
    CovFinder
        The instance of the CovFinder class.

    """
    data = cov_finder_linear_model.data
    # get the posterior information of beta
    beta_info = get_beta_info(cov_finder_linear_model, cov_name="signal")

    # covariate selection
    pre_selected_covs = settings["cov_finder"]["pre_selected_covs"]
    if isinstance(pre_selected_covs, str):
        pre_selected_covs = [pre_selected_covs]
    pre_selected_covs = [col.replace("cov_", "em_") for col in pre_selected_covs]
    if "signal" not in pre_selected_covs:
        pre_selected_covs.append("signal")
    settings["cov_finder"]["pre_selected_covs"] = pre_selected_covs
    candidate_covs = [
        cov_name
        for cov_name in data.covs.keys()
        if cov_name not in pre_selected_covs + ["intercept", "re_signal"]
    ]
    settings["cov_finder"] = {
        **dict(
            num_samples=1000,
            power_range=[-4, 4],
            power_step_size=0.05,
            laplace_threshold=0.00001,
            inlier_pct=1.0,
            bias_zero=True,
        ),
        **settings["cov_finder"],
    }
    cov_finder = CovFinder(
        data,
        covs=candidate_covs,
        beta_gprior_std=0.1 * beta_info[1],
        **settings["cov_finder"],
    )

    return cov_finder


def get_cov_finder_result(
    cov_finder_linear_model: MRBRT, cov_finder: CovFinder
) -> dict:
    """Summarize result from bias covariate selection.

    Parameters
    ----------
    cov_finder_linear_model
        Fitted cov finder linear model.
    cov_finder
        Fitted cov finder model.

    Returns
    -------
    dict
        Result summary for bias covariate selection.

    """
    beta_info = get_beta_info(cov_finder_linear_model, cov_name="signal")
    selected_covs = [
        cov_name for cov_name in cov_finder.selected_covs if cov_name != "signal"
    ]
    cov_finder_result = {
        "beta_sd": float(beta_info[1] * 0.1),
        "selected_covs": selected_covs,
    }
    return cov_finder_result


def get_linear_model(df: DataFrame, cov_finder_result: dict) -> MRBRT:
    """Create linear model for risk curve.

    Parameters
    ----------
    df
        Data frame contains training data without outliers.
    cov_finder_result
        Summary result for bias covariate selection.

    Returns
    -------
    MRBRT
        The linear model for risk curve.

    """
    data = MRData()
    data.load_df(
        df,
        col_obs="ln_rr",
        col_obs_se="ln_rr_se",
        col_covs=["signal", "re_signal"] + cov_finder_result["selected_covs"],
        col_study_id="study_id",
        col_data_id="seq",
    )
    cov_models = [
        LinearCovModel("signal", use_re=False, prior_beta_uniform=[0.0, np.inf]),
        LinearCovModel("re_signal", use_re=True, prior_beta_uniform=[0.0, 0.0]),
        LinearCovModel("intercept", use_re=True, prior_beta_uniform=[0.0, 0.0]),
    ]
    for cov_name in cov_finder_result["selected_covs"]:
        cov_models.append(
            LinearCovModel(
                cov_name, prior_beta_gaussian=[0.0, cov_finder_result["beta_sd"]]
            )
        )
    model = MRBRT(data, cov_models)

    return model


def get_linear_model_summary(
    settings: dict,
    summary: dict,
    df: DataFrame,
    signal_model: MRBeRT,
    linear_model: MRBRT,
) -> dict:
    """Complete the summary from the signal model.

    Parameters
    ----------
    settings
        Settings for the complete summary section.
    summary
        Summary from the signal model.
    df
        Data frame contains the training dataset.
    signal_model
        Fitted signal model for risk curve.
    linear_model
        Fitted linear model for risk curve.

    Returns
    -------
    dict
        Summary file contains all necessary information.

    """
    # load summary
    summary["normalize_to_tmrel"] = settings["score"]["normalize_to_tmrel"]

    # solution of the final model
    beta_info = get_beta_info(linear_model)
    gamma_info = get_gamma_info(linear_model)
    summary["beta"] = [float(beta_info[0]), float(beta_info[1])]
    summary["gamma"] = [float(gamma_info[0]), float(gamma_info[1])]

    # compute the score and add star rating
    risk = np.linspace(*summary["risk_bounds"], 100)
    signal = get_signal(signal_model, risk)
    beta_sd = np.sqrt(beta_info[1] ** 2 + gamma_info[0] + 2 * gamma_info[1])
    pred = signal * beta_info[0]
    inner_ui = np.vstack(
        [
            signal * (beta_info[0] - 1.96 * beta_info[1]),
            signal * (beta_info[0] + 1.96 * beta_info[1]),
        ]
    )
    burden_of_proof = np.vstack(
        [
            signal * (beta_info[0] - 1.645 * beta_sd),
            signal * (beta_info[0] + 1.645 * beta_sd),
        ]
    )
    if settings["score"]["normalize_to_tmrel"]:
        index = np.argmin(pred)
        pred -= pred[index]
        burden_of_proof -= burden_of_proof[:, None, index]
        inner_ui -= inner_ui[:, None, index]

    index = (risk >= summary["risk_score_bounds"][0]) & (
        risk <= summary["risk_score_bounds"][1]
    )
    sign = np.sign(pred)
    if np.any(np.prod(inner_ui[:, index], axis=0) < 0):
        summary["score"] = float("nan")
        summary["star_rating"] = 0
    else:
        score = float(
            ((sign * burden_of_proof)[:, index].mean(axis=1)).min()
        )
        summary["score"] = score
        #Assign star rating based on ROS
        if np.isnan(score):
            summary["star_rating"] = 0
        elif score > np.log(1 + 0.85):
            summary["star_rating"] = 5
        elif score > np.log(1 + 0.50):
            summary["star_rating"] = 4
        elif score > np.log(1 + 0.15):
            summary["star_rating"] = 3
        elif score > 0:
            summary["star_rating"] = 2
        else:
            summary["star_rating"] = 1

    # compute the publication bias
    index = df.is_outlier == 0
    residual = df.ln_rr.values[index] - df.signal.values[index] * beta_info[0]
    residual_sd = np.sqrt(
        df.ln_rr_se.values[index] ** 2 + df.re_signal.values[index] ** 2 * gamma_info[0]
    )
    weighted_residual = residual / residual_sd
    r_mean = weighted_residual.mean()
    r_sd = 1 / np.sqrt(weighted_residual.size)
    pval = 1 - norm.cdf(np.abs(r_mean / r_sd))
    summary["pub_bias"] = int(pval < 0.05)
    summary["pub_bias_pval"] = float(pval)

    return summary


def get_draws(
    settings: dict,
    summary: dict,
    signal_model: MRBeRT,
) -> tuple[DataFrame, DataFrame]:
    """Create risk curve draws for the pipeline.

    Parameters
    ----------
    settings
        Settings for complete the summary.
    summary
        Completed summary file.
    signal_model
        Fitted signal model for risk curve.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        Inner and outer draw files.

    """
    if settings["draws"]["risk_lower"] is None:
        risk_lower = summary["risk_bounds"][0]
    else:
        risk_lower = settings["draws"]["risk_lower"]
    if settings["draws"]["risk_upper"] is None:
        risk_upper = summary["risk_bounds"][1]
    else:
        risk_upper = settings["draws"]["risk_upper"]
    risk = np.linspace(risk_lower, risk_upper, settings["draws"]["num_points"])
    signal = get_signal(signal_model, risk)
    inner_beta_sd = summary["beta"][1]
    outer_beta_sd = np.sqrt(
        summary["beta"][1] ** 2 + summary["gamma"][0] + 2 * summary["gamma"][1]
    )
    inner_beta_samples = np.random.normal(
        loc=summary["beta"][0], scale=inner_beta_sd, size=settings["draws"]["num_draws"]
    )
    outer_beta_samples = np.random.normal(
        loc=summary["beta"][0], scale=outer_beta_sd, size=settings["draws"]["num_draws"]
    )
    inner_draws = np.outer(signal, inner_beta_samples)
    outer_draws = np.outer(signal, outer_beta_samples)
    df_inner_draws = pd.DataFrame(
        np.hstack([risk[:, None], inner_draws]),
        columns=["risk"] + [f"draw_{i}" for i in range(settings["draws"]["num_draws"])],
    )
    df_outer_draws = pd.DataFrame(
        np.hstack([risk[:, None], outer_draws]),
        columns=["risk"] + [f"draw_{i}" for i in range(settings["draws"]["num_draws"])],
    )

    return df_inner_draws, df_outer_draws


def get_quantiles(
    settings: dict,
    summary: dict,
    signal_model: MRBeRT,
) -> tuple[DataFrame, DataFrame]:
    """Create risk curve quantiles for the pipeline.

    Parameters
    ----------
    settings
        The settings for complete the summary.
    summary
        The completed summary file.
    signal_model
        Fitted signal model risk curve.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        Inner and outer quantile files.

    """
    if settings["draws"]["risk_lower"] is None:
        risk_lower = summary["risk_bounds"][0]
    else:
        risk_lower = settings["draws"]["risk_lower"]
    if settings["draws"]["risk_upper"] is None:
        risk_upper = summary["risk_bounds"][1]
    else:
        risk_upper = settings["draws"]["risk_upper"]
    risk = np.linspace(risk_lower, risk_upper, settings["draws"]["num_points"])
    signal = get_signal(signal_model, risk)
    inner_beta_sd = summary["beta"][1]
    outer_beta_sd = np.sqrt(
        summary["beta"][1] ** 2 + summary["gamma"][0] + 2 * summary["gamma"][1]
    )
    # get quantiles
    quantiles = np.asarray(settings["draws"]["quantiles"])
    signal_sign_index = np.zeros(signal.size, dtype=int)
    signal_sign_index[signal < 0] = 1
    inner_beta_quantiles = [
        norm.ppf(quantiles, loc=summary["beta"][0], scale=inner_beta_sd),
        norm.ppf(1 - quantiles, loc=summary["beta"][0], scale=inner_beta_sd),
    ]
    inner_beta_quantiles = np.vstack(inner_beta_quantiles).T
    outer_beta_quantiles = [
        norm.ppf(quantiles, loc=summary["beta"][0], scale=outer_beta_sd),
        norm.ppf(1 - quantiles, loc=summary["beta"][0], scale=outer_beta_sd),
    ]
    outer_beta_quantiles = np.vstack(outer_beta_quantiles).T
    inner_quantiles = [
        inner_beta_quantiles[i][signal_sign_index] * signal
        for i in range(len(quantiles))
    ]
    inner_quantiles = np.vstack(inner_quantiles).T
    outer_quantiles = [
        outer_beta_quantiles[i][signal_sign_index] * signal
        for i in range(len(quantiles))
    ]
    outer_quantiles = np.vstack(outer_quantiles).T

    df_inner_quantiles = pd.DataFrame(
        np.hstack([risk[:, None], inner_quantiles]),
        columns=["risk"] + list(map(str, quantiles)),
    )
    df_outer_quantiles = pd.DataFrame(
        np.hstack([risk[:, None], outer_quantiles]),
        columns=["risk"] + list(map(str, quantiles)),
    )

    return df_inner_quantiles, df_outer_quantiles


def plot_signal_model(
    name: str,
    summary: dict,
    df: DataFrame,
    signal_model: MRBeRT,
    show_ref: bool = True,
) -> Figure:
    """Plot the signal model

    Parameters
    ----------
    name
        Name of the pair.
    summary
        Summary from the signal model.
    df
        Data frame contains training data.
    signal_model
        Fitted signal model for risk curve.
    show_ref
        Whether to show the reference line. Default is `True`.

    Returns
    -------
    Figure
        The figure object for signal model.

    """
    # create fig obj
    fig, ax = plt.subplots(figsize=(8, 5))

    # plot data
    _plot_data(name, summary, df, ax, signal_model=signal_model, show_ref=show_ref)

    # plot curve
    risk = np.linspace(*summary["risk_bounds"], 100)
    signal = get_signal(signal_model, risk)
    if summary["normalize_to_tmrel"]:
        signal -= signal.min()
    ax.plot(risk, signal, color="#008080")

    return fig


def plot_linear_model(
    name: str,
    summary: dict,
    df: DataFrame,
    signal_model: MRBeRT,
    linear_model: MRBRT,
    show_ref: bool = True,
) -> Figure:
    """Plot the linear model

    Parameters
    ----------
    name
        Name of the pair.
    summary
        Completed summary file.
    df
        Data frame contains the training data.
    signal_model
        Fitted signal model for risk curve.
    linear_model
        Fitted linear model for risk curve.
    show_ref
        Whether to show the reference line. Default is `True`.

    Returns
    -------
    Figure
        The figure object for linear model.

    """
    # create fig obj
    fig, ax = plt.subplots(1, 2, figsize=(16, 5))

    # plot data
    _plot_data(name, summary, df, ax[0], signal_model, linear_model, show_ref=show_ref)

    # plot curve and uncertainty
    beta = summary["beta"]
    gamma = summary["gamma"]
    risk = np.linspace(*summary["risk_bounds"], 100)
    signal = get_signal(signal_model, risk)

    inner_beta_sd = beta[1]
    outer_beta_sd = np.sqrt(beta[1] ** 2 + gamma[0] + 2 * gamma[1])

    pred = np.outer(
        np.array(
            [
                beta[0] - 1.96 * outer_beta_sd,
                beta[0] - 1.96 * inner_beta_sd,
                beta[0],
                beta[0] + 1.96 * inner_beta_sd,
                beta[0] + 1.96 * outer_beta_sd,
            ]
        ),
        signal,
    )

    if summary["normalize_to_tmrel"]:
        pred -= pred[:, [np.argmin(pred[2])]]

    log_bprf = pred[2] * (1.0 - 1.645 * outer_beta_sd / beta[0])

    ax[0].plot(risk, pred[2], color="#008080")
    ax[0].fill_between(risk, pred[0], pred[4], color="gray", alpha=0.2)
    ax[0].fill_between(risk, pred[1], pred[3], color="gray", alpha=0.2)
    ax[0].plot(risk, log_bprf, color="red")

    # plot funnel
    _plot_funnel(summary, df, ax[1])

    return fig


def _plot_data(
    name: str,
    summary: dict,
    df: DataFrame,
    ax: Axes,
    signal_model: MRBeRT = None,
    linear_model: Optional[MRBRT] = None,
    show_ref: bool = True,
) -> Axes:
    """Plot data points

    Parameters
    ----------
    name
        Name of the pair.
    summary
        The summary of the signal model.
    ax
        Axes of the figure. Usually corresponding to one panel of a figure.
    signal_model
        Fitted signal model for risk curve.
    linear_model
        Fitted linear model for risk curve. Default is `None`. When it is `None`
        the points are plotted reference to original signal model. When linear
        model is provided, the points are plotted reference to the linear model
        risk curve.
    show_ref
        Whether to show the reference line. Default is `True`.

    Returns
    -------
    Axes
        Return the axes back for further plotting.

    """
    # compute the position of the reference point
    ref_risk = df[["ref_risk_lower", "ref_risk_upper"]].values.mean(axis=1)
    alt_risk = df[["alt_risk_lower", "alt_risk_upper"]].values.mean(axis=1)
    ref_ln_rr = signal_model.predict(
        MRData(
            covs={
                "ref_risk_lower": np.repeat(summary["risk_bounds"][0], ref_risk.size),
                "ref_risk_upper": np.repeat(summary["risk_bounds"][0], ref_risk.size),
                "alt_risk_lower": ref_risk,
                "alt_risk_upper": ref_risk,
            }
        )
    )
    if linear_model is not None:
        ref_ln_rr *= linear_model.beta_soln[0]
    alt_ln_rr = ref_ln_rr + df.ln_rr.values

    # shift data position normalize to tmrel
    if summary["normalize_to_tmrel"]:
        risk = np.linspace(*summary["risk_bounds"], 100)
        signal = get_signal(signal_model, risk)
        if linear_model is not None:
            signal *= linear_model.beta_soln[0]
        ref_ln_rr -= signal.min()
        alt_ln_rr -= signal.min()

    # plot data points
    index = df.is_outlier == 1
    ax.scatter(
        alt_risk,
        alt_ln_rr,
        s=5 / df.ln_rr_se.values,
        color="#008080",
        alpha=0.5,
        edgecolor="none",
    )
    ax.scatter(
        alt_risk[index],
        alt_ln_rr[index],
        s=5 / df.ln_rr_se.values[index],
        color="red",
        alpha=0.5,
        marker="x",
    )
    if show_ref:
        for x_0, y_0, x_1, y_1 in zip(alt_risk, alt_ln_rr, ref_risk, ref_ln_rr):
            ax.plot([x_0, x_1], [y_0, y_1], color="#008080", linewidth=0.5, alpha=0.5)

    # plot support lines
    ax.axhline(0.0, linewidth=1, linestyle="-", color="gray")
    for b in summary["risk_score_bounds"]:
        ax.axvline(b, linewidth=1, linestyle="--", color="gray")

    # add title and label
    rei, _ = tuple(name.split("-"))
    ax.set_title(name.replace("-", " / "), loc="left")
    ax.set_xlabel(f"{rei} ({summary['risk_unit']})")
    ax.set_ylabel("ln relative risk")

    return ax


def _plot_funnel(
    summary: dict,
    df: DataFrame,
    ax: Axes,
) -> Axes:
    """Plot the funnel plot

    Parameters
    ----------
    summary
        Complete summary file.
    df
        Data frame that contains training data.
    ax
        Axes of the figure. Usually corresponding to one panel of a figure.

    Returns
    -------
    Axes
        Return the axes back for further plotting.

    """
    # add residual information
    beta, gamma = summary["beta"], summary["gamma"]
    residual = df.ln_rr.values - df.signal.values * beta[0]
    residual_sd = np.sqrt(df.ln_rr_se.values**2 + df.re_signal.values**2 * gamma[0])

    # plot funnel
    index = df.is_outlier == 1
    sd_max = residual_sd.max() * 1.1

    ax.set_ylim(sd_max, 0.0)
    ax.scatter(residual, residual_sd, color="#008080", alpha=0.5, edgecolor="none")
    ax.scatter(residual[index], residual_sd[index], color="red", alpha=0.5, marker="x")
    ax.fill_betweenx(
        [0.0, sd_max],
        [0.0, -1.96 * sd_max],
        [0.0, 1.96 * sd_max],
        color="gray",
        alpha=0.2,
    )
    ax.plot([0.0, -1.96 * sd_max], [0.0, sd_max], linewidth=1, color="gray")
    ax.plot([0.0, 1.96 * sd_max], [0.0, sd_max], linewidth=1, color="gray")
    ax.axvline(0.0, color="gray", linewidth=1, linestyle="--")
    ax.set_xlabel("residual")
    ax.set_ylabel("residual sd")

    return ax

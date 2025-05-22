import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bopforge.utils import get_beta_info, get_gamma_info
from matplotlib.pyplot import Axes, Figure
from mrtool import MRBRT, CovFinder, LinearCovModel, MRData
from pandas import DataFrame
from scipy.stats import norm


def get_signal_model(settings: dict, df: DataFrame) -> MRBRT:
    """Create signal model for outliers identification and covariate selection
    step.

    Parameters
    ----------
    settings
        Dictionary contains all the settings.
    df
        Data frame contains the training data.

    Returns
    -------
    MRBRT
        Signal model to access the strength of the prior on the bias-covariate.

    """
    col_covs = [col for col in df.columns if col.startswith("cov_")]
    data = MRData()
    data.load_df(
        df,
        col_obs="ln_rr",
        col_obs_se="ln_rr_se",
        col_covs=col_covs,
        col_study_id="study_id",
        col_data_id="seq",
    )
    cov_models = [LinearCovModel("intercept", use_re=False)]

    signal_model = MRBRT(data, cov_models, **settings["signal_model"])

    return signal_model


def add_cols(df: DataFrame, signal_model: MRBRT) -> DataFrame:
    """Add columns of outlier indicator.

    Parameters
    ----------
    df
        Data frame that contains the training data.
    signal_model
        Fitted signal model.

    Returns
    -------
    DataFrame
        DataFrame with additional columns of oulier indicator.

    """
    data = signal_model.data
    is_outlier = (signal_model.w_soln < 0.1).astype(int)
    df = df.merge(
        pd.DataFrame({"seq": data.data_id, "is_outlier": is_outlier}),
        how="outer",
        on="seq",
    )

    return df


def get_signal_model_summary(name: str, df: DataFrame) -> dict:
    """Create signal model summary.

    Parameters
    ----------
    name
        Name of the pair.
    df
        Data frame that contains the training dataset.

    Returns
    -------
    dict
        Summary dictionary from the signal model.

    """
    summary = {
        "name": name,
        "risk_type": str(df.risk_type.values[0]),
    }
    return summary


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
    beta_info = get_beta_info(cov_finder_linear_model, cov_name="intercept")

    # covariate selection
    if "intercept" not in settings["cov_finder"]["pre_selected_covs"]:
        settings["cov_finder"]["pre_selected_covs"].append("intercept")
    candidate_covs = [
        cov_name
        for cov_name in data.covs.keys()
        if cov_name not in settings["cov_finder"]["pre_selected_covs"] + ["intercept"]
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


def get_cov_finder_result(cov_finder_linear_model: MRBRT, cov_finder: MRBRT) -> dict:
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
    beta_info = get_beta_info(cov_finder_linear_model, cov_name="intercept")
    selected_covs = [
        cov_name for cov_name in cov_finder.selected_covs if cov_name != "intercept"
    ]

    # save results
    cov_finder_result = {
        "beta_sd": float(beta_info[1] * 0.1),
        "selected_covs": selected_covs,
    }

    return cov_finder_result


def get_linear_model(df: DataFrame, cov_finder_result: dict) -> MRBRT:
    """Create linear model for effect.

    Parameters
    ----------
    df
        Data frame contains training data without outliers.
    cov_finder_result
        Summary result for bias covariate selection.

    Returns
    -------
    MRBRT
        The linear model for effect.

    """
    data = MRData()
    data.load_df(
        df,
        col_obs="ln_rr",
        col_obs_se="ln_rr_se",
        col_covs=cov_finder_result["selected_covs"],
        col_study_id="study_id",
        col_data_id="seq",
    )
    cov_models = [
        LinearCovModel("intercept", use_re=True),
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
    summary: dict,
    df: DataFrame,
    linear_model: MRBRT,
) -> dict:
    """Complete the summary from the signal model.

    Parameters
    ----------
    summary
        Summary from the signal model.
    df
        Data frame contains the all dataset.
    linear_model
        Fitted linear model for effect.

    Returns
    -------
    dict
        Summary file contains all necessary information.

    """
    beta_info = get_beta_info(linear_model, cov_name="intercept")
    gamma_info = get_gamma_info(linear_model)
    summary["beta"] = [float(beta_info[0]), float(beta_info[1])]
    summary["gamma"] = [float(gamma_info[0]), float(gamma_info[1])]

    # compute the score and add star rating
    beta_sd = np.sqrt(beta_info[1] ** 2 + gamma_info[0] + 2 * gamma_info[1])
    sign = np.sign(beta_info[0])
    inner_ui = beta_info[0] - sign * 1.96 * beta_info[1]
    burden_of_proof = beta_info[0] - sign * 1.645 * beta_sd

    if inner_ui * beta_info[0] < 0:
        summary["score"] = float("nan")
        summary["star_rating"] = 0
    else:
        score = float(0.5 * sign * burden_of_proof)
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
    residual = df.ln_rr.values[index] - beta_info[0]
    residual_sd = np.sqrt(df.ln_rr_se.values[index] ** 2 + gamma_info[0])
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
) -> tuple[DataFrame, DataFrame]:
    """Create effect draws for the pipeline.

    Parameters
    ----------
    settings
        Settings for complete the summary.
    summary
        Summary of the models.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        Inner and outer draw files.

    """
    beta_info = summary["beta"]
    gamma_info = summary["gamma"]
    inner_beta_sd = beta_info[1]
    outer_beta_sd = np.sqrt(beta_info[1] ** 2 + gamma_info[0] + 2 * gamma_info[1])
    inner_beta_samples = np.random.normal(
        loc=beta_info[0], scale=inner_beta_sd, size=settings["draws"]["num_draws"]
    )
    outer_beta_samples = np.random.normal(
        loc=beta_info[0], scale=outer_beta_sd, size=settings["draws"]["num_draws"]
    )
    df_inner_draws = pd.DataFrame(
        inner_beta_samples[None, :],
        columns=[f"draw_{i}" for i in range(settings["draws"]["num_draws"])],
    )
    df_outer_draws = pd.DataFrame(
        outer_beta_samples[None, :],
        columns=[f"draw_{i}" for i in range(settings["draws"]["num_draws"])],
    )

    return df_inner_draws, df_outer_draws


def get_quantiles(
    settings: dict,
    summary: dict,
) -> tuple[DataFrame, DataFrame]:
    """Create effect quantiles for the pipeline.

    Parameters
    ----------
    settings
        The settings for complete the summary.
    summary
        The completed summary file.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        Inner and outer quantile files.

    """
    beta_info = summary["beta"]
    gamma_info = summary["gamma"]
    inner_beta_sd = beta_info[1]
    outer_beta_sd = np.sqrt(beta_info[1] ** 2 + gamma_info[0] + 2 * gamma_info[1])
    inner_beta_quantiles = norm.ppf(
        settings["draws"]["quantiles"], loc=beta_info[0], scale=inner_beta_sd
    )
    outer_beta_quantiles = norm.ppf(
        settings["draws"]["quantiles"], loc=beta_info[0], scale=outer_beta_sd
    )

    df_inner_quantiles = pd.DataFrame(
        inner_beta_quantiles[None, :],
        columns=[str(q) for q in settings["draws"]["quantiles"]],
    )
    df_outer_quantiles = pd.DataFrame(
        outer_beta_quantiles[None, :],
        columns=[str(q) for q in settings["draws"]["quantiles"]],
    )

    return df_inner_quantiles, df_outer_quantiles


def plot_linear_model(
    summary: dict,
    df: DataFrame,
) -> Figure:
    """Plot the linear model

    Parameters
    ----------
    summary
        Completed summary file.
    df
        Data frame contains the training data.

    Returns
    -------
    Figure
        The figure object for linear model.

    """
    # create fig obj
    fig, ax = plt.subplots(figsize=(8, 5))

    # plot funnel
    _plot_funnel(summary, df, ax)
    ax.set_title(summary["name"].replace("-", " / "), loc="left")

    return fig


def _plot_funnel(summary: dict, df: DataFrame, ax: Axes) -> Axes:
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
    beta_inner_sd = beta[1]
    beta_outer_sd = np.sqrt(beta[1] ** 2 + gamma[0] + 2.0 * gamma[1])
    beta_inner = [beta[0] - 1.96 * beta_inner_sd, beta[0] + 1.96 * beta_inner_sd]
    beta_outer = [beta[0] - 1.96 * beta_outer_sd, beta[0] + 1.96 * beta_outer_sd]

    # plot data
    ax.scatter(df.ln_rr, df.ln_rr_se, color="#008080", alpha=0.4, edgecolor="none")
    outlier_index = df.is_outlier == 1
    ax.scatter(
        df.ln_rr[outlier_index],
        df.ln_rr_se[outlier_index],
        color="red",
        alpha=0.4,
        marker="x",
    )

    # plot funnel
    se_max = df.ln_rr_se.max()
    ax.fill_betweenx(
        [0.0, se_max],
        [beta[0], beta[0] - 1.96 * se_max],
        [beta[0], beta[0] + 1.96 * se_max],
        color="gray",
        alpha=0.2,
    )
    ax.plot(
        [beta[0], beta[0] - 1.96 * se_max], [0.0, se_max], linewidth=1, color="gray"
    )
    ax.plot(
        [beta[0], beta[0] + 1.96 * se_max], [0.0, se_max], linewidth=1, color="gray"
    )
    ax.set_ylim([se_max, 0.0])

    # plot vertical lines
    ax.axvline(beta[0] - np.sign(beta[0]) * 1.645 * beta_outer_sd, color="red")
    ax.axvline(0.0, color="gray", linestyle="--", linewidth=1)
    ax.axvline(beta[0], color="#008080")
    ax.fill_betweenx(
        [0.0, se_max],
        [beta_inner[0]] * 2,
        [beta_inner[1]] * 2,
        color="#008080",
        alpha=0.2,
    )
    ax.fill_betweenx(
        [0.0, se_max],
        [beta_outer[0]] * 2,
        [beta_outer[1]] * 2,
        color="#008080",
        alpha=0.2,
    )

    # set title and labels
    ax.set_xlabel("ln_rr")
    ax.set_ylabel("ln_rr_se")

    return ax

"""
Module to add uncertainty related columns to clinical data. This was inherited from the
elmo repo around 2019. Most methods remain the same but some updates were made to optimize
code by operating vector-wise rather than element-wise.

Some important abbreviations used throughout-
ss: sample size
ess: effective sample size
ui: uncertainty interval, eg lower, upper
se: standard error
p: mean values

"""
import warnings
from typing import List, Optional

import numpy as np
import pandas as pd
from scipy import stats
from scipy.interpolate import interp1d
from statsmodels.distributions.empirical_distribution import ECDF

# This map is from From elmo.uncertainty
measure_map = {
    3: ("proportion", "non-ratio"),  # YLD
    5: ("proportion", "non-ratio"),  # Prevalence
    6: ("rate", "non-ratio"),  # Incidence
    7: ("rate", "non-ratio"),  # Remission
    8: ("rate", "non-ratio"),  # Duration
    9: ("rate", "non-ratio"),  # Exccess Mortality Rate
    10: ("rate", "non-ratio"),  # Prevalence * Excess Mortality Rate
    11: ("rate", "ratio"),  # Relative Risk
    12: ("rate", "ratio"),  # Standardized Mortality Ratio
    13: ("rate", "non-ratio"),  # With-Condition Mortality Rate
    14: ("rate", "non-ratio"),  # All-Cause Mortality Rate
    15: ("rate", "non-ratio"),  # Cause-Specific Mortality Rate
    16: ("rate", "non-ratio"),  # Other Cause Mortality Rate
    17: ("proportion", "non-ratio"),  # Case Fatality Rate
    18: ("proportion", "non-ratio"),  # Proportion
    19: ("rate", "non-ratio"),  # Continuous
    41: ("rate", "non-ratio"),  # Susceptible incidence
    42: ("rate", "non-ratio"),
}  # Total incidence

# From epi.uncertainty_type
uncertainty_type_map = {
    1: "Standard error",
    2: "Effective sample size",
    3: "Confidence interval",
    4: "Sample size",
}


class UncertaintyError(Exception):
    pass


def fill(df: pd.DataFrame, propagate_cols: Optional[List[str]] = None) -> pd.DataFrame:
    """Main interface for modifying a dataframe to add all types of uncertainty values
    required for upload and the refresh process.

    Args:
        df (pd.DataFrame): Table to calcuate uncertainty values for.
        propagate_cols (List[str], optional): Any standard error columns that need
            to be included into the 'standard_error' column calculated by
            fill_uncertainty().
            NOTE: All propagate_cols must be in the same space as 'standard_error'.

    Returns:
        pd.DataFrame: Input table with uncertainty and uncertainty interval added.
    """
    df = prep_cols(df=df)
    _check_input(df=df)
    if len(df) == 0:
        warnings.warn("Dataframe is empty.")
        return df

    df = fill_mean_ss_cases(df=df)
    df = fill_uncertainty_type(df=df)
    df = fill_ess_ss(df=df)
    df = fill_uncertainty(df=df)
    if propagate_cols:
        df = propagate_uncertainty(df=df, propagate_cols=propagate_cols)

    df = update_zero_upper_se(df=df)
    validate_ui_se_values(df=df)

    return df


def prep_cols(df: pd.DataFrame) -> pd.DataFrame:
    # We have two types of columns, required and additional. Required columns
    # are columns that we cannot run calculations with (see _check_input for
    # more requirements) . Additional columns are columns that we can add on
    # and fill if they are missing.

    _additional_columns = [
        "cases",
        "standard_error",
        "effective_sample_size",
        "sample_size",
        "uncertainty_type",
        "uncertainty_type_value",
        "uncertainty_type_id",
        "lower",
        "upper",
        "mean",
    ]

    # If any of the additional columns don't exists create them
    for column in _additional_columns:
        if column not in df:
            df[column] = None

    return df


def _check_input(df: pd.DataFrame) -> None:
    """Checks if df can be used to generate uncertainty.

    :raises: UncertaintyError
    :returns: None
    :rtype: NoneType

    """
    # Check if all required columns are present
    _required_columns = ["measure_id"]
    missing_columns = [col for col in _required_columns if col not in df.columns]
    if missing_columns:
        raise UncertaintyError(f"Missing required columns: {missing_columns}")

    # All measures must be in the measure_map
    measure_ids = df.measure_id.unique()
    illegal_measures = set(measure_ids) - set(measure_map.keys())
    if illegal_measures:
        raise UncertaintyError(
            f"The following measures are not valid: {illegal_measures}\n"
            f"Valid measures: {list(measure_map.keys)}"
        )

    # To calculate the mean we must either have
    # the mean OR (cases & (SS/ESS))
    can_calc_mean = df["mean"].notnull() | (
        df.cases.notnull() & (df.sample_size.notnull() | df.effective_sample_size.notnull())
    )
    if not can_calc_mean.all():
        raise UncertaintyError(
            f"Can't calculate the mean for the following rows: " f"{df.index[~can_calc_mean]}"
        )
    # All rows must have SE, ESS, SS or (upper and lower) to
    # calculate uncertainty
    can_calc_uncertainty = (
        df.standard_error.notnull()
        | df.effective_sample_size.notnull()
        | df.sample_size.notnull()
        | (df.lower.notnull() & df.upper.notnull())
    )
    if not can_calc_uncertainty.all():
        raise UncertaintyError(
            f"Can't calculate the uncertainty for the following rows: "
            f"{df.index[~can_calc_uncertainty]}"
        )


def fill_mean_ss_cases(df: pd.DataFrame) -> pd.DataFrame:
    """Fill the mean / sample size / cases column based on the values
    of the other two"""

    df.loc[df["mean"].isnull(), "mean"] = (
        df.loc[df["mean"].isnull(), "cases"] / df.loc[df["mean"].isnull(), "sample_size"]
    )

    df.loc[df.cases.isnull(), "cases"] = (
        df.loc[df.cases.isnull(), "mean"] * df.loc[df.cases.isnull(), "sample_size"]
    )

    # weird bug where sample_size is broadcast to list of nans
    # if cases is column of nans of dtype O
    case_col = df.loc[df.sample_size.isnull(), "cases"]
    if case_col.dtype.char == "O":
        case_col = case_col.astype("float")

    df.loc[df.sample_size.isnull(), "sample_size"] = (
        case_col / df.loc[df.sample_size.isnull(), "mean"]
    )
    return df


def fill_ess_ss(df: pd.DataFrame) -> pd.DataFrame:
    """
    fill in effective_sample_size from sample_size so that uncertainty
    can be recalculated
    """
    df.loc[df.effective_sample_size.isnull(), "effective_sample_size"] = df.loc[
        df.effective_sample_size.isnull(), "sample_size"
    ]
    return df


def fill_uncertainty_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    fill in uncertainty_type and uncertainty_type_id for each row given
    the available uncertainties
    """

    df.loc[
        (df.uncertainty_type.isnull()) & (df.standard_error.notnull()),
        "uncertainty_type_id",
    ] = 1

    df.loc[
        (df.uncertainty_type.isnull()) & (df.effective_sample_size.notnull()),
        "uncertainty_type_id",
    ] = 2

    df.loc[
        (df.uncertainty_type.isnull()) & (df.lower.notnull()) & (df.upper.notnull()),
        "uncertainty_type_id",
    ] = 3

    df.loc[
        (df.uncertainty_type.isnull()) & (df.sample_size.notnull()),
        "uncertainty_type_id",
    ] = 4

    uncertainty = df.uncertainty_type_id.replace(uncertainty_type_map)
    df.loc[:, "uncertainty_type"] = uncertainty
    return df


def fill_uncertainty(df: pd.DataFrame) -> pd.DataFrame:
    """Selects the correct uncertainty back-calculation method based
    on the measure type and fills uncertainty in the input dataframe"""

    df.loc[df.uncertainty_type_value.isnull(), "uncertainty_type_value"] = 95

    filled_outputs = []
    for measure_id, measure_df in df.groupby("measure_id"):
        ptype, rtype = measure_map[measure_id]
        filled_outputs.append(fill_single_measure(measure_df, measure_id, ptype, rtype))
    return pd.concat(filled_outputs)


def se_from_ess(
    p: np.ndarray, ess: np.ndarray, param_type: str, quantile: float = 0.975
) -> np.ndarray:
    """Calculates standard error from effective sample size and mean."""
    if param_type not in ["proportion", "rate"]:
        raise UncertaintyError(
            f"Unknown paramater: {param_type} while calculating SE from ESS"
        )

    cases = p * ess

    if param_type == "proportion":
        if (p > 1).any():
            raise UncertaintyError("Cannot create uncertainty for a proportion larger than 1.")
        # Use wilson's method to calculate uncertainty for proportions
        # https://en.wikipedia.org/wiki/Binomial_proportion_confidence_
        # interval#Wilson_score_interval
        z = stats.norm.ppf(quantile)
        return np.sqrt(p * (1 - p) / ess + z**2 / (4 * ess**2))
    elif param_type == "rate":
        # If we have 5 or less cases interpolate the SE.
        # If we have more than 5 cases use the SE from a poisson distribution
        interpolate_se = ((5 - p * ess) / ess + p * ess * np.sqrt(5 / ess**2)) / 5
        poisson_se = np.sqrt(p / ess)
        return np.where(cases > 5, poisson_se, interpolate_se)
    else:
        raise UncertaintyError("Invalid param_type encountered while calculating SE from ESS")


def ui_from_ess(
    p: np.ndarray,
    ess: np.ndarray,
    param_type: str,
    measure_id: int,
    quantile: float = 0.975,
) -> pd.DataFrame:
    """Calculates the lower and upper bound of the uncertainty interval"""
    cases = p * ess
    se = se_from_ess(p, ess, param_type=param_type, quantile=quantile)
    z = stats.norm.ppf(quantile)

    lower = np.repeat(0.0, len(cases))
    upper = np.repeat(0.0, len(cases))

    # Wilson's method:
    use_wilsons_mask = np.repeat(False, len(cases))
    if measure_id == 17:  # 17 is CFR - Case Fatality Rate
        use_wilsons_mask = ~use_wilsons_mask
    elif param_type == "proportion":
        use_wilsons_mask = cases > 5
    if use_wilsons_mask.any():
        mid = 1 / (1 + z**2 / ess) * (p + z**2 / (2 * ess))
        diff = z * se
        np.putmask(lower, use_wilsons_mask, mid - diff)
        np.putmask(upper, use_wilsons_mask, mid + diff)

    # Calculate from SE:
    use_se_mask = np.repeat(False, len(cases))
    if param_type == "rate":
        use_se_mask = ~use_se_mask
    else:
        use_se_mask = cases <= 5
    if use_se_mask.any():
        ui = ui_from_se(
            p=p[use_se_mask],
            se=se[use_se_mask],
            param_type=param_type,
        )
        lower[use_se_mask] = ui["lower"]
        upper[use_se_mask] = ui["upper"]

    lower[lower < 0] = 0

    return pd.DataFrame({"lower": lower, "upper": upper})


def se_ui_from_ess(
    p: np.ndarray,
    ess: np.ndarray,
    param_type: str,
    measure_id: int,
) -> pd.DataFrame:
    """calculates standard error and upper and lower estimates using the
    estimated sample size"""
    result = ui_from_ess(p, ess, param_type, measure_id)
    result["standard_error"] = se_from_ess(p, ess, param_type)
    return result[["lower", "upper", "standard_error"]]


def se_from_ui(
    p: np.ndarray,
    lower: np.ndarray,
    upper: np.ndarray,
    method: str,
    confidence: int = 0.95,
) -> np.ndarray:
    """Calculates standard error from the uncertainty interval"""

    if method not in ["ratio", "non-ratio"]:
        raise UncertaintyError(
            "Received a bad method: {} while calculating SE from UI".format(method)
        )

    quantile = 1 - (1 - confidence) / 2
    if method == "ratio":
        n = np.log(upper) - np.log(lower)
        d = 2 * stats.norm.ppf(quantile)
        se = (np.exp(n / d) - 1) * p
    elif method == "non-ratio":
        high = upper - p
        low = p - lower
        se = np.where(high > low, high, low)
        se = se / stats.norm.ppf(quantile)
    return se


def ui_from_se(
    p: np.ndarray, se: np.ndarray, param_type: str, confidence: int = 0.95
) -> pd.DataFrame:
    """Calculates Uncertainty interval based on standard error"""
    lower_quantile = (1 - confidence) / 2
    upper_quantile = 1 - lower_quantile

    lower = p + stats.norm.ppf(lower_quantile) * se
    lower[lower < 0] = 0

    upper = p + stats.norm.ppf(upper_quantile) * se
    if param_type == "proportion":
        upper[upper > 1] = 1

    return pd.DataFrame({"lower": lower, "upper": upper})


def fill_single_measure(
    df: pd.DataFrame, measure_id: int, param_type: str, ratio_type: str
) -> pd.DataFrame:
    """Fills in uncertainty for a single measure

    :param df: a pd.DataFrame
    :param measure_id int: the measure id of the data in the df
    :param_type str: "proportion" or "rate"
    :ratio_type str: "ratio" or "non-ratio"

    :return: the original df with uncertainty estimates filled in
    :rtype: pd.DataFrame

    """

    missing_se = pd.isnull(df["standard_error"])
    missing_ui = pd.isnull(df["lower"]) | pd.isnull(df["upper"])

    # The subset of the dataframe where SE and UI are already filled
    df_ok = df[~(missing_se) & ~(missing_ui)]

    # Subset where SE is missing, but UI is present
    df_se_from_ui = df[(missing_se) & ~(missing_ui)]
    df_se_from_ui.reset_index(inplace=True, drop=True)
    if not df_se_from_ui.empty:
        df_se_from_ui["standard_error"] = se_from_ui(
            p=df_se_from_ui["mean"].astype(float).values,
            lower=df_se_from_ui["lower"].astype(float).values,
            upper=df_se_from_ui["upper"].astype(float).values,
            method=ratio_type,
            confidence=(df_se_from_ui["uncertainty_type_value"].astype(float).values / 100),
        )

    # Subset where UI is missing, but SE is present
    df_ui_from_se = df[~(missing_se) & (missing_ui)]
    df_ui_from_se.reset_index(inplace=True, drop=True)
    if not df_ui_from_se.empty:
        df_ui_from_se[["lower", "upper"]] = ui_from_se(
            p=df_ui_from_se["mean"].astype(float).values,
            se=df_ui_from_se["standard_error"].astype(float).values,
            param_type=param_type,
        )

    # Subset where SE and UI are missing, but ESS is present
    df_se_ui_from_ess = df[(missing_se) & (missing_ui)]
    df_se_ui_from_ess.reset_index(inplace=True, drop=True)
    if not df_se_ui_from_ess.empty:
        result = se_ui_from_ess(
            p=df_se_ui_from_ess["mean"].astype(float).values,
            ess=df_se_ui_from_ess["effective_sample_size"].astype(float).values,
            param_type=param_type,
            measure_id=measure_id,
        )

        df_se_ui_from_ess.upper = result.upper
        df_se_ui_from_ess.lower = result.lower
        df_se_ui_from_ess.standard_error = result.standard_error

    # Consolidate all filled dataframes
    filled_df = pd.concat(
        [df_ok, df_se_from_ui, df_ui_from_se, df_se_ui_from_ess], ignore_index=True
    )

    if df.shape != filled_df.shape:
        raise UncertaintyError("Creating uncertainty changed the shape of the data")

    return filled_df


def combine_standard_error(df: pd.DataFrame, standard_error_cols: List[str]) -> pd.DataFrame:
    """Combines any standard error columns into a single 'standard_error'.

    Args:
        df (pd.DataFrame): Table with standard error column(s).
        standard_error_cols (List[str]): Any standard error columns that
            need to be combined.

    Returns:
        pd.DataFrame: Input table with error columns combined.
    """

    # Rename them in case of overlap with final_col.
    col_og_map = {col: f"{col}_og" for col in standard_error_cols}
    df = df.rename(columns=col_og_map)
    se_cols = list(col_og_map.values())

    # Take the sqrt of their squared sum.
    df["standard_error"] = ((df[se_cols] ** 2).sum(axis=1)) ** 0.5
    df = df.drop(se_cols, axis=1)

    return df


def propagate_uncertainty(df: pd.DataFrame, propagate_cols: List[str]) -> pd.DataFrame:
    """For any sources with additional uncertainty included from processing,
    propagate these uncertainties into the base 'standard_error' value.
    NOTE: All propagate_cols must be in the same space as 'standard_error'.

    Args:
        df (pd.DataFrame): Table with standard error column(s).
        propagate_cols (List[str]): Uncertainty columns that need to be
            propagated into 'standard_error'.

    Raises:
        KeyError: If standard_error column is not present.
        KeyError: An expected error column is not present.

    Returns:
        pd.DataFrame: Input table with error columns combined and a new ui interval.
    """

    if "standard_error" not in df.columns:
        raise KeyError("Missing 'standard_error' column to propagate into.")

    if len(set(propagate_cols) - set(df.columns)) > 0:
        raise KeyError("Missing expected additional error column.")

    # Reset ui to avoid 'df_ok' in fill_single_measure()
    df["lower"], df["upper"] = np.nan, np.nan
    standard_error_cols = propagate_cols + ["standard_error"]
    df = combine_standard_error(df=df, standard_error_cols=standard_error_cols)
    df = fill_uncertainty(df=df)

    return df


def add_ui(df: pd.DataFrame, drop_draw_cols: bool = False) -> pd.DataFrame:
    """
    Given a dataframe with draws present use them to calculate 4 measures of uncertainty-
    mean, upper, lower, and median.
    """

    draw_columns = df.filter(regex="^draw").columns.tolist()
    draws_df = df[draw_columns].values

    df["mean"] = df[draw_columns].mean(axis=1)

    df["median_CI_team_only"] = np.nan
    df["median_CI_team_only"] = df[draw_columns].median(axis=1)

    df["lower"] = np.percentile(draws_df, 2.5, axis=1)
    df["upper"] = np.percentile(draws_df, 97.5, axis=1)

    if drop_draw_cols:
        df = df.drop(draw_columns, axis=1)

    return df


def update_zero_upper_se(df: pd.DataFrame, cap: float = 0.01) -> pd.DataFrame:
    """There are edge cases where upper==lower==0 due to random sampling
    where over 97.5% of the draws are 0 but mean>0. SE would be 0 as well
    as a results. We replace them with the smallest non-zero upper/SE
    if the total number of such rows are under our cap.

    Args:
        df: dataframe with UI and SE added.
        cap: ratio of zero-upper rows. Defaults to 0.001.

    Raises:
        ValueError: if cases column is not present
        RuntimeError: if the ratio of zero-upper rows is too high

    Returns:
        dataframe with 0 upper and SE replaced.
    """
    if "cases" not in df.columns:
        raise ValueError("Missing cases column in the df")

    zero_pct = len(df.loc[(df["cases"] == 0) & (df["upper"] == 0)]) / len(
        df.loc[df["cases"] == 0]
    )
    print(f"There are {zero_pct * 100}% rows with zero upper among all row with cases 0.")
    if zero_pct > cap:
        raise RuntimeError("Too many rows with upper == 0 where there are no cases")

    min_non_zero_upper = min(df.loc[(df["cases"] == 0) & (df["upper"] > 0)]["upper"])
    df.loc[(df["cases"] == 0) & (df["upper"] == 0), "upper"] = min_non_zero_upper

    min_non_zero_se = min(
        df.loc[(df["cases"] == 0) & (df["standard_error"] > 0)]["standard_error"]
    )
    df.loc[
        (df["cases"] == 0) & (df["standard_error"] == 0), "standard_error"
    ] = min_non_zero_se

    return df


def validate_ui_se_values(df: pd.DataFrame) -> None:
    """Validate UI and SE values. Namely
    upper > lower
    SE > 0
    upper >= mean

    Args:
        df: dataframe with UI and SE filled
    """
    query1 = "upper <= lower"
    query2 = "standard_error <= 0"
    query3 = "upper < mean"

    if not df.query(query1).empty:
        raise RuntimeError("There are row(s) where upper <= lower")
    if not df.query(query2).empty:
        raise RuntimeError("There are row(s) where SE <= 0")
    if not df.query(query3).empty:
        raise RuntimeError("There are row(s) where upper < mean")


def fill_zero_draws(df: pd.DataFrame, draw_cols: List[str]) -> pd.DataFrame:
    """Fill all draws with 0 for mean-zero rows only

    Args:
        df (pd.DataFrame): Clinical data to generate draws from.
                            All rows here should have mean 0.
        draw_cols (List[str]): Draw columns to create and fill, eg 'draw_1', 'draw_2', etc.

    Returns:
        pd.DataFrame: Clinical data with each draw_col == 0.
    """
    df_list = [df]
    for draw in draw_cols:
        single_draw_df = pd.DataFrame({draw: [0 for _ in range(len(df))]})
        df_list.append(single_draw_df)

    df = pd.concat(df_list, axis=1)

    df = df.drop("mean", axis=1)

    return df


def draws_from_poisson(df: pd.DataFrame, draw_cols: List[str]) -> pd.DataFrame:
    """Create admission draws from a random poisson distribution.

    Args:
        df (pd.DataFrame): Clinical data to generate draws from. Must include a 'mean' and
                           'sample_size' column in order to create cases to use to pull draws.
        draw_cols (List[str]): Draw columns to create and fill, eg 'draw_1', 'draw_2', etc.

    Returns:
        pd.DataFrame: Clinical data with each draw_col in rate space.
    """
    # set seed to ensure consistency between runs
    np.random.seed(1)

    df["cases"] = df["mean"] * df["sample_size"]

    df_list = [df]
    for draw in draw_cols:
        single_draw_df = pd.DataFrame(
            {draw: np.random.poisson(lam=df["cases"].tolist(), size=len(df))}
        )
        single_draw_df[draw] = single_draw_df[draw] / df["sample_size"]
        df_list.append(single_draw_df)

    df = pd.concat(df_list, axis=1)

    df = df.drop(["cases", "mean"], axis=1)

    return df


def draws_from_invECDF(df: pd.DataFrame, draw_cols: List[str]) -> pd.DataFrame:
    """Create admission draws from inverse Empirical CDF for mean zero rows only.

    Args:
        df (pd.DataFrame): Clinical data to generate draws from. Must include a 'mean' and
                           'sample_size' column in order to create cases to use to pull draws.
        draw_cols (List[str]): Draw columns to create and fill, eg 'draw_1', 'draw_2', etc.

    Returns:
        pd.DataFrame: Clinical data with each draw_col in rate space.
    """
    if not all(df["mean"] == 0):
        raise RuntimeError("Not all means in the given df are 0!")

    # make confidence interval df with lower and upper bounds
    CI_matrix = make_CI_matrix(df)
    draws_df = pd.DataFrame(
        list(CI_matrix.apply(invECDF_sampling, draw_cols=draw_cols, axis=1)), columns=draw_cols
    )
    draws_df = draws_df.replace(-np.Inf, 0)

    df = pd.concat([df, draws_df.reset_index(drop=True)], axis=1)

    df = df.drop("mean", axis=1)

    return df


def make_CI_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Create a matrix of confidence interval bounds.
    Assume 1001 quantiles and approximate as rate/incidence, which would result in
    2002 CI bounds.

    Args:
        df (pd.DataFrame): Clinical data to generate draws from. Must include a 'mean' and
                           'sample_size' column.

    Returns:
        pd.DataFrame: 2002 CI bounds.
    """
    df_list = []

    for i in np.arange(0, 1.001, 0.001):
        conf = ui_from_ess(
            p=df["mean"].astype(float).values,
            ess=df["sample_size"].astype(float).values,
            param_type="rate",
            measure_id=6,
            quantile=i,
        )
        draws_df = pd.DataFrame({f"low_{i}": conf["lower"], f"upper_{i}": conf["upper"]})
        del conf
        df_list.append(draws_df)

    confs = pd.concat(df_list, axis=1)
    del df_list

    return confs


def invECDF_sampling(row: pd.Series, draw_cols: List[str]) -> pd.Series:
    """Use the inverse transform method to create a random number generator from a
    random sample by estimating the inverse CDF function using interpolation.

    Args:
        row (pd.Series): a single row of the CI bounds.
        draw_cols (List[str]): Draw columns to create and fill, eg 'draw_1', 'draw_2', etc.

    Returns:
        pd.Series: interpolated random draws.
    """
    # set seed to ensure consistency between runs
    np.random.seed(1)

    ecdf = ECDF(row)
    inv_cdf = interp1d(ecdf.y, ecdf.x, bounds_error=False, assume_sorted=False, fill_value=0)
    r = np.random.uniform(0, 1, len(draw_cols))
    ys = inv_cdf(r)

    return ys
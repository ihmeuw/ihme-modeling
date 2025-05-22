"""Module containing high level api for splitting"""

from typing import Literal

import numpy as np
import pandas as pd

from pydisagg.models import DisaggModel, LogOddsModel
from pydisagg.typing import DataFrame, NDArray


def split_datapoint(
    observed_total: float,
    bucket_populations: NDArray,
    rate_pattern: NDArray,
    observed_total_se: float | None = None,
    model: DisaggModel = LogOddsModel(),
    output_type: Literal["count", "rate"] = "count",
    normalize_pop_for_average_type_obs: bool = False,
    pattern_covariance: NDArray | None = None,
) -> tuple | NDArray:
    """Disaggregate a datapoint using the model given as input.
    Defaults to assuming multiplicativity in the odds ratio

    * If output_type=='total', then this outputs estimates for the observed
      amount in each group such that the sum of the point estimates equals the
      original total
    * If output_type=='rate', then this estimates rates for each group
      (and doesn't multiply the rates out by the population)


    Parameters
    ----------
    observed_total
        aggregated observed_total across all buckets, value to be split
    bucket_populations
        population size in each bucket
    rate_pattern
        Rate Pattern to use, should be an estimate of the rates in each bucket
            that we want to rescale
    observed_total_se
        standard error of observed_total, by default None
    output_type
        One of 'total' or 'rate'
        Type of splitting to perform, whether to disaggregate and return the
        estimated total in each group, or estimate the rate per population unit.
    model
        DisaggModel to use, by default LMOModel(1)
    normalize_pop_for_average_type_obs
        Whether or not to normalize populations to sum to 1, this is appropriate
        when the output_type is rate and when the aggregated observation is an
        average--whether an aggregated rate or a mean of a continuous measure
        over different groups
    pattern_covariance
        2d Numpy array with covariance matrix of pattern.

    Returns
    -------
    tuple | NDArray
        If standard errors are available, this will return the tuple
            (
                estimate_in_each_bucket,
                se_of_estimate_bucket,
            )
        Otherwise, if standard errors are not available,
        this will return a numpy array of the disaggregated estimates

    Notes
    -----
    If no observed_total_se is given, returns point estimates
    If observed_total_se is given, then returns a tuple
        (point_estimate,standard_error)

    """
    if not np.issubdtype(bucket_populations.dtype, np.number):
        raise ValueError("All elements in bucket_populations must be numeric")
    if not np.issubdtype(rate_pattern.dtype, np.number):
        raise ValueError("All elements in rate_pattern must be numeric")

    if output_type not in ["count", "rate"]:
        raise ValueError("output_type must be one of either 'count' or 'rate'")

    if normalize_pop_for_average_type_obs is True:
        processed_bucket_populations = bucket_populations / np.sum(
            bucket_populations
        )
    else:
        processed_bucket_populations = bucket_populations.copy()

    if output_type == "count":
        point_estimates = model.split_to_counts(
            observed_total, rate_pattern, processed_bucket_populations
        )
        if (observed_total_se is not None) and (pattern_covariance is None):
            fitted_beta = model.fit_beta(
                observed_total, rate_pattern, processed_bucket_populations
            )
            standard_errors = model.count_split_standard_errors(
                fitted_beta,
                rate_pattern,
                processed_bucket_populations,
                observed_total_se,
            )
            return point_estimates, standard_errors

        if (observed_total_se is not None) and (pattern_covariance is not None):
            fitted_beta = model.fit_beta(
                observed_total, rate_pattern, processed_bucket_populations
            )

            cov_mat = model.count_split_covariance_uncertainty(
                fitted_beta,
                rate_pattern,
                processed_bucket_populations,
                observed_total_se,
                pattern_covariance,
            )
            standard_errors = np.sqrt(np.diag(cov_mat))

            return point_estimates, standard_errors

        return point_estimates

    if output_type == "rate":
        point_estimates = model.split_to_rates(
            observed_total,
            rate_pattern,
            processed_bucket_populations,
            reduce_output=True,
        )
        if (observed_total_se is not None) and (pattern_covariance is None):
            fitted_beta = model.fit_beta(
                observed_total, rate_pattern, processed_bucket_populations
            )
            standard_errors = model.rate_standard_errors(
                fitted_beta,
                rate_pattern,
                processed_bucket_populations,
                observed_total_se,
            )
            return point_estimates, standard_errors

        if (observed_total_se is not None) and (pattern_covariance is not None):
            fitted_beta = model.fit_beta(
                observed_total, rate_pattern, processed_bucket_populations
            )

            cov_mat = model.rate_split_covariance_uncertainty(
                fitted_beta,
                rate_pattern,
                processed_bucket_populations,
                observed_total_se,
                pattern_covariance,
            )
            standard_errors = np.sqrt(np.diag(cov_mat))

            return point_estimates, standard_errors

        return point_estimates


def split_dataframe(
    groups_to_split_into: list,
    observation_group_membership_df: DataFrame,
    population_sizes: DataFrame,
    rate_patterns: DataFrame,
    use_se: bool = False,
    model: DisaggModel = LogOddsModel(),
    output_type: Literal["count", "rate"] = "count",
    demographic_id_columns: list | None = None,
    normalize_pop_for_average_type_obs: bool = False,
) -> DataFrame:
    """Disaggregate datapoints and pivots observations into estimates for each
    group per demographic id

    * If output_type=='total', then this outputs estimates for the observed
      amount in each group such that the sum of the point estimates equals the
      original total
    * If output_type=='rate', then this estimates rates for each group
      (and doesn't multiply the rates out by the population)

    Parameters
    ----------
    groups_to_split_into
        list of groups to disaggregate observations into
    observation_group_membership_df
        Dataframe with columns demographic_id, pattern_id, obs,
        and columns for each of the groups_to_split_into
        with dummy variables that represent whether or not
        each group is included in the observations for that row.
        This also optionally contains a obs_se column which will be used if
        use_se is True. demographic_id represents the population that the
        observation comes from pattern_id gives the baseline that should be used
        for splitting
    population_sizes
        Dataframe with demographic_id as the index containing the
        size of each group within each population (given the demographic_id)
        INDEX FOR THIS DATAFRAME MUST BE DEMOGRAPHIC ID(PANDAS MULTIINDEX OK)
    rate_patterns
        dataframe with pattern_id as the index, and columns
        for each of the groups_to_split where the entries represent the rate
        pattern in the given group to use for pydisagg.
    use_se
        whether or not to report standard errors along with estimates
        if set to True, then observation_group_membership_df must have an obs_se
        column, by default False
    model
        DisaggModel to use for splitting, by default LogOddsModel()
    output_type
        One of 'total' or 'rate'
        Type of splitting to perform, whether to disaggregate and return the
        estimated total in each group, or estimate the rate per population unit.
    demographic_id_columns
        Columns to use as demographic_id
        Defaults to None. If None is given, then we assume
        that there is a already a demographic id column that matches the index
        in population_sizes. Otherwise, we create a new demographic_id column,
        zipping the columns chosen into tuples
    normalize_pop_for_average_type_obs
        Whether or not to normalize populations to sum to 1, this is appropriate
        when the output_type is rate and when the aggregated observation is an
        average--whether an aggregated rate or a mean of a continuous measure
        over different groups

    Returns
    -------
    DataFrame
        Dataframe where each row corresponds to one of obs, with one or
            two columns for each of the groups_to_split_into, giving the estimate
        If use_se==True, then has a nested column indexing, where both the
            point estimate and standard error for the estimate for each group is given.

    """
    if (normalize_pop_for_average_type_obs is True) and (
        output_type == "count"
    ):
        raise Warning(
            "Normalizing populations may not be appropriate here, as we are working with a total"
        )

    splitting_df = observation_group_membership_df.copy()
    if demographic_id_columns is not None:
        splitting_df["demographic_id"] = list(
            zip(*[splitting_df[id_col] for id_col in demographic_id_columns])
        )

    if use_se is False:

        def split_row(x):
            return split_datapoint(
                x["obs"],
                population_sizes.loc[x.name] * x[groups_to_split_into],
                rate_patterns.loc[x["pattern_id"]],
                model=model,
                output_type=output_type,
                normalize_pop_for_average_type_obs=normalize_pop_for_average_type_obs,
            )

        result = (
            splitting_df.set_index("demographic_id")
            .apply(split_row, axis=1)
            .reset_index()
        )
    else:

        def split_row(x):
            raw_split_result = split_datapoint(
                x["obs"],
                population_sizes.loc[x.name] * x[groups_to_split_into],
                rate_patterns.loc[x["pattern_id"]],
                model=model,
                observed_total_se=x["obs_se"],
                output_type=output_type,
                normalize_pop_for_average_type_obs=normalize_pop_for_average_type_obs,
            )
            return pd.Series(
                [
                    (estimate, se)
                    for estimate, se in zip(
                        raw_split_result[0], raw_split_result[1]
                    )
                ],
                index=groups_to_split_into,
            )

        result_raw = splitting_df.set_index("demographic_id").apply(
            split_row, axis=1
        )
        point_estimates = result_raw.applymap(lambda x: x[0])
        standard_errors = result_raw.applymap(lambda x: x[1])
        result = pd.concat(
            [point_estimates, standard_errors], keys=["estimate", "se"], axis=1
        ).reset_index()  # .groupby(level=0).sum()

    if demographic_id_columns is not None:
        demographic_id_df = pd.DataFrame(
            dict(zip(demographic_id_columns, zip(*result["demographic_id"]))),
            index=result.index,
        )
        result = pd.concat([demographic_id_df, result], axis=1).drop(
            "demographic_id", axis=1
        )

    return result

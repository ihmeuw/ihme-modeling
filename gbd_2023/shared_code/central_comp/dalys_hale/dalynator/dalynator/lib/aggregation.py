from functools import reduce
from typing import List, Optional

import pandas as pd

from core_maths.aggregate import aggregate
from gbd.constants import age, metrics, sex
from hierarchies.dbtrees import agetree
from transforms.transforms import DEFAULT_MERGE_COLUMNS, transform_metric

from dalynator.data_container import DataContainer
from dalynator.lib.metric_conversion import convert_number_to_rate
from dalynator.lib.utils import get_index_draw_columns

BASIC_AGGREGATES = [age.ALL_AGES, age.AGE_STANDARDIZED]


def _cross_join(x: pd.DataFrame, y: pd.DataFrame) -> pd.DataFrame:
    """Returns the cartesian product of the DataFrames x and y"""
    x["cj_key"] = 1
    y["cj_key"] = 1
    return x.merge(y, on="cj_key").drop("cj_key", axis=1)


def _fill_cart_product(
    df: pd.DataFrame, index_cols: List[str], age_groups: List[int]
) -> pd.DataFrame:
    """ Add the given age groups to the df and expand to the full cartesian product of
        the index cols, filling with 0. Used for aggregation in rate space where we
        may have non-square data due to restrictions
    """
    dem_cols = ["location_id", "year_id", "measure_id", "sex_id"]
    gbd_cols = list(set(index_cols) - set(dem_cols))
    dfs_to_join = [pd.DataFrame({col_name: df[col_name].unique()}) for col_name in dem_cols]
    dfs_to_join.append(pd.DataFrame({"age_group_id": age_groups}))
    expanded_df = reduce(_cross_join, dfs_to_join)
    expanded_df = _cross_join(expanded_df, df[gbd_cols].drop_duplicates())
    expanded_df = expanded_df.merge(df, how="left").fillna(0)
    return expanded_df


def calculate_age_aggregates(
    data_frame: pd.DataFrame,
    data_container: DataContainer,
    extra_aggregates: List[int],
    include_pre_df: bool = True,
    age_group_set_id: Optional[int] = None,
) -> pd.DataFrame:
    """Calculate age aggregates in number or rate space. Used for age aggregation
    of draws, summaries, and population dataframes. Calculates the all-ages and
    age-standerdized aggregates, as well as any additional age groups specified
    with the extra_aggregates parameter.

    Arguments:
        data_frame (pd.DataFrame): dataframe containing draws or summaries of
            most-detailed age groups. Data must contain only one metric, and that
            metric must be either NUMBER or RATE
        data_container (DataContainer): the cache object containing age weights
            and population estimates.
        extra_aggregates (List[int]): a list of aggregate age groups to calculate
            in addition to the all-ages and age-standardized aggregates
        include_pre_df (bool): whether to return the original most-detailed age
            groups in the results along with the aggregate data. Default is True
        age_group_set_id (int): passed through to dbtrees.agetree(). If provided,
            this arg allows selection of a specific age group set associated with
            a release_id. If not provided, or None passed,
            agetree defaults to using the value 24
            "The disaggregated age groups used by GBD for all rounds".
    """
    index_cols, data_cols = get_index_draw_columns(data_frame)
    index_cols.remove("age_group_id")

    # Create the unique list of novel age aggregates to create
    existing_age_groups = data_frame.age_group_id.unique()
    age_groups = list(
        set(BASIC_AGGREGATES + extra_aggregates).difference(existing_age_groups)
    )

    # Make sure DF only contains 1 metric, either NUMBER or RATE.
    # Cannot aggregate on mixed metrics
    df_metrics = data_frame.metric_id.unique()
    if len(df_metrics) > 1:
        raise ValueError(
            f"Can only aggregate on one metric at a time. Got {df_metrics}."
        )
    else:
        metric = df_metrics[0]
    if metric not in [metrics.NUMBER, metrics.RATE]:
        raise ValueError(
            f"Can only aggregate NUMBER or RATE dataframes. Got metric_id {metric}"
        )

    # Compute each requested aggregate one at a time
    results = []
    pop_df = data_container["pop"]
    for age_group_id in age_groups:
        aadf = data_frame.copy()
        age_tree = agetree(
            age_group_id=age_group_id,
            release_id=data_container.release_id,
            age_group_set_id=age_group_set_id,
        )

        if age_group_id == age.AGE_STANDARDIZED:
            # Convert from count to rate space if necessary
            if metric == metrics.NUMBER:
                aadf = convert_number_to_rate(
                    df=aadf,
                    pop_df=pop_df,
                    include_pre_df=False,
                    merge_columns=data_container.pop_merge_columns,
                )

            # Merge on age weights
            input_length = len(aadf)
            aadf = aadf.merge(data_container["age_weights"], on="age_group_id", how="inner")
            if len(aadf) != input_length:
                raise ValueError("Missing age weights needed for age-standardized rates")

            # Aggregate with weighted sum of age weights
            aggregation_type = "wtd_sum"
            weight_col = "age_group_weight_value"
            aadf = aggregate(
                aadf[index_cols + data_cols + [weight_col]],
                data_cols,
                index_cols,
                aggregation_type,
                weight_col=weight_col,
                normalize=None,
            )
        else:
            # Filter to child age groups
            child_age_groups = list(set([node.id for node in age_tree.leaves()]))
            aadf = aadf[aadf["age_group_id"].isin(child_age_groups)]

            if metric == metrics.RATE:
                aadf = _fill_cart_product(aadf, index_cols, child_age_groups)

                weight_col = "pop_scaled"
                aggregation_type = "wtd_sum"

                # Merge on population
                input_length = len(aadf)
                aadf = aadf.merge(
                    data_container["pop"],
                    on=data_container.pop_merge_columns,
                    how="inner")
                if len(aadf) != input_length:
                    raise ValueError("Missing populations needed for aggregation of rates")
            else:
                weight_col = None
                aggregation_type = "sum"

            # Aggregate with sum or weighted sum
            aadf = aggregate(
                aadf, data_cols, index_cols, aggregation_type, weight_col=weight_col
            )
        aadf["age_group_id"] = age_group_id
        results.append(aadf)

    # Create emtpy results so concat doesn't fail if we didn't add aggregates
    if not results:
        results.append(pd.DataFrame())

    if include_pre_df:
        return pd.concat([data_frame] + results)
    else:
        return pd.concat(results)


def aggregate_population(
    df: pd.DataFrame,
    release_id: int,
    age_group_ids: List[int],
    age_group_set_id: Optional[int] = None,
) -> pd.DataFrame:
    """Aggregates population by age.

    Aggregates to any new age_group_ids provided as well as ALL_AGES.

    Args:
        df: the input DataFrame, assumed to have these columns:
            ["location_id", "year_id", "sex_id", "age_group_id", "pop_scaled"]
        release_id: specifies which age hierarchies will be used.
        age_group_ids: a list of age_group_ids which will be aggregation destinations
            if they do not already exist in df.
        age_group_set_id (int): passed through to dbtrees.agetree(). If provided,
            this arg allows selection of a specific age group set associated with
            a release_id. If not provided, or None passed,
            agetree defaults to using the value 24
            "The disaggregated age groups used by GBD for all rounds".

    Returns:
        output_df: The input DataFrame, concatenated with new aggregation rows.

    """
    index_cols, data_cols = get_index_draw_columns(df)
    return_cols = ["location_id", "year_id", "sex_id", "age_group_id", "pop_scaled"]
    index_cols.remove("age_group_id")
    index_cols.remove("pop_scaled")
    data_cols = ["pop_scaled"]

    existing_age_groups = df.age_group_id.unique()
    age_groups = list(set([age.ALL_AGES] + age_group_ids).difference(existing_age_groups))
    results = []
    for age_group_id in age_groups:
        aadf = df.copy()
        age_tree = agetree(
            age_group_id=age_group_id,
            release_id=release_id,
            age_group_set_id=age_group_set_id,
        )
        # Filter to child age groups
        child_age_groups = list(set([node.id for node in age_tree.leaves()]))
        aadf = aadf[aadf["age_group_id"].isin(child_age_groups)]

        aadf = aggregate(df=aadf, value_cols=data_cols, index_cols=index_cols, operator="sum")
        aadf["age_group_id"] = age_group_id
        results.append(aadf)

    output_df = pd.concat([df] + results)[return_cols]

    return output_df


def aggregate_sexes(
    data_frame: pd.DataFrame,
    include_pre_df: bool = True,
    pop_merge_columns: List[str] = DEFAULT_MERGE_COLUMNS,
    pop_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Aggregate sexes in number space, converting to and from number space if necessary.

    Args:
        data_frame: the input DataFrame. If in rate space, assumed to satisfy the
            requirements for transform_metric
        include_pre_df (bool): whether to return the original de-aggregated sex
            groups in the results along with the aggregate data. Default is True
        pop_merge_columns: passed to transform_metric if data_frame is in rate space.
        pop_df: passed to transform_metric if data_frame is in rate space.

    Returns:
        sex_df: The sex-aggregated dataframe, optionally including the de-aggregated input.

    """
    pre_sex_df = data_frame.copy()
    df_metrics = pre_sex_df.metric_id.unique()
    if len(df_metrics) > 1:
        raise ValueError(f"Can only combine sexes for one metric at a time. Got {df_metrics}")
    else:
        in_metric = df_metrics[0]

    if in_metric == metrics.RATE:
        if pop_df is None:
            raise ValueError(
                "aggregate_sexes requires a population dataframe when in RATE space."
            )
        to_sum_df = transform_metric(
            df=pre_sex_df,
            from_id=metrics.RATE,
            to_id=metrics.NUMBER,
            pop_df=pop_df,
            merge_columns=pop_merge_columns,
        )
    elif in_metric == metrics.NUMBER:
        to_sum_df = pre_sex_df
    else:
        raise ValueError(
            "aggregate_sexes can only work with metric_ids "
            f"{metrics.RATE} or {metrics.NUMBER}. Got {in_metric}."
        )

    index_cols, _ = get_index_draw_columns(data_frame)
    group_by_columns = list(set(index_cols) - {"sex_id"})
    sex_df = to_sum_df.groupby(group_by_columns).sum()
    sex_df["sex_id"] = sex.BOTH
    sex_df = sex_df.reset_index()

    if in_metric == metrics.RATE:
        sex_df = transform_metric(
            df=sex_df,
            from_id=metrics.NUMBER,
            to_id=metrics.RATE,
            pop_df=pop_df,
            merge_columns=pop_merge_columns,
        )
    sex_df["metric_id"] = in_metric

    if include_pre_df:
        sex_df = sex_df.append(pre_sex_df)
    return sex_df

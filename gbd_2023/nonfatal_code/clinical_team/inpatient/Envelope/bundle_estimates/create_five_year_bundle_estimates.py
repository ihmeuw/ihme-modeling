"""
1)	The data is too large to process at once so it is split it up by age, sex and bins of
    5 years worth of data to aggregate (eg 1997-2002, 2003-2007, etc)
2)	Draws have already been creating using PopEstimates
3)	Next we need to make sure the data is square, so that we're adding years
    together properly. We will only include a bundle in the square data for
    a given location and year if it has been diagnosed in that location in any other year.
    After making square we re-apply age/sex restrictions.
4)	Then we merge on denominators.
5)	Convert from rate to count space by multiplying draws by the population or
    maternal sample size.
6)	Next the code attaches the inpatient admissions to use for squared data (eg zeros).
7)	We sum the numerators and denominators together to get 5 year values. An example
    here is probably helpful. If we have only 3 years of data available from 1994, 1995, 1996
    then we'd sum all the numerator cases, say 10, 0 and 20 and all the denominator populations
    say 1000, 1020 and 1070 to get 5 year values of 30 in the numerator and 3090 in the
    denominator and year start/end values of 1993-1997. This is repeated for each draw,
    although the denominator is the same for each draw. Only the numerator changes.
8)	To return to rate space we divide the summed numerators by the summed denominators.

"""
import time
import warnings

import pandas as pd
from crosscutting_functions import demographic, legacy_pipeline

import inpatient.Envelope.bundle_estimates.shared_bundle_estimate_funcs as sbef
from inpatient.Envelope import apply_bundle_cfs as abc


def rate_to_count(df: pd.DataFrame, drop_rate: bool = True) -> pd.DataFrame:
    """
    Converts information in ratespace to countspace by mutliplying rates by
    population.

    Parameters:
        df: Clinical data which must already have populations attached.
        drop_rate: Whether or not to drop the columns in rate-space after converting to counts.
    """

    if "population" not in df.columns:
        raise ValueError("'population' has not been attached yet.")

    if "draw_0" not in df.columns:
        raise ValueError("missing draws")
    # we want to take all draws
    cols_to_count = df.filter(regex="draw_").columns.tolist()

    # do the actual conversion
    print("going from rate to count space.")
    start = time.time()
    for col in cols_to_count:
        df[col + "_count"] = df[col] * df["population"]

    if drop_rate:
        # Drop the rate columns
        df = df.drop(cols_to_count, axis=1)
    run_m = (time.time() - start) / 60
    print("It took {} min to go from rate to count space".format(round(run_m, 3)))
    return df


def aggregate_to_dismod_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to collapse data into 5-year bands.  This is done in order to
    reduce the strain on on dismod by reducing the number of rows of data. Data
    must already be square! Data must be in count space!

    Before we have years like 1990, 1991, ..., 2014, 2015.  After we will have:
        1988-1992
        1993-1997
        1998-2002
        2003-2007
        2008-2012
        2013-2017

    Parameters:
        df: Pandas DataFrame
            Contains data at the bundle level that has been made square.
    """
    chk = "draw_0_count"

    zero_msg = """There are no rows with zeros, implying
        that the data has not been made square.  This function should be ran
        after the data is square"""
    if not (df[chk] == 0).any():
        warnings.warn(zero_msg)

    if df[chk].max() > df["population"].max():
        rows = len(df[df[chk] > df["population"]])
        warnings.warn(f"{rows} draw 0 counts are larger than population.")

    # we need to switch back to year start end
    df = df.rename(columns={"year_id": "year_start"})
    df["year_end"] = df["year_start"]

    # first change years to 5-year bands
    df = demographic.year_binner(df)

    # we are mixing data from different NIDs together.  There are special NIDs
    # for aggregated data that the Data Indexers
    # makes for us.
    warnings.warn("update this to use the new source table in the database")
    df = legacy_pipeline.apply_merged_nids(df, assert_no_nulls=False, fillna=True)

    # rename "denominator" to "sample_size" in df (not covered_df)
    df = df.rename(columns={"denominator": "sample_size"})

    cols_to_sum = df.filter(regex="count$").columns.tolist()
    cols_to_sum = cols_to_sum + ["population", "sample_size"]
    pre_cases = 0
    for col in cols_to_sum:
        pre_cases += df[col].sum()

    groups = [
        "location_id",
        "year_start",
        "year_end",
        "age_group_id",
        "sex_id",
        "nid",
        "representative_id",
        "bundle_id",
        "estimate_id",
    ]

    sum_dict = dict(list(zip(cols_to_sum, ["sum"] * len(cols_to_sum))))
    print("performing the groupby and sum across dismod years")
    years = df.groupby(groups).size().reset_index(drop=True)
    df = df.groupby(groups).agg(sum_dict).reset_index()
    df["years"] = years

    post_cases = 0
    for col in cols_to_sum:
        post_cases += df[col].sum()

    accepted_loss = 300
    lost_cases = abs(pre_cases - post_cases)
    print(
        "The acceptable loss is {} cases. {} cases were lost to floating point error".format(
            accepted_loss, round(lost_cases, 2)
        )
    )
    assert (
        lost_cases < accepted_loss
    ), "some cases were lost. " "From {} to {}. A difference of {}".format(
        pre_cases, post_cases, abs(pre_cases - post_cases)
    )

    return df


def count_to_rate(df: pd.DataFrame, review: bool) -> pd.DataFrame:
    """
    Function that transforms from count space to rate space by dividing by
    dividing by the (aggregated) population.  Returns two dataframes.  This
    performs the transformation for each dataframe separatly.
    Pass in both dataframes!  The order of the return dataframes is important!
    The main part of hosital data, i.e., sources without full coverage, is
    returned first.  Second item returned is the fully covered sources.

    Example call:
        not_fully_covered, full_covered = count_to_rate(df, covered_df)

    Parameters:
        df: df containing most hospital data, the sources that are not fully
            covered.
        review: Whether or not to return the count columns for review.
                If False, the count columns will be dropped.
    """

    # get a list of the count columns
    cnt_cols = df.filter(regex="count$").columns

    print("going from count to rate space.")
    for col in cnt_cols:
        new_col_name = col[:-6]
        df[new_col_name] = df[col] / df["population"]

    if not review:
        # drop the count columns
        df = df.drop(cnt_cols, axis=1)

    return df


def create_source_map(df):
    """
    Create a map from location and year to source. Used to vaildate data shape after
    we map to bundle but before we apply the CFs
    """

    source_map = df[["location_id", "year_id", "source"]].drop_duplicates()
    source_map = source_map.rename(columns={"year_id": "year_start"})
    source_map["year_end"] = source_map["year_start"]
    source_replace = {
        "CHN_SHD": "CHN_NHSIRS",
        "GEO_Discharge_16_17": "GEO_COL_14", 
        "GEO_Discharge": "GEO_COL",
    }

    for key, value in source_replace.items():
        source_map.loc[source_map["source"] == key, "source"] = value
    # bin to 5 year groups so it matches with the data after agg to dismod
    source_map = demographic.year_binner(source_map).drop_duplicates()
    assert len(source_map) == len(
        source_map[["location_id", "year_start"]].drop_duplicates()
    ), "These duplicates will break the merge"
    return source_map


def apply_all_populations_in_5_yr_space(
    df,
    config_args,
    helpers,
):
    """
    Apply all three populations type regardless of bundle or full coverage status
    and bin to five years
    """
    df = sbef.merge_population(
        df,
        run_id=config_args.run_id,
    )

    df = rate_to_count(df, drop_rate=True)

    # now that data has been made square attach sample size for those rows
    df = sbef.merge_denominator(
        df,
        config_args.run_id,
        full_coverage_sources=helpers["full_coverage_sources"],
    )

    df = aggregate_to_dismod_years(df)
    df = count_to_rate(df=df, review=False)

    return df


def run_helper(df, config_args, helpers):
    df = sbef.make_square_bundle_shape(df, config_args, helpers)
    return apply_all_populations_in_5_yr_space(df, config_args, helpers)


def main(config_args):
    """
    Aggregates an age/sex subset of clinical data to five year bins and applies the correction
    factors to the aggregated results.
    """
    start = time.time()
    df, helpers = sbef.prep_data(config_args)
    helpers["source_map"] = create_source_map(df)

    df = run_helper(
        df=df,
        config_args=config_args,
        helpers=helpers,
    )

    # merge on source
    pre = df.shape
    df = df.merge(
        helpers["source_map"], how="left", on=["location_id", "year_start", "year_end"]
    )
    assert pre[0] == df.shape[0], "row counts changed"
    assert pre[1] == df.shape[1] - 1, "col counts changed"

    df = sbef.write_draws(df, config_args)
    runm = (time.time() - start) / 60
    print("prepping to apply bundle data w/env uncertainty ran in {} minutes".format(runm))

    # # pull in the apply_env_only main functions here
    base = FILEPATH
    pic = FILEPATH
    abc.apply_bundle_cfs_main(
        df=df,
        age_group_id=config_args.age_group,
        sex_id=config_args.sex,
        year=config_args.year_start,
        run_id=config_args.run_id,
        full_cover_stat=pic.cf_agg_stat,
        bin_years=config_args.bin_years,
        draws=config_args.draws,
    )

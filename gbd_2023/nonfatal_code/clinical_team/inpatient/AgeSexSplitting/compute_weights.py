import warnings

import pandas as pd
from crosscutting_functions.get-cached-population import cached_pop_tools
from crosscutting_functions import demographic, legacy_pipeline

# load our functions
from crosscutting_functions.mapping import clinical_mapping, clinical_mapping_db


def test_weight_sources(df: pd.DataFrame, clinical_age_group_set_id: int) -> None:
    # confirm the sources are good for splitting
    no_min = []
    no_max = []
    valid_ages = demographic.get_hospital_age_groups(
        clinical_age_group_set_id=clinical_age_group_set_id
    )
    for source in df.source.unique():
        age_min = df[df.source == source].age_start.min()
        age_max = df[df.source == source].age_start.max()
        if age_min != valid_ages.age_start.min():
            no_min.append(source)
        if age_max != valid_ages.age_start.max():
            no_max.append(source)

    bad_sources = no_min + no_max
    if bad_sources:
        assert False, (
            "source(s) {} don't have min age {} and source(s)"
            " {} don't have max age start {}".format(
                no_min, valid_ages.age_start.min(), no_max, valid_ages.age_start.max()
            )
        )


def _check_for_missing_icgs(df, map_version="current"):
    """Get a list of all the ICG ids present in ICD 9 and 10 maps"""
    full_icg_list = clinical_mapping_db.get_clinical_process_data(
        "cause_code_icg", map_version=map_version
    )
    full_icg_list = (
        full_icg_list.loc[full_icg_list["code_system_id"].isin([1, 2]), "icg_id"]
        .unique()
        .tolist()
    )
    missing_icgs = set(full_icg_list) - set(df["icg_id"].unique())

    return missing_icgs


def add_missing_icgs(df, map_version="current"):
    """Append on missing ICGs to every demographic group in the data"""
    missing_icgs = _check_for_missing_icgs(df, map_version=map_version)
    if missing_icgs:
        print(
            f"The following ICGs are missing from "
            f"the data to make weights. Appending them on {missing_icgs}"
        )
        cols = [
            "age_group_id",
            "sex_id",
            "location_id",
            "year_start",
            "year_end",
            "source",
        ]
        tmp = df[cols].drop_duplicates().copy()
        print(
            f"The df we will use to add on missing ICGs has {len(tmp)} rows"
            f" and {tmp['age_group_id'].unique().size} ages, age groups: "
            f"{tmp['age_group_id'].unique().tolist()}"
        )
        for icg in missing_icgs:
            tmp2 = tmp.copy()
            tmp2["icg_id"] = icg
            tmp2["product"] = 0
            df = pd.concat([tmp2, df], sort=False, ignore_index=True)
    else:
        pass

    return df


def sqr_by_group(df, groups, level, clinical_age_group_set_id):
    """Inputs hospital data at the ICG level and
    a list of groups to identify how to square data"""

    df = df.drop(["age_start", "age_end"], axis=1)

    tmp_list = []  # to append each df to
    # group the df by the group cols and loop over each subset
    for a_group, a_df in df.groupby(groups):
        tmp_list.append(
            legacy_pipeline.make_zeros(
                a_df, cols_to_square="product", etiology=level  # ages=ages Nope!
            )
        )

    df = pd.concat(tmp_list, sort=False, ignore_index=True)
    df = demographic.all_group_id_start_end_switcher(
        df=df, clinical_age_group_set_id=clinical_age_group_set_id, remove_cols=False
    )
    # confirm there aren't age groups that are entirely Zero
    tq = (
        df[["source", "age_group_id", "product"]].groupby(["source", "age_group_id"]).nunique()
    )
    if tq["product"].min() == 1:
        res = tq[tq["product"] == 1]
        raise ValueError(
            f"There is a source, age group combo with only a single unique value "
            f"which is probably a Zero and means the entire age group is missing {res}"
        )

    return df


def prep_weights(
    df,
    round_id,
    level,
    run_id,
    clinical_age_group_set_id,
    squaring_method,
    inp_pipeline=True,
    map_version="current",
):
    """
    Function that computes weights for use in age sex splitting.

    Parameters:
        df: pandas DataFrame
            input data to inform weights. Should already have the appropriate
            age groups.  Should have column "product" which is the product
            of cause_fraction and the hospital utilization envelope.
            age_group_id should be present.  df is used only as an input to
            make weights.
        level: string
            Must be "bundle_id" or "icg_id". indicates if we're making
            bundle level weights or cause level weights. Note bundle ID
            weights will fail
        squaring_method: str
            Must be 'broad' or 'bundle_source_specific'. Broad means bundles
            that are never coded for a data source will be added as zeros,
            while the other does not
    Returns:
        DataFrame that has weights computed at level of the Parameter 'level'
    """

    # input asserts
    assert level in df.columns, "{} is not present in the data".format(level)
    assert "product" in df.columns, "Product column must be present"
    assert "source" in df.columns, "Source column must be present"

    if level not in ("bundle_id", "icg_id"):
        raise ValueError("level must either be 'bundle_id' or 'icg_id'")

    if squaring_method not in ("broad", "bundle_source_specific"):
        raise ValueError(
            "squaring method must be either 'broad' or " "'bundle_source_specific'"
        )

    # input data should already have appropriate age groups if this is round 2

    print("Getting {} weights...".format(level))

    # remove the all sexes sex id
    df = df[df.sex_id != 3].copy()

    # add a product of zero for any ICGs that may be missing
    if level == "icg_id":
        df = add_missing_icgs(df, map_version=map_version)

    # code is set up to use both age_start/age_end and age_group_id
    df = demographic.all_group_id_start_end_switcher(
        df, clinical_age_group_set_id, remove_cols=False
    )

    test_weight_sources(
        df[["source", "age_start", "age_end"]].drop_duplicates(),
        clinical_age_group_set_id=clinical_age_group_set_id,
    )

    if inp_pipeline:
        # keep relevant columns
        # icg_id should be kept regarless if these are
        # icg_id or bundle weights.
        keep_cols = [
            "age_group_id",
            "age_start",
            "age_end",
            "location_id",
            "sex_id",
            "year_end",
            "year_start",
            "product",
            "icg_id",
        ]
        if squaring_method == "bundle_source_specific":
            new_square_cols = ["source"]
            keep_cols = keep_cols + new_square_cols

        df = df[keep_cols].copy()

    # make square aka cartesian. We want population to be contributed by all
    # age-countries, regardless if there are any cases for all age-country
    # pairs For every location-year that we already have, we want all age-sex
    # (and bundle/cause) combinations.  This introduces Nulls where there
    # wasn't any data, which are then filled with zeros
    if squaring_method == "bundle_source_specific":
        # This will square all age groups in the data (ie our 25 good ones for GBD2020)
        # regardless of if they're present in the original source, so still 'too square'
        df = legacy_pipeline.make_zeros(df, cols_to_square="product", etiology=level)

        if inp_pipeline:
            df = df.drop(new_square_cols, axis=1)

    else:
        assert False, f"{squaring_method} isn't a recognized squaring method"

    msg = "Finished making the data square"
    print(msg)

    # merge pop on so we can convert to count space, so we can do addition

    # get info for pop to use for testing
    age_list = list(df.age_group_id.unique())
    loc_list = list(df.location_id.unique())
    year_list = list(df.year_start.unique())

    # new pop method
    # if this ever needs to be True refactor based on that need
    sum_under1 = False
    pop = cached_pop_tools.get_cached_pop(run_id=run_id, sum_under1=sum_under1)

    assert_msg = """IDs present in pop dataframe are not a subset of the IDs
                  present in main dataframe. This will lead to missing
                  weights"""

    assert set(age_list).issubset(set(pop.age_group_id.unique())), assert_msg
    assert set(loc_list).issubset(set(pop.location_id.unique())), assert_msg
    assert set(year_list).issubset(set(pop.year_id.unique())), assert_msg

    if pop.shape[0] == 0:
        raise ValueError("Population has no rows!")

    if pop.population.isnull().any():
        nw = pop[pop["weight"].isnull()]
        m = (
            f"In Popluation there are {len(nw)} Null weights. This has most "
            f"likely identified an issue in the get population method."
        )
        raise ValueError(m)

    pop = pop.drop("pop_run_id", axis=1)

    pop["year_start"] = pop["year_id"]
    pop["year_end"] = pop["year_id"]
    pop = pop.drop("year_id", axis=1)

    # merge pop
    pre_shape = df.shape[0]
    df = df.merge(
        pop,
        how="left",
        on=["location_id", "year_start", "year_end", "age_group_id", "sex_id"],
    )
    assert pre_shape == df.shape[0], "Merging on pop changed the # of rows"
    print("Population has been merged onto the data")

    assert df.loc[:, ["product", "population"]].notnull().all().all()

    # multiply by pop to get into count space so we can get to
    # age / sex / bundle groups
    df["counts"] = df["product"] * df["population"]

    # aggregate want groups of age, sex, level (disease cause),
    # while summing pop and counts
    group_cols = ["age_end", "age_start", "age_group_id", "sex_id", level]

    assert df.loc[:, ["counts", "population"] + group_cols].notnull().all().all()

    df = df.groupby(by=group_cols).agg({"counts": "sum", "population": "sum"}).reset_index()
    print("Groupby is complete, calculating first pass of weights")

    assert df.notnull().all().all(), "there are nulls in the data"

    # divide by pop to get back into ratespace
    df["weight"] = df["counts"] / df["population"]

    assert df.loc[:, ["weight"]].notnull().all().all()

    return df


def compute_weights(
    df,
    round_id,
    run_id,
    squaring_method,
    clinical_age_group_set_id,
    next_age_sex_weights_version_id,
    level="icg_id",
    overwrite_weights=False,
    inp_pipeline=True,
    map_version="current",
):
    """
    Args:
        df: pandas DataFrame that contains the data that you want to use to
            inform weights. Should already have the appropriate
            age groups.  Should have column "product" which is the product
            of cause_fraction and the hospital utilization envelope.
            age_group_id should be present. df is used only as an input to
            the other function, in order to make weights.
        round_id: int
            indicates what round of weights we're on, which affects file names
            for output. Should either be 1 or 2
        squaring_method: str
            'broad' or 'bundle_source_specific'. Indicates when a zero should be added.
            if broadwill add zeros for all sources, if bundle_source_specific will only
            add a zero whena given disease was coded by that source
        clinical_age_group_set_id: int
            indicates which set of 'good' age groups to ues. 1 represents the 21 good ages
            used in GBD2019 and earlier and 2 indicates the new 25 age groups for GBD2020
        next_age_sex_weights_version_id: int
            This will be used to reference / create the next folder in the age_sex_weights
            versioning structure in FILEPATH
        level: str
            (deprecated) I'm pretty sure this must be 'icg_id' in order to make weights
            correctly and could
            potentially be removed
        overwrite_weights: Boolean
            weights are written as a csv within a run_id for use in the
            pipeline if True, write weights regardless of whether they already
            exist or not
        inp_pipeline: Boolean
            Modifying this function for use outside of the inp_pipeline in
            July 2019 but would like to retain behavior within the pipeline
    Returns:
        string "Done"
    """

    assert round_id == 1 or round_id == 2, "round_id isn't 1 or 2, it's {}".format(round_id)

    # get weights
    df = prep_weights(
        df,
        round_id=round_id,
        clinical_age_group_set_id=clinical_age_group_set_id,
        level=level,
        run_id=run_id,
        squaring_method=squaring_method,
        inp_pipeline=inp_pipeline,
        map_version=map_version,
    )

    print("done getting {} weights".format(level))

    if df["weight"].isnull().any():
        nw = df[df["weight"].isnull()]
        m = (
            f"There are {len(nw)} Null weights. This has most likely "
            f"identified an issue in the compute_weights method."
        )
        raise ValueError(m)

    # apply restrictions from clinical_mapping database
    clinical_mapping.apply_restrictions(
        df,
        clinical_age_group_set_id=clinical_age_group_set_id,
        age_set="binned",
        cause_type=level[:-3],
        prod=False,
        map_version=map_version,
    )

    # check the weights group by age and sex, combining different
    # nonfatal_cause_names in one group
    groups = df.groupby(["age_group_id", "sex_id"])

    bad = []  # list to append to
    # loop over each group
    for group in groups:
        # group[1] contains a dataframe of a specific group
        if (group[1].weight == 0).all():
            # group[0] is a tuple contains the age, sex that ID the group
            bad.append(group[0])  # add it to the list

    warning_message = """
    There are age-sex groups in the weights where every weight is zero

    Here's some tuples (age_group_id, sex_id) that identify these groups:
    {g}
    """.format(
        g=", ".join(str(g) for g in bad)
    )

    if len(bad) > 0:
        warnings.warn(warning_message)

    # only write data when running within the inpatient pipeline
    if inp_pipeline:

        if overwrite_weights:
            df.drop(["age_start", "age_end", "counts", "population"], axis=1).to_csv(
                FILEPATH,
                index=False,
            )

        # save
        if round_id == 1:
            round_name = "one"
        if round_id == 2:
            round_name = "two"
        # save for splitting
        df.drop(["age_start", "age_end", "counts", "population"], axis=1).to_csv(
            FILEPATH,
            index=False,
        )

        # result for tableau
        df = df["sex_id"].replace([1, 2], ["Male", "Female"])
        df.to_csv(FILEPATH,
            index=False,
        )

    # otherwise return the dataframe of weights
    else:
        return df

    return "done"

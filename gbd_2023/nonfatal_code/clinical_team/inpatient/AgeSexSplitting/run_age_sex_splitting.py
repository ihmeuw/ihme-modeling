import warnings

import pandas as pd
from crosscutting_functions import demographic, general_purpose, legacy_pipeline
from crosscutting_functions.mapping import clinical_mapping, clinical_mapping_db
from db_tools.ezfuncs import query

from inpatient.AgeSexSplitting.age_sex_split import split_age_sex


def simple_sex_split(df, map_version="current"):
    """
    Note: Currently commenting this out. The age-sex restricted weights should have a
    value of zero

    perform a super simple sex splitting on values where sex is unknown/both but the
    icg maps to only 1 sex

    Params:
        df : (pandas.dataframe) icg level hospital data that's being fed into age-sex splitting
        map_version : (str or int)  Can be a specific int, but defaults to 'current'
        which matches our process

    """
    pre = df.shape
    cases = df["val"].sum()
    if "icg_id" and "icg_name" not in df.columns:
        raise ValueError("We need icg id and name to merge on restricts")
    sex_diff = set(df.sex_id.unique()) - set([1, 2, 3])
    if sex_diff:
        raise ValueError(f"There are some unexpected sex values here {sex_diff}")

    restrict = clinical_mapping_db.get_clinical_process_data(
        "icg_properties", map_version=map_version, prod=True
    )
    # retain only icgs with a sex restriction
    restrict = restrict.query("male == 0 or female == 0").copy()
    assert len(restrict.query("male == 1 and female == 1")) == 0, "logic was wrong"

    # keep only the restriction cols we want
    restrict = restrict[["icg_id", "icg_name", "male", "female"]]
    # merge on restrict

    df = df.merge(restrict, how="left", on=["icg_id", "icg_name"], validate="m:1")
    assert len(df) == pre[0]

    # assign both sex data to the female sex when
    # restricted to exclude males
    df.loc[(df["male"] == 0) & (df["sex_id"] == 3), "sex_id"] = 2
    df.loc[(df["female"] == 0) & (df["sex_id"] == 3), "sex_id"] = 1  # then the other

    # drop the cols we used to identify single sex restrictions
    df = df.drop(["male", "female"], axis=1)
    post = df.shape

    if pre[0] != post[0] or pre[1] != post[1]:
        raise ValueError(f"This df changed shape!! from {pre} to {post}")
    assert cases == df["val"].sum(), "CASES changed!!!"

    return df


def run_age_sex_splitting(
    df,
    run_id,
    clinical_age_group_set_id,
    map_version,
    age_sex_weights_version_id,
    round_id=0,
    verbose=False,
    write_viz_data=True,
    level="icg_id",
    weight_path=None,
    inp_pipeline=True,
):
    """
    Takes in dataframe of data that you want to split, and a list of sources
    that need splitting, and then uses age_sex_splitting.py to split.  Returns
    The split data.  Along the way saves pre split and post split data in a
    format that's good for plotting.

    As of 6/16/2017 this function assumes
    that the weights you want to use are already present. There is some
    skeletal stuff to deal with multiple rounds for weights but it's not
    currently fleshed out.

    Parameters
        df: Pandas DataFrame
            The data to be split. can contain data that doesn't need to be split
            The age sex splitting code checks what parts of the data need
            splitting.
        round_id: int
            Specifies what round of splitting to run. used for file names.  If
            it's set to 1 or 2, it'll save a file that can be used for
            visualization.  If it's anything else nothing happens.
        verbose: Boolean
            Specifies if information about age groups before and after, and case
            counts before and after, should be printed to the screen.
        weight_path: str
            the location of the weights. If level is icg_id then it will pull them
            from within the inpatient run_id. If level is bundle_id then a weight
            path must be defined
    """

    if write_viz_data:
        # write data used for visualization
        if round_id == 1 or round_id == 2:
            # save pre_split data for viz, attach location name and bundle name and save
            pre_split = df.copy()
            pre_split = demographic.all_group_id_start_end_switcher(
                pre_split, clinical_age_group_set_id
            )
            pre_split = pre_split.merge(
                query(
                    """
                                              QUERY
                                              """,
                    conn_def="shared",
                ),
                how="left",
                on="location_id",
            )
            pre_split = pre_split.drop(
                [
                    "year_end",
                    "age_group_unit",
                    "age_end",
                    "nid",
                    "facility_id",
                    "representative_id",
                    "diagnosis_id",
                    "outcome_id",
                    "metric_id",
                ],
                axis=1,
            )
            pre_split.to_csv(FILEPATH.format(round_id),
                index=False,
                encoding="utf-8",
            )

    if inp_pipeline:
        from inpatient.Clinical_Runs.utils.constants import InpRunSettings

        # drop data and apply age/sex restrictions
        df = legacy_pipeline.drop_data(
            df, verbose=False, gbd_start_year=InpRunSettings.GBD_START_YEAR
        )

    # perform a simple age_sex splitting
    # df = simple_sex_split(df)

    print("Applying restrictions")
    df = clinical_mapping.apply_restrictions(
        df,
        clinical_age_group_set_id=clinical_age_group_set_id,
        age_set="age_group_id",
        cause_type=level[:-3],
        prod=True,
        map_version=map_version,
    )

    # list of col names to compare for later use
    pre_cols = df.columns

    df_list = []  # initialize empty list to concat split dfs to

    # columns that uniquely identify rows of data
    id_cols = [
        "source",
        "nid",
        level,
        "year_id",
        "age_group_id",
        "sex_id",
        "location_id",
    ]
    # GBD2020 new age groups are 2, 3, 34, 238, 388, 389
    # update to use clinical age group sets
    perfect_ages = (
        demographic.get_hospital_age_groups(clinical_age_group_set_id)["age_group_id"]
        .sort_values()
        .tolist()
    )
    perfect_sexes = [1, 2]
    rows = df.shape[0]
    numer = 0

    for source in df.source.sort_values().unique():  # sort then split
        # make df of one source that needs to be split
        splitting_df = df[df.source == source].copy()

        # with groupbys, convert back to str dtype
        splitting_df = general_purpose.cat_to_str(splitting_df)

        numer += splitting_df.shape[0]
        print(f"\n\nBeginning to split {source}")
        if verbose:
            print("{}'s age groups before:".format(source))
            print(
                ", ".join(
                    map(
                        str,
                        splitting_df["age_group_id"].sort_values().unique().tolist(),
                    )
                )
            )

        # create year_id
        splitting_df["year_id"] = splitting_df["year_start"]
        splitting_df = splitting_df.drop(["year_start", "year_end"], axis=1)
        if verbose:
            print("now splitting {}".format(source))
        # check if data needs to be split, if not append to dflist 
        if (
            set(splitting_df.age_group_id.unique()).symmetric_difference(set(perfect_ages))
            == set()
            and set(splitting_df.sex_id.unique()).symmetric_difference(set()) == set()
        ):
            df_list.append(splitting_df)
            continue
        # the function does the splitting
        print("Performing split_age_sex")
        split_df = split_age_sex(
            df=splitting_df,
            id_cols=id_cols,
            run_id=run_id,
            clinical_age_group_set_id=clinical_age_group_set_id,
            value_column="val",
            level_of_analysis=level,
            fix_gbd2016_mistake=False,
            weight_path=weight_path,
            age_sex_weights_version_id=age_sex_weights_version_id,
        )
        if verbose:
            print(
                "Orig value sum {} - New value sum {} = {} \n".format(
                    splitting_df.val.sum().round(3),
                    split_df.val.sum().round(3),
                    splitting_df.val.sum().round(3) - split_df.val.sum().round(3),
                )
            )
        pre_val = splitting_df.val.sum()
        post_val = split_df.val.sum()
        if pre_val - post_val > (pre_val * 0.005):
            warnings.warn(
                "Too many cases were lost, a {} percent change (1 - post/pre)".format(
                    (1 - (float(post_val) / float(pre_val))) * 100
                )
            )

        if verbose:
            print("{}'s ages after:".format(source))
            print(
                ", ".join(map(str, split_df["age_group_id"].sort_values().unique().tolist()))
            )
        # append split data to our list of dataframes
        df_list.append(split_df)
        print("Finished. {}% done".format(float(numer) / rows * 100))

    # bring the list of split DFs back together
    print("Bringing DFs back together")
    df = pd.concat(df_list, sort=False).reset_index(drop=True)

    # check that we have the right number of age groups
    good_ages = demographic.get_hospital_age_groups(
        clinical_age_group_set_id=clinical_age_group_set_id
    )
    assert (
        df[["age_group_id"]].drop_duplicates().shape[0] == good_ages.age_group_id.unique().size
    )
    age_group_diff = set(df["age_group_id"].unique()).symmetric_difference(
        good_ages["age_group_id"]
    )
    if age_group_diff:
        raise ValueError(f"Some age groups are off {age_group_diff}")

    # Compare data after splitting to before splitting
    df["year_start"] = df["year_id"]
    df["year_end"] = df["year_id"]
    df = df.drop(["year_id"], axis=1)
    assert set(df.columns).symmetric_difference(set(pre_cols)) == set()

    if write_viz_data:
        # data used for visualization
        if round_id == 1 or round_id == 2:
            viz = df.merge(
                query(
                    """
                                    QUERY
                                    """,
                    conn_def="shared",
                ),
                how="left",
                on="location_id",
            )

            viz = demographic.all_group_id_start_end_switcher(viz, clinical_age_group_set_id)

            viz.drop(
                [
                    "year_end",
                    "age_group_unit",
                    "age_end",
                    "nid",
                    "facility_id",
                    "representative_id",
                    "diagnosis_id",
                    "outcome_id",
                    "metric_id",
                ],
                axis=1,
            ).to_csv(FILEPATH.format(round_id),
                index=False,
                encoding="utf-8",
            )

    return df

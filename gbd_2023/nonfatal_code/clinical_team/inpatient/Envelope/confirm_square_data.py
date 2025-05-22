"""
This script checks that a given set of inpatient data is square

Using these solutions
1) (locations) assume master data has all of our locations
2) (bundles) assume the icd mapping data has a complete list of bundles by
    location, use this to identify good bundle+loc combos
"""

import glob
import itertools
import os
import re
import warnings

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.api.pipeline_wrappers import (
    InpatientWrappers,
)
from crosscutting_functions import demographic, legacy_pipeline
from crosscutting_functions.mapping import clinical_mapping, clinical_mapping_db
from db_tools.ezfuncs import query

from inpatient.Clinical_Runs.utils.constants import RunDBSettings


def get_loc_year_from_master(run_id, binned_years):
    """read in location_id and years from each master_data file
    This is basically solution 1 above
    """

    files = glob.glob(FILEPATH.format(run_id)
    )

    df_list = []

    for f in files:
        print(f)
        tmp = pd.read_hdf(f)
        tcols = ["location_id", "year_start", "year_end", "facility_id"]
        tmp = tmp[tcols].drop_duplicates()
        # retain only inpatient master data
        tmp = tmp[tmp["facility_id"].isin(["inpatient unknown", "hospital"])]
        df_list.append(tmp)

    df = pd.concat(df_list, sort=False, ignore_index=True)
    if binned_years:
        df = df[df.year_start > 1988].copy()
        df = demographic.year_binner(df)
    df.drop_duplicates(inplace=True)

    return df


def get_bundle_loc_from_mapping(run_id, clinical_age_group_set_id):
    """read in bundles by source from icd mapping, then merge on bundle_id
    Use legacy_pipeline.drop_data to remove the same observations we remove from the
    pipeline
    This is solution 2 above
    """

    # the icd mapping file name changes depending on map version and date
    icd_path = (FILEPATH
    )
    files = glob.glob(icd_path)
    assert len(files) == 1, "Too many icd mapping files to read in !!!"
    df = pd.read_hdf(
        files[0],
        columns=[
            "location_id",
            "icg_id",
            "facility_id",
            "diagnosis_id",
            "source",
            "year_start",
            "age_group_id",
            "sex_id",
        ],
    )

    # use the actual function our inp pipeline uses to drop data
    from inpatient.Clinical_Runs.utils.constants import InpRunSettings

    df = legacy_pipeline.drop_data(
        df, verbose=False, gbd_start_year=InpRunSettings.GBD_START_YEAR
    )

    drop_cols = ["facility_id", "diagnosis_id", "year_start"]
    df.drop(drop_cols, axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

    map_version = re.search("_v\d+_", os.path.basename(files[0])).group()
    map_version = int(re.sub("[^\d+]", "", map_version))

    bundles = query(QUERY
    )
    df = df.merge(bundles, how="inner", on=["icg_id"])

    df = clinical_mapping.apply_restrictions(
        df=df,
        age_set="age_group_id",
        cause_type="bundle",
        clinical_age_group_set_id=clinical_age_group_set_id,
        map_version=map_version,
    )

    # not all locations will have a bundle_id, but we assume the reporting
    # source at least has to potential to map a given bundle to a certain loc
    src_bundles = df[["source", "bundle_id"]].drop_duplicates()

    df = df[["source", "location_id"]].drop_duplicates()

    df = df.merge(src_bundles, how="outer", on=["source"])

    assert df.isnull().sum().sum() == 0, "There are nulls here {}".format(df.isnull().sum())

    return df.reset_index(drop=True)


def expandgrid(*itrs):
    # create a template df with every possible combination of
    #  age/sex/year/location to merge results onto
    # define a function to expand a template with the cartesian product
    product = list(itertools.product(*itrs))
    return {"Var{}".format(i + 1): [x[i] for x in product] for i in range(len(itrs))}


def make_sqr(ages, sexes, locations):
    dat = pd.DataFrame(expandgrid(ages, sexes, locations))
    dat.columns = ["age_group_id", "sex_id", "location_id"]
    return dat


def confirm_claims_square(df, run_id, clinical_age_group_set_id, map_version):
    """
    We have a limited amount of claims data, so some of the squaring issues are more tractable
    """

    sex_list = [1, 2]
    age_list = (
        demographic.get_hospital_age_groups(clinical_age_group_set_id)["age_group_id"]
        .unique()
        .tolist()
    )

    missing_list = []

    # get bundles where est id is 17 or 21
    clinical_bundle = clinical_mapping_db.get_active_bundles(
        cols=["bundle_id", "estimate_id"], estimate_id=[17, 21], map_version=map_version
    )
    for b in clinical_bundle.bundle_id.unique():
        for est in clinical_bundle.query("bundle_id == @b")["estimate_id"]:
            # skip bundles that should be removed from the bundle table for claims
            if b in [
                181,
                195,
                196,
                198,
                213,
                766,
                6113,
                6116,
                6119,
                6122,
                6125,
            ]:  
                warnings.warn(
                    "Skipping the bad bundle: {} in the table".format(b)
                )
                continue

            # SGP only has estimates for inp data
            if est == 17:
                locs = df.location_id.unique().tolist()
                loc_yr_merge = df[["location_id", "year_start_id"]].drop_duplicates().copy()
            else:
                locs = df.query("location_id != 69").location_id.unique().tolist()
                loc_yr_merge = (
                    df.query("location_id != 69")[["location_id", "year_start_id"]]
                    .drop_duplicates()
                    .copy()
                )

            # make sqr by age/sex/loc
            sqr = make_sqr(ages=age_list, sexes=sex_list, locations=locs)

            sqr = sqr.merge(loc_yr_merge, how="outer", on="location_id")
            sqr["bundle_id"] = b
            sqr = clinical_mapping.apply_restrictions(
                sqr,
                age_set="age_group_id",
                cause_type="bundle",
                clinical_age_group_set_id=clinical_age_group_set_id,
                map_version=map_version,
            )

            # just an estimate and bundle of claims data
            tmp = df.query("bundle_id == @b & estimate_id == @est").copy()

            # outer merge together
            merge_on = [
                "age_group_id",
                "sex_id",
                "location_id",
                "year_start_id",
                "bundle_id",
            ]
            tmpm = tmp.merge(sqr, how="outer", on=merge_on)
            miss_df = tmpm[tmpm.estimate_id.isnull()]
            miss_df["est_id_shouldbe"] = est
            missing_list.append(miss_df)

    res = pd.concat(missing_list, sort=False, ignore_index=True)

    return res


def identify_missing_denoms(
    run_id, clinical_age_group_set_id, bin_years, merge_age_start=True
):
    """This will compare the inpatient denominators (all cause admissions) for a given run
    against the clinical age group set param to identify where we have no admissions at all
    """

    denom = pd.read_csv(
        (FILEPATH
        )
    )

    ages = demographic.get_hospital_age_groups(clinical_age_group_set_id)
    exp_df = []
    for loc in denom.location_id.unique():
        for y in denom.loc[denom.location_id == loc, "year_id"].unique():
            for s in [1, 2]:
                tmp = denom.query("location_id == @loc and year_id == @y and sex_id == @s")
                if tmp.shape[0] != len(ages):
                    missing = sorted(list(set(ages.age_group_id) - set(tmp.age_group_id)))
                    for age in missing:
                        tdf = pd.DataFrame(
                            {
                                "age_group_id": age,
                                "sex_id": s,
                                "location_id": loc,
                                "year_start": y,
                            },
                            index=[0],
                        )
                        exp_df.append(tdf)
    columns = [
        "location_id",
        "year_start",
        "sex_id",
        "age_group_id",
        "denominator_not_present",
    ]
    if exp_df:
        exp_df = pd.concat(exp_df, sort=False, ignore_index=True).sort_values(
            ["location_id", "year_start", "sex_id", "age_group_id"]
        )
    else:
        print("There were no missing hospital denominators, congrats!")
        # return empty dataframe
        return pd.DataFrame(columns=columns)

    if bin_years:
        exp_df["year_end"] = exp_df["year_start"]
        exp_df = demographic.year_binner(exp_df).drop_duplicates().drop("year_end", axis=1)

    if merge_age_start:
        exp_df = exp_df.merge(ages, how="left", on="age_group_id", validate="m:1")

    exp_df["denominator_not_present"] = True

    return exp_df[columns]


def confirm_inpatient_square(
    df, run_id, clinical_age_group_set_id, map_version, bin_years, sex_list=[1, 2]
):
    """given inpatient data, a run id and age/sex info confirm that the
    provided data is squared
    This generally fails due to known issues. On failing it returns a dataframe of the
    missing data"""

    # get the injury CFs to figure out which operations were removed
    iw = InpatientWrappers(run_id, RunDBSettings.iw_profile)
    inj_dir = iw.pull_version_dir("inj_cf_en")
    inj_file = "FILEPATH

    if not bin_years:
        inj_file = "inj_factors.H5"

    inj_cfs = pd.read_hdf(FILEPATH)
    inj_cfs = inj_cfs[
        (inj_cfs.facility_id == "inpatient unknown") | (inj_cfs.facility_id == "hospital")
    ].drop(["facility_id", "prop", "factor"], 1)

    # make sure sexes don't contain duplicated values
    assert len(sex_list) == pd.Series(sex_list).unique().size

    ages = demographic.get_hospital_age_groups(clinical_age_group_set_id)
    age_start_list = ages["age_start"].unique().tolist()

    if "year_start_id" in df.columns:
        df.rename(columns={"year_start_id": "year_start"}, inplace=True)
    if "year_end_id" in df.columns:
        df.rename(columns={"year_end_id": "year_end"}, inplace=True)

    # get locations, years, nids and bundles from a run. These will be used to
    # skip expected bundle/location missingness
    sqr_dir = FILEPATH
    loc_yr_path = FILEPATH
    b_loc_path = FILEPATH

    if os.path.exists(loc_yr_path):
        loc_yr = pd.read_csv(loc_yr_path)
    else:
        loc_yr = get_loc_year_from_master(run_id=run_id, binned_years=bin_years)
        loc_yr.to_csv(loc_yr_path, index=False)

    if os.path.exists(b_loc_path):
        b_loc = pd.read_csv(b_loc_path)
    else:
        b_loc = get_bundle_loc_from_mapping(
            run_id=run_id, clinical_age_group_set_id=clinical_age_group_set_id
        )
        b_loc.to_csv(b_loc_path, index=False)

    lyb = b_loc.merge(loc_yr, how="left", on=["location_id"])

    missing_list = []
    not_sqr = []
    # get the list of bundles we actually refresh with. These are our trusted bundles
    clinical_bundle = query(QUERY,
    )

    total = clinical_bundle.bundle_id.unique().size
    counter = 0

    for b in clinical_bundle.bundle_id.sort_values().unique():
        counter += 1
        print(f"Beginning to process bundle_id {b}, number {counter} of {total}")

        for est in clinical_bundle.query("bundle_id == @b")["estimate_id"]:
            bundle_source = lyb.loc[lyb["bundle_id"] == b, "source"].unique().tolist()
            src_locs = (
                lyb.loc[lyb["source"].isin(bundle_source), "location_id"].unique().tolist()
            )
            # make a square data set to merge existing data onto
            sqr = make_sqr(ages=age_start_list, sexes=sex_list, locations=src_locs)
            sqr.rename(columns={"age_group_id": "age_start"}, inplace=True)
            sqr = sqr.merge(ages, how="left", on=["age_start"], validate="m:1")

            # add on unique years by source
            yr = lyb[lyb["source"].isin(bundle_source)]
            sqr = sqr.merge(
                yr[["location_id", "year_start"]].drop_duplicates(),
                how="outer",
                on="location_id",
            )

            sqr["bundle_id"] = b
            # apply age/sex restrictions to remove pregnant men, etc
            sqr = clinical_mapping.apply_restrictions(
                sqr,
                age_set="binned",
                cause_type="bundle",
                clinical_age_group_set_id=clinical_age_group_set_id,
                map_version=map_version,
            )
            # subset our existing data
            tmp = df.query("bundle_id == @b & estimate_id == @est").copy()

            # outer merge together
            merge_on = [
                "age_group_id",
                "sex_id",
                "location_id",
                "year_start",
                "bundle_id",
            ]
            tmpm = tmp.merge(sqr, how="outer", on=merge_on)

            # remove certain location/years for the injury CF
            if est == 22:
                inj_merge = inj_cfs.drop("remove", 1).columns.tolist()
                tmpm = tmpm.merge(inj_cfs, how="left", on=inj_merge, validate="m:1")
                tmpm = tmpm[tmpm["remove"] == 0]
                tmpm.drop("remove", 1, inplace=True)

            if tmpm.age_start.isnull().sum() != 0:
                not_sqr.append(b)
            miss_df = tmpm[tmpm.estimate_id.isnull()]
            miss_df["missing_estimate_id"] = est
            missing_list.append(miss_df)

    # return missingness by age/sex/loc/year/bundle/estimate
    if len(missing_list) != 0:
        # check for missing rows and record them in a missing df
        res = pd.concat(missing_list, sort=False, ignore_index=True)
        return res
    else:
        return "Test Passed: The df is square"


def remove_known_issues(test):
    """We know that certain bundles aren't present and that maternal data
    has too many ages, remove these from the test"""

    # these bundles have no CF models
    drop_bundles = [
        53,
        61,
        93,
        94,
        168,
        181,
        195,
        196,
        198,
        207,
        208,
        213,
        762,
        763,
        764,
        765,
        766,
        3137,
        6572,
    ]
    real_drop = test.loc[test.bundle_id.isin(drop_bundles), "bundle_id"].sort_values().unique()
    print(f"\n\nThere is hardcoding in this function to drop bundles {real_drop}\n\n")
    test = test[~test.bundle_id.isin(drop_bundles)]

    test["estimate_id"] = test["missing_estimate_id"]


    test = test[(~test.estimate_id.isin([6, 7, 8, 9])) | (test.age_start != 55)]

    problem_subset = test.loc[
        (test.bundle_id.isin([541, 6929])) & (test.age_group_id.isin([388, 389]))
    ]
    test = test.drop(problem_subset.index).reset_index()

    return test


def remove_age_sex_gaps(df, missing_denom):
    """
    Remove missing age_group_ids and sex_ids for select sources.

    * KAZ is  u15 missing ages
    * LBY is missing 0-1 age group and 2-5 for sex_id 2.
    """
    if not missing_denom.empty:
        cols = ["location_id", "age_group_id", "sex_id"]
        lby_vals = [
            (location, age, sex)
            for location in [147]
            for age in [2, 3, 388, 389, 238]
            for sex in [1, 2]
        ]
        lby_vals.append((147, 34, 2))

        kaz_vals = [
            (location, age, sex)
            for location in [36]
            for age in [2, 3, 388, 389, 238, 34, 6, 7]
            for sex in [1, 2]
        ]
        exp_missing_denom = pd.DataFrame(lby_vals + kaz_vals, columns=cols)
        missing_denom = missing_denom.merge(
            exp_missing_denom,
            on=cols,
            how="outer",
            indicator=True,
        )

        missing_denom = (
            missing_denom[missing_denom._merge == "left_only"]
            .drop("_merge", axis=1)
            .reset_index(drop=True)
        )
        df = df.merge(
            exp_missing_denom,
            on=cols,
            how="outer",
            indicator=True,
        )
        df = df[df._merge == "left_only"].drop("_merge", axis=1).reset_index(drop=True)
    return df, missing_denom


def validate_test_df(df, clinical_age_group_set_id, run_id, map_version, bin_years):
    """Runs the main test, then removes known issues and identifies
    rows with zero hosp admissions, ie no sample size and checks
    for unexpected missingness again"""

    df = confirm_inpatient_square(
        df=df,
        run_id=run_id,
        clinical_age_group_set_id=clinical_age_group_set_id,
        map_version=map_version,
        bin_years=bin_years,
        sex_list=[1, 2],
    )
    if isinstance(df, str):
        if df == "Test Passed: The df is square":
            return f"The inpatient data for run {run_id} is as square as we expect"

    missing_denom = identify_missing_denoms(
        run_id=run_id,
        clinical_age_group_set_id=clinical_age_group_set_id,
        bin_years=bin_years,
    )
    df, missing_denom = remove_age_sex_gaps(df, missing_denom)
    df = remove_known_issues(df)
    # missing_denom df might be null is there are no missing denoms

    if not missing_denom.empty:
        merge_cols = missing_denom.drop("denominator_not_present", axis=1).columns.tolist()
        df = df.merge(missing_denom, how="left", on=merge_cols, validate="m:1")

        if df["denominator_not_present"].isnull().any():
            raise ValueError(
                (
                    f"This test failed! there's missing square data in "
                    f"{df.denominator_not_present.isnull().sum()} rows"
                )
            )
    else:
        return f"The inpatient data for run {run_id} is as square as we expect"

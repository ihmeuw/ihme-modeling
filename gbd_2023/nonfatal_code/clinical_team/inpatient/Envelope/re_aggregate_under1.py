"""
Beginning with Run 18 will we try to provide the 4 new (for GBD2020) age groups
under 1 along with the 0-1 aggregated estimates and another live birth
prevalence estimate for select bundles

"""

import glob
import warnings

import numpy as np
import pandas as pd
from crosscutting_functions import demographic, general_purpose, legacy_pipeline
from db_tools.ezfuncs import query

from inpatient.AgeSexSplitting import under1_adjustment
from inpatient.Envelope import apply_bundle_cfs
from crosscutting_functions import uncertainty


class AggU1:
    """
    Make an aggregate under 1 class. Creates and stores a bunch of attributes
    and properties to perform this
    """

    def __init__(
        self,
        run_id,
        sex_id,
        year_start,
        lb_agid,
        input_age_set,
        full_coverage_sources,
        bin_years,
    ):
        """
        Params:
            run_id (int):
                The usual clinical run_id
            sex_id (int):
                1 or 2
            year_start (int):
                Beginning of the 5 year processing group, used
                to assign self.year_end by adding 4
            lb_agid (int):
                The age group id to use for live birth estimates.
                This will overwrite whatever value is returned from
                the covariate estimates func
            input_age_set (int):
                A clinical age group set id, used to identify which
                age groups to aggregate.
        """
        self.run_id = run_id
        self.sex_id = sex_id
        self.year_start = year_start
        self.year_end = year_start
        self.lb_agid = lb_agid
        self.input_age_set = input_age_set
        self.full_coverage_sources = full_coverage_sources
        self.bin_years = bin_years
        self.set_ages
        self.set_base
        self.set_file_list

        if bin_years:
            self.year_end = year_start + 4

    @property
    def set_ages(self):
        """Use the clinical age group set to get age_group_ids/start/end"""
        self.ages = demographic.get_hospital_age_groups(
            clinical_age_group_set_id=self.input_age_set
        )

    @property
    def set_base(self):
        """Define the path to read from using run_id"""
        self.base = FILEPATH

    @property
    def set_file_list(self):
        """Create a list of files to read in for just U1 ages"""
        self.u1_age_groups = (
            self.ages.loc[self.ages["age_start"] < 1, "age_group_id"].unique().tolist()
        )

        self.files = [
            glob.glob(FILEPATH
            )
            for age_group in self.u1_age_groups
        ]
        self.files = [item for sublist in self.files for item in sublist]
        print(f"This job will read in the files {self.files}")
        assert len(self.files) == 4, "Why aren't there 4 files?"

    def _filter_clinical_loc_years(self):
        """The clinical data is very uneven within year groupings. We need to
        identify exactly which years to pull in live births for

        There were multiple methods discussed to do this. The best method would
        probably be to retain the years present as a string or category on the
        same row of aggregated data. But this method should work too.

        We're using the solution of pull apply_env data to solve this problem.
        If we want to update in the future this method will need to be changed.
        """

        loc_years = pd.read_hdf(
            (FILEPATH),
            columns=["location_id", "nid", "year_id", "source"],
        ).drop_duplicates()
        loc_years = loc_years[
            (loc_years.year_id >= self.year_start) & (loc_years.year_id <= self.year_end)
        ]
        drop_locs = [37, 36]  # KGZ and KAZ have no data for 0-14 year olds
        loc_years = loc_years[~loc_years.location_id.isin(drop_locs)]
        loc_years["year_start"] = loc_years["year_id"]

        # NIDs were single year but need merged
        if self.bin_years:
            loc_years = legacy_pipeline.apply_merged_nids(loc_years)
        loc_years.drop(["source", "year_start"], axis=1, inplace=True)

        test = self.df[["location_id", "nid", "years"]].drop_duplicates().copy()
        ly_test = loc_years.groupby(["location_id", "nid"]).year_id.size().reset_index()
        ly_test.rename(columns={"year_id": "ly_years"}, inplace=True)
        test = test.merge(ly_test, how="left", on=["location_id", "nid"], validate="1:1")

        test = test[~test.location_id.isin([521, 4733])]

        failures = []
        if test.isnull().any().any():
            failures.append("There can not be missing test values")
            warnings.warn("failed null test")
            test.to_csv(
                (FILEPATH
                ),
                index=False,
            )
        if not (test["years"] == test["ly_years"]).all():
            failures.append("The year counts don't match!")
            warnings.warn("There was a failed year test. why??")
            test.query("years != ly_years").to_csv(
                (
                    FILEPATH
                ),
                index=False,
            )
        if failures:
            e = (
                "One or more tests failed while filter location-years for the "
                "live birth covariate. Review FILEPATH"
                "agg_u1_tests for the failed tests"
            )
            raise ValueError(e)

        self.plb = self.live_births.copy()
        pre_lb = self.live_births.shape
        self.live_births = self.live_births.merge(
            loc_years, how="inner", on=["location_id", "year_id"], validate="m:1"
        )
        post_lb = self.live_births.shape
        diff = pre_lb[0] - post_lb[0]
        print(
            (
                f"By filtering on year we've removed {diff} rows from the "
                f"live birth data. from {pre_lb} to {post_lb}"
            )
        )

    def set_live_births(self):
        """Get live birth covariate per
        Haidong and write it to drive as well as assigning it to the class
        """

        self.live_births = legacy_pipeline.get_covar_for_clinical(
            run_id=self.run_id,
            covariate_name_short="live_births_by_sex",
            iw_profile="clinical",
            year_id=list(range(self.year_start, self.year_end + 1)),
            location_id=self.df.location_id.unique().tolist(),
        )
        self._filter_clinical_loc_years()

        # manually set the age group
        self.live_births["age_group_id"] = self.lb_agid
        lb_path = (
            f"{self.base}"
        )
        self.live_births.to_csv(lb_path, index=False)
        self._clean_covariate_data()

        self._agg_lb_years()

    def _clean_covariate_data(self):
        """covariate output has some unneeded columns and use a clearer name
        for live births
        """
        drops = [
            "location_name",
            "age_group_name",
            "sex",
            "covariate_name_short",
            "covariate_id",
            "model_version_id",
            "lower_value",
            "upper_value",
        ]
        self.live_births.drop(drops, axis=1, inplace=True)
        self.live_births.rename(columns={"mean_value": "live_births"}, inplace=True)

    def _agg_lb_years(self):
        """
        Drops cols and when replace the single year live birth counts with 5 year aggregations
        when year_end != year_start
        """

        self.live_births.drop("year_id", axis=1, inplace=True)
        self.live_births["year_start"] = self.year_start
        self.live_births["year_end"] = self.year_end

        assert not self.live_births.isnull().any().any(), "Can't drop rows"

        sum_cols = ["live_births"]
        group_cols = self.live_births.columns.drop(sum_cols).tolist()

        sum_cols = dict(zip(sum_cols, ["sum"] * len(sum_cols)))
        self.live_births = self.live_births.groupby(group_cols).agg(sum_cols).reset_index()

        # write the aggregated data out
        lb_path = (FILEPATH
        )
        self.live_births.to_csv(lb_path, index=False)

    def create_lb_ests(self):
        """We also want to use the same aggregated denominators, but divide them
        by the live birth 'population' to get prevalence rates"""

        self.set_live_births()
        lb_df = self.df.copy()
        lb_df["age_group_id"] = self.lb_agid
        id_cols = [
            "age_group_id",
            "location_id",
            "nid",
            "sex_id",
            "year_start",
            "year_end",
        ]
        lb_df = lb_df.merge(self.live_births, how="left", on=id_cols, validate="m:1")
        assert not lb_df.drop("mean", 1).isnull().any().any()

        lb_df.drop(lb_df.filter(regex="draw_").columns.tolist(), 1, inplace=False).to_csv(
            (FILEPATH
            ),
            index=False,
        )

        lb_df.drop("population", 1, inplace=True)
        lb_df.rename(columns={"live_births": "population"}, inplace=True)

        # replace sample size with pop for lb_agid and full cover sources
        lb_df.loc[lb_df["source"].isin(self.full_coverage_sources), "sample_size"] = lb_df.loc[
            lb_df["source"].isin(self.full_coverage_sources), "population"
        ]

        self.df = pd.concat([self.df, lb_df], sort=False, ignore_index=True)

    def _filter_age_groups(self):
        """We should only be reading in U1 ages but filter to make sure!"""
        self.df = self.df[self.df["age_group_id"].isin(self.u1_age_groups)]
        if len(self.df) == 0:
            raise ValueError("Where'd all the observations go?")
        if set(self.df["age_group_id"].unique()).symmetric_difference(self.u1_age_groups):
            raise ValueError("It looks like not all age groups are present...")

    def _get_df(self):
        self.df = pd.concat(
            [pd.read_hdf(f) for f in self.files], sort=False, ignore_index=True
        )
        print("Data has been captured")
        self._filter_age_groups()
        self._set_conversion_cols()
        self._cap_insane_values()
        self._make_zero_flagged_draws_zeros()

    def _reset_age_group(self):
        """Just do this manually"""
        self.df["age_group_id"] = 28

    def _set_conversion_cols(self):
        """Define cols to convert when going from rates->counts->rates"""
        self.conv_cols = self.df.filter(regex="^draw_").columns.tolist()
        self.conv_cols += ["mean"]

    def _cap_insane_values(self):
        """CF models are still wild, cap RATES at 1 million per person!"""
        for col in self.conv_cols:
            self.df.loc[self.df[col] > 1e6, col] = 1e6

    def _make_zero_flagged_draws_zeros(self):
        """For rows where original mean would be 0 (zero flagged),
        replace the sampled draws with all 0 here so we can aggregate
        """
        draw_cols = [col for col in self.df.columns if col.startswith("draw_")]
        df_zero = self.df.loc[self.df["zero_flag"] == 1].copy().reset_index(drop=True)
        df_nonzero = self.df.loc[self.df["zero_flag"] == 0].copy().reset_index(drop=True)
        for col in draw_cols:
            df_zero[col] = 0

        self.df = pd.concat([df_nonzero, df_zero], ignore_index=True)
        # can drop zero_flagged col
        self.df = self.df.drop(["zero_flag"], axis=1)
        del df_nonzero, df_zero

    def _drop_ui_cols(self):
        """Upper and lower can't be used, gotta re-calculate"""
        self.df.drop(["upper", "lower", "median_CI_team_only"], 1, inplace=True)

    # go from rates to counts using population
    def _space_converter(self, conversion, apply_time_scalar=False):
        """Convert between rate and count space"""

        if conversion == "rate":
            for col in self.conv_cols:
                pre_median = self.df.loc[self.df[col] != 0, col].median()
                self.df[col] = self.df[col] / self.df["population"]
                post_median = self.df.loc[self.df[col] != 0, col].median()

                if np.isnan(pre_median):
                    pass
                else:
                    if pre_median < post_median:
                        raise ValueError(
                            (
                                "Why is the median before conversion "
                                "smaller than the median after conversion "
                                "for a conversion FROM counts?"
                            )
                        )

        elif conversion == "count":
            if apply_time_scalar:
                warnings.warn(
                    (
                        "I don't think this method is correct. "
                        "Why systemically scale up the counts??"
                    )
                )
                # attach the scalars to make under 1 readjustment
                full_agedf = query(
                    (
                        """
                        QUERY"""
                    ),
                    conn_def="epi",
                )
                self.df = under1_adjustment.df_calculate_scalar(
                    df=self.df, conversion="rate_to_count", full_agedf=full_agedf
                )
                for col in self.conv_cols:
                    pre_median = self.df.loc[self.df[col] != 0, col].median()
                    self.df[col] = (self.df[col] * self.df["population"]) / self.df["scalar"]
                    post_median = self.df.loc[self.df[col] != 0, col].median()
                    if np.isnan(pre_median):
                        pass
                    else:
                        if pre_median > post_median:
                            raise ValueError(
                                (
                                    "Why is the median before conversion "
                                    "larger for a conversion FROM rates?"
                                )
                            )

                self.df.drop("scalar", axis=1, inplace=True)
            else:
                for col in self.conv_cols:
                    pre_median = self.df.loc[self.df[col] != 0, col].median()
                    self.df[col] = self.df[col] * self.df["population"]
                    post_median = self.df.loc[self.df[col] != 0, col].median()

                    if np.isnan(pre_median):
                        pass
                    else:
                        if pre_median > post_median:
                            raise ValueError(
                                (
                                    "Why is the median before conversion "
                                    "larger for a conversion FROM rates?"
                                )
                            )

        else:
            raise ValueError(f"What is {conversion}")

    def _gen_sum_dict(self):
        sum_cols = self.conv_cols + [
            "population",
            "sample_size",
        ]
        self.sum_dict = dict(zip(sum_cols, ["sum"] * len(sum_cols)))

    def agg_to_u1(self):
        """Perform the groupby and aggregate"""
        self._reset_age_group()
        self._gen_sum_dict()

        # groupby and aggregate
        group_cols = [
            c for c in self.df.columns.tolist() if c not in list(self.sum_dict.keys())
        ]
        print(f"Grouping by these cols {group_cols}")
        pre = len(self.df)

        assert self.df["age_group_id"].unique().size == 1, "bad"
        assert self.df[group_cols].isnull().sum().sum() == 0, "these are gonna be dropped"
        self.df = self.df.groupby(group_cols).agg(self.sum_dict).reset_index()
        post = len(self.df)
        print(f"Row counts reduced from {pre} to {post}")
        # mean for draw cols is no longer correct
        self.df["mean"] = np.nan

    def pipeline_wrapper(self, map_version):
        """call the actual functions from the pipeline to continue processing this data
        in exactly the same way as the inp pipeline
        Note- we're not actually re-applying the correction factors, just re-doing all
        the processing after they've been applied b/c we have to agg at the draw level
        """

        # create a backup copy of data to loop over multi 'ages'
        self.back = self.df.copy()

        for age in self.back["age_group_id"].unique():
            # subset to a single age group
            self.df = self.back[self.back["age_group_id"] == age].copy()

            self.df = uncertainty.add_ui(self.df)

            draw_cols = self.df.filter(regex="^draw_").columns.tolist()
            self.df = self.df.drop(draw_cols, axis=1)

            self.df = swap_zero_upper(df=self.df)

            self.df = apply_bundle_cfs.clean_after_write(self.df)

            # merge measures on using icg measures
            self.df = apply_bundle_cfs.merge_measure(self.df, map_version)

            # set nulls
            self.df = apply_bundle_cfs.align_uncertainty(self.df)

            # apply the 5 year inj corrections
            self.df = legacy_pipeline.apply_inj_corrections(
                df=self.df, run_id=self.run_id, bin_years=self.bin_years, iw_profile="clinical"
            )

            # apply e code proportion cutoff, removing rows under cutoff
            self.df = legacy_pipeline.remove_injuries_under_cutoff(
                df=self.df, run_id=self.run_id, bin_years=self.bin_years, iw_profile="clinical"
            )

            # final write to /share
            write_path = (FILEPATH.format(
                    rid=self.run_id, age=age, sex=self.sex_id, year=self.year_start
                )
            )
            self.df.to_csv(write_path, index=False)


def swap_zero_upper(df: pd.DataFrame) -> pd.DataFrame:
    """There could be demos where all 4 U1 groups have original mean 0.
    Their upper post-UI would be 0 as well. We swap with with each demo's min.

    Args:
        df: df with UIs added

    Returns:
        df with upper 0 replaced
    """
    pre = len(df)
    df_zero = df.loc[df["upper"] == 0].copy().reset_index(drop=True)
    df_nonzero = df.loc[df["upper"] != 0].copy().reset_index(drop=True)

    # i is a tuple of unique (bundle_id, estimate_id)
    for i, tmp_df in df_zero.groupby(["bundle_id", "estimate_id"]):
        group_df = df_nonzero.loc[
            (df_nonzero["bundle_id"] == i[0]) & (df_nonzero["estimate_id"] == i[1])
        ]
        if group_df.empty:
            min_upper = min(df_nonzero.loc[(df_nonzero["estimate_id"] == i[1]), "upper"])
        else:
            min_upper = min(group_df["upper"])
        df_zero.loc[
            (df_zero["bundle_id"] == i[0]) & (df_zero["estimate_id"] == i[1]), "upper"
        ] = min_upper

    df = pd.concat([df_nonzero, df_zero], ignore_index=True)
    del df_nonzero, df_zero

    if len(df) != pre:
        raise RuntimeError("Replacing uppers somehow added/removed rows")

    return df


def concat_agged_data(run_id, cf_agg_stat):
    """processed in parallel by year and sex, concat back together"""

    files = glob.glob(
        (FILEPATH
        )
    )
    df = pd.concat([pd.read_csv(f) for f in files], sort=False, ignore_index=True)

    if cf_agg_stat == "median":
        # for rows with uncertainty, replace mean with median
        df.loc[df["lower"].notnull(), "mean"] = df.loc[
            df["lower"].notnull(), "median_CI_team_only"
        ]
    else:
        pass

    # drop cols that don't align with env unc
    df = df.drop(["median_CI_team_only"], axis=1)

    return df


# run it all
def create_u1_and_lb_aggs(
    run_id,
    input_age_set,
    year_start_list,
    cf_agg_stat,
    map_version,
    full_coverage_sources,
    bin_years,
    lb_agid=164,
):
    """Runs the main bits of the agg class then writes an aggregated CSV output

    Params:
        run_id (int):
            a clinical run_id
        input_age_set (int):
            The clinical age set to reduce, currently only '2' works
        year_start_list (list):
            processes data by sex and year, matching the para jobs (sans age)
        cf_agg_stat (str):
            should we take a midpoint from the draws using 'median' or 'mean'
        lb_agid (int):
            The age_group_id to use for the live birth data. It's currently
            marked as 'all_ages' when it comes out of get covar ests so this
            will overwrite whatever value is there.
    """
    base = FILEPATH

    if input_age_set not in [2]:
        raise ValueError(
            (
                f"Are you sure you want to run this "
                f"with age set {input_age_set}? It "
                f"has only been tested with clinical "
                f"age set 2."
            )
        )

    for sex_id in [1, 2]:
        for year_start in year_start_list:
            agg = AggU1(
                run_id=run_id,
                input_age_set=input_age_set,
                sex_id=sex_id,
                year_start=year_start,
                full_coverage_sources=full_coverage_sources,
                lb_agid=lb_agid,
                bin_years=bin_years,
            )
            agg._get_df()
            agg._drop_ui_cols()
            agg._space_converter(conversion="count")
            agg.agg_to_u1()
            agg.create_lb_ests()

            # write intermediate data in count space if needed for review
            general_purpose.write_hosp_file(
                agg.df,
                write_path=(FILEPATH
                ),
                backup=False,
            )
            agg._space_converter(conversion="rate")

            warnings.warn(
                """
            re-create the end of the inp pipeline and write outputs
            """
            )
            agg.pipeline_wrapper(map_version)

    df = concat_agged_data(run_id, cf_agg_stat)
    df.to_csv(FILEPATH, index=False)
    return

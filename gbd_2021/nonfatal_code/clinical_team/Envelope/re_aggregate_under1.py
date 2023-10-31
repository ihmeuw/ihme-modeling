"""
Beginning with Run 18 will we try to provide the 4 new (for GBD2020) age groups
under 1 along with the 0-1 aggregated estimates and another live birth
prevalence estimate for select bundles

"""

import pandas as pd
import numpy as np
import glob
import ipdb
import warnings
from db_tools.ezfuncs import query
from clinical_info.Functions import hosp_prep, gbd_hosp_prep
from clinical_info.Envelope import apply_bundle_cfs
from clinical_info.AgeSexSplitting import under1_adjustment
from clinical_info.Database.bundle_relationships import relationship_methods


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
        gbd_round_id,
        decomp_step,
        lb_agid,
        input_age_set,
        full_coverage_sources,
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
            gbd_round_id (int):
                Identifies gbd cycle for shared funcs
            decomp_step (int):
                Identifies gbd step for shared funcs
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
        self.year_end = year_start + 4
        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.lb_agid = lb_agid
        self.input_age_set = input_age_set
        self.full_coverage_sources = full_coverage_sources
        self.set_ages
        self.set_base
        self.set_file_list

    @property
    def set_ages(self):
        """Use the clinical age group set to get age_group_ids/start/end"""
        self.ages = hosp_prep.get_hospital_age_groups(
            clinical_age_group_set_id=self.input_age_set
        )

    @property
    def set_base(self):
        """Define the path to read from using run_id"""
        self.base = f"FILEPATH"

    @property
    def set_file_list(self):
        """Create a list of files to read in for just U1 ages"""
        self.u1_age_groups = (
            self.ages.loc[self.ages["age_start"] < 1, "age_group_id"].unique().tolist()
        )

        self.files = [glob.glob(f"FILEPATH") for age_group in self.u1_age_groups]
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
            (f"FILEPATH"), columns=["location_id", "nid", "year_id", "source"],
        ).drop_duplicates()
        loc_years = loc_years[
            (loc_years.year_id >= self.year_start)
            & (loc_years.year_id <= self.year_end)
        ]
        drop_locs = [37]  # KGZ has no data for 0-14 year olds
        loc_years = loc_years[~loc_years.location_id.isin(drop_locs)]
        loc_years["year_start"] = loc_years["year_id"]
        # NIDs were single year but need merged
        loc_years = hosp_prep.apply_merged_nids(loc_years)
        loc_years.drop(["source", "year_start"], axis=1, inplace=True)

        test = self.df[["location_id", "nid", "years"]].drop_duplicates().copy()
        ly_test = loc_years.groupby(["location_id", "nid"]).year_id.size().reset_index()
        ly_test.rename(columns={"year_id": "ly_years"}, inplace=True)
        test = test.merge(
            ly_test, how="outer", on=["location_id", "nid"], validate="1:1"
        )

        # tricky issue where this location does not have U1 data for 1 of 3 years
        # due to differing sources
        test = test[~test.location_id.isin([521, 4733])]

        failures = []
        if test.isnull().any().any():
            failures.append("There can not be missing test values")
            warnings.warn("failed null test")
            test.to_csv(
                (f"FILEPATH"), index=False,
            )
        if not (test["years"] == test["ly_years"]).all():
            failures.append("The year counts don't match!")
            warnings.warn("There was a failed year test. why??")
            test.query("years != ly_years").to_csv(
                (f"FILEPATH"), index=False,
            )
        if failures:
            e = (
                "One or more tests failed while filter location-years for the "
                "live birth covariate. Review FILEPATH for the failed tests"
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
        """Use decomp step and gbd_id to get the live birth covariate per
        Haidong and write it to drive as well as assigning it to the class
        """

        self.live_births = gbd_hosp_prep.get_covar_for_clinical(
            run_id=self.run_id,
            covariate_name_short="live_births_by_sex",
            year_id=list(range(self.year_start, self.year_end + 1)),
            location_id=self.df.location_id.unique().tolist(),
        )
        self._filter_clinical_loc_years()

        # manually set the age group
        self.live_births["age_group_id"] = self.lb_agid
        lb_path = f"FILEPATH"
        self.live_births.to_csv(lb_path, index=False)
        self._clean_covariate_data()
        # remove the columns we won't use
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
        """Replace the single year live birth counts with 5 year aggregations
        """

        self.live_births.drop("year_id", axis=1, inplace=True)
        self.live_births["year_start"] = self.year_start
        self.live_births["year_end"] = self.year_end

        assert not self.live_births.isnull().any().any(), "Can't drop rows"
        # match what we do with population, so just use a point estimate
        sum_cols = ["live_births"]
        group_cols = self.live_births.columns.drop(sum_cols).tolist()

        sum_cols = dict(zip(sum_cols, ["sum"] * len(sum_cols)))
        self.live_births = (
            self.live_births.groupby(group_cols).agg(sum_cols).reset_index()
        )

        # write the aggregated data out
        lb_path = f"FILEPATH"
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

        lb_df.drop(
            lb_df.filter(regex="draw_").columns.tolist(), 1, inplace=False
        ).to_csv(
            (f"FILEPATH"), index=False,
        )

        lb_df.drop("population", 1, inplace=True)
        lb_df.rename(columns={"live_births": "population"}, inplace=True)

        # replace sample size with pop for lb_agid and full cover sources
        lb_df.loc[
            lb_df["source"].isin(self.full_coverage_sources), "sample_size"
        ] = lb_df.loc[lb_df["source"].isin(self.full_coverage_sources), "population"]

        self.df = pd.concat([self.df, lb_df], sort=False, ignore_index=True)

    def _filter_age_groups(self):
        """We should only be reading in U1 ages but filter to make sure!"""
        self.df = self.df[self.df["age_group_id"].isin(self.u1_age_groups)]
        if len(self.df) == 0:
            raise ValueError("Where'd all the observations go?")
        if set(self.df["age_group_id"].unique()).symmetric_difference(
            self.u1_age_groups
        ):
            raise ValueError("It looks like not all age groups are present...")

    def _get_df(self):
        self.df = pd.concat(
            [pd.read_hdf(f) for f in self.files], sort=False, ignore_index=True
        )
        print("Data has been captured")
        self._filter_age_groups()
        self._set_conversion_cols()
        self._cap_insane_values()

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
                full_agedf = query((f"QUERY"), conn_def="CONN",)
                self.df = under1_adjustment.df_calculate_scalar(
                    df=self.df, conversion="rate_to_count", full_agedf=full_agedf
                )
                for col in self.conv_cols:
                    pre_median = self.df.loc[self.df[col] != 0, col].median()
                    self.df[col] = (self.df[col] * self.df["population"]) / self.df[
                        "scalar"
                    ]
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
            "maternal_sample_size",
        ]
        self.sum_dict = dict(zip(sum_cols, ["sum"] * len(sum_cols)))

    def _add_none_cf_type(self):
        """Mean0 was getting a null cf type which isn't good for groupbys, replace with str"""
        self.df["cf_type"].fillna("none", inplace=True)

    def agg_to_u1(self):
        """Perform the groupby and aggregate"""
        self._reset_age_group()
        self._add_none_cf_type()
        self._gen_sum_dict()

        # groupby and aggregate
        group_cols = [
            c for c in self.df.columns.tolist() if c not in list(self.sum_dict.keys())
        ]
        print(f"Grouping by these cols {group_cols}")
        pre = len(self.df)
        assert self.df["age_group_id"].unique().size == 1, "bad"
        assert (
            self.df[group_cols].isnull().sum().sum() == 0
        ), "these are gonna be dropped"
        self.df = self.df.groupby(group_cols).agg(self.sum_dict).reset_index()
        post = len(self.df)
        print(f"Row counts reduced from {pre} to {post}")
        # mean for draw cols is no longer correct
        self.df.loc[self.df["use_draws"], "mean"] = np.nan

    def pipeline_wrapper(self):
        """call the actual functions from the pipeline to continue processing this data
        in exactly the same way as the inp pipeline
        """

        # create a backup copy of data b/c we'll need to loop over multi 'ages'
        self.back = self.df.copy()

        parent_method = relationship_methods.ParentInjuries(run_id=self.run_id)
        parent_method.get_tables()

        for age in self.back["age_group_id"].unique():
            # subset to a single age group
            self.df = self.back[self.back["age_group_id"] == age].copy()

            self.df = apply_bundle_cfs.add_UI(self.df)

            draw_cols = self.df.filter(regex="^draw_").columns.tolist()
            assert len(draw_cols) == 1000 or len(draw_cols) == 0, "wrong num draws"
            self.df.drop(draw_cols, axis=1, inplace=True)

            self.df = apply_bundle_cfs.clean_after_write(self.df)

            # unify sample size
            self.df = apply_bundle_cfs.identify_correct_sample_size(
                self.df, full_coverage_sources=self.full_coverage_sources
            )

            # merge measures on using icg measures
            self.df = apply_bundle_cfs.merge_measure(
                self.df, parent_method.bundle_relationship
            )

            # set nulls
            self.df = apply_bundle_cfs.align_uncertainty(self.df)

            # apply the 5 year inj corrections
            self.df = hosp_prep.apply_inj_corrections(self.df, self.run_id)

            # apply e code proportion cutoff, removing rows under cutoff
            self.df = hosp_prep.remove_injuries_under_cutoff(self.df, self.run_id)

            # append and apply the haqi correction
            self.df = apply_bundle_cfs.apply_haqi_corrections(self.df, self.run_id)

            # final write to /share
            write_path = "FILEPATH"
            self.df.to_csv(write_path, index=False)


def concat_agged_data(run_id, cf_agg_stat):
    """processed in parallel by year and sex, concat back together
    """

    files = glob.glob((f"FILEPATH"))
    l = []
    for f in files:
        l.append(pd.read_csv(f))
    df = pd.concat(l, sort=False, ignore_index=True)

    if cf_agg_stat == "median":
        # for rows with uncertainty, replace mean with median
        df.loc[df["lower"].notnull(), "mean"] = df.loc[
            df["lower"].notnull(), "median_CI_team_only"
        ]
    else:
        pass

    # drop cols that don't align with env unc
    df.drop(["use_draws", "median_CI_team_only"], axis=1, inplace=True)

    return df


# run it all
def create_u1_and_lb_aggs(
    run_id,
    gbd_round_id,
    decomp_step,
    input_age_set,
    year_start_list,
    cf_agg_stat,
    full_coverage_sources,
    lb_agid=164,
):
    """Runs the main bits of the agg class then writes an aggregated CSV output

    Params:
        run_id (int):
            a clinical run_id
        gbd_round_id (int):
            identify the gbd cycle
        decomp_step (str):
            identify the step within a gbd cycle, used with get covar ests
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
    base = f"FILEPATH"

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
                decomp_step=decomp_step,
                gbd_round_id=gbd_round_id,
                input_age_set=input_age_set,
                sex_id=sex_id,
                year_start=year_start,
                full_coverage_sources=full_coverage_sources,
                lb_agid=lb_agid,
            )
            agg._get_df()
            agg._drop_ui_cols()
            agg._space_converter(conversion="count")
            agg.agg_to_u1()
            agg.create_lb_ests()

            # write intermediate data in count space if needed for review
            hosp_prep.write_hosp_file(
                agg.df, write_path=(f"FILEPATH"), backup=False,
            )
            agg._space_converter(conversion="rate")

            warnings.warn(
                """
            re-create the end of the inp pipeline and write outputs"""
            )
            agg.pipeline_wrapper()

    df = concat_agged_data(run_id, cf_agg_stat)
    df.to_csv(f"FILEPATH", index=False)
    return

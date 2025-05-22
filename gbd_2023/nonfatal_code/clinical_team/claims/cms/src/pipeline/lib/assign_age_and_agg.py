import datetime

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.mapping.clinical_mapping import apply_restrictions

from cms.helpers.intermediate_tables.denom.U1_age_days import age_lookup, gbd_age_days


class EstimateNumerator:
    """
    Class creates the numerator, for a given bundle estimate id, by creating
    age at date of admission (eg. service date) and then re-aggergate
    data based on age and column values unique to the
    cms_system (medicare or medicaid) and deliverable (gbd or ushd)

    Assumes that the input is de-duplicated claims.
    """

    def __init__(self, df, clinical_age_group_set_id, logger, map_version):
        self.df = df
        self.clinical_age_group_set_id = clinical_age_group_set_id
        self.logger = logger
        self.map_version = map_version

    def apply_bundle_age_sex_restriction(self):
        """
        Drop rows based on age sex restrictions
        """

        # note that the demo tbls have a 0 sex_id flag.
        # we made the decision to retain all data in tbl fields
        # for a as long as possible
        self.df = self.df[self.df.sex_id.isin([1, 2])]

        self.df = apply_restrictions(
            self.df,
            age_set="indv",
            cause_type="bundle",
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            break_if_not_contig=False,
            map_version=self.map_version,
        )

    def set_service_date_floor(self):
        """Service date on claims for anyone over 1 will be adjusted to be no earlier than 1/1 of the reporting year"""

        pre = len(self.df)
        self.df["rough_age"] = self.df.year_id - self.df.dob.dt.year  # age req to find U1 data

        # isolating mismatched years for those older than 1. replacing service_start dates before the reporting year (year_id) with
        # the date 1/1{year_id}
        cond = "(self.df.service_start.dt.year < self.df.year_id) & (self.df.rough_age > 1)"
        adjust_years = self.df.loc[eval(cond), "year_id"]
        adjust_dates = pd.to_datetime(
            pd.Series([datetime.date(year, 1, 1) for year in adjust_years])
        )
        adjust_dates.index = adjust_years.index
        self.df.loc[eval(cond), "service_start"] = adjust_dates
        post = len(self.df)

        u1_cond = "(self.df.rough_age > 1)"
        # Comfirm that for anyone older than 1 the service start date and the reporting date match
        assert (
            self.df[eval(u1_cond)].year_id == self.df[eval(u1_cond)].service_start.dt.year
        ).all(), "The service start adjustment failed"
        self.df.drop("rough_age", axis=1, inplace=True)
        self.logger.info(
            f"{pre-post} rows were adjusted to match the service_start year with the reporting year for anyone older than 1 y.o."
        )

    def remove_future_service_dates(self):
        """Remove any claims which have a service date beyond the reporting year"""
        pre = len(self.df)
        self.df = self.df[self.df.service_start.dt.year <= self.df.year_id]
        post = len(self.df)

        self.logger.info(
            f"{pre - post} rows were removed due to service starts later than reporting years"
        )

    def svc_dt_age(self):
        """
        create age based on service_start date.

        NOTE: This function transforms datetime object to be represented in a 30.5 days per month year
        space to calculate age. This decision was made to stay consistent with the data created in
        age_days.py
        """

        self.remove_future_service_dates()
        self.set_service_date_floor()

        for col in ["dob", "service_start"]:
            self.df[f"{col}_year_days"] = (
                (self.df[col].dt.year * 366)
                + (self.df[col].dt.month * 30.5)
                + (self.df[col].dt.day)
            )

        self.df["age"] = (self.df.service_start_year_days - self.df.dob_year_days) / 366

        # remove and quantify null and negative ages, only happening in max
        pre = len(self.df)
        self.df = self.df[self.df.dob <= self.df.service_start]
        post = len(self.df)
        self.logger.info(
            f"{pre-post} rows have been removed due to null OR negative ages. There were {pre} rows now there are {post} rows"
        )

        assert all(self.df.age >= 0), "Negative ages. Check service_start_year_days"
        assert all(self.df.age.notnull()), "Null ages, check service start year days and dob"
        self.df["age_floor"] = np.floor(self.df.age)

        # Keep age float type. Retain under 1 age detail otherwise
        # keep age as a whole number
        self.df["age"] = np.where(self.df.age_floor >= 1, self.df.age_floor, self.df.age)
        self.df.drop(["service_start_year_days", "dob_year_days", "age_floor"], axis=1)

        if self.df[self.df.age < 1].shape[0] >= 1:
            self.convert_u1_age_groups()

    def convert_u1_age_groups(self):
        """
        The denominator bins u1 ages into age start year space (eg 0.0875 is replaced with
        the age of 0.07671233). In order to be aligned with the denom we need to convert u1
        ages to these bins.
        """

        u1 = self.df[self.df.age < 1].copy()
        pre = u1.shape[0]

        self.df = self.df[self.df.age >= 1]

        u1["age"] = u1.age * 366

        # round to the nearest half decimal places.
        u1["age"] = round(u1["age"] * 2) / 2

        # merge on age in day space bin, and re-label
        # bin age as 'age' column
        al = pd.concat(age_lookup())
        al.rename({"ages": "age"}, axis=1, inplace=True)
        u1 = u1.merge(al, on="age")
        assert pre == u1.shape[0], "Merge on age_lookup failed"
        u1.drop("age", axis=1, inplace=True)
        u1.rename({"age_stop": "age"}, axis=1, inplace=True)

        # merge on terminal age, in year space, bin for
        # a given u1 age group.
        pre = u1.shape[0]

        gad = pd.concat(gbd_age_days())
        u1 = u1.merge(gad, on="age")
        assert pre == u1.shape[0], "Merge on gbd_age_days failed"
        u1.drop("age", axis=1, inplace=True)
        u1.rename({"gbd_age": "age"}, axis=1, inplace=True)

        self.df = pd.concat([self.df, u1], sort=False)
        assert pre == len(
            self.df[self.df.age < 1]
        ), "The count of under 1 rows of data have changed"

    def agg_counts(self, deliverable, cms_system):
        self.df["val"] = 1

        # agg columns to match denom
        gb_cols = (
            constants.cms_system_deliverable_gb_dict["demo"]
            + constants.cms_system_deliverable_gb_dict["est"]
            + deliverable.groupby_cols
        )

        self.df = self.df.groupby(gb_cols).agg({"val": "sum"}).reset_index()

        if len(self.df) == 0:
            self.logger.info("All grouped data has been lost, perhaps there were nulls?")
            assert False, "The groupby has removed all rows of data."


def run(
    df,
    clinical_age_group_set_id,
    logger,
    cms_system,
    deliverable,
    map_version,
):
    """For use in the main cms create estimates function"""
    pre = len(df)
    est_num = EstimateNumerator(df, clinical_age_group_set_id, logger, map_version)
    est_num.svc_dt_age()
    logger.info(
        f"{pre - len(est_num.df)} rows were lost from starting DF when adding age at date of service"
    )

    # set min and max age to filter ages above/below. depends on both cms system and deliverable
    min_age = constants.sys_deliverable_ages[cms_system][deliverable.name]["min_age"]
    max_age = constants.sys_deliverable_ages[cms_system][deliverable.name]["max_age"]

    age_cond = f"(est_num.df['age'] >= {min_age}) & (est_num.df['age'] <= {max_age})"
    est_num.df = est_num.df[eval(age_cond)]
    logger.info(
        f"{pre - len(est_num.df)} rows were lost from starting DF when removing ages outside of CMS system range"
    )

    # now apply restrictions without those bad ages
    est_num.apply_bundle_age_sex_restriction()

    removed = pre - len(est_num.df)
    logger.info(
        f"DF has {len(est_num.df)} rows. {removed} rows have been removed from numerators b/c they're outside of acceptable ages by deliverable and cms system or outside of age/sex restrictions"
    )

    est_num.agg_counts(deliverable, cms_system)
    logger.info(
        f"The DF was originally {pre} rows and is now {len(est_num.df)} rows after single-year age aggregation"
    )
    return est_num.df

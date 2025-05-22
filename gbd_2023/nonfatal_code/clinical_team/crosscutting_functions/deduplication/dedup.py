"""
This Class will filter and de-duplicate data at the bundle level. If you have a df
with multiple bundles present you'll need to loop over and subset for each bundle
id or better yet parallelize to process all bundles at once.

Workflow
1) Filter data based on estimate_id class to meet estimate requirements.
2) Extract and validate bundle id.
3) Get bundle-measure from clinical db and compare against data measure.
    3.a) Multi-step process. If measure is present in data compare it
         against DATABASE. If measure is not present
         in data then use measure from DATABASE.
         If bundle is not active, and therefore not present in that table then
         pull in the measure using the clinical mapping module.
4) Use measure to identify deduplication method.
5) Sort the data by enrollee_col and service_start_col.
6) Perform deduplication algorithm based on measure.
    Prevalence bundles-
        1) Assign 2 otp rule attr using flat file.
        2) If 2 otp rule, drop single otp visits.
        3) Deduplicate by enrollee_col, year_id and entity_id.
        4) Return results.
    Incidence bundles-
        1) Require a duration column.
        2) IF 365 day duration, calculate by dropping duplicates in the year.
        3) Pass enrollees with a single claim to final inc array.
        4) Run deduplication on the remaining enrollee data.
        5) Return results.
"""

import warnings
from typing import List, Union

import numpy as np
import pandas as pd
from crosscutting_functions.mapping import clinical_mapping_db
from crosscutting_functions.mapping.bundle_relationships import relationship_tables
from db_tools.ezfuncs import query

from crosscutting_functions.deduplication import deduplicate_single_entity, estimates


class ClinicalDedup:
    """Main class to handle the deduplication of individually identifiable data.

    Example:
    # instantiate with your column names and desired estimate_id as variables
    dedup = ClinicalDedup(my_enrollee_col, my_date_col, my_year_col, my_estimate_id)
    df = dedup.main(df=df)  # deduplicates the df object and returns it
    """

    df: pd.DataFrame
    enrollee_col: str
    service_start_col: str
    year_col: str
    estimate_id: int
    map_version: Union[int, str]

    def __init__(
        self,
        enrollee_col: str,
        service_start_col: str,
        year_col: str,
        estimate_id: int,
        map_version: Union[int, str],
        verbose: bool = True,
    ) -> None:
        """
        enrollee_col: Name of individual identifier column, ie 'bene_id', 'enrolid', etc
        service_start_col: Name of the datetime column which contains
                        the date of service start or admission.
        year_col: Name of year col, ie 'year_id' or 'year_start'.
        estimate_id: One of the estimate_ids we use. Used to determine if
                     deduplication should be applied and for the 2 otp rule.
        map_version: Clinical map version which is used to pull data from
                     the DATABASE db.
        verbose: Prints additional information while running if True.
        """

        self.enrollee_col = enrollee_col
        self.service_start_col = service_start_col
        self.year_col = year_col
        self.estimate_id = estimate_id
        self.verbose = verbose

        self.estimate_class = getattr(estimates, f"ESTIMATE_{self.estimate_id}")

        self.entity_type = "bundle_id"
        self.dedup_cols = [
            self.enrollee_col,
            self.year_col,
            self.entity_type,
        ]  # ASSUMES estimates at the year level

        if map_version == "current":
            self.map_version = clinical_mapping_db.get_current_map_version()
        else:
            self.map_version = map_version

    def validate_id_date_cols(self) -> None:
        """Run some checks on the personal identifier and date columns in the data.

        Raises:
            ValueError if service/admission startcolumn is not datetime.
            ValueError if enrollee identifier column is NA (such as None or numpy.NaN).
        """
        if self.df[self.enrollee_col].isna().any():
            raise ValueError("The enrollee identifier column contains NAs")

        diff = set([self.service_start_col]) - set(
            self.df.select_dtypes(include=[np.datetime64]).columns  # type:ignore[list-item]
        )
        if diff:
            raise ValueError(f"The service start date column is not a datetime object {diff}")

    def get_cm_measure(self) -> pd.DataFrame:
        """Pull the bundle measure from clinical mapping."""

        measure_df = clinical_mapping_db.get_bundle_measure(self.map_version)
        measure_df["measure_id"] = np.nan
        measure_df.loc[measure_df.bundle_measure == "prev", "measure_id"] = 5
        measure_df.loc[measure_df.bundle_measure == "inc", "measure_id"] = 6
        measure_df = measure_df.loc[
            measure_df[self.entity_type] == self.entity_id, ["measure_id"]
        ]
        return measure_df

    def get_db_measure(self) -> int:
        """Pull the bundle's measure_id from the database using the active bundle
        metadata table. If that fails (inactive bundles?) pull the measure
        from the bundle properties table.

        Raises:
            ValueError if more than a single measure_id values are returned.
        """

        dql = """QUERY"""
        mdf = query(dql, conn_def="CONN_DEF")
        if len(mdf) == 0:
            # the measure_id is not present on active bundle meta
            mdf = self.get_cm_measure()

        if len(mdf) != 1:
            raise ValueError("We expect exactly 1 measure value - {mdf}")
        measure_id = mdf.measure_id.iloc[0]
        return measure_id

    def verify_measure(self) -> None:
        """If measure_id exists in data compare it to the db extracted value.

        Raises:
            ValueError if the measure in the data does not match the measure pulled from
            the database.
        """

        db_measure = self.get_db_measure()
        if "measure_id" in self.df.columns:
            self.measure_id = self.df.measure_id.unique().tolist()
            if len(self.measure_id) != 1:
                raise ValueError("There must be exactly 1 measure_id")
            self.measure_id = self.measure_id[0]
            # if there's a measure in the data, compare it to the epi database
            if self.measure_id != db_measure:
                raise ValueError(
                    "The measure in the db doesn't match the "
                    "data, and fail mode is set to hard so this "
                    "code ends here"
                )
        else:
            self.measure_id = db_measure
            if self.verbose:
                print("no measure_id in the data, using database measure")
                print(f"The measure id {db_measure} will be used. 5 is prev, 6 is inc")

    def required_columns(self) -> None:
        """
        Currently every row must be identified as outpatient, abbreviated to otp via an
        is_otp column where the value 1 identifies an outpatient visit and 0 identifies
        a  non-outpatient encounter (eg a hospital admission).

        Raises:
            KeyError if is_otp column is not present in data.
            KeyError if diagnosis_id column is not present in data.
        """

        if "is_otp" not in self.df.columns:
            raise KeyError(
                "This class Requires an is_otp column. If the dataframe "
                "you're passing to the class is entirely inpatient then "
                "create a new column with only the value 0. If the df "
                "contains outpatient data then each otp row MUST be "
                "identified with a value of 1."
            )

        if "diagnosis_id" not in self.df.columns:
            raise KeyError(
                "This class requires a diagnosis_id column to perform filters "
                "to meet the definition of estimate_id"
            )

        unique_otp = self.df["is_otp"].unique().tolist()
        diff = set(unique_otp) - set([0, 1])
        if diff:
            raise ValueError(f"The only acceptable is_otp values are 0, 1 {diff}")
        if self.verbose:
            print(f"is_otp value counts {self.df.is_otp.value_counts()}")

    def verify_entity_type(self) -> None:
        """The entity_type column is present and contains only a single unique integer.

        Raises:
            KeyError if the column name assigned to self.entity_type is not
            present in the data.
            ValueError if there is more than a single unique value in the entity type column.
            TypeError if the entity ID is not an integer.
        """

        if self.entity_type not in self.df.columns:
            raise KeyError(f"There must be a {self.entity_type} col present in the dataframe")
        entities = self.df[self.entity_type].unique()
        if entities.size != 1:
            raise ValueError(f"There must be a single unique entity_id present {entities}")
        self.entity_id = entities[0]
        if not isinstance(self.entity_id, np.integer):
            raise TypeError(f"entity_id must be an integer {self.entity_id}")

    def run_prevalence(self) -> None:
        """Deduplicate prevalence bundles."""
        # apply the two outpatient rule, or not, depending on ID.
        self.get_two_otp_rule_state()
        if not isinstance(self.two_otp_rule, bool):
            raise TypeError("The two outpatient rule attribute must be a bool")

        if self.two_otp_rule and self.estimate_id in (21,):
            if self.verbose:
                print(
                    "Two otp rule is True. Entity will be processed with 2 "
                    "outpatient only visits required rather than 1"
                )
            self.apply_two_otp_rule()
            # adjust the unique enrollees list, there WILL be some removed
            self.unique_enrollees = self.df[self.enrollee_col].unique().size
        else:
            if self.verbose:
                print("Two outpatient rule will not be applied")
            pass

        # apply the main prevalence deduplication method
        self.standard_deduplication()

    def remove_same_day_claims(self) -> None:
        """Remove claims duplicates by adm_date, ie same-day claims. Sorting will
        be quite important here and it may be useful to review the rows which
        are dropped and if they contain any info not present in the rows kept.
        """

        pre = len(self.df)
        self.df = self.df.drop_duplicates(
            subset=self.dedup_cols + [self.service_start_col], keep="first"
        )
        if self.verbose:
            print(
                f"{pre - len(self.df)} rows have been dropped due to multiple claims "
                "being on the same day"
            )

    def get_two_otp_rule_state(self) -> None:
        """For a given bundle_id and estimate_id determine if the 2 outpatient
        rule applies. Note, it usually applies. Exceptions are skin-related bundles.

        Raises:
            ValueError if the input entity is anything other than bundle.
            ValueError if a bundle which was previously using the 2 otp rule was cloned.
        """
        if self.entity_type != "bundle_id":
            raise ValueError("This method relies heavily on Bundle-specific assumptions.")
        # Unfortunately two otp rule is still stored in a flat file
        cause_name = self.entity_type.split("_")[0]
        unadj_causes = pd.read_csv(
            "FILEPATH"
        )
        if (
            self.entity_id
            in unadj_causes.loc[
                unadj_causes["adj_ms_prev_otp"] == 0, self.entity_type
            ].tolist()
        ):
            warnings.warn(
                "Running test to confirm "
                "bundle is NOT in bundle swap table if bundle IS in unadjusted causes"
            )

            relationship_table, _ = relationship_tables.get_tables()
            # Find if the bundle_id has ever been cloned. This would mean the
            # 2 otp rule is NOT being applied to a bundle that it SHOULD be applied to
            relationship_table = relationship_table.query(
                f"origin_bundle_id == {self.entity_id} and relationship_id == 4"
            )
            if len(relationship_table) != 0:
                raise ValueError(
                    f"There are historical bundle clone records\n\n{relationship_table}"
                )
            print("Setting the two otp rule to False b/c bundle IS in the unadjusted list")
            self.two_otp_rule = False
        else:
            self.two_otp_rule = True

    def apply_two_otp_rule(self) -> None:
        """
        If the two outpatient rule is True, then we ignore any enrollees who have just
        a bundle diagnosis from a single outpatient visit.
        """
        pre = len(self.df)
        pre_enrollees = self.df[self.enrollee_col].unique().size

        # set dummy var to sum
        self.df["rows"] = 1
        # get a sum of rows to use to filter
        self.df["keep"] = self.df.groupby(
            [self.enrollee_col, self.year_col, self.entity_type]
        )["rows"].transform("sum")
        if self.verbose:
            print("Applying the 2 claims outpatient rule.")
        cond = "(self.df['keep'] > 1) | (self.df['is_otp'] == 0)"
        self.df = self.df[eval(cond)]
        self.df = self.df.drop(["rows", "keep"], axis=1)
        post = len(self.df)
        post_enrollees = self.df[self.enrollee_col].unique().size
        self.two_otp_rows_dropped = pre - post
        self.two_otp_enrollees_dropped = pre_enrollees - post_enrollees
        if self.verbose:
            print(
                f"{self.two_otp_rows_dropped} rows have been "
                f"removed for {self.two_otp_enrollees_dropped} enrollees"
            )

    def standard_deduplication(self) -> None:
        """
        Drop all duplicate observations along a set of a columns. Assumes data has
        been sorted and is being processed year-by-year."""

        pre = len(self.df)
        self.df = self.df.drop_duplicates(subset=self.dedup_cols, keep="first")
        if self.verbose:
            print(
                f"{pre - len(self.df)} rows have been dropped in the standard prevalence "
                "deduplication process"
            )

    def dedup_inc(self) -> None:
        """
        Sub-routine for deduplicating bundles which have an incidence measure.
        If the duration in days is 365 then this will be processed identically to
        prevalence bundles, although the two otp rule is not applied.
        """

        self.prep_for_incidence()

        if (self.df["bundle_duration"] == 365).all():
            self.long_duration()
        else:
            self.final_inc: List[pd.DataFrame] = (
                []
            )  # list to append deduped dataframe results to
            self.get_enrollees_with_1_claim()
            self.run_incidence()
            self.concat_inc()
            print("Data has been concatted and ready to return")

        # remove duration related cols
        for drop_col in ["adm_date", "adm_limit", "bundle_duration"]:
            if drop_col in self.df.columns:
                self.df = self.df.drop(drop_col, axis=1)

    def long_duration(self) -> None:
        """If the bundle has a duration of 365 then process like prevalence.

        Raises:
            ValueError if more than a single duration is present.
            ValueError if the duration is not exactly 365.
        """

        duration = self.df["bundle_duration"].unique().tolist()
        if len(duration) != 1:
            raise ValueError(f"There are multiple durations: {duration}")
        duration = duration[0]
        if duration != 365:
            raise ValueError("This should only be run when duration is a year")

        # now de-dupe like prevalence
        self.standard_deduplication()

    def get_enrollees_with_1_claim(self) -> None:
        """
        When enrollees have only a single claim we don't need to pass them to
        the deduplication function.

        Raises:
            ValueError if any individuals have more than a single observation in the data.
        """

        # get a table with number of records per unique enrolid
        m = self.df.groupby(self.enrollee_col).size().reset_index()

        # remove enrollees with multiple claims
        m = m[m[0] == 1]

        # Move the enrollees with only a single claim into the final array
        id_array = m[self.enrollee_col]
        inc_indv = self.df[self.df[self.enrollee_col].isin(id_array)].copy()
        self.final_inc.append(inc_indv)

        # remove these enrollees from the object that goes to incidence deduplication
        pre = len(self.df)
        self.df = self.df[~self.df[self.enrollee_col].isin(id_array)].copy()
        post = len(self.df)
        if self.verbose:
            print(
                f"There were {pre-post} rows removed from self.df "
                f"and {len(inc_indv)} rows added to self.final_inc"
            )
        if not (self.df[self.enrollee_col].value_counts() > 1).all():
            raise ValueError("There are enrollee IDs with fewer than 2 value counts")

    def prep_for_incidence(self) -> None:
        """Add a bundle_duration column to the data if not already attached."""
        if "bundle_duration" in self.df.columns:
            pass
        else:
            # just attach a bundle duration column
            duration_df = clinical_mapping_db.create_bundle_durations(
                map_version=self.map_version
            )
            self.df = self.df.merge(
                duration_df[[self.entity_type, "bundle_duration"]], how="left"
            )

    def run_incidence(self) -> None:
        """Use the entity specific duration window to deduplicate any observations for the
        same individual within that window."""

        self.df = self.df.sort_values([self.enrollee_col, self.service_start_col])
        print("Sorting is complete..")

        years = sorted([y for y in self.df["year_id"].unique()])
        for year in years:  # important! estimates at the year level
            year_df = self.df[self.df.year_id == year].copy()
            print("Copied a year of data")
            self.df = self.df[self.df.year_id != year]
            self.final_inc.append(
                deduplicate_single_entity(
                    individual_df=year_df,
                    enrollee_col=self.enrollee_col,
                    service_start_col=self.service_start_col,
                )
            )

    def concat_inc(self) -> None:
        """Concatenate and then delete a list of incidence DataFrames."""
        self.df = pd.concat(self.final_inc, sort=False, ignore_index=True)
        del self.final_inc

    def estimate_filters(self) -> None:
        """Filter data to meet estimate_id requirements. Filter columns are is_otp and
        diagnosis_id."""
        if self.estimate_class.primary_dx_only:
            # remove non-primary diagnosis codes for primary-only estimates
            self.df = self.df[self.df["diagnosis_id"] == 1]

        # remove appropriate inp/otp rows
        self.df = self.df[self.df["is_otp"].isin(self.estimate_class.is_otp_vals)]

    def validate_results(self) -> None:
        """Run some final validations after deduplication.

        Raises:
            ValueError if the count of unique enrollees before and after don't match. Note,
            this value is re-calculated after the 2 otp rule is applied.
        """
        if self.df[self.enrollee_col].unique().size != self.unique_enrollees:
            raise ValueError("There's a difference in the count of unique individuals.")

    def validate_estimate(self) -> None:
        """Confirm that data which does not fit an estimate definition is not present.

        Raises:
            ValueError if estimate_id and is_otp values don't align.
            ValueError if estimate_id and diagnosis_id values don't align.
            ValueError if estimate_id and is_otp specific row counts don't match.
        """

        failures = []
        if self.estimate_id in (14, 15, 16, 17):
            if self.df.is_otp.sum() != 0:
                failures.append(
                    f"\nThe estimate is {self.estimate_id} but there "
                    f"are {self.df.is_otp.sum()} rows of outpatient data!"
                )
        if self.estimate_id in (14, 15):
            non_pri = len(self.df[self.df.diagnosis_id != 1])
            if non_pri != 0:
                failures.append(f"There are {non_pri} rows with non-primary diagnoses!")
        if self.estimate_id in (18, 19, 21):
            is_otp_rows = len(self.df[self.df.is_otp == 1])
            if is_otp_rows == 0:
                failures.append(
                    f"There should be at least 1 row of outpatient data for "
                    f"estimate_id {self.estimate_id}"
                )
            if self.estimate_id in (18, 19) and is_otp_rows != len(self.df):
                failures.append(
                    f"Estimate {self.estimate_id} is an outpatient only estimate. "
                    "Inpatient data is not expected"
                )
        if failures:
            raise ValueError("\n".join(failures))

    def main(self, df: pd.DataFrame, create_backup: bool = False) -> pd.DataFrame:
        """Class' main method to perform the deduplication process. Deduped
        data will be returned.

        df: Contains individual level data for a single bundle with rows removed
            based on criteria outlined in thie module's docstring and class methods.

        Raises:
            ValueError if deduplication is attempted on anything other than bundle data.
        """
        self.df = df
        del df

        self.estimate_filters()
        if len(self.df) == 0:
            # all data has been removed during estimate filtering process
            return self.df

        if not self.estimate_class.apply_deduplication:
            self.df["estimate_id"] = self.estimate_id
            return self.df

        self.required_columns()
        self.validate_id_date_cols()

        if self.entity_type != "bundle_id":
            raise ValueError("We can only process at the bundle level currently")
        self.verify_entity_type()
        self.verify_measure()

        self.unique_enrollees = self.df[self.enrollee_col].unique().size

        if create_backup:  # make copy of data before running any de-dupes
            self.bak = self.df.copy()

        # sort the data
        sort_cols = [self.enrollee_col, self.service_start_col]
        self.df = self.df.sort_values(by=sort_cols)
        # remove same day admits
        self.remove_same_day_claims()

        # then follow inc OR prev workflow
        if self.measure_id == 5:
            # prev workflow
            self.run_prevalence()
        elif self.measure_id == 6:
            # incidence workflow
            self.dedup_inc()
        else:
            raise ValueError(f"We need an acceptable measure_id {self.measure_id}")
        self.validate_results()
        self.validate_estimate()

        self.df["estimate_id"] = self.estimate_id

        if self.verbose:
            print("De-duplication is complete")
        return self.df

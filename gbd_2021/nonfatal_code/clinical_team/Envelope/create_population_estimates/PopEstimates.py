"""
The Python Classes needed to transform our inpatient hospital
data to the population level. We do this 1 of two ways
1) by applying the hospital utilization envelope
2) by dividing by GBD population estimates

For maternal data we make an additional adjustment to transform our
population of interest from all women to live births

This module just defines the classes, methods and attributes needed
to perform those tasks.

In the future we may--
1) Divide all maternal env draws by the maternal adjustment rate
2) Generate draws for UTLA data using Wilson's approx
    (we do this in the uploader)
"""

import pandas as pd
import numpy as np
from db_tools.ezfuncs import query

from clinical_info.Functions import gbd_hosp_prep as ghp
from clinical_info.Envelope.create_population_estimates import icg_prep_maternal_data
from clinical_info.AgeSexSplitting import under1_adjustment


class PopEstimates:
    """base class for creating population level estimates
    """

    def __init__(self, name, run_id, age, sex, gbd_round_id, decomp_step):
        self.name = name
        self.run_id = run_id
        self.age = age
        self.sex = sex
        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.input_data_type = None
        self.use_draws = None
        self.DFpath = None
        self.df = None
        self.denom_df = None
        self.base_path
        self.get_clinical_age_set

    @property
    def base_path(self):
        """Set the run_id filepath to read/write data"""
        self._base_path = f"FILEPATH"

    @property
    def get_clinical_age_set(self):
        run_pickle = pd.read_pickle(f"FILEPATH")
        self.clinical_age_group_set_id = run_pickle.clinical_age_group_set_id

    def ReadDF(self, filepath):
        """Read in an HDF or CSV"""
        exten = filepath[-3:]
        if exten == ".H5":
            df = pd.read_hdf(filepath)
        elif exten == "csv":
            df = pd.read_csv(filepath)
        else:
            assert False, f"not sure which function to use to read {filepath}"
        df = self._keep_age_sex(df)
        df = self._align_year_col(df)

        if len(df) == 0:
            raise ValueError(f"Reading in the data at {filepath} failed")
        return df

    def _align_year_col(self, df):
        """Input data has both year start/end and year_id
        We're going with just year_id"""

        df_cols = df.columns.tolist()
        if "year_id" not in df_cols and "year_start" in df_cols:
            df.rename(columns={"year_start": "year_id"}, inplace=True)
        if "year_end" in df_cols:
            df.drop("year_end", axis=1, inplace=True)
        year_cols = df.filter(regex="year_").columns.tolist()
        if year_cols != ["year_id"]:
            raise ValueError(
                f"something went wrong we got {year_cols} " "but only want 'year_id'"
            )
        return df

    def _keep_age_sex(self, df):
        """processing in parallel by age and sex.
        Filter out unneeded data"""

        df = df[(df["age_group_id"] == self.age) & (df["sex_id"] == self.sex)]
        return df

    def create_case_counts(self):
        """Include a column for cases in maternal and full cover data"""
        assert "cases" not in self.mat_df.columns, "case is present?"
        self.mat_df["cases"] = self.mat_df["mean"] * self.mat_df["sample_size"]

        if self.name == "full_coverage":
            assert "cases" not in self.df.columns, "case is present?"
            self.df["cases"] = self.df["mean"] * self.df["sample_size"]

    def check_draws(self):
        """Using the use_draws attribute to determine if they should or shouldn't be present"""

        if not isinstance(self.use_draws, bool):
            raise ValueError("use_draws has not been assigned a boolean value")

        draw_cols = self.df.filter(regex="draw_").columns.tolist()

        if self.use_draws:
            if not draw_cols:
                raise AttributeError("draws should be present")
        else:
            if draw_cols:
                raise AttributeError("draws should not be present")

    def _load_mat_denoms(self):
        """Load in the maternal denom file for adjusting maternal ICGs"""
        denom_path = f"FILEPATH"
        self.mat_denom = pd.read_hdf(denom_path, key="df")

    def _clean_mat_denoms(self):
        """Drop some unneeded columns and rename other cols"""
        denom_cols = self.mat_denom.columns.tolist()
        drops = [d for d in denom_cols if d in ["asfr_mean", "ifd_mean"]]
        if drops:
            self.mat_denom.drop(drops, axis=1, inplace=True)
        # clarify exactly which are the maternal columns
        self.mat_denom.rename(
            columns={
                "sample_size": "maternal_sample_size",
                "mean_raw": "maternal_mean",
            },
            inplace=True,
        )

    def _merge_mat_denoms(self):
        """Attach maternal denom data to maternal subset of inpatient data"""
        merge_cols = ["age_group_id", "location_id", "sex_id", "year_id"]
        self.mat_df = self.mat_df.merge(
            self.mat_denom, how="left", on=merge_cols, validate="m:1"
        )

    def _clean_mat_cols(self):
        """Switch the sample size name back, drop maternal denom and gen draws"""
        self.mat_df.rename(
            columns={"maternal_sample_size": "sample_size"}, inplace=True
        )
        self.mat_df.drop(["maternal_mean"], axis=1, inplace=True)

        if self.name == "hue":
            print(
                "Adding blank draw cols for maternal data with env,"
                "this will probably be removed if we apply maternal"
                "adjustment to every draw"
            )
            for i in range(0, 1000, 1):
                self.mat_df[f"draw_{i}"] = np.nan

    def _remove_non_maternal_ages(self):
        """We need to update our maternal restriction ages to match what
           Modelers expect, basically just the 9 age groups below
           However until we do that we'll just refactor copying over the hardcoding"""

        mat_age_group_ids = [7, 8, 9, 10, 11, 12, 13, 14, 15]
        self.mat_df = self.mat_df[self.mat_df.age_group_id.isin(mat_age_group_ids)]
        print("Maternal adjusted age groups have been manually adjusted.")

    def adjust_mat_denom(self):
        """Live births are the population of interest for maternal causes
        adjust them accordingly"""

        # make a copy of df with just maternal ICGs
        self.mat_df = icg_prep_maternal_data.select_maternal_data(
            self.df.copy(), gbd_round_id=self.gbd_round_id
        )
        self._remove_non_maternal_ages()

        mat_cols = self.mat_df.columns.tolist()
        if "sample_size" in mat_cols:
            self.mat_df.drop("sample_size", axis=1, inplace=True)

        # pull in the maternal denoms and set good col names
        self._load_mat_denoms()
        self._clean_mat_denoms()
        # merge on maternal denoms
        self._merge_mat_denoms()

        if self.use_draws:
            assert "mean" not in self.mat_df.columns, "this is a test"
            # calculate mean from the draws
            self.mat_df["mean"] = self.mat_df[self.draw_cols].mean(axis=1)
            self.mat_df.drop(self.draw_cols, axis=1, inplace=True)
        else:
            assert "mean" in self.mat_df.columns, "mean gotta be here!"
        # divide mean rate by mean_raw rate from denom
        self.mat_df["mean"] = self.mat_df["mean"] / self.mat_df["maternal_mean"]

        # set estimate_id for maternal adjusted inp data
        self.mat_df["estimate_id"] = 6
        self._clean_mat_cols()

    def _assign_col_vals(self):
        """Add cols for run_id and source type"""
        self.df["run_id"] = self.run_id
        self.df["source_type_id"] = 10  # inp data
        self.mat_df["run_id"] = self.run_id
        self.mat_df["source_type_id"] = 10  # inp data

    def format_output_columns(self):
        """drop unneeded columns"""

        self._assign_col_vals()
        pre_cols = self.df.columns.tolist()
        base_cols = [  # Demographic identifiers
            "age_group_id",
            "sex_id",
            "location_id",
            "year_id",
            "source",
            "nid",
            # Process cols
            "run_id",
            "source_type_id",
            "diagnosis_id",
            "representative_id",
            # Cause Estimate cols
            "icg_id",
            "icg_name",
            "mean",
            "sample_size",
            "cases",
            "estimate_id",
        ]

        draw_cols = [f"draw_{i}" for i in range(0, 1000, 1)]
        losses = set(pre_cols) - set(base_cols + draw_cols)
        print(f"The cols {losses} will be dropped")
        self.df = self.df[base_cols + draw_cols]
        if len(self.mat_df) > 0:
            self.mat_df = self.mat_df[base_cols + draw_cols]
        else:
            self.mat_df = self.mat_df[base_cols]


class HUE(PopEstimates):
    """for hospital data that uses the envelope"""

    def __init__(
        self, run_id, age, sex, gbd_round_id, decomp_step, env_path, name="hue"
    ):
        super().__init__(
            name=name,
            run_id=run_id,
            age=age,
            sex=sex,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
        )
        self.use_draws = True
        self.input_data_type = "admit_fractions"
        self.env_path = env_path
        # self.set_age_start
        # self.envelope_path
        self.DFpath = f"FILEPATH"

    def clean_env_cols(self):
        """ drop the env cols we don't need"""
        if not isinstance(self.denom_df, pd.DataFrame):
            raise ValueError("denominator df isn't a DataFrame!")

        self.denom_df.rename(columns={"year_start": "year_id"}, inplace=True)
        # drop unneeded cols
        env_cols = self.denom_df.columns
        drops = [
            "measure_id",
            "modelable_entity_id",
            "mean",
            "upper",
            "lower",
            "age_start",
            "age_end",
            "year_end",
        ]
        drops = [d for d in drops if d in env_cols]
        self.denom_df.drop(drops, axis=1, inplace=True)

    def set_draw_cols(self):
        """Create list of draw column names"""
        self.draw_cols = self.denom_df.filter(regex="^draw_").columns.tolist()

    def merge_on_env(self):
        """ read in the actual data and merges it together"""

        merge_cols = ["age_group_id", "sex_id", "location_id", "year_id"]

        # read and clean data
        self.df = self.ReadDF(self.DFpath)
        self.denom_df = self.ReadDF(self.env_path)
        self.clean_env_cols()
        self.set_draw_cols()

        # merge data
        self.df = self.df.merge(
            self.denom_df, how="left", on=merge_cols, validate="m:1"
        )

        if self.df[self.draw_cols].isnull().any().any():
            raise ValueError(
                ("We do not expect ANY null " "envelope draws after merging")
            )

    def test_for_env(self):
        """couple tests before apply env"""
        failures = []
        if "cause_fraction" not in self.df.columns:
            failures.append("The admission fraction column is not present")

        if self.df["cause_fraction"].isnull().any():
            failures.append("We can't have Null admission fractions")

        if len(self.draw_cols) != 1000:
            failures.append("There should be 1,000 draws from the envelope")

        if failures:
            raise ValueError(failures)

    def create_env_estimate(self):
        """Multiply env draws by cause fractions"""
        self.test_for_env()
        self.check_draws()

        # loop over the draw columns and multiply env rate by admit fraction
        for draw in self.draw_cols:
            self.df[draw] = self.df["cause_fraction"] * self.df[draw]

        # set the estimate_id !!
        self.df["estimate_id"] = 1

    def gen_null_cols(self):
        """HUE data uses draws, but we will explicitly generate the
        columns below in order to match the full coverage data"""
        for col in ["sample_size", "mean", "cases"]:
            self.df[col] = np.nan

    def HueMain(self):
        """Run all the pop estimates methods"""

        # merge and apply the Hospital Utilization Envelope
        print("Apply HUE")
        self.merge_on_env()
        self.create_env_estimate()
        # adjust denoms for maternal ICGs
        print("Adjust maternal envelope denoms")
        self.adjust_mat_denom()
        self.create_case_counts()
        # format columns and stuff for output
        print("Prepping final columns")
        self.gen_null_cols()
        self.format_output_columns()


class FullCoverage(PopEstimates):
    """for hospital data that DOES NOT use the envelope"""

    def __init__(
        self, run_id, age, sex, gbd_round_id, decomp_step, name="full_coverage"
    ):
        super().__init__(
            name=name,
            run_id=run_id,
            age=age,
            sex=sex,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
        )
        self.use_draws = False
        self.input_data_type = "admissions"

        self.DFpath = f"FILEPATH"

    def gen_blank_draws(self):
        """Need blank draw cols when env isn't used
           They'd be implicitly added during pd.concat
           but explicit > implicit"""

        self.check_draws()
        for i in range(0, 1000, 1):
            self.df[f"draw_{i}"] = np.nan

    def test_fc_estimate(self):
        """ two tests, are val and sample_size present and
        do they contain unique values"""

        cols = self.df.columns.tolist()
        failures = []
        if "val" not in cols:
            failures.append("val column is not in this df")
        else:
            vals = self.df["val"].unique().size
            if vals < 10:
                failures.append(f"there are only {vals} unique admissions")

        if "sample_size" not in cols:
            failures.append("sample_size column is not in this df")
        else:
            samps = self.df["sample_size"].unique().size
            if samps < 10:
                failures.append(f"there are only {samps} unique sample sizes")

        if failures:
            raise ValueError(failures)

    def create_fc_estimate(self):
        """Create population level estimates using full coverage"""

        self.df = self.ReadDF(self.DFpath)
        # get sample size using hosp prep function
        self.df = ghp.get_sample_size(
            self.df,
            self.gbd_round_id,
            self.decomp_step,
            run_id=self.run_id,
            use_cached_pop=True,
            fix_group237=False,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
        )
        self.df = self._align_year_col(self.df)
        self.df.rename(columns={"population": "sample_size"}, inplace=True)

        # few tests
        self.test_fc_estimate()

        self.gen_blank_draws()

        # divide val by sample size to create mean
        self.df["mean"] = self.df["val"] / self.df["sample_size"]

        # adjust the mean by the under1 scalar so that the
        # rate matches with population
        # if the age set doesn't contain U1 detail then it doesn't do anything
        for adj_col in ["mean", "val"]:  # do both just in case
            self.df = under1_adjustment.adjust_under1_data(
                self.df,
                col_to_adjust=adj_col,
                conversion="count_to_rate",
                clinical_age_group_set_id=self.clinical_age_group_set_id,
            )

        # set estimate_id!!
        self.df["estimate_id"] = 1

    def FullCoverMain(self):
        """Run all the full coverage methods"""

        # Divide admits by population
        print("Creating full coverage estimates")
        self.create_fc_estimate()

        # Adjust maternal denominators
        print("Adjust full coverage mat denoms")
        self.adjust_mat_denom()
        self.create_case_counts()

        # format columns and stuff for output
        print("Prepping final columns")
        self.format_output_columns()

"""
The Python Classes needed to transform our inpatient hospital
data to the population level. We do this 1 of two ways
1) by applying the hospital utilization envelope
2) by dividing by GBD population estimates

This module just defines the classes, methods and attributes needed
to perform those tasks.
"""

from pathlib import Path

import numpy as np
import pandas as pd
from clinical_functions import legacy_pipeline, pipeline

from inpatient.AgeSexSplitting import under1_adjustment
from crosscutting_functions import uncertainty


class PopEstimates:
    """Base class for creating population level estimates.

    Flavors currently only include HUE (Hospital Utilization Envelope)
    and full coverage, perhaps someday outpatient??
    """

    def __init__(self, name: str, run_id: int, age: int, sex: int, draws: int):
        """Initial arguments for population estimates base class.

        Args:
            name: Name of a class instance. So far we've named them after their child classes.
            run_id: Standard clinical run_id used for IO.
            age: age_group_id used to filter data and convert in parallel.
            sex: sex_id used to filter data and convert in parallel.
            draws: The number of draws to use for a given run.
        """
        self.name = name
        self.run_id = run_id
        self.age = age
        self.sex = sex
        self.draws = draws
        self.input_data_type = None
        self.DFpath = None
        self.df = None
        self.denom_df = None
        self.base_path
        self.get_clinical_age_set
        self.make_draw_col_names

    @property
    def base_path(self):
        """Set the run_id filepath to read/write data"""
        self._base_path = FILEPATH

    @property
    def get_clinical_age_set(self):
        run_pickle = pd.read_pickle(FILEPATH)
        self.clinical_age_group_set_id = run_pickle.clinical_age_group_set_id

    @property
    def make_draw_col_names(self):
        self.draw_cols = [f"draw_{i}" for i in range(0, self.draws, 1)]

    def ReadDF(self, filepath):
        """Read in an HDF or CSV"""
        exten = filepath[-3:]

        if not Path(filepath).exists():
            return None

        if exten == ".H5":
            df = pd.read_hdf(filepath)
        elif exten == "csv":
            df = pd.read_csv(filepath)
        else:
            assert False, f"not sure which function to use to read {filepath}"

        if len(df.columns) == 0:
            return None

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
            df = df.rename(columns={"year_start": "year_id"})
        if "year_end" in df_cols:
            df = df.drop("year_end", axis=1)
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

    def _assign_col_vals(self):
        """Add cols for run_id and source type"""
        self.df["run_id"] = self.run_id
        self.df["source_type_id"] = 10  # inp data

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
            "sample_size",
            "estimate_id",
        ]

        losses = set(pre_cols) - set(base_cols + self.draw_cols)
        print(f"The cols {losses} will be dropped")
        self.df = self.df[base_cols + self.draw_cols]


class HUE(PopEstimates):
    """for hospital data that uses the envelope"""

    def __init__(self, run_id: int, age: int, sex: int, draws: int, env_path: str, name="hue"):
        super().__init__(name=name, run_id=run_id, age=age, sex=sex, draws=draws)
        self.input_data_type = "admit_fractions"
        self.env_path = env_path

        self.DFpath = FILEPATH

    def clean_env_cols(self):
        """drop the env cols we don't need"""
        if not isinstance(self.denom_df, pd.DataFrame):
            raise TypeError("denominator df isn't a DataFrame!")

        self.denom_df = self.denom_df.rename(columns={"year_start": "year_id"})
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
        self.denom_df = self.denom_df.drop(drops, axis=1)

    def merge_on_env(self):
        """read in the actual data and merges it together"""

        merge_cols = ["age_group_id", "sex_id", "location_id", "year_id"]

        # read and clean data
        self.df = self.ReadDF(self.DFpath)
        if self.df is None:
            return
        self.denom_df = self.ReadDF(self.env_path)
        self.denom_df = pipeline.downsample_draws(df=self.denom_df, draws=self.draws)
        self.clean_env_cols()

        # merge data
        self.df = self.df.merge(self.denom_df, how="left", on=merge_cols, validate="m:1")

        if self.df[self.draw_cols].isnull().any().any():
            raise ValueError(("We do not expect ANY null " "envelope draws after merging"))

    def test_for_env(self):
        """couple tests before apply env"""
        failures = []
        if "cause_fraction" not in self.df.columns:
            failures.append("The admission fraction column is not present")

        if self.df["cause_fraction"].isnull().any():
            failures.append("We can't have Null admission fractions")

        if failures:
            raise ValueError(failures)

    def create_env_estimate(self):
        """Multiply env draws by cause fractions"""
        self.test_for_env()

        # loop over the draw columns and multiply env rate by admit fraction
        for draw in self.draw_cols:
            self.df[draw] = self.df["cause_fraction"] * self.df[draw]

        # set the estimate_id
        self.df["estimate_id"] = 1

    def gen_null_cols(self):
        """HUE data uses draws, but we will explicitly generate the
        columns below in order to match the full coverage data"""
        self.df["sample_size"] = np.nan

    def HueMain(self):
        """Run all the pop estimates methods"""

        # merge and apply the Hospital Utilization Envelope
        print("Apply HUE")
        self.merge_on_env()
        if self.df is None:
            return
        self.create_env_estimate()

        # format columns for output
        print("Prepping final columns")
        self.gen_null_cols()
        self.format_output_columns()


class FullCoverage(PopEstimates):
    """for hospital data that DOES NOT use the envelope"""

    def __init__(self, run_id: int, age: int, sex: int, draws: int, name="full_coverage"):
        super().__init__(
            name=name,
            run_id=run_id,
            age=age,
            sex=sex,
            draws=draws,
        )
        self.input_data_type = "admissions"

        self.DFpath = (
           FILEPATH
        )

    def generate_draws(self) -> None:
        """Create draws using the mean and sample size columns in the data."""
        if "mean" not in self.df.columns:
            raise KeyError("Mean is not present cannot generate draws")

        pre_draw = len(self.df)

        df_nonzero = self.df.loc[self.df["mean"] != 0].copy().reset_index(drop=True)
        df_zero = self.df.loc[self.df["mean"] == 0].copy().reset_index(drop=True)

        df_nonzero = uncertainty.draws_from_poisson(df=df_nonzero, draw_cols=self.draw_cols)
        df_zero = uncertainty.fill_zero_draws(df=df_zero, draw_cols=self.draw_cols)

        if set(df_nonzero.columns) != set(df_zero.columns):
            raise RuntimeError("Column names don't match!")

        self.df = pd.concat([df_nonzero, df_zero], ignore_index=True)

        if len(self.df) != pre_draw:
            raise ValueError("Generating draws somehow added/removed rows")

    def test_fc_estimate(self):
        """
        two tests, are val and sample_size present and
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
        """Create population level estimates using full coverage."""

        self.df = self.ReadDF(self.DFpath)
        if self.df is None:
            return

        # get sample size using hosp prep function
        self.df = legacy_pipeline.get_sample_size(
            self.df,
            run_id=self.run_id,
            use_cached_pop=True,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
        )
        self.df = self._align_year_col(self.df)
        self.df = self.df.rename(columns={"population": "sample_size"})

        # few tests
        self.test_fc_estimate()

        # divide val by sample size to create mean
        self.df["mean"] = self.df["val"] / self.df["sample_size"]

        self.generate_draws()

        # adjust the mean by the under1 scalar so that the rate matches with population
        # if the age set doesn't contain U1 detail then it doesn't do anything
        self.df = under1_adjustment.adjust_under1_data(
            self.df,
            col_to_adjust=self.draw_cols,
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
        if self.df is None:
            return

        # format columns and stuff for output
        print("Prepping final columns")
        self.format_output_columns()

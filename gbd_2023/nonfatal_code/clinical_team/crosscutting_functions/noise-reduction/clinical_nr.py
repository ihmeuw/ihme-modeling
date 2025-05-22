import warnings
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from db_tools.ezfuncs import query
from loguru import logger
from patsy import dmatrices
from statsmodels.genmod.generalized_linear_model import GLMResultsWrapper

from crosscutting_functions import demographic


class ClinicalNoiseReduction:
    """Base class for running noise reduction.

    We may want to build out subclasses for other pipelines in the
    future. For now this class is only used with claims data.

    Args:
        name:
            Name of the class instance.
        run_id:
        subnational:
            If True aggregate then rake, if False no agg or raking.
        model_group:
            An ordered string that will be used to subset data. MUST BE IN THE
            FOLLOWING ORDER
                '{bundle_id}_{estimate_id}_{sex_id}_{parent_location_id}'
            There is a validation test but it may not catch overlapping IDs
        model_type:
            A human readable string, only working model type currently is 'Poisson'.
        df_path:
            Filepath the data for NR should be read from. Data MUST be in rate space
            AND contain a sample_size column to convert to counts. Data will be
            subset using the values in self.model_group.
        cols_to_nr:
            Columns in the cf to noise reduce. The mean of the columns will be used
            to fit the model, then each column will be NR'd.
            They MUST be in rate space and then it will be converted to counts,
            and back again to rate for the final results.
        floor_type:
            Method to use when setting the floor. Currently only 'minimum_observed'.
        model_failure_sub:
            Clear and simple subroutine to create predictions/variance that
            should never fail if a model doesn't converge. Currently only working
            backup method is 'fill_average'.
    """

    df: pd.DataFrame
    national_df: pd.DataFrame
    raked_df: pd.DataFrame

    def __init__(
        self,
        name: str,
        run_id: Optional[int],
        subnational: bool,
        model_group: str,
        model_type: str,
        df_path: Optional[str],
        cols_to_nr: List[str],
        floor_type: str = "minimum_observed",
        model_failure_sub: str = "fill_average",
    ):
        self.name = name
        self.run_id = run_id
        self.subnational = subnational
        self.model_group = model_group
        self.model_type = model_type
        self.cols_to_nr = cols_to_nr
        self.model_failure_sub = model_failure_sub
        self.df_path = df_path
        self.extract_from_model_group
        self.floor_type = floor_type

        self.nr_cols = [
            "country_name",
            "std_err_preds",
            "std_err_data",
            "model_variance",
            "data_variance",
            "nr_data_component",
            "nr_model_component",
            "variance_numerator",
            "variance_denominator",
        ]

    @property
    def extract_from_model_group(self) -> None:
        """Order is important here, pull the int values for the IDs below
        out of the model_group attr"""
        keys = ["bundle_id", "estimate_id", "sex_id", "group_location_id"]
        vals = [int(v) for v in self.model_group.split("_")]
        self.model_val_dict = dict(zip(keys, vals))
        self._validate_model_group_val()

    @property
    def check_required_columns(self) -> None:
        """Certain columns are necessary to perform noise reduction in Marketscan
        """
        self.req_cols = self.cols_to_nr + [
            "sample_size",
            "sex_id",
            "age_start",
            "location_id",
            "year_id",
            "bundle_id",
            "estimate_id",
        ]
        self.df[self.req_cols]

    def _validate_model_group_val(self) -> None:
        """fail if the model group order is incorrect"""
        failures = []
        col_tables = {
            "bundle_id": "DATABASE",
            "estimate_id": "DATABASE",
            "sex_id": "DATABASE",
            "group_location_id": "DATABASE",
        }
        for col, table in col_tables.items():
            if "group_" in col:
                tcol = col.replace("group_", "")
            else:
                tcol = col
            q = "QUERY" 
            t = query(q, conn_def="CONN_DEF")
            if len(t) != 1:
                failures.append(f"{tcol} {self.model_val_dict[col]} is not valid")
        if failures:
            raise ValueError(f"The following validatations failed {failures}")
        else:
            pass

    def ReadDF(self) -> None:
        """Read in an HDF or CSV and then do initial column prep
        """
        if not self.df_path:
            raise RuntimeError("self.df_path must be present to read data.")
        exten = self.df_path[-3:]
        if exten == ".H5":
            self.df = pd.DataFrame(pd.read_hdf(self.df_path))
        elif exten == "csv":
            self.df = pd.read_csv(self.df_path)
        else:
            raise RuntimeError(f"not sure which function to use to read {self.df_path}")
        self._eval_group()
        self.check_required_columns
        self._create_col_to_fit_model()
        self.check_square()
        self.create_count_col()

    def _create_col_to_fit_model(self) -> None:
        """Fit a model on just the mean of all the draws. If there's only 1
        column than it doesn't matter. The mean of 1 col is equal
        to the values of that 1 col
        """

        self.df["model_fit_col"] = self.df[self.cols_to_nr].mean(axis=1)

        # set the floor using the average of the draws
        self._set_floor()

    def _eval_group(self) -> None:
        """Subset to data for modeling and add a country id"""

        # subset row based on data to be used in NR
        mask = (
            f"(self.df['bundle_id'] == {self.model_val_dict['bundle_id']}) &"
            f"(self.df['estimate_id'] == {self.model_val_dict['estimate_id']}) &"
            f"(self.df['sex_id'] == {self.model_val_dict['sex_id']})"
        )
        self.df = self.df[eval(mask)]

        self.df = demographic.map_to_country(self.df, self.release_id)
        self.df = self.df[
            self.df["country_location_id"] == self.model_val_dict["group_location_id"]
        ]

        # mean rates above one produce null variance, breaking the NR algo
        for col in self.cols_to_nr:
            self.df.loc[self.df[col] > 1, col] = 1

    def _assign_release_id(self, release_id: int) -> None:
        self.release_id = release_id

    def cast_to_categorical(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fixed effects on age/location/year by converting from integer dtype
        columns to categorical variable
        """

        # manually set the age reference point at 50
        if 50 in df.age_start.unique().tolist():
            cat_order = [50] + [a for a in df.age_start.unique() if a != 50]
        else:
            cat_order = df.age_start.unique().tolist()

        # convert cols to category dtype
        df["age_start"] = pd.Categorical(df["age_start"], categories=cat_order, ordered=False)
        df["location_id"] = pd.Categorical(df["location_id"])
        df["year_id"] = pd.Categorical(df["year_id"])

        return df

    def cast_to_int(self, df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """convert a list of columns to integer. Used on the categorical cols
        after a model is fit"""
        for col in cols:
            df[col] = pd.to_numeric(df[col], downcast="integer", errors="raise")
        return df

    def create_national_df(self) -> None:
        """Groupby and sum the count cols up to the country level for a model

        Raises:
            ValueError if row count doesn't match demographics combinations.
            ValueError if row count of df changes.
        """

        pre = len(self.df)
        if self.df["location_id"].unique().size > 1:
            logger.info("aggregating a national level df")
            sum_cols = self.count_cols_to_nr + ["sample_size", "model_fit_col_counts"]
            sum_dict = dict(zip(sum_cols, ["sum"] * len(sum_cols)))

            drops = ["sample_size", "location_id", "model_fit_col", "model_fit_col_counts"]
            drops = drops + self.cols_to_nr + self.count_cols_to_nr
            group_cols = self.df.columns.drop(drops).tolist()
            self.national_df = self.df.groupby(group_cols).agg(sum_dict).reset_index()
            self.national_df["location_id"] = self.national_df["country_location_id"]

            if not (
                self.national_df.year_id.unique().size
                * self.national_df.sex_id.unique().size
                * self.national_df.age_start.unique().size
                == len(self.national_df)
            ):
                raise ValueError("there arent the right number of rows")
            if len(self.df) != pre:
                raise ValueError("The rows changed")

            for col in self.count_cols_to_nr + ["model_fit_col_counts"]:
                # re-create the rate col average
                rate_col = col.replace("_counts", "")
                self.national_df[rate_col] = (
                    self.national_df[col] / self.national_df["sample_size"]
                )

    def check_square(self) -> None:
        """
        Raises:
            ValueError if row counts are off by 1.5% from square data expectation.
        """
        if len(self.df[self.df["model_fit_col"] == 0]) == 0:
            warnings.warn(("There are NO zero estimates in this dataframe"))

        # The data doesn't need to be perfectly square (small sparse pops create
        # missing sample sizes) but it should be very very close
        for year in self.df.year_id.unique():  # handle asymmetric locs by year
            mask = f"self.df.year_id == {year}"
            exp_rows = (
                self.df[eval(mask)].year_id.unique().size
                * self.df[eval(mask)].sex_id.unique().size
                * self.df[eval(mask)].age_start.unique().size
                * self.df[eval(mask)].location_id.unique().size
            )
            obs_rows = len(self.df[eval(mask)])
            row_diff = ((exp_rows - obs_rows) / obs_rows) * 100
            if abs(row_diff) > 1.5:
                raise ValueError(
                    (
                        "We expect square data but there's more than a "
                        f"1.5 percent difference of what we expect: {row_diff}"
                    )
                )

    def create_count_col(self) -> None:
        """The models must be run in count space!
        populate a _counts column from mean times sample size"""
        self.count_cols_to_nr = [f"{c}_counts" for c in self.cols_to_nr]
        for col in self.cols_to_nr + ["model_fit_col"]:
            count_name = col + "_counts"
            self.df[count_name] = self.df[col] * self.df["sample_size"]

    def stdp(self, df: pd.DataFrame, j: int, vcov: pd.DataFrame) -> float:
        """
        Multiply predictor values by variance matrix to get standard error for
        a predicted value. This should match the Stata function `stdp`
        Currently this loops of a dataframe of predictors row-wise. 

        Args:
        df:
            DF of predictors
        j (int):
            Is a row number.
        vcov
            Is the variance-covariance matrix from the model.

        Returns:
        se:
            Standard error of the predicted point.
        """

        se = np.sqrt(
            df.iloc[j].to_numpy().transpose().dot(vcov.to_numpy()).dot(df.iloc[j].to_numpy())
        )

        return se

    def _generate_model_formula(self, model_df: pd.DataFrame) -> str:
        """Setup the regression expression in patsy notation"""
        model_formula = "model_fit_col_counts ~ "
        X_cols = ["age_start", "year_id", "location_id"]
        for X_col in X_cols:
            # if there's more than a single unique value append it to the formula
            if model_df.loc[model_df["model_fit_col_counts"] != 0, X_col].unique().size > 1:
                model_formula += f"{X_col} + "
        model_formula = model_formula[:-3]  # remove trailing ' + '
        return model_formula

    def fill_average(self, df: pd.DataFrame) -> pd.DataFrame:
        """When data is quite sparse a model may not converge. In those cases
        we fill the predictions with the average
        """

        col_to_avg = "model_fit_col"

        # merge on average case rate by age
        age_avg = df.groupby("age_start").agg({col_to_avg: "mean"}).reset_index()
        age_avg.rename(columns={col_to_avg: "predicted_rate"}, inplace=True)

        df = df.merge(age_avg, how="left", on="age_start", validate="m:1")
        df[f"{col_to_avg}_preds"] = df["predicted_rate"] * df["sample_size"]

        # create standard error
        df["std_err_preds"] = 0
        df["model_variance"] = 0

        return df

    def fit_model(
        self, model_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Optional[GLMResultsWrapper]]:
        """Use the model type attr to decide which model to run. Currently
        only the code for a poisson model is working.

        Raises:
            ValueError if self.model_type is not supported.
        """
        model_df = self.cast_to_categorical(model_df)

        return_model_object: Optional[GLMResultsWrapper]
        model_object: GLMResultsWrapper

        expr = self._generate_model_formula(model_df)
        logger.info(f"This model will use the formula {expr}")

        if self.model_type == "Poisson":
            # Set up the X and y matrices for the training and testing data sets.
            y_df, X_df = dmatrices(expr, model_df, return_type="dataframe")

            # Using the statsmodels GLM class, train the Poisson regression
            # model on the data set.
            non_zero_rows = len(y_df[y_df.iloc[:, 0] != 0])
            if non_zero_rows <= 7:
                logger.info("Filling the average instead of fitting poisson")
                self.converged = False
                return_model_object = None
            else:
                model_object = sm.GLM(
                    y_df,
                    X_df,
                    offset=np.log(model_df["sample_size"]),
                    family=sm.families.Poisson(),
                ).fit()

                self.converged = model_object.converged
                return_model_object = model_object

            logger.info(f"Poisson model converged? {self.converged}")
            model_df["converged"] = self.converged

            if not self.converged:
                if self.model_failure_sub == "fill_average":
                    # Fit the avverage.
                    model_df = self.fill_average(model_df)
                    return model_df, return_model_object
                else:
                    raise ValueError(
                        f"This method is not recognized: {self.model_failure_sub}"
                    )
            # Print out the training summary.
            logger.info(model_object.summary())

            # Store the fitted predictions.
            model_df["model_fit_col_preds"] = pd.Series(model_object.mu)

        else:
            raise ValueError(
                (
                    f"Model type {self.model_type} unrecognized. "
                    "Acceptable values are currently "
                    "'Poisson' and 'NB'"
                )
            )

        if return_model_object:
            # Now re-make rates and then return the df obj.
            model_df["predicted_rate"] = (
                model_df["model_fit_col_preds"] / model_df["sample_size"]
            )

            # Manually add in standard error.
            vcov = model_object.cov_params()
            err_list = []
            for j in range(0, len(X_df)):
                err_list.append(self.stdp(df=X_df, j=j, vcov=vcov))
            model_df["std_err_preds"] = err_list

            # Add on the model and data variance values.
            model_df = self._set_model_variance(model_df, model_var_method="original")

            return_model_object = model_object

        return model_df, return_model_object

    def _set_model_variance(self, df: pd.DataFrame, model_var_method: str) -> pd.DataFrame:
        if model_var_method == "original":
            df["model_variance"] = (
                np.exp(df["std_err_preds"] - 1) * df["predicted_rate"]
            ) ** 2
        else:
            raise ValueError(
                (
                    f"Unrecognized method for calculating model "
                    f"variance {model_var_method}. Acceptable values "
                    f"are currently 'original' and 'from_model_package'"
                )
            )
        df["model_variance"] = np.nan_to_num(df["model_variance"])
        return df

    def _set_data_variance(
        self, df: pd.DataFrame, col: str, data_var_method: str
    ) -> pd.DataFrame:
        """data_var_method is a string which determines which formula to use when calculating
        the variance of a data point.
        Original: use the original formula. Mainly keeping if we need to reproduce
                  results

        Updated: add the fixed denom.

        Raises:
            ValueError if the data_var_method is not supported.
        """

        if data_var_method == "original":
            cf_component = df[col] * (1 - df[col])
            df["std_err_data"] = np.sqrt(
                (cf_component / df["sample_size"]) + ((1.96**2) / (4 * df["sample_size"] ** 2))
            )
            df["data_variance"] = df["std_err_data"] ** 2

        elif data_var_method == "updated":
            cf_component = df[col] * (1 - df[col])
            df["data_variance"] = (
                (cf_component / df["sample_size"]) + ((1.96**2) / (4 * df["sample_size"] ** 2))
            ) / ((1 + 1.96**2 / df["sample_size"]) ** 2)
            df["std_err_data"] = np.sqrt(df["data_variance"])

        else:
            raise ValueError(
                (
                    f"Unrecognized method for calculating data "
                    f"variance {data_var_method}. Acceptable values "
                    f"are currently 'original' and 'updated'"
                )
            )

        if df["data_variance"].isnull().any():
            raise ValueError("Do we expect null variance values? Because we're seeing them")
        return df

    def noise_reduce(self, df: pd.DataFrame) -> pd.DataFrame:
        """This is the actual noise reduction step, it uses 4 inputs in the
        formula (model_variance * data_rate + data_variance * model_rate) /
                        model_variance + data_variance
        Note: estimates Must be in rate space
        NR formula is unchanged

        Raises:
            ValueError if the noise reduction process creates null predictions.
        """

        # set the list of noise reduced columns as an attribute to use later
        self.noise_reduced_cols = [f"{c}_final" for c in self.cols_to_nr]
        self.noise_reduced_count_cols = [f"{c2}_counts" for c2 in self.noise_reduced_cols]

        for col_to_nr in self.cols_to_nr:
            # set the variance for this data column
            df = self._set_data_variance(df=df, col=col_to_nr, data_var_method="updated")

            # generate the data and model "components" for noise reduction
            df["nr_data_component"] = df[col_to_nr] * (
                df["model_variance"] / (df["model_variance"] + df["data_variance"])
            )

            df["nr_model_component"] = df["predicted_rate"] * (
                df["data_variance"] / (df["model_variance"] + df["data_variance"])
            )

            # Create the actual noise reduced estimate
            df[f"{col_to_nr}_final"] = df["nr_data_component"] + df["nr_model_component"]

            df["variance_numerator"] = df["model_variance"] * df["data_variance"]
            df["variance_denominator"] = df["model_variance"] + df["data_variance"]

            df["variance"] = df["variance_numerator"] / df["variance_denominator"]
            # Create noise reduced counts
            df[f"{col_to_nr}_final_counts"] = df[f"{col_to_nr}_final"] * df["sample_size"]

            if not df[f"{col_to_nr}_final"].notnull().all():
                raise ValueError("There are NULL noise reduced values.")

        # switch back from cats to int dtypes
        df = self.cast_to_int(df, cols=["age_start", "location_id", "year_id"])
        return df

    def _set_floor(self) -> None:
        """Set the pre NR floor we'll use for post model results

        Raises:
            ValueError if the floor_type is not supported.
            FloorError if the floor is 0, >1 or null.
        """

        if self.floor_type == "minimum_observed":
            # use the lowest mean value
            self._floor = self.df.loc[self.df["model_fit_col"] > 0, "model_fit_col"].min()
        else:
            raise ValueError(f"Floor type {self.floor_type} is not understood")

        if self._floor == 0:
            raise FloorError("NR floor can't be zero")
        if self._floor >= 1:
            raise FloorError("NR floor is >= 1? Impossible.")
        if pd.isnull(self._floor):
            raise FloorError("NR floor can't be null.")

    def apply_floor(self) -> None:
        """If post NR estimates are below lowest observed preNR estimate
        then replace that with a zero"""
        for col in self.noise_reduced_cols:
            self.df.loc[self.df[col] < self._floor, col] = 0

    def raking(self) -> None:
        """Placeholder for now
        """
        raker = Raker(
            df=self.raked_df, cols_to_rake=self.noise_reduced_count_cols, double=False
        )
        self.raked_df = raker.get_computed_dataframe()


class Raker:
    """
    Main changes are-
    - removed code to use a location hierarchy, we're staring with just US MS data
    - double raking will break. need to think about this more before using it
      on something like the UTLA data.

    Sqeeze/Expand sub national deaths into national deaths.

    Adjusts deaths and therefore cause fractions at the subnational unit
    to take into account the ratio of subnational to national deaths
    """

    def __init__(self, df: pd.DataFrame, cols_to_rake: List[str], double=False):
        """Initialize the object with some info on columns we need.
        Params
        df: (pd.DataFrame)
            Contains the data to rake, both national and subnational if avail
        cols_to_rake: (list)
            A list of column names to rake MUST BE IN RATE SPACE
        double: (bool)
            Whether or not to 'double' rake the data, ie from UTLAs up to
            national.
        """

        self.df = df
        self.double = double
        self.merge_cols = ["sex_id", "age_start", "bundle_id", "year_id"]
        self.cols_to_rake = cols_to_rake

        self.death_prop_cols = [(x + "_prop") for x in self.cols_to_rake]

    def get_computed_dataframe(self) -> pd.DataFrame:
        # get raked data
        df = self.standard_rake(self.df)

        return df

    def standard_rake(self, df: pd.DataFrame) -> pd.DataFrame:
        # grab the length of the incoming dataframe
        start = len(df)

        # prep dataframe
        df = self.flag_aggregates(df)

        # if there are no subnational locations, why rake?
        if 0 in df["is_nat"].unique():

            aggregate_df = self.prep_aggregate_df(df)
            subnational_df = self.prep_subnational_df(df)
            sub_and_agg = subnational_df.merge(aggregate_df, on=self.merge_cols, how="left")
            for rake_col in self.cols_to_rake:
                sub_and_agg.loc[
                    sub_and_agg["{}_agg".format(rake_col)].isnull(), "{}_agg".format(rake_col)
                ] = sub_and_agg["{}_sub".format(rake_col)]

            sub_and_agg.loc[sub_and_agg["sample_size_agg"].isnull(), "sample_size_agg"] = (
                sub_and_agg["sample_size_sub"]
            )
            df = df.merge(sub_and_agg, how="left", on=self.merge_cols)

            # do not want to add or drop observations
            end = len(df)
            if start != end:
                raise ValueError(
                    "The number of rows have changed."
                )
            df = self.replace_metrics(df)
            df = self.cleanup(df)
        return df

    def cleanup(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop unnecessary columns."""
        sub_cols = [x for x in df.columns if "sub" in x]
        agg_cols = [x for x in df.columns if "agg" in x]
        prop_cols = [x for x in df.columns if "prop" in x]
        df = df.drop(sub_cols + agg_cols + prop_cols, axis=1)
        return df

    def prep_subnational_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prep sub national dataframe.

        Set a temporary non-zero deaths floor (needed for division later)
        and create subnational sample_size and deaths columns.
        """
        df = df[df["is_nat"] == 0]
        sub_total = df.groupby(self.merge_cols, as_index=False)[
            self.cols_to_rake + ["sample_size"]
        ].sum()

        # create _sub columns
        for rake_col in self.cols_to_rake:
            sub_total.loc[sub_total[rake_col] == 0, rake_col] = 0.0001
            sub_total.rename(columns={rake_col: rake_col + "_sub"}, inplace=True)
        sub_total.rename(columns={"sample_size": "sample_size_sub"}, inplace=True)

        sub_total = sub_total[self.merge_cols + [x for x in sub_total.columns if "sub" in x]]

        return sub_total

    def flag_aggregates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flag if the location_id is a subnational unit or not."""
        df.loc[df["location_id"] == df["country_location_id"], "is_nat"] = 1
        df.loc[df["location_id"] != df["country_location_id"], "is_nat"] = 0
        df = df.drop("country_location_id", axis=1)
        return df

    def replace_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adjust deaths based on national: subnational deaths ratio."""

        for rake_col in self.cols_to_rake:
            # change deaths
            df["{}_prop".format(rake_col)] = (
                df["{}_agg".format(rake_col)] / df["{}_sub".format(rake_col)]
            )

        for rake_col in self.cols_to_rake:
            pre_cases = (df.loc[df["is_nat"] == 0, rake_col]).sum().round(1)

            df.loc[df["is_nat"] == 0, rake_col] = df[rake_col] * df["{}_prop".format(rake_col)]

            # change the rate col
            cf_col = rake_col.replace("_counts", "")
            logger.info(f"Adjusting the column {cf_col}")
            df.loc[df["is_nat"] == 0, cf_col] = (
                df.loc[df["is_nat"] == 0, rake_col] / df.loc[df["is_nat"] == 0, "sample_size"]
            )
            df.loc[df[cf_col] > 1, cf_col] = 1

            post_cases = (df.loc[df["is_nat"] == 0, rake_col]).sum()
            nat_cases = df.loc[df["is_nat"] == 1, rake_col].sum()
            logger.info(
                f"Subnat cases have gone from {pre_cases} to "
                f"{post_cases.round(1)} "
                f"compared to {nat_cases.round(1)} national cases"
            )
            if not np.isclose(post_cases, nat_cases):
                raise RakingError(f"Raking has failed for col {rake_col}")

        return df

    def prep_aggregate_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sum deaths at the national level."""
        df = df[df["is_nat"] == 1]

        for rake_col in self.cols_to_rake:
            df = df.rename(columns={rake_col: rake_col + "_agg"})

        # sample_size aggregates are only used for Other_Maternal source
        df = df.rename(columns={"sample_size": "sample_size_agg"})

        df = df[self.merge_cols + [x for x in df.columns if "_agg" in x]]
        df = df.groupby(self.merge_cols, as_index=False).sum()

        return df


class FloorError(Exception):
    "Raised when there is an impossible floor value."
    pass


class RakingError(Exception):
    "Raised when national and subnational sums are not close."
    pass

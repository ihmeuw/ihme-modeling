

import pandas as pd
import numpy as np
import linearmodels as lm
from patsy import dmatrices
import statsmodels.api as sm
import statsmodels.formula.api as smf
import warnings

from db_tools.ezfuncs import query
from clinical_info.Functions import hosp_prep, gbd_hosp_prep


class ClinicalNR():
    """base class for running noise reduction

    We may want to build out subclasses for inp or otp pipelines in the
    future, but for now the Marketscan MVP will be just the base class

    Params:

    name (str):
        name of the class instance
    run_id (int):
        clinical run id, used to pull population and estimate data
    bundle_id (int):
        bundle_id to return NR'd results for
    estimate_id (int):
        estimate_id to NR results
    group_location_id (int):
        Use in raking. If national data loc_id == group_loc_id, if subnational
        it's equal to the corresponding country location_id
    subnational (bool):
        if True aggregate then rake, if False no agg or raking
    cols_to_nr (list):
        Columns in the cf to noise reduce. The mean of the columns will be used
        to fit the model, then each column will be NR'd.
        They MUST be in rate space and then it will be converted to counts,
        and back again to rate for the final results
    model_group (str):
        An ordered string that will be used to subset data. MUST BE IN THE
        FOLLOWING ORDER
            '{bundle_id}_{estimate_id}_{sex_id}_{parent_location_id}'
        There is a validation test but it may not catch overlapping IDs
    model_type (str):
        A human readable string, only working model type currently is 'Poisson'
    model_failure_sub (str):
        Clear and simple subroutine to create predictions/variance that
        should never fail if a model doesn't converge. Currently only working
        backup method is 'fill_average'
    df_path (str):
        Filepath the data for NR should be read from. Data MUST be in rate space
        AND contain a sample_size column to convert to counts. Data will be
        subset using the values in self.model_group
    df (pd.DataFrame):
        The data to feed into the model. The model will be fit from a column of
        draw averages. NR class will create this df using the df_path attr
    national_df (pd.DataFrame):
        data aggregated to the country level, if necessary. NR class will create
        this df using the self.df and group_location_id objects
    floor_type (float):
        Method to use when setting the floor. Currently only 'minimum_observed'
    """
    def __init__(self, name, run_id,
                 subnational, model_group, model_type, df_path,
                 cols_to_nr, floor_type='minimum_observed',
                 model_failure_sub='fill_average'):
        self.name = name
        self.run_id = run_id
        self.subnational = subnational
        self.model_group = model_group
        self.model_type = model_type
        self.cols_to_nr = cols_to_nr
        self.model_failure_sub = model_failure_sub
        self.df_path = df_path
        self.df = None
        self.national_df = None
        self.extract_from_model_group
        self.floor_type = floor_type

        self.nr_cols = ['country_name', 'std_err_preds', 'std_err_data',
                        'model_variance', 'data_variance', 'nr_data_component',
                        'nr_model_component', 'variance_numerator',
                        'variance_denominator']

    @property
    def extract_from_model_group(self):
        """Order is important here, pull the int values for the IDs below
        out of the model_group attr"""
        keys = ['bundle_id', 'estimate_id', 'sex_id', 'group_location_id']
        vals = [int(v) for v in self.model_group.split("_")]
        self.model_val_dict = dict(zip(keys, vals))
        self._validate_model_group_val()

    @property
    def check_required_columns(self):
        """Certain columns are necessary to perform noise reduction in Marketscan"""
        self.req_cols = self.cols_to_nr + ['sample_size', 'sex_id', 'age_start',
                         'location_id', 'year_id', 'bundle_id', 'estimate_id']

        self.df[self.req_cols]

    def _validate_model_group_val(self):

        failures = []
        col_tables = {'bundle_id': 'bundle.bundle',
                      'estimate_id': 'clinical.estimate',
                      'sex_id': 'shared.sex',
                      'group_location_id': 'shared.location'}
        for col, table in col_tables.items():
            if 'group_' in col:
                tcol = col.replace('group_', '')
            else:
                tcol = col
            q = QUERY
            t = query(q)
            if len(t) != 1:
                failures.append(f"{tcol} {self.model_val_dict[col]} is not valid")
        if failures:
            raise ValueError(f"The following validatations failed {failures}")
        else:
            pass

    def ReadDF(self):
        exten = self.df_path[-3:]
        if exten == '.H5':
            self.df = pd.read_hdf(self.df_path)
        elif exten == 'csv':
            self.df = pd.read_csv(self.df_path)
        else:
            assert False, f'not sure which function to use to read {self.df_path}'
        self._eval_group()
        self.check_required_columns
        self._create_col_to_fit_model()
        self.check_square()
        self.create_count_col()

    def _create_col_to_fit_model(self):

        self.df['model_fit_col'] = self.df[self.cols_to_nr].mean(axis=1)

        # set the floor using the average of the draws
        self._set_floor()

    def _eval_group(self):
        """Subset to data for modeling and add a country id
        """

        # subset row based on data to be used in NR
        mask = (f"(self.df['bundle_id'] == {self.model_val_dict['bundle_id']}) &"
                f"(self.df['estimate_id'] == {self.model_val_dict['estimate_id']}) &"
                f"(self.df['sex_id'] == {self.model_val_dict['sex_id']})")
        self.df = self.df[eval(mask)]

        self.df = gbd_hosp_prep.map_to_country(self.df)
        self.df = self.df[self.df['country_location_id'] == self.model_val_dict['group_location_id']]

        # mean rates above one produce null variance, breaking the NR algo
        for col in self.cols_to_nr:
            self.df.loc[self.df[col] > 1, col] = 1

    def cast_to_categorical(self, df):
        """
        Fixed effects on age/location/year by converting from integer dtype
        columns to categorical variable

        Future TODO:
        Hardcoding here is possibly unsustainable. Manually sets the age
        reference point at 50 to mimic Stata NR process. Not sure how much
        this matters and it seems too blunt"""

        # manually set the age reference point at 50
        if 50 in df.age_start.unique().tolist():
            cat_order = [50] + [a for a in df.age_start.unique() if a != 50]
        else:
            cat_order = df.age_start.unique().tolist()

        # convert cols to category dtype
        df['age_start'] = pd.Categorical(df['age_start'],
                                         categories=cat_order,
                                         ordered=False)
        df['location_id'] = pd.Categorical(df['location_id'])
        df['year_id'] = pd.Categorical(df['year_id'])

        return df

    def cast_to_int(self, df, cols):
        """convert a list of columns to integer. Used on the categorical cols
        after a model is fit"""
        for col in cols:
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='raise')
        return df

    def create_national_df(self):
        """Groupby and sum the count cols up to the country level for a model
        """

        pre = len(self.df)
        if self.df['location_id'].unique().size > 1:
            print("aggregating a national level df")
            sum_cols = (self.count_cols_to_nr +
                        ['sample_size', 'model_fit_col_counts'])
            sum_dict = dict(zip(sum_cols, ['sum'] * len(sum_cols)))

            drops = ['sample_size', 'location_id',
                     'model_fit_col', 'model_fit_col_counts']
            drops = drops + self.cols_to_nr + self.count_cols_to_nr
            group_cols = self.df.columns.drop(drops).tolist()
            self.national_df = (self.df.groupby(group_cols).
                                agg(sum_dict).
                                reset_index())
            self.national_df['location_id'] = self.national_df['country_location_id']

            assert (self.national_df.year_id.unique().size *
                    self.national_df.sex_id.unique().size *
                    self.national_df.age_start.unique().size == len(self.national_df)),\
                        'there arent the right number of rows'
            assert len(self.df) == pre, 'The rows changed'

            for col in self.count_cols_to_nr + ['model_fit_col_counts']:
                # re-create the rate col average
                rate_col = col.replace('_counts', '')
                self.national_df[rate_col] = (self.national_df[col] /
                                                    self.national_df[
                                                        'sample_size'])
        else:
            pass
        return

    def check_square(self):
        """Do we always expect at least 1 observation to be zero? I'm not sure
        """
        if len(self.df[self.df['model_fit_col'] == 0]) == 0:
            warnings.warn(("There are NO zero estimates in this dataframe, is "
                           "it square??"))

        # The data doesn't need to be perfectly square (small sparse pops create
        # missing sample sizes) but it should be very very close
        exp_rows = (self.df.year_id.unique().size *
                    self.df.sex_id.unique().size *
                    self.df.age_start.unique().size *
                    self.df.location_id.unique().size)
        obs_rows = len(self.df)
        row_diff = ((exp_rows - obs_rows) / obs_rows) * 100
        if abs(row_diff) > 1:
            raise ValueError(("We expect square data but there's more than a "
                              "one percent difference of what we expect"))

    def create_count_col(self):
        """The models must be run in count space!
        populate a _counts column from mean times sample size"""
        self.count_cols_to_nr = [f'{c}_counts' for c in self.cols_to_nr]
        for col in self.cols_to_nr + ['model_fit_col']:
            count_name = col + '_counts'
            self.df[count_name] = (self.df[col] * self.df['sample_size'])

    def stdp(self, df, j, vcov):
        """
        Multiply predictor values by variance matrix to get standard error for
        a predicted value. This should match the Stata function `stdp`
        Currently this loops of a dataframe of predictors row-wise. There
        must be a faster way to do this but the data so far is fairly small

        Params:
        df (pd.DataFrame):
            DF of predictors (thanks dmatrices!!)
        j (int):
            is a row number and
        vcov (pd.DataFrame)
            is the variance-covariance matrix from the model
        Returns:
        se (float):
            standard error of the predicted point
        """

        se = (np.sqrt(df.iloc[j].to_numpy().transpose().dot(vcov.to_numpy()).
              dot(df.iloc[j].to_numpy())))

        return se

    def _generate_model_formula(self, model_df):
        """ Setup the regression expression in patsy notation
        """
        model_formula = f"model_fit_col_counts ~ "
        X_cols = ['age_start', 'year_id', 'location_id']
        for X_col in X_cols:
            # if there's more than a single unique value append it to the formula
            if model_df.loc[model_df[X_col] != 0, X_col].unique().size > 1:
                model_formula += f"{X_col} + "
        model_formula = model_formula[:-3]  # remove trailing ' + '
        return model_formula

    def fill_average(self, df):
        """When data is quite sparse a model may not converge. In those cases
        we fill the predictions with the average
        """

        # CoD does this on the cause fraction column, so in rate space
        col_to_avg = 'model_fit_col'

        # merge on average case rate by age
        age_avg = df.groupby('age_start').agg({col_to_avg: 'mean'}).reset_index()
        age_avg.rename(columns={col_to_avg: 'predicted_rate'}, inplace=True)

        df = df.merge(age_avg, how='left', on='age_start', validate='m:1')
        df[f'{col_to_avg}_preds'] = df['predicted_rate'] * df['sample_size']

        # create standard error
        df['std_err_preds'] = 0
        df['model_variance'] = 0

        return df

    def fit_model(self, model_df):
        """Use the model type attr to decide which model to run. Currently
        only the code for a poisson model is working"""

        model_df = self.cast_to_categorical(model_df)
        expr = self._generate_model_formula(model_df)
        print(f'This model will use the formula {expr}')

        if self.model_type == 'NB':
            assert False, 'This is on spec'
            # first fit a poisson
            self.model_type = 'Poisson'
            tmp, tmp_model_object = self.fit_model(model_df=model_df)
            assert tmp_model_object is not None,\
                "Poisson not fit. Something went wrong"
            self.model_type = 'NB'  # reset back to NB model type

            #TODO clean this bit below up BB_COUNT is the y variable
            # then calculate alpha
            # auxiliary OLS regression on the Poisson fit
            #Add the lambda vector as a new column called 'BB_LAMBDA' to the Data Frame of the training data set
            model_df['BB_LAMBDA'] = model_object.mu
            #add a derived column called 'AUX_OLS_DEP' to the pandas Data Frame. This new column will store the values of the dependent variable of the OLS regression
            model_df['AUX_OLS_DEP'] = model_df.apply(lambda x: ((x['model_fit_col'] - x['BB_LAMBDA'])**2 - x['model_fit_col']) / x['BB_LAMBDA'], axis=1)
            #use patsy to form the model specification for the OLSR
            ols_expr = """AUX_OLS_DEP ~ BB_LAMBDA - 1"""
            #Configure and fit the OLSR model
            aux_olsr_results = smf.ols(ols_expr, model_df).fit()
            #Print the regression params
            print(aux_olsr_results.params)

            # then fit an NB
            # statsmodels glm with negbin family, probs
            #train the NB2 model on the training data set
            nb2_training_results = sm.GLM(y_train, X_train,family=sm.families.NegativeBinomial(alpha=aux_olsr_results.params[0])).fit()
            #print the training summary
            print(nb2_training_results.summary())
            #make some predictions using our trained NB2 model
            nb2_predictions = nb2_training_results.get_prediction(X_test)
            #print out the predictions
            predictions_summary_frame = nb2_predictions.summary_frame()
            print(predictions_summary_frame)


        elif self.model_type == 'Poisson':
            # Set up the X and y matrices for the training and testing data sets
            y_df, X_df = dmatrices(expr, model_df, return_type='dataframe')

            # Using the statsmodels GLM class, train the Poisson regression
            # model on the data set
            non_zero_rows = len(y_df[y_df.iloc[:, 0] != 0])
            if non_zero_rows <= 6:
                print("Filling the average instead of fitting poisson")
                converged = False
                model_object = None
            else:
                model_object = sm.GLM(y_df, X_df,
                                    offset=np.log(model_df['sample_size']),
                                    family=sm.families.Poisson()).fit()

                converged = model_object.converged
            print(f"Poisson model converged? {converged}")
            model_df['converged'] = converged
            if not converged:
                if self.model_failure_sub == 'fill_average':
                    # fit the avverage
                    model_df = self.fill_average(model_df)
                    return model_df, model_object
                else:
                    raise ValueError(f'This method is not recognized: {self.model_failure_sub}')

            # print out the training summary
            print(model_object.summary())

            # store the fitted predictions
            model_df[f'model_fit_col_preds'] = pd.Series(model_object.mu)
        else:
            raise ValueError((f"Model type {self.model_type} unrecognized. "
                               "Acceptable values are currently "
                               "'Poisson' and 'NB'"))

        # now re-make rates and then return the df obj
        model_df['predicted_rate'] = (model_df['model_fit_col_preds'] /
                                      model_df['sample_size'])

        # manually add in standard error
        vcov = model_object.cov_params()
        err_list = []
        for j in range(0, len(X_df)):
            err_list.append(self.stdp(df=X_df, j=j, vcov=vcov))
        model_df['std_err_preds'] = err_list

        # add on the model and data variance values
        model_df = self._set_model_variance(model_df,
                                            model_var_method='original')

        return model_df, model_object

    def _set_model_variance(self, df, model_var_method):
        if model_var_method == 'original':
            # This was the method used in GBD2019 and earlier's Stata code
            # (exp(std_err_`sex_id' - 1) * predicted_cf_`sex_id') ^ 2
            # ((exponentiated model stderr - 1) * predictedRATE) ^ 2
            df['model_variance'] = (
                    np.exp(df['std_err_preds'] - 1) *
                    df['predicted_rate']) ** 2

        elif model_var_method == 'from_model_package':
            assert False, "need to look into how to pull variance from like statsmodels"
        else:
            raise ValueError((f"Unrecognized method for calculating model "
                              f"variance {model_var_method}. Acceptable values "
                              f"are currently 'original' and 'from_model_package'"))
        df['model_variance'] = np.nan_to_num(df['model_variance'])
        return df

    def _set_data_variance(self, df, col, data_var_method):
        """Use the updated formula that CoD will use in GBD2020 onward
        data_var_method is a string which determines which formula to use when calculating
        the variance of a data point.
        Original: use the formula used in Stata/CoD up until GBD2020. Note: it's missing the
                  denominator from the updated method. Mainly keeping if we need to reproduce results

        Updated: add the fixed denom"""

        if data_var_method == 'original':
            cf_component = df[col] * (1 - df[col])
            df['std_err_data'] = np.sqrt(
                (cf_component / df['sample_size']) +
                ((1.96**2) / (4 * df['sample_size'] ** 2))
            )
            df['data_variance'] = df['std_err_data'] ** 2

        elif data_var_method == 'updated':
            cf_component = df[col] * (1 - df[col])
            df['data_variance'] = (
                ((cf_component / df['sample_size']) +
                ((1.96**2) / (4 * df['sample_size'] ** 2))) /
                ((1 + 1.96**2 / df['sample_size'])**2)
            )
            df['std_err_data'] = np.sqrt(df['data_variance'])

        else:
            raise ValueError((f"Unrecognized method for calculating data "
                              f"variance {data_var_method}. Acceptable values "
                              f"are currently 'original' and 'updated'"))

        if df['data_variance'].isnull().any():
            raise ValueError("Do we expect null variance values? Because we're seeing them")
        return df

    def noise_reduce(self, df):
        """This is the actual noise reduction step, it uses 4 inputs in the
        formula (model_variance * data_rate + data_variance * model_rate) / 
                        model_variance + data_variance
        Note: estimates Must be in rate space
        NR formula is unchanged, Borrowed the source code from CoD's repo"""

        # set the list of noise reduced columns as an attribute to use later
        self.noise_reduced_cols = [f'{c}_final' for c in self.cols_to_nr]
        self.noise_reduced_count_cols = [f'{c2}_counts' for c2
                                         in self.noise_reduced_cols]

        for col_to_nr in self.cols_to_nr:
            # set the variance for this data column
            df = self._set_data_variance(df=df, col=col_to_nr,
                                         data_var_method='updated')

            # generate the data and model "components" for noise reduction
            df['nr_data_component'] = (
                    df[col_to_nr] *
                    (df['model_variance'] /
                        (df['model_variance'] + df['data_variance']))
            )

            df['nr_model_component'] = (
                    df['predicted_rate'] *
                    (df['data_variance'] /
                        (df['model_variance'] + df['data_variance']))
            )

            # Create the actual noise reduced estimate
            df[f'{col_to_nr}_final'] = (df['nr_data_component'] +
                                        df['nr_model_component'])

            # Copied from CoD, perhaps the NR'd variance estimate?
            df['variance_numerator'] = (
                df['model_variance'] * df['data_variance']
            )
            df['variance_denominator'] = (
                df['model_variance'] + df['data_variance']
            )
            # so this will just be the last data vairance column?
            df['variance'] = (
                df['variance_numerator'] / df['variance_denominator']
            )
            # Create noise reduced counts
            df[f'{col_to_nr}_final_counts'] = df[f'{col_to_nr}_final'] * df['sample_size']

            if not df[f'{col_to_nr}_final'].notnull().all():
                raise ValueError("There are NULL noise reduced values. Unacceptable.")

        # switch back from cats to int dtypes
        df = self.cast_to_int(df, cols=['age_start', 'location_id', 'year_id'])
        return df

    def _set_floor(self):

        if self.floor_type == 'minimum_observed':
            # use the lowest mean value
            self._floor = self.df.loc[self.df['model_fit_col'] > 0, 'model_fit_col'].min()
        else:
            raise ValueError(f"Floor type {self.floor_type} is not understood")

        assert self._floor != 0, "floor can't be zero"
        assert self._floor < 1, "floor is 1? that can't be correct"

    def apply_floor(self):
        """If post NR estimates are below lowest observed preNR estimate
        then replace that with a zero"""
        for col in self.noise_reduced_cols:
            self.df.loc[self.df[col] < self._floor, col] = 0

    def raking(self):
        raker = Raker(df=self.raked_df,
                      cols_to_rake=self.noise_reduced_count_cols,
                      double=False)
        self.raked_df = raker.get_computed_dataframe()


class Raker():

    def __init__(self, df, cols_to_rake, double=False):
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
        self.merge_cols = ['sex_id', 'age_start',
                           'bundle_id', 'year_id']
        self.cols_to_rake = cols_to_rake

        self.death_prop_cols = [(x + '_prop') for x in self.cols_to_rake]

    def get_computed_dataframe(self):
        # get raked data
        if self.double:
            assert False, 'not ready!'
        else:
            df = self.standard_rake(self.df)
        return df

    def standard_rake(self, df):

        start = len(df)

        # prep dataframe
        df = self.flag_aggregates(df)

        if 0 in df['is_nat'].unique():

            aggregate_df = self.prep_aggregate_df(df)
            subnational_df = self.prep_subnational_df(df)
            sub_and_agg = subnational_df.merge(
                aggregate_df, on=self.merge_cols, how='left'
            )
            for rake_col in self.cols_to_rake:
                sub_and_agg.loc[
                    sub_and_agg['{}_agg'.format(rake_col)].isnull(),
                    '{}_agg'.format(rake_col)
                ] = sub_and_agg['{}_sub'.format(rake_col)]

            sub_and_agg.loc[
                sub_and_agg['sample_size_agg'].isnull(), 'sample_size_agg'
            ] = sub_and_agg['sample_size_sub']
            df = df.merge(sub_and_agg, how='left', on=self.merge_cols)

 
            end = len(df)
            assert start == end, "The number of rows have changed,"\
                                 " this really shouldn't happen."
            df = self.replace_metrics(df)
            df = self.cleanup(df)
        return df

    def cleanup(self, df):
        """Drop unnecessary columns."""
        sub_cols = [x for x in df.columns if 'sub' in x]
        agg_cols = [x for x in df.columns if 'agg' in x]
        prop_cols = [x for x in df.columns if 'prop' in x]
        df = df.drop(sub_cols + agg_cols + prop_cols, axis=1)
        return df

    def prep_subnational_df(self, df):
        """Prep sub national dataframe.

        Set a temporary non-zero deaths floor (needed for division later)
        and create subnational sample_size and deaths columns.
        """
        df = df[df['is_nat'] == 0]
        sub_total = df.groupby(
            self.merge_cols, as_index=False
        )[self.cols_to_rake + ['sample_size']].sum()

        # create _sub columns
        for rake_col in self.cols_to_rake:
            sub_total.loc[sub_total[rake_col] == 0, rake_col] = .0001
            sub_total.rename(
                columns={rake_col: rake_col + '_sub'}, inplace=True
            )
        sub_total.rename(columns={'sample_size': 'sample_size_sub'}, inplace=True)

        sub_total = sub_total[
            self.merge_cols + [x for x in sub_total.columns if 'sub' in x]
        ]

        return sub_total

    def flag_aggregates(self, df):
        """Flag if the location_id is a subnational unit or not."""

        df.loc[df['location_id'] == df['country_location_id'], 'is_nat'] = 1
        df.loc[df['location_id'] != df['country_location_id'], 'is_nat'] = 0
        df = df.drop('country_location_id', axis=1)
        return df

    def replace_metrics(self, df):
        """Adjust deaths based on national: subnational deaths ratio."""

        for rake_col in self.cols_to_rake:
            # change deaths
            df['{}_prop'.format(rake_col)] = \
                df['{}_agg'.format(rake_col)] / df['{}_sub'.format(rake_col)]

        for rake_col in self.cols_to_rake:
            pre_cases = (df.loc[df['is_nat'] == 0, rake_col]).sum().round(1)

            df.loc[df['is_nat'] == 0, rake_col] = \
                df[rake_col] * df['{}_prop'.format(rake_col)]

            # change the rate col
            cf_col = rake_col.replace("_counts", "")
            print(f"Adjusting the column {cf_col}")
            df.loc[df['is_nat'] == 0, cf_col] = (df.loc[df['is_nat'] == 0,
                                                 rake_col] /
                                                 df.loc[df['is_nat'] == 0,
                                                 'sample_size'])
            df.loc[df[cf_col] > 1, cf_col] = 1

            post_cases = (df.loc[df['is_nat'] == 0, rake_col]).sum()
            nat_cases = df.loc[df['is_nat'] == 1, rake_col].sum()
            print(f'Subnat cases have gone from {pre_cases} to '
                  f'{post_cases.round(1)} '
                  f'compared to {nat_cases.round(1)} national cases')
            if not np.isclose(post_cases, nat_cases):
                raise ValueError(f"Raking has failed for col {rake_col}")

        return df

    def prep_aggregate_df(self, df):
        """Sum deaths at the national level."""
        df = df[df['is_nat'] == 1]

        for rake_col in self.cols_to_rake:
            df = df.rename(columns={rake_col: rake_col + '_agg'})

  
        df = df.rename(columns={'sample_size': 'sample_size_agg'})

        df = df[
            self.merge_cols + [x for x in df.columns if '_agg' in x]
        ]
        df = df.groupby(self.merge_cols, as_index=False).sum()

        return df

    def rake_detail_to_intermediate(self, df, location_hierarchy, intermediate_locs):
        """Raking the detailed locations to their non-national parent. Have to do this
        individually by each intermediate location.
        """
        # add parent_id to the data
        df = add_location_metadata(df, ['parent_id'],
                                   location_meta_df=location_hierarchy
                                   )
        # loop through each intermediate loc and rake
        dfs = []
        for loc in intermediate_locs:
            temp = df.loc[(df.parent_id == loc) |
                          (df.location_id == loc)
                          ]
            # treating the intermediate location as a national to align with
            # the logic that identifies aggregates in the raker
            temp.loc[temp.location_id == loc, 'location_id'] = temp['parent_id']
            temp.drop('parent_id', axis=1, inplace=True)
            temp = self.standard_rake(temp, location_hierarchy)
            dfs.append(temp)
        df = pd.concat(dfs, ignore_index=True)
        # subset to just the detail locs, we'll add the intermediate and national on later
        df = df.loc[df.is_nat == 0]
        return df

    def double_rake(self, df, location_hierarchy):
        """Method built to rake twice when one location level exists between
        most detailed and the national level. Perhaps in time this can be expanded
        to function for datasets where multiple locations levels exists between
        most detailed and the national level (GBR only?).
        """
        start = len(df)
        # flag location levels
        df = add_location_metadata(df, ['level'],
                                   location_meta_df=location_hierarchy
                                   )
        report_if_merge_fail(df, 'level', 'location_id')

        # isolate the different location levels in the dataframe, assumption that levels
        # in the data are 3(natl), 4(intermediate/state), and 5(most detailed)
        national = (df['level'] == 3)
        intermediate = (df['level'] == 4)
        detail = (df['level'] == 5)
        # some checks:
        assert len(df[detail]) > 0, "Double raking assumes there is subnational data in the" \
            " dataframe, however no subnational detail is present"
        assert len(df[intermediate]) > 0, "Attempting to double rake, but there are no" \
            " intermediate locations in the data to rake to first"
        intermediate_locs = df[intermediate].location_id.unique().tolist()
        df.drop('level', axis=1, inplace=True)

        # rake the detailed locs to their parent first (first rake of double raking)
        non_national = df.loc[detail | intermediate]
        single_raked_detail_locs = self.rake_detail_to_intermediate(
            non_national, location_hierarchy, intermediate_locs)
        # now take the subnationals that have been raked to the intermediate locations
        # and rake them to the national (the second rake of double raking)
        detail_and_national = pd.concat([single_raked_detail_locs, df.loc[national]],
                                        ignore_index=True
                                        )
        detail_df = self.standard_rake(detail_and_national, location_hierarchy)
        detail_df = detail_df.loc[detail_df.is_nat == 0]

        # rake the intermediate level to the national
        intermediate_and_national = df.loc[intermediate | national]
        intermediate_and_national = self.standard_rake(
            intermediate_and_national, location_hierarchy)

        # concat the national, single raked intermediate locs, and double raked detail locs
        df = pd.concat([detail_df, intermediate_and_national], ignore_index=True)
        assert start == len(df), "The number of rows have changed,"\
            " this really shouldn't happen."
        return df
from tools.testing import Test, TestError

@Test.wrap
def not_null(df, ignore=[]):
    """True if a Pandas.DataFrame doesn't contain any null values"""
    columns = [col for col in df if col not in ignore]
    return df[columns].notnull().all().all()

@Test.wrap
def unique_column(df, column):
    """True if a Pandas.DataFrame has a single value for a given column"""
    if column not in df.columns:
        raise TestError(f"DataFrame doesn't have a `{column}` column")
    return len(df[column].unique()) == 1

@Test.wrap
def no_duplicates(df):
    """True if the Pandas.DataFrame doesn't contain duplicate rows"""
    return not df.duplicated().any()

@Test.wrap
def contains_column(df, column):
    """ True if the dataframe contains the column"""
    return column in df.columns

@Test.wrap
def prevalence_under_1(df):
    """ True if the Pandas.DataFrame has means under 1 for prevalence measures"""
    if "mean" not in df.columns or "measure_id" not in df.columns:
        raise TestError("DataFrame is missing a mean or measure_id column")
    return (df.loc[df.measure_id == 5, "mean"] <= 1).all()

@Test.wrap
def lower_less_than_mean(df):
    """ True if the Pandas.DataFrame has a lower less than the mean"""
    if "mean" not in df.columns or "lower" not in df.columns:
        raise TestError("DataFrame doesn't have a mean or lower column")
    return (df["lower"] <= df["mean"]).all()

@Test.wrap
def upper_greater_than_mean(df):
    """ True if the Pandas.DataFrame has an upper greater than the mean"""
    if "mean" not in df.columns or "upper" not in df.columns:
        raise TestError("DataFrame doesn't have a mean or upper column")
    return (df["upper"] >= df["mean"]).all()


@Test.wrap
def _upload_columns_test(df):
    """True if the Pandas.DataFrame contains all the columns needed to upload to the DB"""
    required_columnms = ['bundle_id',
                         'merged_nid',
                         'estimate_id',
                         'location_id',
                         'sex_id',
                         'year_start',
                         'year_end',
                         'age_group_id',
                         'measure_id',
                         'source_type_id',
                         'representative_id',
                         'uncertainty_type_id',
                         'uncertainty_type_value',
                         'mean',
                         'lower',
                         'upper',
                         'standard_error',
                         'effective_sample_size',
                         'sample_size',
                         'cases']
    
    for column in required_columnms:
        if column not in df:
            return False
    return True

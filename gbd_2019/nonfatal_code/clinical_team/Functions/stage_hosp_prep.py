import warnings
import pandas as pd
import numpy as np


def get_neonatal_df(round_to=5):
    """
    Stata has some precision issues, and we don't really need 9 decimal places for these,
    so round to 5 digits
    """
    df = pd.DataFrame({'age_group_id': [2, 3, 4],
                       'age_group_name': ['Early Neonatal', 'Late Neonatal', 'Post Neonatal'],
                       'age_start': [0, 0.01917808, 0.07671233],
                       'age_end': [0.01917808, 0.07671233, 1]})
    for col in ['age_start', 'age_end']:
        df[col] = df[col].round(round_to)
    return df[['age_group_id', 'age_group_name', 'age_start', 'age_end']]


def convert_age_units(df, unit_dict):
    """
    df (DataFrame): includes coded age_group_units,
    unit_dict (dictionary): identifies the units as hours, days, months, years, etc
            eg unit_dict = {'days': 2, 'months': 3}
            Acceptable terms are 'days', 'months', 'years', 'centuries'
    """
    accepted_keys = set(['days', 'months', 'years', 'centuries'])
    set_diff = set(unit_dict.keys()) - accepted_keys
    assert not set_diff, "{} is not an acceptable unit. We only understand {}".format(set_diff, accepted_keys)

    df['age_adjusted'] = 0
    cond_dict = {"(df['age_group_unit'] == unit_dict['days']) & (df['age'] <= 6)": 0,
                 "(df['age_group_unit'] == unit_dict['days']) & (df['age'] > 6) & (df['age'] <= 27)": 0.01917808,
                 "(df['age_group_unit'] == unit_dict['days']) & (df['age'] > 27) & (df['age'] < 365)": 0.07671233,
                 "(df['age_group_unit'] == unit_dict['months']) & (df['age'] >= 1) & (df['age'] <= 12)": 0.07671233,
                 "(df['age_group_unit'] == unit_dict['days']) & (df['age'] == 365)": 1}
    for key in list(cond_dict.keys()):
        if 'months' in key and 'months' not in set(unit_dict.keys()):
            pass
        else:
            value = cond_dict[key]
            df.loc[eval(key), ['age', 'age_group_unit', 'age_adjusted']] = [value, unit_dict['years'], 1]


    if 'centuries' in list(unit_dict.keys()):
        df.loc[df.age_group_unit == unit_dict['centuries'], ['age', 'age_group_unit', 'age_adjusted']] = [99, unit_dict['years'], 1]


    assert (df.loc[df['age_adjusted'] == 0, 'age_group_unit'] == unit_dict['years']).all(), "looks like some age groups other than years missed adjustment"
    df['age_group_unit'] = 1

    assert (df['age_group_unit'] == 1).all(), 'review the data that is not currently in years'
    df.drop('age_adjusted', axis=1, inplace=True)
    return df


def test_age_binning(df):
    """
    Test the age binner to make sure that;
    Age is always >= age start and <= age end
    Age is never negative
    Age is never above 125
    Warn if age is between 110 and 125
    """

    output_list = []
    if (df.age < 0).any():
        output_list.append(df[df.age < 0])
        print("There are some negative ages")

    if (df.age >= df.age_start).all() == False:
        output_list.append(df[df.age < df.age_start])
        print("Age is less than age start")

    if (df.age <= df.age_end).all() == False:
        output_list.append(df[df.age > df.age_end])
        print("Age is greater than age start")

    if (df.age > 125).any():
        output_list.append(df[df.age > 125])
        print("There are impossibly old ages present")


    if len(output_list) > 0:
        dat = pd.concat(output_list)
        print(dat)
        assert False, "One or more of the tests didn't pass"
    else:

        if (df.age > 110).any():
            print(df[df.age > 110])
            warnings.warn("There are {} rows with very, very old ages present".
                          format(df.age[df.age > 110].size))
        return


def age_binning(df, drop_age=False, terminal_age_in_data=True, allow_neonatal_bins=False):
    """
    function accepts a pandas DataFrame that contains age-detail and bins them
    into age ranges. Assumes that the DataFrame passed in contains a column
    named 'age'. terminal_age_in_data is used in formatting when the oldest
    age group present in the data is the terminal group.  Null values in 'age'
    will be given age_start and age_end representing all ages.

    Example: 32 would become 30, 35

    Example call: df = age_binning(df)

    The age_start bins are [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55,
    60, 65, 70, 75, 80, 85, 90, 95]

    The test will break if there are ages above 125
    """

    if df.age.max() > 99:
        warnings.warn("There are ages older than 99 in the data. Our terminal "
                      "age group is 95-125. Any age older than 99 will be "
                      "changed to age 99")
        df.loc[df.age > 99, 'age'] = 99

    if df.age.isnull().sum() > 0:
        warnings.warn("There are {} null ages in the data. Be aware: these "
                      "will be converted to age_start = 0 and age_end = 125 "
                      "and age will be arbitrarily converted to 0".\
                      format(df.age.isnull().sum()))


    max_age = df.age.max()

    df['age'] = pd.to_numeric(df['age'], errors='raise')



    if allow_neonatal_bins:
        age_bins = np.append(np.array([0, 0.019178, 0.076712, 1, 5]), np.arange(10, 101, 5))
        age_start_list = np.append(np.array([0, 0.019178, 0.076712, 1]), np.arange(5, 96, 5))
        age_end_list = np.append(np.array([0.019178, 0.076712, 1]), np.arange(5, 96, 5))
    else:

        age_bins = np.append(np.array([0, 1, 5]), np.arange(10, 101, 5))
        age_start_list = np.append(np.array([0, 1]), np.arange(5, 96, 5))
        age_end_list = np.append(np.array([1]), np.arange(5, 96, 5))

    age_end_list = np.append(age_end_list, 125)


    df['age_start'] = pd.cut(df['age'], age_bins, labels=age_start_list,
                             right=False)
    df['age_end'] = pd.cut(df['age'], age_bins, labels=age_end_list,
                           right=False)

    max_age = df.loc[df.age == max_age, 'age_start'].unique()
    assert len(max_age) == 1
    max_age = max_age[0]

    if terminal_age_in_data:

        df.loc[df['age_start'] == max_age, 'age_end'] = 125
    else:
        df.loc[df['age_start'] == 95, 'age_end'] = 125



    df['age_start'] = pd.to_numeric(df['age_start'], errors='raise')

    df['age_end'] = pd.to_numeric(df['age_end'], errors='raise')



    df.loc[df['age'].isnull(), ['age_start', 'age_end']] = [0, 125]


    df['age'].fillna(0, inplace=True)


    df.loc[(df.age_start.isnull()) | (df.age_end.isnull()),
           ['age_start', 'age_end']] =\
        df.loc[(df.age_start.isnull()) | (df.age_end.isnull()), 'age']



    if df.age.max() > 114:
        df.loc[(df.age > 114) & (df.age < 125),
               ['age', 'age_start', 'age_end']] = [114, 110, 115]
    test_df = test_age_binning(df)
    if test_df is not None:
        return test_df


    if drop_age:

        df.drop('age', axis=1, inplace=True)


    return(df)


def test_case_counts(df, compare_df):
    """
    Make sure the inpatient data isn't losing or gaining any admissions, or somehow transfering cases by sex or icd
    """
    dfcols = df.columns
    if 'val' in dfcols:
        sumcol = 'val'
    elif 'cases' in dfcols:
        sumcol = 'cases'

    bad_results = []


    if not df[sumcol].sum().round(3) == compare_df[sumcol].sum().round(3):
        bad_results.append("The total case count test failed")

    if not df.loc[df.age_start < 1, sumcol].sum().round(3) == compare_df.loc[compare_df.age_start < 1, sumcol].sum().round(3):
        bad_results.append("The under 1 case count test failed")


    grouper_cols = ['sex', 'sex_id', 'location_id', 'year', 'year_id', 'year_start', 'cause_code']
    groups = [c for c in grouper_cols if c in dfcols]
    print("grouping data by {}".format(groups))

    if not df.groupby(groups).agg({sumcol: 'sum'}).reset_index().equals(compare_df.groupby(groups).agg({sumcol: 'sum'}).reset_index()):
        bad_results.append("The groupby with {} summed by {} has failed".format(sumcol, groups))

    if bad_results:
        return bad_results
    else:
        return "test_case_counts has passed!!"

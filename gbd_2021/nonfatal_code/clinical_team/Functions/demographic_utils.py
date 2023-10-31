"""
Space for some functions I've been using to manipulate data demographic info
"""
import db_queries
import pandas as pd

from clinical_info.Functions import hosp_prep


def rate_count_switcher(df, gbd_round_id, decomp_step, rate_cols=['mean', 'lower', 'upper']):
    """
    3 types of year columns
    year_id
    year_start, year_end - both identical
    year_start, year_end - agged to five year groups

    2 types of age columns
    age_start, age_end
    age_group_id
    """
    pre_cols = df.columns

    if 'age_group_id' not in pre_cols:
        good_ages = hosp_prep.get_hospital_age_groups()
        df = df.merge(good_ages[['age_start', 'age_group_id']], how='left', on='age_start', validate='m:1')
    if 'year_id' in pre_cols:
        df['year_start'], df['year_end'] = df['year_id'], df['year_id']
    if 'sex_id' not in pre_cols:
        assert set(df['sex'].unique()) == set(['Male', 'Female'])
        df['sex_id'] = 2
        df.loc[df['sex'] == 'Male', 'sex_id'] = 1

    years = list(np.arange(1990, 2018, 1))
    ages = df.age_group_id.unique().tolist()
    locs = df.location_id.unique().tolist()

    # get pop
    assert False, "Is this being used? Update to pull in cached population from a run_id"
    pop = db_queries.get_population(gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                                    year_id=years, age_group_id=ages, sex_id=[1,2],
                                    location_id=locs)
    pop['year_start'], pop['year_end'] = pop['year_id'], pop['year_id']
    pop.drop(['year_id', 'run_id'], axis=1, inplace=True)

    # agg pop to five year bands
    agg_pop = pop.copy()
    agg_pop = hosp_prep.year_binner(agg_pop)
    agg_pop = agg_pop.groupby(['age_group_id', 'year_start', 'year_end', 'location_id', 'sex_id']).\
                            agg({'population': 'mean'}).reset_index()

    if (df['year_start'] + 4 == df['year_end']).all():
        df = df.merge(agg_pop, how='left',
                    on=['age_group_id', 'sex_id', 'location_id', 'year_start', 'year_end'],
                    validate='m:1')
    else:
        df = df.merge(pop, how='left',
                    on=['age_group_id', 'sex_id', 'location_id', 'year_start', 'year_end'],
                    validate='m:1')

    assert df['population'].isnull().sum() == 0

    for col in rate_cols:
        df["count_{}".format(col)] = df[col] * df['population']

    return df


def retain_good_age_groups(df, clinical_age_group_set_id):

    if 'age_group_id' not in df.columns:
        raise ValueError("retain_good_age_groups won't work without age_group_id")

    ages = hosp_prep.get_hospital_age_groups(clinical_age_group_set_id)
    pre = len(df)
    pre_ages = df['age_group_id'].unique().tolist()
    df = df[df['age_group_id'].isin(ages['age_group_id'])]
    post = len(df)
    dropped_ages = set(pre_ages) - set(df['age_group_id'].unique().tolist())
    print(f"We've dropped {pre - post} rows with 'bad' age group IDs: {dropped_ages}")

    return df


def remove_age_group_incompletes(df, complete_cols,
                                 clinical_age_group_set_id,
                                 write_path=None):


    df = retain_good_age_groups(df, clinical_age_group_set_id)
    ages = hosp_prep.get_hospital_age_groups(clinical_age_group_set_id)

    grouped_ages = df[complete_cols + ['age_group_id']].groupby(cols).nunique().drop(cols, axis=1).reset_index()

    if write_path is not None:
        grouped_ages.to_csv(write_path, index=False)


    grouped_ages = grouped_ages[grouped_ages['age_group_id'] == len(ages)]
    grouped_ages.drop('age_group_id', axis=1, inplace=True)


    df = df.merge(grouped_ages, how='right', on=complete_cols, validate='m:1')

    return df

def retain_age_sex_split_age_groups(df, run_id, round_id,
                                    clinical_age_group_set_id,
                                    drop_incomplete_loc_years=False):


    parent_path = FILEPATH

    pre_path = f"{parent_path}/FILEPATH{round_id}"
    store_source_ages(df=df, write_path=pre_path)

    pre_sources = df['source'].unique().tolist()
    df = retain_good_age_groups(df, clinical_age_group_set_id)

    if drop_incomplete_loc_years:
        review_path = f'{parent_path}/FILEPATH{round_id}.csv'


        df = remove_age_group_incompletes(df=df, complete_cols=['location_id', 'year_start'],
                                          clinical_age_group_set_id=clinical_age_group_set_id,
                                          write_path=review_path)
    else:
        pass

    post_sources = df['source'].unique().tolist()
    lost_src_count = len(pre_sources) - len(post_sources)
    print(f"We have completely removed {lost_src_count} sources.")
    if lost_src_count > 0:
        print(f"The sources dropped were {set(pre_sources) - set(post_sources)}")

    write_path = f"{parent_path}/source_age_info/round_{round_id}_final_ages.csv"
    store_source_ages(df=df, write_path=write_path)

    return df

def get_year_cols(columns):

    ycols = ['year_id', 'year_start', 'year_end']
    years = [y for y in ycols if y in columns]

    return years

def store_source_ages(df, write_path):

    years = get_year_cols(df.columns.tolist())
    groups = ['source', 'location_id', 'age_group_id'] + years
    tmp = df.groupby(groups).val.sum().reset_index()
    tmp.to_csv(write_path, index=False)

    return

def sum_under1_data(df, group_cols, sum_cols, clinical_age_group_set_id):


    ages = hosp_prep.get_hospital_age_groups(clinical_age_group_set_id)
    ages = ages[ages['age_start'] < 1]

    if 28 in df['age_group_id'].unique().tolist():

        df = df[df['age_group_id'] != 28]


    init_case_dict = {}
    for sum_col in sum_cols:
        init_case_dict[sum_col] = df[sum_col].sum().round(1)

    # split between under and over 1
    u1 = df[df['age_group_id'].isin(ages['age_group_id'])].copy()
    if len(u1) == 0:
        print(f"There is no under 1 data for clinical age set {clinical_age_group_set_id}")
    u1['age_group_id'] = 28

    df = df[~df['age_group_id'].isin(ages['age_group_id'])]

    sum_dict = dict(zip(sum_cols, ['sum'] * len(sum_cols)))
    assert u1.isnull().sum().sum() == 0, "There are nulls which will get lost"
    col_diff = set(u1.columns) - set(group_cols + sum_cols)
    if col_diff:
        print((f"The columns {col_diff} will not be used in the groupby "
                "filled with nulls during concatination with over 1 data"))
    u1 = u1.groupby(group_cols).agg(sum_dict).reset_index()

    df = pd.concat([df, u1], sort=False, ignore_index=True)

    for sum_col in sum_cols:
        pre = init_case_dict[sum_col]
        post = df[sum_col].sum().round(1)
        if pre != post:
            raise ValueError((f"Started with {pre} cases and finished with "
                              f"{post} cases which means something went wrong"))
    return df

def confirm_age_group_set(age_col, clinical_age_group_set_id, full_match):
    """Raise a val error if a set of unique age_group_ids perfectly matches the
    clinical age group set or is a subset of it

    Params:
        age_col (list or vector):
            Object castable to set containing age_group_ids
        clinical_age_group_set_id (int):
            The clinical sets we use to identify which age_groups we're processing
        full_match (bool):
            Identifies if a perfect match is required, or if we only need to
            confirm that age_col is present in the CAGSI

    Returns:
        Nothing
    """

    if len(age_col) != len(set(age_col)):
        raise ValueError("A unique vector or list is expected")
    ages = hosp_prep.get_hospital_age_groups(clinical_age_group_set_id)

    if full_match:
        diff_groups = set(age_col).symmetric_difference(ages['age_group_id'])
        if diff_groups:
            raise ValueError((f"The data doesn't contain the exact age groups "
                            f"expected. Diff is {diff_groups}"))
    else:
        diff_age = set(age_col) - set(ages['age_group_id'])
        if diff_age:
            raise ValueError((f"Age group id {age_col} is not present in "
                              f"clinical age set {clinical_age_group_set_id}"))
    return

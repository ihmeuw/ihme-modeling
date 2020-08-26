"""
Space for some functions I've been using to manipulate data demographic info
"""
import getpass
import sys
import db_queries

user = getpass.getuser()
sys.path.append("FILEPATH".format(user))
import hosp_prep


def rate_count_switcher(df, gbd_round_id, decomp_step, rate_cols=['mean', 'lower', 'upper']):
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


    pop = db_queries.get_population(gbd_round_id=gbd_round_id, decomp_step=decomp_step, year_id=years, age_group_id=ages, sex_id=[1,2],
                                    location_id=locs)
    pop['year_start'], pop['year_end'] = pop['year_id'], pop['year_id']
    pop.drop(['year_id', 'run_id'], axis=1, inplace=True)


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

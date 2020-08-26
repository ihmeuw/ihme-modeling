import pandas as pd
import numpy as np
import warnings
import db_queries
import db_tools
import ipdb

def quick_bear_bones_structure_check(df):
    """A fast assertion that the dataframe meets bear bones structure reqs
    """
    return True



def get_age_groups():
    """Retrieve age groups from the shared.age_group table"""
    age_groups = db_tools.ezfuncs.query("""
        SELECT *
        FROM shared.age_group
    """, conn_def='cod')
    return age_groups


def prep_age_groups(age_groups):
    """Prep age groups for use in age sex splitting"""
    age_groups = age_groups.copy()
    keep_cols = ['age_group_id',
                 'age_group_years_start', 'age_group_years_end']
    age_groups = age_groups[keep_cols]
    return age_groups


def get_ages_in_aggregate_id(age_group_table, good_age_group_ids, age_group_id,
                             window_extension=0.001):
    """Retrieve the ages in a group"""

    expected_columns = ['age_group_id', 'age_group_years_start',
                        'age_group_years_end']
    missing_cols = set(expected_columns) - set(age_group_table.columns)
    assert missing_cols == set(), \
        "Missing {} in given age_group_table".format(missing_cols)


    use_age_group_id = age_group_id
    if age_group_id == -1:
        use_age_group_id = 22


    assert use_age_group_id in set(age_group_table.age_group_id), \
        "Given age_group_id {} not in given " \
        "age group table".format(age_group_id)


    missing_age_group_ids = set(good_age_group_ids) - \
        set(age_group_table.age_group_id)
    assert missing_age_group_ids == set(), \
        "Missing age group ids {} in given " \
        "age_group_table".format(missing_age_group_ids)

    age_set = age_group_table.loc[
        age_group_table['age_group_id'].isin(good_age_group_ids)]

    start = age_group_table.loc[
        age_group_table['age_group_id'] == use_age_group_id,
        'age_group_years_start']
    assert len(start) == 1
    start = start.iloc[0]
    end = age_group_table.loc[
        age_group_table['age_group_id'] == use_age_group_id,
        'age_group_years_end']
    assert len(end) == 1
    end = end.iloc[0]
    df = age_set.loc[
        (age_set['age_group_years_start'] >= (start - window_extension)) &
        (age_set['age_group_years_end'] <= (end + window_extension))
    ].copy()
    df.loc[:, 'agg_age_group_id'] = int(age_group_id)
    return df[['agg_age_group_id', 'age_group_id']]


def prep_age_aggregate_to_detail_map(age_group_table, good_age_group_ids):
    """Return a mapping from agg_age_group_id to age_group_id

    One agg_age_group_id has 1 or more age_group_ids. Uses the age start
    and end for each age group id in the given age group table.

    Returns:
        df - pandas DataFrame with columns ['agg_age_group_id', 'age_group_id']
    """

    all_age_group_ids = list(set(age_group_table.age_group_id))

    all_age_group_ids.append(-1)

    age_maps = []
    failed_ages = []
    for age_group_id in all_age_group_ids:
        try:
            age_map = get_ages_in_aggregate_id(
                age_group_table,
                good_age_group_ids,
                age_group_id
            )
            age_maps.append(age_map)
        except:
            failed_ages.append(age_group_id)
    detail_age_map = pd.concat(age_maps, sort=False, ignore_index=True)
    return detail_age_map


def prep_sex_aggregate_to_detail_map():
    """Return a mapping from agg_sex_id to sex_id.

    Returns:
        df (pandas.DataFrame): ['agg_sex_id', 'sex_id']
    """
    df = pd.DataFrame(
        columns=['agg_sex_id', 'sex_id'],
        data=[
            [3, 1],
            [3, 2],
            [9, 1],
            [9, 2],
            [1, 1],
            [2, 2]
        ]
    )
    return df



def get_id_cols(stage):
    """Retrive the identifying columns for a given stage."""
    pass



def get_age_weights(run_id, weight_path, distribution_set_version_id=15, level_of_analysis='cause_id'):
    """Get age weights from the database"""

    if level_of_analysis == 'icg_id':



        weights = pd.read_csv(r"FILENAME"
                              r"FILENAME"
                              r"FILEPATH".\
                              format(run_id))
    elif level_of_analysis == 'bundle_id':
        assert weight_path, "We don't know which CSV of weights to read into age-sex splitting"
        weights = pd.read_csv(weight_path)
    else:
        weights = db_tools.ezfuncs.query(
            """
                "DB QUERY"

            """.format(distribution_set_version_id), conn_def='cod'
        )
    return weights


def separate_detailed_from_aggregate(df, good_age_group_ids,
                                     unsplittable_ages, unsplittable_sexes):
    """Split dataframe into what is to be split and what is not.

    Args:
        df: dataframe with columns age_group_id and sex_id
        good_age_group_ids: list of ages desired in final set
        unsplittable_ages (list):
            list of ages that can't be split at all (say, 1yr-2yr)
        unsplittable_sexes (list):
            list of sexes that can't be split at all (who knows?)

    Returns:
        split_dict: dict with two keys,
            'nosplit' referencing the dataframe that should be
                avoided by age sex splitting altogether
            'split' referencing the dataframe that should be split
    """

    is_good_age = df['age_group_id'].isin(good_age_group_ids)
    is_unsplittable_age = df['age_group_id'].isin(unsplittable_ages)
    no_age_splitting = is_good_age | is_unsplittable_age


    is_good_sex = df['sex_id'].isin([1, 2])
    is_unsplittable_sex = df['sex_id'].isin(unsplittable_sexes)
    no_sex_splitting = is_good_sex | is_unsplittable_sex


    no_splitting = no_age_splitting & no_sex_splitting
    nosplit_df = df[no_splitting]
    split_df = df[~no_splitting]


    return {'nosplit': nosplit_df, 'split': split_df}


def prep_child_to_available_parent_map(cause_set_id, gbd_round_id,
                                       available_cause_ids, as_dict=False):
    """Prep a mapping of cause_id to the most detailed available parent.

    For a given cause hierarchy, and a list of "available" causes, return
    a mapping from each cause in the hierarchy to the most detailed available
    cause that is "available" and in that cause's path_to_top_parent.
    "available": icg_id/cause_id are present in the weights
    dataframe

    Arguments:
        cause_set_id (int): from shared.cause_set in the database
        gbd_round_id (int): from shared.gbd_round in the database
            together, cause_set_id and gbd_round_id determine the
            active cause set version id to use from
            shared.cause_hierarchy_history
        available_cause_ids (list of ints): all must be cause ids in
            shared.cause
        as_dict (bool): If False, returns a dataframe instead of a dict

    Returns:
        cause_map (dict): a dictionary from cause_id to available_cause_id
        or, if as_dict == False: a dataframe ['cause_id', 'parent_cause_id']

    This function isn't used if the data that is being split is at the
    icg_id level.
    """
    causes = db_queries.get_cause_metadata(
        cause_set_id=cause_set_id,
        gbd_round_id=gbd_round_id
    )
    cause_levels = causes.path_to_top_parent.str.split(',').apply(pd.Series, 1)
    cause_tree = pd.concat(
        [causes[['cause_id', 'path_to_top_parent', 'level']], cause_levels],
        axis=1
    )
    cause_tree = cause_tree.drop(['path_to_top_parent', 'level'], axis=1)
    cause_tree = cause_tree.set_index(['cause_id']).stack().reset_index()
    cause_tree = cause_tree.rename(
        columns={'level_1': 'par_level', 0: 'parent_cause_id'}
    )
    cause_tree['parent_cause_id'] = cause_tree['parent_cause_id'].astype(int)

    cause_availability = {c: 1 for c in available_cause_ids}
    cause_tree['available'] = \
        cause_tree['parent_cause_id'].map(cause_availability).fillna(0)

    available_cause_map = cause_tree.query('available == 1')
    available_cause_map['max_level_available'] = \
        available_cause_map.groupby('cause_id')['par_level'].transform(max)
    available_cause_map = available_cause_map.query(
        'par_level == max_level_available'
    )
    available_cause_map = available_cause_map[['cause_id', 'parent_cause_id']]
    assert not available_cause_map[['cause_id']].duplicated().any()
    missing = set(causes.cause_id) - set(available_cause_map['cause_id'])
    if len(missing) > 0:
        raise AssertionError(
            "Was not able to find parent in given available cause "
            "ids list for these cause ids: \n{}".format(missing)
        )

    if as_dict:
        available_cause_map = available_cause_map.set_index('cause_id')
        available_cause_map = available_cause_map.to_dict()['parent_cause_id']
    return available_cause_map


def prep_cause_to_weight_cause_map(cause_set_id, gbd_round_id, weight_causes,
                                   level_of_analysis='cause_id'):
    """Get the right distribution to use based on those available.

    Defaults to most detailed parent cause of each cause id in the given
    hierarchy that is in the weight casues list, unless specific exceptions
    are coded.
    """

    if level_of_analysis == 'cause_id':

        weight_cause_map = prep_child_to_available_parent_map(
            cause_set_id,
            gbd_round_id,
            weight_causes
        )
        weight_cause_map = weight_cause_map.rename(
            columns={'parent_cause_id': 'weight_cause_id'}
        )


        causes = db_queries.get_cause_metadata(
            cause_set_id=cause_set_id,
            gbd_round_id=gbd_round_id
        )
        acauses = causes[['cause_id', 'acause']].set_index(
            'cause_id').to_dict()['acause']


        paths = causes[['cause_id', 'path_to_top_parent']].set_index(
            'cause_id').to_dict()['path_to_top_parent']
        weight_cause_map['path_to_top_parent'] = \
            weight_cause_map['cause_id'].map(paths)


        weight_cause_map.loc[
            weight_cause_map['cause_id'] == 843,
            'weight_cause_id'] = 344

        weight_cause_map.loc[
            weight_cause_map['cause_id'].isin([743, 919]),
            'weight_cause_id'] = 294

        weight_cause_map.loc[
            weight_cause_map['path_to_top_parent'].str.contains(',366,'),
            'weight_cause_id'] = 366

        weight_cause_map.loc[
            weight_cause_map['cause_id'].isin([855, 854, 851]),
            'weight_cause_id'] = 730

        weight_cause_map.loc[
            weight_cause_map['cause_id'] == 940,
            'weight_cause_id'] = 716
        weight_cause_map['acause'] = weight_cause_map['cause_id'].map(acauses)
        weight_cause_map['weight_acause'] = \
            weight_cause_map['weight_cause_id'].map(acauses)
    else:



        weight_cause_map = pd.DataFrame({level_of_analysis: weight_causes,
                                         'weight_{}'.format(level_of_analysis): weight_causes})

    return weight_cause_map[[level_of_analysis,
                             'weight_{}'.format(level_of_analysis)]]


def report_if_merge_fail(df, check_col, id_cols):
    """Report a merge failure if there is one"""
    merge_fail_text = """
        Could not find {check_col} for these values of {id_cols}:

        {values}
    """
    if df[check_col].isnull().values.any():

        missing = df.loc[df[check_col].isnull(), id_cols].drop_duplicates()
        raise AssertionError(
            merge_fail_text.format(check_col=check_col,
                                   id_cols=id_cols, values=missing)
        )
    else:
        pass


def prep_split_df(df, pop_df, weight_df, age_detail_map,
                  sex_detail_map, cause_to_weight_cause_map, id_cols,
                  level_of_analysis='cause_id'):
    """
    Merge all the information onto df necessary to calculate split val

    Args:
        df (DataFrame): data to merge stuff onto
        pop_df (DataFrame): has population information for the demographics in
            df
        weight_df (DataFrame): Has the weights that will be used to split
            aggregated sexes and ages
        age_detail_map (DataFrame): Has two columns that act as a mapping from
            aggregated age groups to good age groups
        sex_detail_map (DataFrame): Has two columns that act as a mapping from
            aggregated sex groups to good sex groups
        cause_to_weight_cause_map (DataFrame): Acts as a mapping between causes
            incase weights from a certain cause need to be applied to a
            a different cause.  CoD has lots of exceptions.
        id_cols (list): list of columns that must exist in df and identify
            observations. Used to preserve df in every way except for splitting
            value_column, age_group_id, and sex_id.
        level_of_analysis (str): what etiology you want to use
    """
    df = df.rename(
        columns={
            'sex_id': 'agg_sex_id',
            'age_group_id': 'agg_age_group_id'
        }
    )


    df = df.merge(sex_detail_map, how='left')

    report_if_merge_fail(df, 'sex_id', ['agg_sex_id'])


    df = df.merge(age_detail_map, how='left')

    report_if_merge_fail(df, 'age_group_id', ['agg_age_group_id'])


    pop_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    df = df.merge(pop_df, on=pop_cols, how='left')
    report_if_merge_fail(df, 'population', pop_cols)





    df = df.merge(cause_to_weight_cause_map, on=[level_of_analysis],
                  how='left')




    report_if_merge_fail(df, 'weight_{}'.format(level_of_analysis),
                         [level_of_analysis])




    weight_df.rename(columns={level_of_analysis:
                              'weight_{}'.format(level_of_analysis)},
                     inplace=True)
    merge_cols = ['age_group_id', 'sex_id',
                  'weight_{}'.format(level_of_analysis)]
    df = df.merge(
        weight_df,
        on=merge_cols,
        how='left'
    )

    report_if_merge_fail(df, 'weight', merge_cols)



    this_id_cols = list(id_cols)
    this_id_cols.append('agg_age_group_id')
    this_id_cols.append('agg_sex_id')


    assert not df[this_id_cols].duplicated().values.any(), ("there are "
        "duplicated rows")

    return df


def calculate_detail_val(df, id_cols, fix_gbd2016_mistake=True,
                         value_column='deaths'):
    """
    Calculate detail val using population and relative rates
    This is the core of age sex splitting
    """


    agg_name = 'agg_{}'.format(value_column)
    assert 'exp_val' not in df.columns, \
        "Unexpected: exp_val already in columns"
    assert 'sum_exp_val' not in df.columns, \
        "Unexpected: sum_exp_val already in columns"
    assert agg_name not in df.columns, \
        "Unexpected: {} already in columns".format(agg_name)

    df = df.rename(columns={value_column: agg_name})
    df['exp_val'] = df['weight'] * df['population']
    group_cols = list(id_cols)
    group_cols.remove('age_group_id')
    group_cols.remove('sex_id')
    group_cols.append('agg_age_group_id')
    group_cols.append('agg_sex_id')
    df['sum_exp_val'] = df.groupby(group_cols)['exp_val'].transform(sum)


    if fix_gbd2016_mistake:



        df.loc[df['sum_exp_val'] == 0, 'exp_val'] = 1

        df.loc[
            df['sum_exp_val'] == 0,
            'sum_exp_val'
        ] = df.groupby(group_cols)['exp_val'].transform(sum)


    df[value_column] = df['exp_val'] * (df[agg_name] / df['sum_exp_val'])


    if not fix_gbd2016_mistake:

        df[value_column] = df[value_column].fillna(0)

    return df


def get_active_cause_set_version(cause_set_id, gbd_round_id=4):
    """Pull the best cause set version"""
    q = """SQL"""
    result = db_tools.ezfuncs.query(q.format(cause_set_id, gbd_round_id),
                                    conn_def='cod')
    assert result.shape == (1, 1)
    return result.iloc[0, 0]


def split_age_sex(df, run_id, id_cols, gbd_round_id, decomp_step,
                  value_column='deaths', fix_gbd2016_mistake=True, cause_set_id=4,
                  keep_weight_cause_used=False, gbd_team_for_ages='cod',
                  level_of_analysis='cause_id', weight_path=None):
    """Split value_column into detailed age and sex groups.

    Applies a relative rate splitting algorithm with a K-multiplier that
    adjusts for the specific population that the data to be split applies to.

    Arguments:
        df (pandas.DataFrame): must contain all columns needed to merge on
            population: ['location_id', 'age_group_id', 'sex_id', 'year_id'].
            Must be unique on id_cols.
        id_cols (list): list of columns that must exist in df and identify
            observations. Used to preserve df in every way except for splitting
            value_column, age_group_id, and sex_id.
        value_column (str): must be a column in df that contains values to be
            split
        fix_gbd2016_mistake (bool): a weird option to allow CoD team to keep
            an error in the code to maintain comparability to previous data.
            Not really worth explaining, leave as True.
        gbd_round_id (int): what gbd round. This determines the detailed age
            groups and the cause hierarchy to split on
        cause_set_id (int): determines what cause ids to use. will fail
            if you have causes in df outside of this cause hierarchy
        keep_weight_cause_used (bool): keep a column, 'weight_cause_id', that
            lets you know which distribution was used to split the data
        gbd_team_for_ages (str): what gbd team to use to call the shared
            function db_queries.get_demographics

    Returns:
        split_df (pandas.DataFrame): contains all the columns passed in df,
            but all age_group_id values will be detailed, all sex_ids will be
            detailed (1, 2), and val will be split into these detailed ids.
    """

    orig_val_sum = df[value_column].sum()


    assert quick_bear_bones_structure_check(df)






    good_age_group_ids = db_queries.get_demographics(
        gbd_team_for_ages,
        gbd_round_id=5
    )['age_group_id']
    if level_of_analysis == 'icg_id' or level_of_analysis == 'bundle_id':
        good_age_group_ids.remove(2)
        good_age_group_ids.remove(3)
        good_age_group_ids.remove(4)
        good_age_group_ids.append(28)



    locations_in_data = list(set(df.location_id))
    years_in_data = list(set(df.year_id))
    pop_df = db_queries.get_population(
        age_group_id=good_age_group_ids,
        location_id=locations_in_data,
        year_id=years_in_data,
        sex_id=[1, 2],
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step
    )


    age_groups = get_age_groups()
    age_groups = prep_age_groups(age_groups)


    age_detail_map = prep_age_aggregate_to_detail_map(age_groups,
                                                      good_age_group_ids)


    unsplittable_ages = set(df['age_group_id']) - \
        set(age_detail_map['agg_age_group_id'])
    warn_text = """
        These age group ids cannot be split onto the given age group set.

        {}
    """.format(unsplittable_ages)
    if len(unsplittable_ages) > 0:
        warnings.warn(warn_text)


    sex_detail_map = prep_sex_aggregate_to_detail_map()
    unsplittable_sexes = set(df['sex_id']) - set(sex_detail_map['agg_sex_id'])
    warn_text = """
        These sex ids cannot be split onto Males/Females.

        {}
    """.format(unsplittable_sexes)
    if len(unsplittable_sexes) > 0:
        warnings.warn(warn_text)


    weight_df = get_age_weights(run_id=run_id,\
                                level_of_analysis=level_of_analysis,
                                weight_path=weight_path)
    weight_causes = weight_df[level_of_analysis].unique()










    cause_to_weight_cause_map = \
        prep_cause_to_weight_cause_map(cause_set_id,
                                       gbd_round_id, weight_causes,
                                       level_of_analysis=level_of_analysis)




    sep_df_dict = separate_detailed_from_aggregate(
        df,
        good_age_group_ids,
        unsplittable_ages,
        unsplittable_sexes
    )
    nosplit_df = sep_df_dict['nosplit']
    split_df = sep_df_dict['split']


    split_df = prep_split_df(
        split_df,
        pop_df,
        weight_df,
        age_detail_map,
        sex_detail_map,
        cause_to_weight_cause_map,
        id_cols,
        level_of_analysis=level_of_analysis
    )



    split_df = calculate_detail_val(
        split_df,
        id_cols,
        fix_gbd2016_mistake=fix_gbd2016_mistake,
        value_column=value_column
    )


    split_df = split_df[nosplit_df.columns]
    final_df = nosplit_df.append(split_df, ignore_index=True)


    group_columns = list(nosplit_df.columns)
    group_columns.remove(value_column)
    final_df = final_df.groupby(group_columns,
                                as_index=False)[value_column].sum()


    val_diff = abs(final_df[value_column].sum() - orig_val_sum)
    if not np.allclose(val_diff, 0):
        text = "Difference of {} {} from age sex " \
               "splitting".format(val_diff, value_column)
        if fix_gbd2016_mistake:
            raise AssertionError(text)
        else:
            warnings.warn(text)

    if level_of_analysis=='icg_id':



        if final_df[id_cols].duplicated().values.any():
            group_cols = list(final_df.columns)
            group_cols.remove('val')


            final_df = final_df.groupby(by=group_cols).agg({"val": "sum"}).\
                reset_index()



    assert not final_df[id_cols].duplicated().values.any(), ("there are "
        "duplicated rows in the final_df")


    bad = set(final_df.age_group_id) - set(good_age_group_ids)
    if len(bad) > 0:
        text = "Some age group ids still aggregate: {}".format(bad)
        raise AssertionError(text)


    assert set(final_df[level_of_analysis]) == set(df[level_of_analysis])

    return final_df



def read_data(source, stage, timestamp='most_recent', refresh_id=None):
    """Read data for a particular source and stage"""


    if timestamp == 'specific_refresh':
        assert refresh_id is not None
        assert type(refresh_id) == int
        timestamp = db_tools.ezfuncs.query(
            """
            SQL
            """.format(source, stage, refresh_id),
            conn_def='cod'
        )
        assert timestamp.shape == (1, 1)
        timestamp = timestamp.iloc[0, 0]

    if timestamp == 'most_recent':
        df = pd.read_csv(
            "FILENAME"
            "FILEPATH".format(source, stage)
        )
    else:
        df = pd.read_csv(
            "FILENAME"
            "FILEPATH".format(source, stage, timestamp)
        )
    return df


def example_test_run_cod():
    mapped_df = read_data("MOZ_MoH_urban", "01_mapped",
                          timestamp='2017_01_03_173506')



    mapped_df = mapped_df.drop(['frmat', 'im_frmat',
                                'cf', 'sample_size'], axis=1)
    mapped_df['site'] = mapped_df['site'].fillna('')
    mapped_df = mapped_df.groupby(
        list(set(mapped_df.columns) - set(['deaths'])),
        as_index=False)['deaths'].sum()


    id_cols = ['code_id', 'sex_id', 'site', 'nid',
               'year_id', 'age_group_id', 'location_id']
    assert not mapped_df[id_cols].duplicated().any()


    t = split_age_sex(mapped_df, id_cols, fix_gbd2016_mistake=True)





    agesplit_df = read_data("MOZ_MoH_urban", "02_agesexsplit",
                            timestamp='2017_01_03_173506')
    prev_deaths = agesplit_df.deaths.sum()
    new_deaths = t.deaths.sum()
    print(
        "New method deaths ({}) - old "
        "method deaths ({}) "
        "= {}".format(new_deaths, prev_deaths, (new_deaths - prev_deaths))
    )

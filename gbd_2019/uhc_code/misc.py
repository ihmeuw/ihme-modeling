import warnings

import pandas as pd

UHC_DIR = FILEPATH


def get_uhc_services(unique=False):
    '''
    Convert UHC service matrix into dictionary by age.
    '''
    matrix_df = pd.read_excel(FILEPATH)
    matrix_df['Population'].fillna(method='ffill', inplace=True)
    service_proxies = dict()
    for population in matrix_df.Population.unique():
        services = matrix_df.loc[matrix_df.Population == population, [i for i in list(matrix_df) if i != 'Population']].values.tolist()
        services = list(set([i.replace('  ', ' ').strip() for j in services for i in j if isinstance(i, str)]))
        service_population = {population:services}
        service_proxies.update(service_population)
    if unique:
        return sorted(set([i for j in service_proxies.values() for i in j if isinstance(i, str)]))
    else:
        return service_proxies


def get_uhc_services_groups():
    '''
    Print column names from matrix.
    '''
    matrix_df = pd.read_excel(FILEPATH)
    service_groups = [i for i in list(matrix_df) if i != 'Population']

    return service_groups


def ids_to_int(df, index_cols):
    '''
    Ensure *_id columns are ints, not floats.
    '''
    for index_col in index_cols:
        df[index_col] = df[index_col].astype(int)

    return df


def indexer(df, index_cols):
  '''
  Index dataframe.
  '''
  df = ids_to_int(df, index_cols)
  df.sort_values(index_cols, inplace=True)
  df.set_index(index_cols, inplace=True)

  return df


def draw_math(df_list, index_cols, draw_cols, operator):
  '''
  Create dataframe by applying draw-wise operation on inputs.

  Should add a broadcasting component.
  '''
  if operator in ['-', '/']:
      assert len(df_list) == 2, 'Only two dataframes allowed for subtraction or division.'
      warnings.warn(
        'Subtracting/dividing df_list[0] by df_list[1] '\
        '(make sure list is ordered correctly).',
        Warning
    )
  assert len(set([len(df) for df in df_list])) == 1, \
      'Not all data frames are same length...'
  assert all([len(set(index_cols) - set(list(df))) == 0 for df in df_list]), \
      'Index columns not in all data frames'
  assert all([len(set(draw_cols) - set(list(df))) == 0 for df in df_list]), \
      'Draw columns not in all data frames'
  df_list = [df.copy() for df in df_list]
  df_list = [indexer(df, index_cols) for df in df_list]
  for df in df_list:
      assert df.index.equals(df_list[0].index), 'Index columns not identical'
  final_df = pd.DataFrame(index=df_list[0].index)
  for df in df_list:
      if len(set(draw_cols) - set(list(final_df))) == 1000:
          final_df[draw_cols] = df[draw_cols]
      elif len(set(draw_cols) - set(list(final_df))) == 0:
          final_df[draw_cols] = eval(
              'final_df[draw_cols] ' + operator + ' df[draw_cols]'
          )
      else:
          raise ValueError('Column issue.')
  final_df.reset_index(inplace=True)

  return final_df


def age_standardize(df, id_cols, agg_cols, draw_parameters, uhc_version_dir, counts=True):
    '''
    Limit to desired ages, scale to 1, and age-standardize.
    '''
    # load age weights - limit to included ages and rescale
    age_df = pd.read_hdf(FILEPATH)
    age_df = age_df.loc[
        age_df.age_group_id.isin(draw_parameters['age_group_id'])
    ]
    age_df['age_group_weight_value'] = age_df['age_group_weight_value'] / age_df.age_group_weight_value.sum()
    age_df = age_df[['age_group_id', 'age_group_weight_value']]

    if counts:
        # load pops
        pop_df = pd.read_hdf(FILEPATH)
        pop_df = pop_df.loc[
            (pop_df.location_id.isin(draw_parameters['location_id'])) &
            (pop_df.year_id.isin(draw_parameters['year_id'])) &
            (pop_df.age_group_id.isin(draw_parameters['age_group_id'])) &
            (pop_df.sex_id.isin(draw_parameters['sex_id']))
        ]
        pop_df = pop_df.groupby(
            id_cols + ['age_group_id'], as_index=False
        ).population.sum()

        # convert to rate
        if 'sex_id' in list(df):
            assert len(df.sex_id.unique()) == 1, 'Assumes one sex present (if both, must be aggregated).'
        df = df.merge(pop_df)
        df[agg_cols] = (
            df[agg_cols].values.transpose() / df['population'].values
        ).transpose()

    # add on to dataframe and age-standarize
    df = df.merge(age_df)
    df[agg_cols] = (
        df[agg_cols].values.transpose() * df['age_group_weight_value'].values
    ).transpose()
    df = df.groupby(id_cols, as_index=False)[agg_cols].sum()

    return df


def summarize(df, draw_cols, drop_draw_cols=True):
    '''
    Get mean and 95% uncertainty interval from draws.
    '''
    df = df.copy()
    df['mean'] = df[draw_cols].mean(axis=1)
    df['lower'] = df[draw_cols].quantile(0.025, axis=1)
    df['upper'] = df[draw_cols].quantile(0.975, axis=1)
    if drop_draw_cols:
        df = df.drop(draw_cols, axis=1)

    return df


def connect_list(x, connector='_'):
    '''
    Stitch together list elements.
    '''
    y = [str(xi) for xi in x]
    z = connector.join(y)

    return z

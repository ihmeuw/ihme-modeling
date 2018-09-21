import pandas as pd

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
  '''
  if operator == '/':
      assert len(df_list) == 2, 'Only two dataframes allowed for division.'
      print(
        'Dividing df_list[0] by df_list[1] '\
        '(make sure list is ordered correctly).'
    )
  assert len(set([len(df) for df in df_list])) == 1, \
      'Not all data frames are same length...'
  assert len(set([len(list(df)) for df in df_list])) == 1, \
      'Not all data frames are same width...'
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
          raise ValueError, 'Column issue (should not happen)'
  final_df.reset_index(inplace=True)

  return final_df


def pop_weight_agg(df, agg_variable, agg_include, agg_reassign,
                   index_cols, draw_cols, version_number,
                   return_all=False):
    '''
    Using stored population file, aggregate rate.
    '''
    pop_df = pd.read_csv(
        '/PATH/{}/pop_df.csv'.format(
            version_number
        )
    )
    agg_df = df.merge(pop_df[index_cols + ['population']])
    agg_df = pd.concat(
        [
            agg_df[index_cols],
            agg_df[draw_cols].apply(lambda x: x * agg_df['population']),
            agg_df['population']
        ],
        axis=1
    )
    agg_df = agg_df.loc[agg_df[agg_variable].isin(agg_include)]
    agg_df[agg_variable] = agg_reassign
    agg_df = agg_df.groupby(
        index_cols,
        as_index=False
    )[draw_cols + ['population']].sum()
    agg_df = pd.concat(
        [
            agg_df[index_cols],
            agg_df[draw_cols].apply(lambda x: x / agg_df['population'])
        ],
        axis=1
    )

    if return_all:
        agg_df = df.append(agg_df[list(df)])
    else:
        agg_df = agg_df[list(df)]

    agg_df = agg_df[index_cols + draw_cols].sort_values(index_cols)
    agg_df = agg_df.reset_index(drop=True)

    return agg_df
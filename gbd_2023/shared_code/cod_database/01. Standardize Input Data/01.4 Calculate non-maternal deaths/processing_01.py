from pathlib import Path

import pandas as pd
import numpy as np
from functools import partial, reduce
from correct_study_files import *

from cod_prep.downloaders import get_cause_map, get_cod_ages, add_envelope, add_births
from cod_prep.claude.formatting import finalize_formatting
from cod_prep.utils.formatting import ages
from cod_prep.utils import print_log_message, report_if_merge_fail
from cod_prep.claude.configurator import Configurator

CONF = Configurator()


def read_clean_data(filepath, iso3, index_end, surveyyear, surveyyear2):
  df = pd.read_stata(filepath, convert_categoricals=False)

  df.columns = df.columns.str.lower()

  if 'mm15_01' not in df.columns: 
    for x in range(1, 10):
      df['mm15_0' + str(x)] = ''
    for x in range(10, index_end):
      df['mm15_' + str(x)] = ''

  df = all_corrections_check(df, iso3, surveyyear, index_end)
  needs_province_name_map(iso3, surveyyear, filepath)

  df['mm1_00'] = 2
  df['mm2_00'] = 1
  df['mm3_00'] = df['v012']
  df['mm4_00'] = df['v011']
  df['mm6_00'] = ''
  df['mm7_00'] = ''
  df['mm8_00'] = ''
  df['mm9_00'] = ''
  df['mm15_00'] = ''

  col_list = []
  for index in [1, 2, 3, 4, 6, 7, 8, 9, 15]:
    col_1 = ['mm' + str(index) + '_0' + str(x) for x in range(0, 10)]
    col_2 = ['mm' + str(index) + '_' + str(x) for x in range(10, index_end)]
    cols = col_1 + col_2
    col_list.append(cols)

  flat_list = [x for xs in col_list for x in xs]
  id_vars = ['caseid', 'v001', 'v005', 'v006', 'v007', 'v009', 'v010', 'v011']
  if 'v024' in df.columns:
    id_vars += ['v024']
  df = df[id_vars + flat_list]

  categories = {
    'sex': col_list[0] , 
    'if_alive': col_list[1], 
    'age': col_list[2], 
    'birth_date_cmc': col_list[3], 
    'years_ago_died': col_list[4], 
    'age_death': col_list[5], 
    'death_date_cmc': col_list[6],
    'sib_death_pregnancy': col_list[7],
    'death_year': col_list[8]}


  var_name_cols = []
  sib_dfs = []
  for key, value in categories.items():
      var_name = 'sibling_index_' + key
      sib_reshape = df.melt(id_vars=id_vars,
                   value_vars=value,
                   value_name=key,
                   var_name=var_name)

      sib_reshape['sib_id'] = sib_reshape[var_name].astype(str).str[-2:]

      sib_reshape = sib_reshape[id_vars + ['sib_id'] + [key]]

      sib_dfs.append(sib_reshape)

  merge = partial(pd.merge, on=id_vars + ['sib_id'], how='inner')
  reshape_df = reduce(merge, sib_dfs)

  reshape_df = reshape_df.loc[~reshape_df[['sex', 'if_alive', 'age', 'birth_date_cmc']].isna().all(1)]

  reshape_df = reshape_df.loc[reshape_df['sex'] == 2]

  reshape_df['caseid'] = reshape_df['caseid'].str.strip()

  reshape_df['surveyyear'] = surveyyear
  if surveyyear2 is not None:
    reshape_df['surveyyear2'] = surveyyear2

  reshape_df['iso3'] = iso3
  
  assert not reshape_df['sib_death_pregnancy'].isnull().values.all()

  return reshape_df


def calc_birth_death(df):

  df.loc[df['death_date_cmc'] == '', 'death_date_cmc'] = np.nan
  df.loc[df['years_ago_died'] == '', 'years_ago_died'] = np.nan
  df.loc[df['death_year'] == '', 'death_year'] = np.nan

  df['yod'] = (df['death_date_cmc'].astype(float) - 1) / 12
  df.loc[df['yod'].isna(), 'yod'] = df['v007'] - df['years_ago_died'].astype(float)
  df.loc[df['yod'].isna(), 'yod'] = df['death_year'].astype(float)

  if ((df['yod'] < 1900) | df['yod'].isna()).values.all():
    df['yod'] = df['yod'] + 1900

  df.loc[df['birth_date_cmc'] == '', 'birth_date_cmc'] = np.nan
  df.loc[df['age_death'] == '', 'age_death'] = np.nan

  df['yob'] = (df['birth_date_cmc'].astype(float) - 1) / 12
  df.loc[(df['yob'].isna()) & (df['if_alive'] == 1), 
    'yob'] = df['v007'] - df['age']
  df.loc[(df['yob'].isna()) & (df['if_alive'] == 0), 
    'yob'] = (df['v007'] - (df['age_death'] + df['years_ago_died']))

  if ((df['yob'] < 1900) | df['yob'].isna()).values.all():
    df['yob'] = df['yob'] + 1900

  if (df['v007'] < 1900).values.all():
    df['v007'] = df['v007'] + 1900

  if (df['iso3'] == 'ETH').all():
      df.loc[df['v006'] < 5, 'yob'] = df['yob'] + 7
      df.loc[df['v006'] < 5, 'yod'] = df['yod'] + 7
      df.loc[df['v006'] >= 5, 'yob'] = df['yob'] + 8
      df.loc[df['v006'] >= 5, 'yod'] = df['yod'] + 8

  df['deaths'] = 0 
  df.loc[df['yod'].notnull(), 'deaths'] = 1

  df = df.loc[~((df['yod'].isna()) & (df['if_alive'] == 0)), ]

  df = df.loc[df['if_alive'] <= 1, ]

  df = df.loc[df['yob'].notnull(), ]
  
  return df


def calc_age(df):
  age_formatter = ages.PointAgeFormatter()
  df.loc[(df['if_alive'] == 0), 'age'] = (df['yod'] - df['yob'])
  df.loc[(df['age'].isna()) & (df['if_alive'] == 1), 'age'] = df['v007'] - df['yob']
  df['age_unit']= 'year'
  df = age_formatter.run(df)
  
  df['age_group_id'] = df['age_group_id'].astype(float)
  df.loc[(df['age'].isna()) & (df['sib_death_pregnancy'] != 1), 'age_group_id'] = 24

  return df


def process_dhs_01(filepath, iso3, index_end, surveyyear, surveyyear2=None):
  df = read_clean_data(filepath, iso3, index_end, surveyyear, surveyyear2)
  df = calc_birth_death(df)
  df = calc_age(df)

  return df

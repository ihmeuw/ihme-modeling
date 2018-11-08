from __future__ import division

import pandas as pd

def long_to_dict(draws, summ_cols, index_cols):
    ###############################################
    #Converts a set of draws from long to wide,
    #given a dataframe with draw columns, a specifc
    #set of summary columns (data columns), and
    #index columns. Dataframe with have draws of
    #the format {summ_col_name}_0, etc. Returns a
    #dictionary with each key as a summary column
    #name and each value as a dataframe with data
    #for that summary column name
    ###############################################
    draws = draws[index_cols + summ_cols + ['draw']]
    draws_wide = draws.pivot_table(index=index_cols, columns='draw',
                                   values=summ_cols)
    draw_dict = {}
    for col in summ_cols: 
        draws_sub_col = draws_wide[col]
        draws_sub_col = draws_sub_col.reset_index()
        draws_sub_col.rename(columns=(lambda x: '{}_{}'.format(col, x) if 
                                      str(x).isdigit() else x), inplace=True)
        draw_dict[col] = draws_sub_col
    return draw_dict

def HALE_to_dict(draws, summ_cols, index_cols):
    ###############################################
    #Converts a set of draws from wide to a dict,
    #given a dataframe with draw columns, a
    #specific set of summary columns (data
    #columns), and index columns. Returns a
    #dictionary with each key as a summary column
    #name and each value asa dataframe with data
    #for that summary column name
    ###############################################
    draws_return = draws.set_index(index_cols)
    draw_dict = {}
    sub_draws = [c for c in draws_return.columns if 'draw_' in c]
    draws_sub_col = draws_return[sub_draws]
    draws_sub_col = draws_sub_col.reset_index()
    draw_dict['HALE'] = draws_sub_col
    return draw_dict

def get_summ(df, col, pct_change):
    ###############################################
    #Given a dataframe and a data column name,
    #returns a summary of the data in the
    #dataframe. Includes mean, lower, and upper
    #values, as well as mean, lower, and upper
    #percent change for the standard year pairs.
    #Results are in the format
    #{data_column_name}_mean, etc.
    ###############################################
    df_mean = df.set_index(['location_id', 'age_group_id', 'sex_id',
                            'year_id'])
    df_mean['{}_mean'.format(col)] = df_mean.mean(axis=1)
    df_mean['{}_lower'.format(col)] = df_mean.quantile(.025, axis=1)
    df_mean['{}_upper'.format(col)] = df_mean.quantile(.975, axis=1)
    df_mean = df_mean[['{}_mean'.format(col),
                       '{}_lower'.format(col),
                       '{}_upper'.format(col)]]
    df_mean.reset_index(inplace=True)

    if pct_change:
        year_pairs = {9999:[1990, 2017], 10000:[1990, 2007], 10001:[2007, 2017]}
        all_change = []
        for change_year, pair in year_pairs.items():
            start_draws = df.loc[df['year_id'] == pair[0]]
            start_draws.drop('year_id', axis=1, inplace=True)
            start_draws.set_index(['location_id', 'age_group_id', 'sex_id'],
                                  inplace=True)
            end_draws = df.loc[df['year_id'] == pair[1]]
            end_draws.drop('year_id', axis=1, inplace=True)
            end_draws.set_index(['location_id', 'age_group_id', 'sex_id'],
                                inplace=True)
            pct_draws = (end_draws.subtract(start_draws,
                                            axis=1)).divide(start_draws,
                                                            axis=1)
            for_calc = pct_draws.copy(deep=True)
            
            pct_draws['{}_mean'.format(col)] = for_calc.mean(axis=1)
            pct_draws['{}_lower'.format(col)] = for_calc.quantile(.025, axis=1)
            pct_draws['{}_upper'.format(col)] = for_calc.quantile(.975, axis=1)
            pct_draws = pct_draws[['{}_mean'.format(col),
                                  '{}_lower'.format(col),
                                  '{}_upper'.format(col)]]
            pct_draws['year_id'] = change_year
            all_change += [pct_draws]
            
        pct_change_means = pd.concat(all_change)
        pct_change_means.reset_index(inplace=True)

        all_draws = df_mean.append(pct_change_means)
        all_draws.set_index(['location_id', 'age_group_id', 'sex_id',
                             'year_id'], inplace=True)
        return all_draws

    else:
        df_mean.set_index(['location_id', 'age_group_id', 'sex_id', 'year_id'],
                          inplace=True)
        return df_mean

def calc_summary(draws, summ_cols, out_dir, location, wide=True,
                 pct_change=True):
    ###############################################
    #Given a dataframe with long draws, a list of
    #summary columns, an out directory a
    #location_id, and an annual results bool
    #summarizes the data in the dataframe's
    #summary columns (mean/upper/lower, as well as
    #pct change for standard year pairs). Outputs
    #result in the out directory, in the format
    #{location_id}_summary.csv
    ###############################################
    index_cols = ['sex_id', 'age_group_id', 'year_id', 'location_id']
    if wide:
        draw_dict = HALE_to_dict(draws, summ_cols, index_cols)
    else:
        draw_dict = long_to_dict(draws, summ_cols, index_cols)
    dfs = []
    for col in summ_cols:
        summ_draws = get_summ(draw_dict[col], col, pct_change)
        dfs.append(summ_draws)
    combo = pd.concat(dfs, axis=1)
    combo.reset_index(inplace=True)
    combo_csv = combo.set_index('location_id')
    combo_csv['year_id'] = combo_csv['year_id'].apply(int)
    combo_csv.to_csv('{out_dir}/{location}_summary.csv'.format(out_dir=out_dir,
                     location=location))

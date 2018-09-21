from __future__ import division

import pandas as pd

def long_to_wide(draws, summ_cols, index_cols):
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

def get_pct_change(df, col):
    year_pairs = {9999:[1990, 2016], 10000:[1990, 2005], 10001:[2005, 2016]}
    all_change = []
    for change_year, pair in year_pairs.iteritems():
        start_draws = df.loc[df['year_id'] == pair[0]]
        start_draws.drop('year_id', axis=1, inplace=True)
        start_draws.set_index(['location_id', 'age_group_id', 'sex_id'],
                              inplace=True)
        end_draws = df.loc[df['year_id'] == pair[1]]
        end_draws.drop('year_id', axis=1, inplace=True)
        end_draws.set_index(['location_id', 'age_group_id', 'sex_id'],
                            inplace=True)
        pct_draws = (end_draws.subtract(start_draws,
                                        axis=1)).divide(start_draws, axis=1)
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
    
    df_mean = df.set_index(['location_id', 'age_group_id', 'sex_id',
                            'year_id'])
    df_mean['{}_mean'.format(col)] = df_mean.mean(axis=1)
    df_mean['{}_lower'.format(col)] = df_mean.quantile(.025, axis=1)
    df_mean['{}_upper'.format(col)] = df_mean.quantile(.975, axis=1)
    df_mean = df_mean[['{}_mean'.format(col),
                       '{}_lower'.format(col),
                       '{}_upper'.format(col)]]
    df_mean.reset_index(inplace=True)
    all_draws = df_mean.append(pct_change_means)
    all_draws.set_index(['location_id', 'age_group_id', 'sex_id', 'year_id'],
                        inplace=True)
    return all_draws

def calc_summary(draws, summ_cols, out_dir, location):
    index_cols = ['sex_id', 'age_group_id', 'year_id', 'location_id']
    draw_dict = long_to_wide(draws, summ_cols, index_cols)
    dfs = []
    for col in summ_cols:
        pct_change_draws = get_pct_change(draw_dict[col], col)
        dfs.append(pct_change_draws)
    combo = pd.concat(dfs, axis=1)
    combo.reset_index(inplace=True)
    combo_csv = combo.set_index('location_id')
    combo_csv.to_csv('{out_dir}/{location}_summary.csv'.format(out_dir=out_dir,
                     location=location))

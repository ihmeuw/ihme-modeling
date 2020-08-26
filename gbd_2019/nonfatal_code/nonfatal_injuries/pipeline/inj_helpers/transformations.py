import os
import pandas as pd
from gbd_inj.inj_helpers import help, inj_info


def split_age(df, year_id, sex_id, root_dir, version):
    df_good_ages = df.loc[-df['age_group_id'].isin([2, 3, 4])]
    index_cols = [col for col in df if not col.startswith('draw')]
    index_cols.remove('age_group_id')
    index_cols.append('age_group_id') 
    df_good_ages.set_index(index_cols, inplace=True)
    
    df_list = [df_good_ages]
    locations = df['location_id'].unique()
    for location in locations:
        df_babies = df.loc[(df['age_group_id'] == 2) & (df['location_id'] == location)]
        df_babies3 = df_babies.copy()
        df_babies4 = df_babies.copy()
        df_babies3['age_group_id'] = 3
        df_babies4['age_group_id'] = 4
        df_babies = pd.concat([df_babies, df_babies3, df_babies4])
        df_babies.set_index(index_cols, inplace=True)
        
        pop = pd.read_hdf(os.path.join(root_dir, "flats", "{}/pops_{}_{}.h5".format(version, year_id, sex_id)),
                          where="location_id == {} and age_group_id in [2,3,4]".format(location)).reset_index()
        pop.drop(['location_id', 'year_id', 'sex_id'], axis=1, inplace=True)
        pop = pop.set_index('age_group_id').squeeze()
        pop_total = pop.sum()
        pop = pop.divide(pop_total)
        
        df_babies = df_babies.multiply(pop, axis=0, level=len(index_cols) - 1)
        
        df_list.append(df_babies)
    
    df = pd.concat(df_list)
    df.reset_index(inplace=True)
    df.sort_values(['location_id', 'age_group_id'], inplace=True)
    
    return df


def create_lt_grid(platform, ages):
    ncodes = [x for x in inj_info.get_lt_ncodes(platform) if x not in inj_info.ST_NCODES]
    grid = pd.DataFrame(help.expandgrid(ncodes, ages))
    grid.columns = ["ncode", "age_gr"]
    grid["platform"] = platform

    for draw in help.drawcols():
        grid[draw] = 1
    return grid

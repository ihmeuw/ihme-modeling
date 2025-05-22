'''
Script generates long term disability weights by location, year, and ncodes 
in the draws space for central comp use

'''
import numpy as np
import os
import pandas as pd
import xarray as xr

import db_queries as db

from FILEPATH import config
from FILEPATH.helpers import calculate_measures, paths, converters, utilities


import warnings
warnings.filterwarnings(action='ignore', message='.*DataFrame is highly fragmented.*')

def rename_columns(df):
    
    df.rename(columns = lambda x: x.replace('draw', 'draw_') if 'draw' in x else x, inplace = True)
    if 'n_code' in df.columns:
        df.rename(columns = {'n_code' : 'ncode'}, inplace = True)
        
        df.set_index('ncode', inplace=True)
        
    return df
    
    
# Load the treated and untreated disability weights
dw_folder = paths.INPUT_DIR/'raw_inputs'
untreat_dw = pd.read_csv(dw_folder/'FILEPATH.csv')
treated_dw = pd.read_csv(dw_folder/'FILEPATH.csv')

# returns list of location_id', 'sex_id', 'age_group_id', 'year_id' per gbd round
dems = db.get_demographics(gbd_team='epi', release_id=config.RELEASE_ID)

untreat_dw = rename_columns(untreat_dw)
treated_dw = rename_columns(treated_dw)

print('Converting dw df')
u_dw = converters.df_to_xr(untreat_dw, wide_dim_name='draw', fill_value=np.nan)
t_dw = converters.df_to_xr(treated_dw, wide_dim_name='draw', fill_value=np.nan)


# Get the percent treated in each country-year, and multiply by dws to get total dw
p_t = calculate_measures.pct_treated()
dw = t_dw * p_t + u_dw * (1 - p_t)


# Load in split proportions for spinal cord injuries and find weighted average disability weight among the 4 splits
n_parent = pd.Series(index=treated_dw.index, data=[n[0:3] for n in treated_dw.index], name='ncode_parent')

drawdict = {'prop_' + d: d for d in utilities.drawcols()}
split_props_list = []
for s in ['a', 'b', 'c', 'd']:
    print(s)
    # load proportion draws
    split_prop = pd.read_csv(FILEPATH)
    split_prop.rename(columns=drawdict, inplace=True)
    split_prop.drop('acause', axis=1, inplace=True)
    for n in ['N33', 'N34']:
        split_props_list.append(split_prop.rename({0: n+s}))

split_props = pd.concat(split_props_list)
split_props.index.rename('ncode', inplace=True)
other_ncodes = pd.DataFrame(index=[n for n in treated_dw.index if len(n) < 4], columns=utilities.drawcols(), data=1)
other_ncodes.loc[['N33', 'N34']] = 0
other_ncodes.index.rename('ncode', inplace=True)
weight = converters.df_to_xr(split_props.append(other_ncodes), wide_dim_name='draw', fill_value=np.nan)

dw['ncode_parent'] = n_parent

print('Grouping data')
final = (dw*weight).groupby('ncode_parent').sum('ncode').rename({'ncode_parent': 'ncode'})


# Save in draws space by location, year, and ncode
print('Saving')

final.to_dataset(dim='draw').to_dataframe().to_hdf(
    FILEPATH,
    key='draws',
    mode='w',
    format='table'
)

print(f'Script completed and saved to {FILEPATH}')
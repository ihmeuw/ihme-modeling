import pandas as pd
import os
import db_queries as db
from gbd_inj.inj_helpers import help, calculate_measures, paths
from fbd_core import etl
import xarray as xr
import numpy as np
import os

dw_folder = os.path.join(paths.INPUT_DIR, 'raw_inputs')
untreat_dw = pd.read_csv(os.path.join(dw_folder, 'lt_u_dws_by_ncode.csv'))
treated_dw = pd.read_csv(os.path.join(dw_folder, 'lt_t_dws_by_ncode.csv'))

coldict = {'draw'+str(n): 'draw_'+str(n) for n in range(1000)}
coldict['n_code'] = 'ncode'
untreat_dw.rename(columns=coldict, inplace=True)
treated_dw.rename(columns=coldict, inplace=True)
untreat_dw.set_index('ncode', inplace=True)
treated_dw.set_index('ncode', inplace=True)

u_dw = etl.df_to_xr(untreat_dw, wide_dim_name='draw', fill_value=np.nan)
t_dw = etl.df_to_xr(treated_dw, wide_dim_name='draw', fill_value=np.nan)


dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)


decomp = 'step1'
p_t = calculate_measures.pct_treated(decomp)
dw = t_dw * p_t + u_dw * (1 - p_t)


n_parent = pd.Series(index=treated_dw.index, data=[n[0:3] for n in treated_dw.index], name='ncode_parent')
spinal_split_folder = "FILEPATH"
drawdict = {'prop_' + d: d for d in help.drawcols()}
split_props_list = []
for s in ['a', 'b', 'c', 'd']:
    split_prop = pd.read_csv(os.path.join(spinal_split_folder, 'prop_' + s + '.csv'))
    split_prop.rename(columns=drawdict, inplace=True)
    split_prop.drop('acause', axis=1, inplace=True)
    for n in ['N33', 'N34']:
        split_props_list.append(split_prop.rename({0: n+s}))

split_props = pd.concat(split_props_list)
split_props.index.rename('ncode', inplace=True)
other_ncodes = pd.DataFrame(index=[n for n in treated_dw.index if len(n) < 4], columns=help.drawcols(), data=1)
other_ncodes.loc[['N33', 'N34']] = 0
other_ncodes.index.rename('ncode', inplace=True)
weight = etl.df_to_xr(split_props.append(other_ncodes), wide_dim_name='draw', fill_value=np.nan)

dw['ncode_parent'] = n_parent

final = (dw*weight).groupby('ncode_parent').sum('ncode').rename({'ncode_parent': 'ncode'})


final.to_dataset(dim='draw').to_dataframe().to_hdf(
    "FILEPATH", 'draws', mode='w', format='table')

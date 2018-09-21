## Arguments
import sys
run_root	  = sys.argv[1]
central_root  = sys.argv[2]
holdout_num	  = int(sys.argv[3])
draws 		  = int(sys.argv[4])
parallel      = int(sys.argv[5])
if parallel == 1:
    loc_start = int(sys.argv[6])
    loc_end = int(sys.argv[7])

## Load
import os
os.chdir('%s' %(central_root))
import st_gpr.gpr as gpr
import pandas as pd
import numpy as np
np.seterr(invalid='ignore')
reload(gpr)

################################
## Load and clean data
################################

print('holdout number: %s' %(holdout_num))
os.chdir('%s' %(run_root))

# Load parameters
params = pd.read_hdf('param_%s.h5' %(holdout_num), 'parameters')

# Load st_amp
st_amp = pd.read_hdf('param_%s.h5' %(holdout_num), 'st_amp')

# Load data
data = pd.read_hdf('temp_%s.h5' %(holdout_num), 'adj_data')

# Load st
st = pd.read_hdf('temp_%s.h5' %(holdout_num), 'st')

# Merge data onto st
df = pd.merge(st, data, how='left', on=['location_id', 'year_id', 'age_group_id', 'sex_id'])

# Merge st_amp onto st
df = pd.merge(df, st_amp, how='left', on=['location_id', 'year_id', 'age_group_id', 'sex_id'])

# Sort
df = df.sort(columns=['location_id', 'year_id', 'age_group_id', 'sex_id'])

#################################
### Setup
#################################

## Set Scale
scale = int(params['gpr_scale'])

## If parallel, subset
if parallel == 1:
    df = df.loc[(df.location_id >= loc_start) & (df.location_id <= loc_end)]

#################################
### Run GPR
#################################

groups = df.groupby(by=['location_id', 'sex_id', 'age_group_id'])
df = groups.apply(lambda x: gpr.fit_gpr(x, obs_variable='data', obs_var_variable='variance', 
                                                mean_variable='st', amp= x['st_amp'].values[0] * 1.4826, scale=scale, draws= draws))

#################################
### Save
#################################

## Clean
if draws == 0:
    df = df[['location_id', 'year_id', 'age_group_id', 'sex_id', 'gpr_mean', 'gpr_lower', 'gpr_upper']]
else:
    draw_cols = [col for col in df.columns if 'draw_' in col]
    cols = ['location_id', 'year_id', 'age_group_id', 'sex_id'] + draw_cols
    df = df[cols]
df = df.drop_duplicates()


## Save
if parallel == 1:
    if not os.path.exists('gpr_temp_%s/' %(holdout_num)):
        os.makedirs('gpr_temp_%s/' %(holdout_num), 0777)
    df.to_csv('gpr_temp_%s/%s_%s.csv' %(holdout_num, loc_start, loc_end), index=False)
else:
    df.to_hdf('temp_%s.h5' %(holdout_num), 'gpr', mode='a', format='fixed')
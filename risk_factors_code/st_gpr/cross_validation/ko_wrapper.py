import sys
sys.dont_write_bytecode = True

run_root = sys.argv[1]
central_root = sys.argv[2]
holdouts = int(sys.argv[3])
holdout_num = int(sys.argv[4])

import pandas as pd
import os
os.chdir(central_root)
import codem_ko as ko

## Settings
seed = 12345

## Bring in data
print('holdout number: %s' %(holdout_num))
os.chdir(run_root)
## Use entire prepped dataset (preko) to create holdouts
data = pd.read_hdf('temp_%s.h5' %(holdout_num), 'preko')
## Reset Index
data = data.reset_index(drop=True)
## Syntax: df, # of holdouts, seed
knockouts = ko.generate_knockouts(data, holdouts=holdouts, seed=seed)
## Subset and convert from boolean to int
frame = pd.DataFrame()
for i in range(0,holdouts+1):
    holdout = knockouts[i]
    holdout = holdout*1 
    holdout['train'] = holdout['train'] + holdout['test1']
    holdout = holdout.drop(['test1', 'test2'], axis =1)
    colname = 'train%s' %(holdouts-i)
    holdout.columns = [colname]
    frame = pd.concat([frame, holdout], axis=1)
    
## Bring back to data_id
output = pd.concat([data['data_id'], frame], axis=1).convert_objects()

## Output
output.to_hdf('param_%s.h5' %(holdout_num), 'kos', mode = 'a', format='f')



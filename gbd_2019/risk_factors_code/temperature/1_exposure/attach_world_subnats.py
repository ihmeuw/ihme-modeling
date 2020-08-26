import pandas as pd
import shutil
import os
import sys

year = sys.argv[1]

df1 = pd.read_csv('FILEPATH')
tmp = pd.read_csv(f'FILEPATH')
mg = pd.merge(tmp,df1,how='left')
mg.subnational_location_id = mg.subnational_location_id.fillna(mg.location_id)
mg = mg[['date', 'dewp',  'temp', 'heat_index', 'population', 'latitude', 'longitude',
       'pixel_id', 'location_id', 'subnational_location_id']]
mg = mg.rename(columns={'location_id':'admin0_loc_id','subnational_location_id':'location_id'})
mg.location_id = mg.location_id.astype(int)

os.makedirs('FILEPATH',exist_ok=True)
mg.to_csv(f'FILEPATH',index=False)
shutil.move(f'FILEPATH',f'FILEPATH')
import pandas as pd
from glob import glob


cf_dir = 'FILEPATH'

cf_files = glob('%s/*.csv' % cf_dir)
dfs = []
for f in cf_files:
    df = pd.read_csv(f)
    dfs.append(df)
    print(f)
    print(len(df.location_id.unique()))
dfs = pd.concat(dfs, axis=0)
dfs = dfs.drop(['Unnamed: 0'], axis=1)
dfs['risk'] = 'nutrition_iron'
dfs['parameter'] = 'mean'
dfs = dfs.sort_values(['location_id', 'year_id', 'age_group_id', 'sex_id'], ascending=[True, True, True, True])
dfs.to_hdf('FILEPATH',
           key='draws',
           mode='w',
           format='table',
           data_columns=['location_id', 'year_id', 'age_group_id', 'sex_id'])

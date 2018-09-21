import pandas as pd
from glob import glob

cf_dir = '{FILEPATH}'

cf_files = glob('%s/*.csv' % cf_dir)
dfs = []
for f in cf_files:
    df = pd.read_csv(f)
    dfs.append(df)
    print f
dfs = pd.concat(dfs, axis=0)
dfs = dfs.drop(['Unnamed: 0', 'mean_hgb'], axis=1)
dfs['risk'] = 'nutrition_iron'
dfs['parameter'] = 'mean'
dfs.sort_values(['location_id', 'year_id', 'age_group_id', 'sex_id'], ascending=[True, True, True, True])
renames = {'normal_hb_%s' % d: 'tmrel_%s' % d for d in range(1000)}
dfs.rename(columns=renames, inplace=True)
dfs.to_hdf('/{FILEPATH}',
           key='draws',
           mode='w',
           format='table',
           data_columns=['location_id', 'year_id', 'age_group_id', 'sex_id'])
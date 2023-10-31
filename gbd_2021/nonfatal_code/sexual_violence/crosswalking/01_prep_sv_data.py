import argparse
import os
import pandas as pd
import numpy as np

df = pd.read_csv('FILEPATH')
print(df.head())

df['vals'] = df[['mean', 'standard_error']].values.tolist()

cv_cols = [col for col in df if col.startswith('cv_')]
df['sum_xcovs'] = df[cv_cols].sum(axis=1)

df['concat'] = ''

for col in cv_cols:
    df.loc[df[col] == 1, 'concat'] += col + ', '

df = df.sort_values(['sum_xcovs'])

cvs = df.concat.unique()
value = ['value' + str(x) for x in range(0, len(cvs) + 1)]
value_dict = dict(zip(cvs, value))

df['value_cols'] = df['concat'].map(value_dict)

df = df.drop_duplicates(subset = ['ihme_loc_id', 'sex_id', 'year_start', 'year_end', 'age_start', 'age_end', 'concat'])

df2 = df[['ihme_loc_id', 'sex_id', 'year_start', 'year_end', 'age_start', 'age_end', 'vals', 'value_cols']]
df2.set_index(['ihme_loc_id', 'sex_id', 'year_start', 'year_end', 'age_start', 'age_end'], inplace=True)

df2 = pd.pivot_table(df2,
                     columns = 'value_cols',
                     values='vals',
                     index=[df2.index.values],
                     aggfunc='first')
df3 = df2.copy()

value = [col for col in df3 if col.startswith('value')]

ratio_cols = []
ran1 = range(1, len(value))
ran1.reverse()

for i in ran1:
    ran2 = range(0, i)
    ran2.reverse()
    for j in ran2:
        col1 = 'value' + str(i)
        col2 = 'value' + str(j)
        newcol = col1 + ':' + col2
        ratio_cols = ratio_cols + [newcol]
        mean = df3[col1].str[0] / df3[col2].str[0]
        se = np.sqrt((df3[col1].str[0]**2 / df3[col2].str[0]**2) * ((df3[col2].str[1]**2/df3[col2].str[0]**2) + (df3[col1].str[1]**2/df3[col1].str[0]**2)))
        d = {'mean': mean, 'se': se}
        ratio_df = pd.DataFrame(data=d)
        df3[newcol] = ratio_df[['mean', 'se']].values.tolist()

df4 = df3[ratio_cols]

df5 = pd.melt(df4)
df5['numerator'] = df5['value_cols'].str[:6]
df5['denominator'] = df5['value_cols'].str[7:]

value_dict2 = dict((y,x) for x,y in value_dict.items())
df5['num'] = df5['numerator'].map(value_dict2).str.replace(',','')
df5['den'] = df5['denominator'].map(value_dict2).str.replace(',','')
df5['ratio'] = df5['value'].str[0]
df5['ratio_se'] = df5['value'].str[1]

df5.rename(columns={'mean':'ratio', 'standard_error':'ratio_se'}, inplace=True)
df5 = df5[['num', 'den', 'ratio', 'ratio_se']]
for c in cv_cols:
    df5[c] = 0

df5.to_csv('FILEPATH', index=False)
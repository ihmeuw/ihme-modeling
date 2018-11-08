import sys
import re

import matplotlib
matplotlib.use("agg")

import numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sns
sns.set_context('poster')
sns.set_style('darkgrid')

assert len(sys.argv) == 3, 'ADDRESS'
cause = sys.argv[1]
print 'cause', cause


pattern = re.compile('([0-9])|(^all$)')
is_acause = re.match(pattern, cause) == None

user = sys.argv[2]

sys.path = "FILEPATH"

import iss
reload(iss.models)

# load data
df = pd.DataFrame()

for sex in [1,2,9]:
    if is_acause:
        df = df.append(iss.data.load('SCD', acause=cause, sex=sex),
                       ignore_index=True)
    else:
        df = df.append(iss.data.load('SCD', cause=cause, sex=sex),
                       ignore_index=True)

df.age = df.age.map({
                     '0-365':'0-365',
                     '1-4':'1-4',
                     '5-14':'5-14',
                     '15-24':'15-24',
                     '25-34':'25-34',
                     '35-44':'35-44',
                     '45-54':'45+', 
                     '45-59':'45+',
                     '55+':'45+',
                     '60+':'45+',
                     '45+':'45+',
                     'all':'all'})
t = df.groupby(['age', 'cause', 'location_id', 'sex', 'source', 'state', 'year'])
df = t.sum().reset_index()

# setup model
m = iss.models.FourWay(iterlim=100)

if len(df) == 0:
    print 'WARNING: no data, skipping'
else:
    df_pred = m.fit_predict(df)
    fname = 'FILENAME'
    print 'saving as', fname

    rows = df_pred.query('age=="55+"').index
    df_pred.loc[rows, 'age'] = '45+'

    df_pred.to_csv(fname)

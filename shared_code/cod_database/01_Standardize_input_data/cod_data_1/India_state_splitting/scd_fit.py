""" Take a cause, load data, fit four-way first-order log-linear model"""

# set up environment
import sys
sys.path += ['.', '..', '/homes/USER/india_state_splittling'] # FIXME: install iss as a module so this is not necessary

import matplotlib
matplotlib.use("agg")

import numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sns
sns.set_context('poster')
sns.set_style('darkgrid')

import iss
reload(iss.models)

assert len(sys.argv) == 2, 'usage: scd_fit.py CAUSE'
cause = sys.argv[1]
print 'cause', cause

# load data
df = pd.DataFrame()
for sex in [1,2,9]:
    df = df.append(iss.data.load('SCD', cause, sex),
                   ignore_index=True)

# setup model
m = iss.models.FourWay(iterlim=100)

if len(df) == 0:
    print 'WARNING: no data, skipping'
else:
    df_pred = m.fit_predict(df)
    fname = '/clustertmp/strUser/iss/scd-deaths-for-cause_%s.csv'%(cause)
    print 'saving as', fname
    df_pred.to_csv(fname)

    for sex in [1,2]:
        t = df_pred[df_pred.sex == sex].copy()
        cause_name = iss.data.cause_name('SCD', cause)
        col = 'Deaths due to ' + cause_name + ' in ' + {1:'Men', 2:'Women'}[sex]
        t[col] = t.deaths

        iss.graphics.plot_micromap(t, var=col)
        plt.savefig('/home/j/WORK/03_cod/03_outputs/02_results/India MCCD GBD 2015/SCD-deaths-for-cause_%s-sex_%d.pdf'%(cause, sex))
        plt.close()

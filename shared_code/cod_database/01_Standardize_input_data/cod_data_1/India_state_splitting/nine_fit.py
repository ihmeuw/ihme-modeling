""" Take a cause, load data, fit three-way first-order log-linear model"""

# set up environment
import sys
sys.path += ['.', '..', '/homes/strUser/india_state_splittling'] # FIXME: install iss as a module so this is not necessary

import matplotlib
matplotlib.use("agg")

import numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sns
sns.set_context('poster')
sns.set_style('darkgrid')

import iss

assert len(sys.argv) == 2, 'usage: nine_fit.py CAUSE'
cause = sys.argv[1]
print 'cause', cause

# setup model
m = iss.models.FourWay(iterlim=100)

# fit for sex 1 and 2, save results, and make summary plots
for sex in [1,2]:
    df = iss.data.load("MCCD_ICD9", cause, sex)

    if len(df) == 0:
        print 'WARNING: no data, skipping'
        continue

    if set(df.age) == set(['all']):
        print 'WARNING: no age-specific data, skipping'

    df_pred = m.fit_predict(df)
    fname = '/clustertmp/strUser/iss/deaths-for-cause_%s-sex_%d.csv'%(cause, sex)
    print 'saving as', fname
    df_pred.to_csv(fname)
        
    col = 'Deaths due to ' + cause + ' in ' + {1:'Men', 2:'Women'}[sex]
    df_pred[col] = df_pred.deaths
    iss.graphics.plot_micromap(df_pred, var=col)
    plt.savefig('/home/j/WORK/03_cod/03_outputs/02_results/India MCCD GBD 2015/deaths-for-cause_%s-sex_%d.pdf'%(cause, sex))
    plt.close()

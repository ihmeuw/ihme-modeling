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

assert len(sys.argv) == 2, 'usage: ten_fit.py CAUSE'
cause = sys.argv[1]
print 'cause', cause

# setup model
m = iss.models.FourWay(iterlim=100)

# fit for sex 1 and 2 separately (making four-way model effectively
# three-way)
for sex in [1,2]:
    # load data
    df = iss.data.load("MCCD_ICD10", cause, sex)

    if len(df) == 0:
        print 'WARNING: no data, skipping'
        continue

    # MCCD ICD 10 data has very little information for years 2011 and
    # 2012, and including it leads to implausible predictions
    # out-of-sample. So we will remove it from the dataframe
    df_11_12 = df[df.year.isin([2011,2012])]
    df = df[~df.year.isin([2011,2012])]

    # fit model
    df_pred = m.fit_predict(df)
    fname = '/clustertmp/strUser/iss/icd10/deaths-for-cause_%s-sex_%d.csv'%(cause, sex)
    print 'saving as', fname

    # select age- state- specific data instead of prediction, when available:
    df_pred = df_pred.append(df[(df.age!='all')&(df.state!='all')])
    df_pred = df_pred.append(df_11_12[(df_11_12.age!='all')&(df_11_12.state!='all')])
    df_pred = df_pred.groupby(['source', 'cause', 'sex', 'year', 'age', 'state']).last()
    df_pred = df_pred.reset_index()
    df_pred = df_pred.drop(['pop'], axis=1)
    df_pred.to_csv(fname)
        
    df_pred = iss.data.merge_in_pop(df_pred)
    col = 'q_%s in %s' % (cause, {1:'Men', 2:'Women'}[sex])
    df_pred[col] = (df_pred.deaths / df_pred['pop'])

    iss.graphics.plot_micromap(df_pred, var=col)
    plt.savefig('/home/j/WORK/03_cod/03_outputs/02_results/India MCCD GBD 2015/icd10/q-for-cause_%s-sex_%d.pdf'%(cause, sex))
    plt.close()

    col = 'deaths(%s) in %s' % (cause, {1:'Men', 2:'Women'}[sex])
    df_pred[col] = df_pred.deaths
    iss.graphics.plot_micromap(df_pred, var=col)
    plt.savefig('/home/j/WORK/03_cod/03_outputs/02_results/India MCCD GBD 2015/icd10/deaths-for-cause_%s-sex_%d.pdf'%(cause, sex))
    plt.close()


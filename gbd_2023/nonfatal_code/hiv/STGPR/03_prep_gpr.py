import sys
import pandas as pd
from glob import glob
import getpass

#reload(st)
# Filepath settings
user = getpass.getuser()
user = getpass.getuser()

sys.path.append('FILEPATH')
from st_gpr import spacetime as st
from scipy import stats
import numpy as np

if sys.argv[1]=='-f':
    date = 'RUNNAME'
else:
    date = sys.argv[1]

lindir = "FILEPATH" + date
stdir = "FILEPATH" + date + "/st"

# Get data
data = pd.read_csv("%s/linear_predictions.csv" % lindir)
# Get data
data = pd.read_csv("%s/linear_predictions.csv" % lindir)
# Get results df
idx_cols = ['year_id', 'age_group_id', 'sex_id']
fs = glob("%s/*.csv" % stdir)
def readf(f):
    #print f
    return pd.read_csv(f).set_index(idx_cols)
results = [readf(f) for f in fs]
results = pd.concat(results, axis=1)
results = results.reset_index()
print(results)
# Calculate MADs.results = sresultss
forgpr = []
for sex in [1, 2]:
    print(sex)
    sdata = data[data.sex_id == sex]
    sresults = results[results.sex_id == sex]
    sresults = sresults.drop('sex_id', axis=1)
    s = st.Smoother(
            sdata, 561,
            datavar='ln_dr',
            modelvar='ln_dr_predicted',
            pred_age_group_ids=data.age_group_id.unique(),
            pred_start_year = data.year_id.min())
    s.results = sresults
    forgpr.append(s.calculate_mad())
forgpr = pd.concat(forgpr)
print(forgpr)
"""
Convert data variances to log space

    Use standard eror as the data variance.  Approximate transformed
    variance using the delta method:
        G(X) = G(mu) + (X-mu)G'(mu) (approximately)
        Var(G(X)) = Var(X)*[G'(mu)]^2 (approximately)

    Examples:
        For G(X) = Logit(X)
        ... Logit(p)' = 1/(p(1-p))
        ... so Var(Logit(X)) = Var(X) / [p(1-p)]^2

        For G(X) = ln(X)
        ... ln'(x) = 1/x
        ... so Var(ln(X)) = Var(X) / (x^2)
"""
forgpr = forgpr.replace({'ln_dr': 0}, 0.000001)
pct25ss = stats.scoreatpercentile(
        forgpr[forgpr['sample_size'].notnull()]['sample_size'], 2.5)
forgpr['sample_size'] = forgpr['sample_size'].fillna(pct25ss)
forgpr['ln_dr'] = np.exp(forgpr['ln_dr'])/100
varX = forgpr['ln_dr']*(1-(forgpr['ln_dr']))/forgpr['sample_size']
forgpr['data_var'] = varX/forgpr['ln_dr']**2
forgpr['ln_dr'] = np.log(forgpr['ln_dr']*100)
# Add MAD into the data variance term
snMAD = forgpr.loc[forgpr.level_4.notnull(), 'mad_level_4']
natMAD = forgpr.loc[forgpr.level_4.isnull(), 'mad_level_3']
forgpr.loc[forgpr.level_4.notnull(), 'data_var'] = (
    forgpr.loc[forgpr.level_4.notnull(), 'data_var']+snMAD**2)
forgpr.loc[forgpr.level_4.isnull(), 'data_var'] = (
    forgpr.loc[forgpr.level_4.isnull(), 'data_var']+natMAD**2)
medVar = stats.scoreatpercentile(
        forgpr[forgpr['data_var'].notnull()]['data_var'], 50)
forgpr['data_var'] = forgpr['data_var'].fillna(medVar)
forgpr.to_csv('%s/forgpr.csv' % lindir, index=False)

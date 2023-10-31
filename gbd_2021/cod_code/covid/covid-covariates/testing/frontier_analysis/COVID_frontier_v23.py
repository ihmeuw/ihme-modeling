# Project: Estimate country-specific COVID daily testing rate caps based on LDI per capita
# Purpose: Run Stochastic Frontier (SFA) function
# Author: INDIVIDUAL_NAME INDIVIDUAL_NAME
# Date created: 7/20/2020
# Temporary working directory: FILEPATH
# Package: https://github.com/UW-AMO/StochasticFrontier, contact INDIVIDUAL_NAME INDIVIDUAL_NAME and INDIVIDUAL_NAME INDIVIDUAL_NAME for questions
# Python environment: FILEPATH

# Setup
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
sys.path.append('~/StochasticFrontier/src/')
from pysfa import *

#####################################################
# First, run frontier only on US subnationals (V23)
#####################################################

# Read in data
df = pd.read_csv('FILEPATH/covid_frontier_data_07_24_withUSAsubnats_v23.csv')
df['log_total_pc_100'] = np.log(df['total_pc_100'])

# Prep inputs and run SFA
m = df.shape[0] # number of observations
s = np.sqrt(df['variance'].values) 
x = df['ldipc'].values # independent variable
z = np.ones((m,1))
d = np.ones((m,1))
y = df['log_total_pc_100'].values # dependent variable
ind = np.argsort(x)
x = x[ind]
y = y[ind]

sfa = SFA(x.reshape(m,1), z, d, s, Y=y, add_intercept_to_x=True)

# add splines
# evenly_spaced_knots = [np.min(x), 23016, 45349, 67682, 90015, np.max(x)]
custom_natl_knots = [500, 1000, 2000, 5000, 10000, 20000, 40000, np.max(x)]
custom_USA_knots = [np.min(x), 50000, 60000, 70000, np.max(x)]
knots = np.array(custom_USA_knots)
degree = 3
sfa.addBSpline(knots, degree, r_linear=False, bspline_mono='increasing', bspline_cvcv='concave')

beta_uprior = np.array([[0.0]*sfa.k_beta, [5000.0]*sfa.k_beta])
gama_uprior = np.array([[0.0]*sfa.k_gama, [0.0]*sfa.k_gama]) # this prior on gamma forces the random effects to zero
deta_uprior = np.array([[0.0]*sfa.k_deta, [np.inf]*sfa.k_deta])

sfa.addUPrior(beta_uprior, gama_uprior, deta_uprior)
sfa.optimizeSFA()

# Optimize and trim 5% of outliers
sfa.optimizeSFAWithTrimming(int(0.95*sfa.N), stepsize=100.0, verbose=True, max_iter=10)

# Plot final SFA with trimmed outliers in red
id_outliers = np.where(sfa.w == 0.0)[0]
f=plt.figure()
plt.plot(x, y, '.')
plt.axvline(x=38174, color = 'grey')
plt.axvline(x=50000, color = 'grey')
plt.axvline(x=60000, color = 'grey')
plt.axvline(x=70000, color = 'grey')
plt.axvline(x=85190, color = 'grey')
plt.plot(x, sfa.X.dot(sfa.beta_soln))
plt.xlabel('LDI per capita')
plt.ylabel('log Daily testing rate per capita')
plt.plot(x[id_outliers], y[id_outliers], 'r.')
plt.show()
f.savefig("FILEPATH/COVID_SFA_level_ylog.pdf", bbox_inches='tight')

id_outliers = np.where(sfa.w == 0.0)[0]
f=plt.figure()
plt.plot(x, y, '.')
plt.axvline(x=38174, color = 'grey')
plt.axvline(x=50000, color = 'grey')
plt.axvline(x=60000, color = 'grey')
plt.axvline(x=70000, color = 'grey')
plt.axvline(x=85190, color = 'grey')
plt.plot(x, sfa.X.dot(sfa.beta_soln))
plt.xlabel('LDI per capita')
plt.ylabel('log Daily testing rate per capita')
plt.xscale('log')
plt.plot(x[id_outliers], y[id_outliers], 'r.')
plt.show()
f.savefig("FILEPATH/COVID_SFA_log_ylog.pdf", bbox_inches='tight')

# grab model fit, and merge onto input data
sfa.estimateRE()
modeldata = pd.DataFrame({'ldipc':x,'log_total_pc_100':y,'sfa_fit':np.exp(sfa.X.dot(sfa.beta_soln))})
df2 = pd.merge(df, modeldata, on = ['log_total_pc_100', 'ldipc'])
df2 = df2.drop_duplicates(subset ="location_name") 
df2 = df2.drop(columns=['date', 'year_id', 'total_pc_100', 'ldipc', 'variance', 'log_total_pc_100'])
df2.to_csv('FILEPATH/COVID_SFA_testing_cap.csv', index = False)

# Create estimates for a few missing countries
df3 = pd.read_csv('~/USA_ldipc_missing_locs.csv')
X_new_ref = df3.eval('ldipc').as_matrix().reshape(1, 1)
v_new_ref = df3.eval('ineff').as_matrix()
y_new_ref = sfa.forcastData(X_new_ref, v_new_ref, add_intercept_to_x=True)
preds = pd.DataFrame({'sfa_fit':np.exp(y_new_ref)})
df4 = pd.merge(df3, preds, left_index = True, right_index = True)
df4 = df4.drop(columns=['ineff', 'ldipc'])
df4.to_csv('FILEPATH/COVID_SFA_testing_cap_missinglocs.csv', index = False)

#####################################################
# Second, run frontier on all-locations for comparison (V22)
#####################################################

# Read in data
ff = pd.read_csv('FILEPATH/covid_frontier_data_07_23_withsubnats_noLUXISL_v22.csv')
ff['log_total_pc_100'] = np.log(ff['total_pc_100'])

# Prep inputs and run SFA
m2 = ff.shape[0] # number of observations
s2 = np.sqrt(ff['variance'].values) 
x2 = ff['ldipc'].values # independent variable
z2 = np.ones((m2,1))
d2 = np.ones((m2,1))
y2 = ff['log_total_pc_100'].values # dependent variable
ind2 = np.argsort(x2)
x2 = x2[ind2]
y2 = y2[ind2]

sfa2 = SFA(x2.reshape(m2,1), z2, d2, s2, Y=y2, add_intercept_to_x=True)

# add splines
knots = np.array(custom_natl_knots)
degree = 3
sfa2.addBSpline(knots, degree, r_linear=False, bspline_mono='increasing', bspline_cvcv='concave')

beta_uprior = np.array([[0.0]*sfa2.k_beta, [5000.0]*sfa2.k_beta])
gama_uprior = np.array([[0.0]*sfa2.k_gama, [0.0]*sfa2.k_gama]) # this prior on gamma forces the random effects to zero
deta_uprior = np.array([[0.0]*sfa2.k_deta, [np.inf]*sfa2.k_deta])

sfa2.addUPrior(beta_uprior, gama_uprior, deta_uprior)
sfa2.optimizeSFA()

# Optimize and trim 10% of outliers
sfa2.optimizeSFAWithTrimming(int(0.90*sfa2.N), stepsize=100.0, verbose=True, max_iter=20)


#################################################
# Finally, create comparison plots of US subnational-specific estimates from V22 vs V23
#################################################

f=plt.figure()
plt.plot(x2, y2, '.')
plt.plot(x, y, 'b.')
plt.plot(x2, sfa2.X.dot(sfa2.beta_soln))
plt.plot(x, sfa.X.dot(sfa.beta_soln), 'r')
plt.xlabel('LDI per capita')
plt.ylabel('log Daily testing rate per capita')
plt.show()
f.savefig("FILEPATH/COVID_SFA_level_ylog.pdf", bbox_inches='tight')

f=plt.figure()
plt.plot(x2, np.exp(y2), '.')
plt.plot(x, np.exp(y), 'b.')
plt.plot(x2, np.exp(sfa2.X.dot(sfa2.beta_soln)))
plt.plot(x, np.exp(sfa.X.dot(sfa.beta_soln)), 'r')
plt.xlabel('LDI per capita')
plt.ylabel('Daily testing rate per capita')
plt.show()
f.savefig("FILEPATH/COVID_SFA_level_yleveled.pdf", bbox_inches='tight')

f=plt.figure()
plt.plot(x2, y2, '.')
plt.plot(x, y, 'b.')
plt.plot(x2, sfa2.X.dot(sfa2.beta_soln))
plt.plot(x, sfa.X.dot(sfa.beta_soln), 'r')
plt.xlabel('LDI per capita')
plt.ylabel('log Daily testing rate per capita')
plt.xscale('log')
plt.show()
f.savefig("FILEPATH/COVID_SFA_log_ylog.pdf", bbox_inches='tight')

f=plt.figure()
plt.plot(x2, np.exp(y2), '.')
plt.plot(x, np.exp(y), 'b.')
plt.plot(x2, np.exp(sfa2.X.dot(sfa2.beta_soln)))
plt.plot(x, np.exp(sfa.X.dot(sfa.beta_soln)), 'r')
plt.xlabel('LDI per capita')
plt.ylabel('Daily testing rate per capita')
plt.xscale('log')
plt.show()
f.savefig("FILEPATH/COVID_SFA_log_yleveled.pdf", bbox_inches='tight')


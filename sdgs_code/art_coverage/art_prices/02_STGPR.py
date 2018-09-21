import os
os.chdir("FILEPATH")
import sys
import pandas as pd
import matplotlib.pyplot as plt
import spacetime.spacetime as st
reload(st)
import gpr.gpr as gpr
reload(gpr)
import numpy as np
def logit(p):
    return np.log(p) - np.log(1 - p)

def inv_logit(p):
    return np.exp(p) / (1 + np.exp(p))
model_version = sys.argv[1]
model_version = str(model_version)	
draws = sys.argv[2]
draws = int(draws)	

data = pd.read_stata('FILEPATH/national_prior.dta'.format(model_version=model_version))
print 'loaded data'

#Run Spacetime Seperately By sex
data['iso3'] = data['ihme_loc_id']
data1 = data.ix[data.sex==1]
s1 = st.Smoother(data1,spacevar='iso3', datavar='logit_data', modelvar='prior', snvar=None)
# Set parameters (can additionally specify omega (age weight, positive real number) and zeta (space weight, between 0 and 1))
s1.lambdaa = 2.0
s1.omega = 1
s1.zeta = .9
# Tell the smoother to calculate both time weights and age weights
s1.time_weights()
s1.age_weights()
# Run the smoother and write the results to a file
s1.smooth()
results1 = s1.format_output(include_mad=True)

data2 = data.ix[data.sex==2]
s2 = st.Smoother(data2,spacevar='iso3', datavar='logit_data', modelvar='prior', snvar=None)
# Set parameters (can additionally specify omega (age weight, positive real number) and zeta (space weight, between 0 and 1))
s2.lambdaa = 2.0
s2.omega = 1
s2.zeta = .9
# Tell the smoother to calculate both time weights and age weights
s2.time_weights()
s2.age_weights()
# Run the smoother and write the results to a file
s2.smooth()
results2 = s2.format_output(include_mad=True)

#Append Sexes
results = results1.append(results2)

#Save ST results for making draws in parallel
results.to_stata('FILEPATH/national_ST_estimates.dta'.format(model_version=model_version))


#Launch GPR draw runs in parallel (need draws for forecasting)
if draws > 0:
	print 'launching GPR draw jobs'
    for loc in results.ihme_loc_id.unique():
        os.popen('qsub -P proj_covariates -N edu_{loc} -pe multi_slot 8 FILEPATH/python_shell.sh FILEPATH/02_GPR_draws.py  {model_version} {draws} {loc} '.format(loc=loc,draws=draws,model_version=model_version))

#Run GPR to make Mean, Upper, Lower 
print 'Running GPR'
df_list = []
gpr_results = pd.DataFrame()
for iso in pd.unique(results['iso3']):
    for sexy in pd.unique(results['sex']):
        for age in pd.unique(results['age']): 
            # Run one country-year-age-sex group at a time
            iso_sex_age_results = results.loc[(results.iso3 == '{iso}'.format(iso=iso)) & (results.sex == sexy) & (results.age == age),:]
            amp = iso_sex_age_results.mad_regional.values[0] * 1.4826 
            gpr_out = gpr.fit_gpr(iso_sex_age_results,amp=amp,scale=40,obs_variable='logit_data', obs_var_variable='logit_data_var', mean_variable='st_prediction',diff_degree=2)
            df_list.append(gpr_out)

gpr_results = pd.concat(df_list)

#Convert GPR Results to Normal Space
for var in ['mean','upper','lower']:
    gpr_results['gpr_{var}'.format(var=var)] = (inv_logit(gpr_results['gpr_{var}'.format(var=var)]) * 18)
    
#Save DTA for graphing,uploading estimates
gpr_results.to_stata('FILEPATH/national_estimates.dta'.format(model_version=model_version))


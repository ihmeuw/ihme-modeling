import numpy as np
from st_gpr import gpr
import pandas as pd
import getpass
import sys

run_date = str(sys.argv[1])

# Filepath settings
user = getpass.getuser()
if user == "":
    code_dir = FILEPATH
else: 
    code_dir = FILEPATH
lindir = FILEPATH
stdir = FILEPATH
params_file = "%s/params.csv" %lindir

# Get model params
params = pd.read_csv(params_file)

# Get data
data = pd.read_csv("%s/forgpr.csv" % lindir)

fits = []
for l in data.location_id.unique():
    for a in data.age_group_id.unique():
        for s in data.sex_id.unique():
            print 'l: %s, a: %s, s: %s' % (l, a, s)

            lparams = params[params.location_id == l]
            amp = lparams.amp.values[0]
            scale = lparams.scale.values[0]

            sdata = data[
                    (data.location_id == l) &
                    (data.age_group_id == a) &
                    (data.sex_id == s)]
            sdata = sdata.sort('year_id')

            # Uncomment this line to use the regional MAD as the amplitude
            mad_lvl_map = {
                'global': 0,
                'superregional': 1,
                'regional': 2,
                'national': 3}

            if amp in mad_lvl_map.keys():
                amp = sdata[
                        "mad_level_%s" % mad_lvl_map[amp]].unique()[0]*1.4826
            else:
                amp = float(amp)

            # Run GPR
            fit = gpr.fit_gpr(
                    sdata,
                    amp,
                    obs_variable='ln_dr',
                    obs_var_variable='data_var',
                    scale=5,
                    draws=0,
                    diff_degree=2)

            """
            Convert back to normal space

                Approximate back-transformed variance using the delta method:
                G(X) = G(mu) + (X-mu)G'(mu) (approximately)
                Var(G(X)) = Var(X)*[G'(mu)]^2 (approximately)

                Examples:
                    For G(X) = Invlogit(X)
                    ... Invlogit'(p) = e^x/[(e^x+1)^2]
                    ... so Var(Logit(X)) = Var(X) * e^2x / [(e^x+1)^4]
                    For G(X) = exp(X)
                    ... exp'(x) = exp
                    ... so Var(exp) = Var(X) * (e^2x)
            """

            fits.append(fit)
fits = pd.concat(fits)
# Drop duplicates that appear because GPR has one row per line of data
fits.drop_duplicates(subset=['location_id','year_id','age_group_id','sex_id'], inplace=True)
fits['gpr_var'] = fits['gpr_var'] * (np.exp(2*fits['gpr_mean']))
fits['gpr_mean'] = np.exp(fits['gpr_mean'])
fits['gpr_lower'] = np.exp(fits['gpr_lower'])
fits['gpr_upper'] = np.exp(fits['gpr_upper'])
fits['st_prediction'] = np.exp(fits['st_prediction'])
fits['ln_dr_predicted'] = np.exp(fits['ln_dr_predicted']) 
fits['age_group_id']=fits['age_group_id'].replace(21,30) 
fits['age_group_id']=fits['age_group_id'].replace(22,31) 
fits['age_group_id']=fits['age_group_id'].replace(23,32) 
fits['age_group_id']=fits['age_group_id'].replace(24,235)
fits[[
    'location_id', 'year_id', 'age_group_id', 'sex_id', 'ln_dr_predicted',
    'st_prediction', 'gpr_mean', 'gpr_lower', 'gpr_upper', 'gpr_var']].to_csv(
    '%s/gpr_results_%s.csv' %(lindir,run_date), index=False)

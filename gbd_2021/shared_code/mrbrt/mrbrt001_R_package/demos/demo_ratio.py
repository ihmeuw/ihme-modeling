
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mrtool import MRData, LogCovModel, MRBRT, MRBeRT



# simulate the curve
def sim_rr(x):
    return (x + 1.0/(0.25 + x))/(1.75)
    


x = np.linspace(0.0, 5.0, 100)
y = sim_rr(x)


plt.plot(x, y)


# reference and altternative exposure
num_obs = 100
alt_exposure = np.random.uniform(0.0, 5.0, size=num_obs)
ref_exposure = np.random.uniform(0.0, 5.0, size=num_obs)



# simulate the observation
obs_se = 0.1
obs = np.log(sim_rr(alt_exposure)) - np.log(sim_rr(ref_exposure)) + np.random.randn(num_obs)*obs_se



data = pd.DataFrame({
    'obs': obs,
    'obs_se': obs_se,
    'cov_ref': ref_exposure,
    'cov_alt': alt_exposure,
    'study_id': 'A'
})




mrdata = MRData()
mrdata.load_df(data,
               col_obs='obs',
               col_obs_se='obs_se',
               col_covs=['cov_ref', 'cov_alt'],
               col_study_id='study_id')
               
               
               
covmodels = [
    LogCovModel(alt_cov='cov_alt',
                ref_cov='cov_ref',
                use_re=True,
                use_spline=True,
                spline_knots_type='domain',
                spline_knots=np.linspace(0.0, 1.0, 5),
                spline_degree=3,
                prior_spline_convexity='convex',
                prior_gamma_uniform=np.array([0.0, 0.0]))
]



mrbrt = MRBRT(mrdata, covmodels)



z_mat = mrbrt.create_z_mat()




mrbrt.fit_model(x0=np.ones(mrbrt.num_vars),
                inner_max_iter=100)
                
data_pred = pd.DataFrame({
    'cov_alt': np.linspace(0.0, 5.0, 100),
    'cov_ref': np.full(100, 0.75),
})
mrdata_pred = MRData()
mrdata_pred.load_df(data_pred,
                    col_covs=['cov_alt', 'cov_ref'])
                    
                    
                    
                    
y_pred = mrbrt.predict(mrdata_pred)



# predicted curve
plt.plot(mrdata_pred.covs['cov_alt'],
         np.exp(mrbrt.predict(mrdata_pred)))
# true curve
plt.plot(x, y)



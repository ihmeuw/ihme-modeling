#
# demo_simple_linear.py
#
# Reed Sorensen
# May 2020
#

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mrtool import MRData, LinearCovModel, MRBRT


np.random.seed(123)
beta = np.array([1.0, 0.5])
gamma = np.array([0.1, 0.0])

num_obs = 200
obs_se = 0.1
covs = np.hstack((np.ones((num_obs, 1)), np.random.randn(num_obs, 1)))

studies = ['A', 'B', 'C', 'D', 'E', 'F']
study_id = np.random.choice(studies, num_obs)


u_dict = {study: np.random.randn(gamma.size)*np.sqrt(gamma) for study in studies}
u = np.array([u_dict[study] for study in study_id])

obs = np.sum(covs*(beta + u), axis=1) + np.random.randn(num_obs)*obs_se


data = pd.DataFrame({
    'obs': obs,
    'obs_se': obs_se,
    'study_id': study_id
})

for i in range(covs.shape[1]):
    data[f'cov{i}'] = covs[:, i]
    
# data.to_csv("misc/demo_simple_linear_data.csv")
    
####
    
mrdata = MRData()
mrdata.load_df(data,
               col_obs='obs',
               col_obs_se='obs_se',
               col_covs=[f'cov{i}' for i in range(covs.shape[1])],
               col_study_id='study_id')
               
               
               
               
covmodels = [
    LinearCovModel('cov0', use_re=True),
    LinearCovModel('cov1', use_re=False)
]


# build mrbrt object
mrbrt = MRBRT(mrdata, covmodels)


# fit mrbrt object
mrbrt.fit_model(x0=np.zeros(mrbrt.num_vars))


plt.scatter(mrbrt.data.obs, mrbrt.predict(mrbrt.data, predict_for_study=True))
plt.scatter(mrbrt.data.obs, mrbrt.predict(mrbrt.data, predict_for_study=False))


beta_samples, gamma_samples = mrbrt.sample_soln(sample_size=100)


draws = mrbrt.create_draws(mrbrt.data,
                           beta_samples=beta_samples,
                           gamma_samples=gamma_samples,
                           random_study=True)
                           
                           
                           


               
               
               

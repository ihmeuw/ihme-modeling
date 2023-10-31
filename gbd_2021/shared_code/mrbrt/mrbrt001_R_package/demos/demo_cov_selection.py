import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mrtool import MRData, LinearCovModel, MRBRT, CovFinder



beta = np.array([1.0, 0.0, 0.0, 0.5, 0.0, 0.7, 0.0])
gamma = np.array([0.0]*7)

num_obs = 100
obs_se = 0.1
covs = np.hstack((np.ones((num_obs, 1)), np.random.randn(num_obs, 6)))

studies = ['A', 'B', 'C']
study_id = np.random.choice(studies, num_obs)


u_dict = {study: np.random.randn(7)*np.sqrt(gamma) for study in studies}
u = np.array([u_dict[study] for study in study_id])

obs = np.sum(covs*(beta + u), axis=1) + np.random.randn(num_obs)*obs_se


data = pd.DataFrame({
    'obs': obs,
    'obs_se': obs_se,
    'study_id': study_id
})

for i in range(7):
    data[f'cov{i}'] = covs[:, i]
    
    


data.head()

mrdata = MRData()
mrdata.load_df(data,
               col_obs='obs',
               col_obs_se='obs_se',
               col_study_id='study_id',
               col_covs=[f'cov{i}' for i in range(7)])
               
               
               
covfinder = CovFinder(mrdata, covs=[f'cov{i}' for i in range(1, 7)],
                      pre_selected_covs=['cov0'],
                      num_samples=1000,
                      power_range=(3, 6),
                      power_step_size=1,
                      laplace_threshold=1e-5)
                      
covfinder.select_covs(verbose=True)

covfinder.selected_covs



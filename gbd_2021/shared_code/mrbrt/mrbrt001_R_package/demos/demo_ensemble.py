
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mrtool import MRData, LinearCovModel, MRBRT, MRBeRT, utils


def curve(x):
    return np.sin(2.0*np.pi*x)
    
np.random.seed(123)
# no beta, since obersvations are defined by the curve
# gamma definition
gamma = np.array([1.0, 0.0])

num_obs = 200
obs_se = 0.1
time = np.linspace(0.0, 1.0, num_obs)

studies = ['A', 'B', 'C', 'D', 'E', 'F']
study_id = np.random.choice(studies, num_obs)


u_dict = {study: np.random.randn(gamma.size)*np.sqrt(gamma) for study in studies}
u = np.array([u_dict[study] for study in study_id])

obs = u[:, 0] + curve(time) + np.random.randn(num_obs)*obs_se



data = pd.DataFrame({
    'obs': obs,
    'obs_se': obs_se,
    'time': time,
    'study_id': study_id
})



mrdata = MRData()
mrdata.load_df(data,
               col_obs='obs',
               col_obs_se='obs_se',
               col_covs=['time'],
               col_study_id='study_id')
               
               
               
covmodels = [
    LinearCovModel('intercept', use_re=True),
    LinearCovModel('time',
                   use_re=False,
                   use_spline=True,
                   spline_knots=np.array([0.0, 0.25, 0.75, 1.0]),
                   spline_degree=3)
]




# build mrbrt object
mrbrt = MRBRT(mrdata, covmodels)



# fit mrbrt object
mrbrt.fit_model(x0=np.zeros(mrbrt.num_vars), inner_max_iter=100)




plt.scatter(mrdata.covs['time'], mrdata.obs, label='data')
plt.scatter(mrdata.covs['time'], mrbrt.predict(mrdata,
                                               predict_for_study=False),
            marker='.',
            label='mean preds')
plt.scatter(mrdata.covs['time'], mrbrt.predict(mrdata,
                                               predict_for_study=True),
            marker='.',
            label='study specfic preds')
plt.plot(time, curve(time), color='k', label='true curve')
plt.legend()



# sample knots
knots_samples = utils.sample_knots(num_intervals=3,
                                   knot_bounds=np.array([[0.0, 0.4], [0.6, 1.0]]),
                                   num_samples=20)
                                   
                                   
knot_samples_plot = plt.boxplot(knots_samples)





mrbert = MRBeRT(mrdata,
                ensemble_cov_model=covmodels[1],
                ensemble_knots=knots_samples,
                cov_models=[covmodels[0]])
                
                
mrbert.fit_model(x0=np.zeros(mrbert.num_vars))


# For predict function of MRBeRT, there is an extra keyword argument called return_avg. 
# When it is True, the function will return an average prediction based on the score, 
# and when it is False the function will return a list of predictions from all groups.

prediction = mrbert.predict(mrdata)
prediction_for_study = mrbert.predict(mrdata, predict_for_study=True)


plt.scatter(mrdata.covs['time'], prediction, marker='.')

beta_samples, gamma_samples = mrbert.sample_soln(sample_size=100)

data_pred = pd.DataFrame({
    'time': np.linspace(0.0, 1.0, 100)
})
mrdata_pred = MRData()
mrdata_pred.load_df(data_pred, col_covs=['time'])


draws = mrbert.create_draws(mrdata_pred,
                            beta_samples,
                            gamma_samples,
                            random_study=False)
draws_with_re = mrbert.create_draws(mrdata_pred,
                                    beta_samples,
                                    gamma_samples,
                                    random_study=True)
                                    
                                    
                                    
draws_lower = np.quantile(draws, 0.025, axis=1)
draws_upper = np.quantile(draws, 0.975, axis=1)
draws_with_re_lower = np.quantile(draws_with_re, 0.025, axis=1)
draws_with_re_upper = np.quantile(draws_with_re, 0.975, axis=1)




plt.fill_between(mrdata_pred.covs['time'], draws_lower, draws_upper, alpha=0.5, color='#808080')
plt.fill_between(mrdata_pred.covs['time'], draws_with_re_lower, draws_with_re_upper, alpha=0.5, color='#808080')
plt.scatter(mrdata.covs['time'], mrdata.obs, label='data')
plt.scatter(mrdata.covs['time'], prediction, marker='.',
            label='mean preds')
plt.scatter(mrdata.covs['time'], prediction_for_study, marker='.',
            label='study specfic preds')
plt.plot(time, curve(time), color='k', label='true curve')
plt.legend()









#
# demo_simple_linear.R
#
# May 2020
#
# R version of 'demo_simple_linear.py'
# Original Python is commented out
#



# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt
# from mrtool import MRData, LinearCovModel, MRBRT
#
# np.random.seed(123)
# beta = np.array([1.0, 0.5])
# gamma = np.array([0.1, 0.0])
#
# num_obs = 200
# obs_se = 0.1
# covs = np.hstack((np.ones((num_obs, 1)), np.random.randn(num_obs, 1)))
#
# studies = ['A', 'B', 'C', 'D', 'E', 'F']
# study_id = np.random.choice(studies, num_obs)
#
#
# u_dict = {study: np.random.randn(gamma.size)*np.sqrt(gamma) for study in studies}
# u = np.array([u_dict[study] for study in study_id])
#
# obs = np.sum(covs*(beta + u), axis=1) + np.random.randn(num_obs)*obs_se
#
#
# data = pd.DataFrame({
#   'obs': obs,
#   'obs_se': obs_se,
#   'study_id': study_id
# })
#
# for i in range(covs.shape[1]):
#   data[f'cov{i}'] = covs[:, i]
#
# data.to_csv("misc/demo_simple_linear_data.csv")
#

data <- read.csv("misc/demo_simple_linear_data.csv")

####

# mrdata = MRData()
# mrdata.load_df(data,
#                col_obs='obs',
#                col_obs_se='obs_se',
#                col_covs=[f'cov{i}' for i in range(covs.shape[1])],
#                col_study_id='study_id')
#
# covmodels = [
#   LinearCovModel('cov0', use_re=True),
#   LinearCovModel('cov1', use_re=False)
# ]

mrdata <- MRData()
mrdata$load_df(
  df = data,
  col_obs = "obs",
  col_obs_se = "obs_se",
  col_covs = list("cov0", "cov1"),
  col_study_id = "study_id"
)

covmodels <- list(
  LinearCovModel("cov0", use_re = TRUE),
  LinearCovModel("cov1")
)


# build and fit mrbrt object
# mrbrt = MRBRT(mrdata, covmodels)
# mrbrt.fit_model(x0=np.zeros(mrbrt.num_vars))
# plt.scatter(mrbrt.data.obs, mrbrt.predict(mrbrt.data, predict_for_study=True))
# plt.scatter(mrbrt.data.obs, mrbrt.predict(mrbrt.data, predict_for_study=False))

mod1 <- MRBRT(mrdata, covmodels)
# mod1$fit_model(x0 = array(rep(0, mod1$num_vars)))
mod1$fit_model(inner_print_level = 5L, inner_max_iter = 500L)

pred1 <- mod1$predict(data = mod1$data, predict_for_study = TRUE)
pred2 <- mod1$predict(data = mod1$data, predict_for_study = FALSE)

df_pred <- data.frame(cov0 = seq(0, 10, by = 0.1), cov1 = seq(0, 10, by = 0.1))
dat_pred <- MRData()
dat_pred$load_df(df = df_pred, col_covs = list("cov0", "cov1"))
pred1 <- mod1$predict(data = dat_pred, predict_for_study = FALSE)

plot(data$obs, pred1)
plot(data$obs, pred2)


# beta_samples, gamma_samples = mrbrt.sample_soln(sample_size=100)
#
#
# draws = mrbrt.create_draws(mrbrt.data,
#                            beta_samples=beta_samples,
#                            gamma_samples=gamma_samples,
#                            random_study=True)









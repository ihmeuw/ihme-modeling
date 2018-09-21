
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
get_ipython().magic(u'matplotlib inline')
plt.style.use('fivethirtyeight')

import sklearn.gaussian_process, sklearn.gaussian_process.kernels

for sex in range(1,3): 
    for aaa in range(3,5):
        data = pd.read_csv("FILEPATH".format(sex, aaa))

        ga = data["ga"].values ## ga.shape = (99, )
        bw = data["bw"].values ## bw.sahpe = (99, )

        X = np.array(data[["ga", "bw"]]) ## X.shape = (99, 2)

        # normalize X --- sklearn covariance prior is isotropic 
        X_normal = (X - X.mean(axis=0))/ X.std(axis=0) 
        
        y_obs = data["ln_mortality_odds"].values ## Coefficients from the dummy variables 
        y_std = data["ln_odds_std_error"].values ## Standard errors of the coefficents
            
        kernel = sklearn.gaussian_process.kernels.Matern(length_scale=.5,
                                                 length_scale_bounds=(.1,100),
                                                 nu=2.5)

        gp = sklearn.gaussian_process.GaussianProcessRegressor(
        normalize_y=True, kernel=kernel,
        alpha=y_std**2, # set data variance _here_
        optimizer='fmin_l_bfgs_b', n_restarts_optimizer=10,) 

        # fit
        gp.fit(X_normal, y_obs)  # then set data values _here_ 

        gpr_ln_mortality_odds, gpr_ln_odds_std_error = gp.predict(X_normal, return_std=True)
        ##get samples if necess

        data['gpr_ln_mortality_odds'] = gpr_ln_mortality_odds
        data['gpr_ln_odds_std_error'] = gpr_ln_odds_std_error
            
        data.to_csv("FILEPATH".format(sex, aaa), index = False)


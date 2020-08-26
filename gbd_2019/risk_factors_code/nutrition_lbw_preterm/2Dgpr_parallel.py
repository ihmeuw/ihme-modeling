
import numpy as np
import pandas as pd
import argparse
import sklearn.gaussian_process, sklearn.gaussian_process.kernels



# ---------------------------------------------
# FUNCTIONS

def run_2Dgpr(input_filepath, output_filepath):
    data = pd.read_csv(input_filepath)

    # Create an array of two dimensions
    X = np.array(data[["dim_x1", "dim_x2"]]) 

    # Normalize array X 
    X_normalized = (X - X.mean(axis=0)) / X.std(axis=0) 
    
    # Store the observations and standard errors
    y_obs = data["y_obs"].values  
    y_std = data["y_std"].values 
    
    # Set up kernel    
    kernel = sklearn.gaussian_process.kernels.Matern(
        length_scale=.5,
        length_scale_bounds=(.1,100),
        nu=2.5)

    # Set up Gaussian Process
    gp = sklearn.gaussian_process.GaussianProcessRegressor(
        normalize_y=True, 
        kernel=kernel,
        alpha=y_std**2, # data variance
        optimizer='fmin_l_bfgs_b', 
        n_restarts_optimizer=10,) 

    # Fit X array and y_obs
    gp.fit(X_normalized, y_obs) 

    # Retrieve the post-GPR observations
    gpr_y_obs, gpr_y_std = gp.predict(X_normalized, return_std=True)   

    data['gpr_y_obs'] = gpr_y_obs
    data['gpr_y_std'] = gpr_y_std
        
    data.to_csv(output_filepath, index = False)


parser = argparse.ArgumentParser()
parser.add_argument("input_filepath", help="filepath to input data to use 2D GPR", type=str)
parser.add_argument("output_filepath", help="filepath to output data post-2D GPR", type=str)
args = parser.parse_args()

run_2Dgpr(args.input_filepath, args.output_filepath)
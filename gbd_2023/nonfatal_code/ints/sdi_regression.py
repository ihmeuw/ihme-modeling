
import numpy as np
import pandas as pd
from scipy.special import logit, expit
from sklearn.linear_model import LinearRegression
from db_queries import get_covariate_estimates, get_demographics, get_demographics_template, \
                       get_location_metadata, get_model_results


def sdi_regression(RELEASE, MODEL_VERSION, MEID):
    # Get demographic levels for this model and release
    demog = get_demographics('epi', release_id = RELEASE)
    
    # Pull the model estimates
    est = get_model_results('epi', gbd_id = MEID, model_version_id = MODEL_VERSION, 
                            release_id = RELEASE, location_id = demog['location_id'], 
                            age_group_id = demog['age_group_id'], sex_id = demog['sex_id'],
                            year_id = demog['year_id'])
                                                                             
    est = est.loc[:, ['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id', 'mean']]
    est_rows = est.shape[0]
    
    # Pull corresponding values of SDI
    sdi = get_covariate_estimates(881, release_id = RELEASE, year_id = demog['year_id'],
                                  location_id = demog['location_id'])
    sdi = sdi.loc[:, ['location_id', 'year_id', 'mean_value']]
    sdi = sdi.rename(columns = {'mean_value': 'sdi'})
        
    # Merge model estimates and SDI
    full = est.merge(sdi, on = ['location_id', 'year_id'])
    
    # Check to ensure that we have the same number of rows after the merge as before
    print(full.shape[0] == est_rows)  
    
    # Create age and sex dummy variables for the regression model
    full = pd.get_dummies(full, columns=['age_group_id', 'sex_id'], drop_first=True)
    full['logit_sdi'] = logit(full['sdi'])
    
    measure_ids = full['measure_id'].unique()
    
    # Initialize a dictionary to store models for each 'measure_id'
    models = {}
    
    for measure_id in measure_ids:
        # Subset the data for the current level of 'measure_id'
        subset = full[full['measure_id'] == measure_id]
    
        # Define the independent variables for the subset
        # Ensure to adjust the column names as per your DataFrame
        X_subset = subset[['logit_sdi'] + [col for col in subset.columns if 'age_group_id_' in col or 'sex_id_' in col]]
        
        # Define the dependent variable for the subset
        # If the measure is continuous don't transform
        if (measure_id==19):
            y_subset = subset['mean']
        else:
            # Make offset 10% of min non-zero value if min value is zero
            offset = np.min(full.query('mean > 0')['mean']) * 0.1 * (np.min(full['mean'])==0)  
            # Log-transform
            y_subset = np.log(subset['mean'] + offset)
        
        
        # Initialize and fit the model for the subset
        model = LinearRegression().fit(X_subset, y_subset)
        
        # Store the fitted model in the dictionary
        models[measure_id] = model.coef_[0]
    
    # Convert dictionary to string so it can easily be passed as a bash submission arguement
    models = str(models).replace(' ', '')
    return models



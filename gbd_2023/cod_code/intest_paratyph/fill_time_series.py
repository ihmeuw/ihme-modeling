import ast
import math
import numpy as np
import pandas as pd
import pymysql 
from scipy.special import logit, expit
from db_queries import get_covariate_estimates, get_demographics, get_demographics_template, get_location_metadata, get_model_results


def fill_dismod_trend(meid, model_version, release, loc, use_covars = True, sdi_coefs = None):
   
    # Check and process SDI coefficients, if given
    if sdi_coefs is not None:
        if isinstance(sdi_coefs, dict):
            sdi_coefs = pd.DataFrame.from_dict(sdi_coefs, orient='index').reset_index()
            sdi_coefs.columns = ['measure_id', 'mean_effect']
            sdi_coefs['covariate_id'] = 881
            sdi_coefs['transform_type_id'] = 2 # indicates that sdi is logit transformed
        else:
            raise ValueError("When sdi_coefs are provided they must be a dictionary.")  
    
    # If SDI coefficients are not given and use_covars is false throw an error
    elif use_covars==False:
        raise ValueError("When use_covars is False, sdi_coefs must be provided and be a dictionary.")

        
    # Get epi years -- Need this to know what years to interpolate between and extrapolate from
    epi_years = get_demographics('epi', release_id = release)['year_id']
    
    
    # Make template data frame of full demographics for prediction + epi estimates
    template =  make_template(meid, release, loc) 

    
    # Get the metadata with covariate fixed effects from the database (this tells us which covariates were included and gives us the coefficients for making predictions)
    if use_covars:
        model_data = get_model_metadata(model_version)
        # If sdi_coefs are given then use SDI to predict for any measures without country covariates
        if sdi_coefs is not None:
            sdi_coefs = sdi_coefs.loc[~sdi_coefs['measure_id'].isin(model_data['measure_id'].unique())]
            model_data = pd.concat([model_data, sdi_coefs])
        
        print(model_data)
    
    # If we're not using covariates, or if there are no country covariates and SDI coefs are provided
    # then use SDI coefs for interpolation
    else:
        model_data = sdi_coefs

    
    # If we're using any covariates to predict then model_data will have non-zero length: incorporate the covariate metadata    
    if len(model_data) > 0:
        # Get the covariate metadata so we know if covariates are age/sex-specific (need this to know how to merge everything together)
        covar_metadata = get_covariate_metadata(model_data)
        #print(covar_metadata)
           
        # Combine the covariate estimates with the template and calculate the linear combination of fixed effects
        template = combine_covariate_estimates(template, model_data, covar_metadata, release)
        
    
    # Group the template by age, location, and sex, and fill in the time-series for each
    grouped = template.groupby(['measure_id', 'age_group_id', 'location_id', 'sex_id'])
    #preds, status = [fill_time_series(group, epi_years, measure_id) for _, group in grouped]
    preds, status = zip(*[fill_time_series(group, epi_years) for _, group in grouped])
    
    # Clean up the prediction data frame for return
    preds = pd.concat(preds, ignore_index=True)
    preds = pd.merge(template, preds, on=['age_group_id', 'location_id', 'sex_id', 'year_id', 'measure_id'], how = 'outer')
    
    preds['pred'] = preds['mean'].fillna(preds['pred'])
    preds['ref_year'] = preds['ref_year'].fillna(preds['year_id'])
    preds = preds[['location_id', 'year_id', 'ref_year', 'age_group_id', 'sex_id', 'measure_id', 'mean', 'pred']]
    
    status = pd.concat(status, ignore_index=True)
    
    return preds, status
    #return template
    



"""
NOTE: this is currently configured to only accept a single location -- 
consider updating to take a list of locations
This isn't urgent, but potentially useful and more generalizable
"""

def make_template(meid, release, loc):
    # Get template with all combinations of all years, ages, and sexes
    template = get_demographics_template('cod', release_id = release)
    if loc in np.unique(template['location_id']):
        template = template.query(f'location_id == {loc}')
    else:
        tmp_loc = template['location_id'].iloc[0]
        template = template.query(f'location_id == {tmp_loc}').drop(columns = 'location_id')
        template['location_id'] = loc
    
    
    
    # Get the model estimates and keep only necessary variables
    est = get_model_results('epi', meid, release_id=release, 
                            location_id = list(template['location_id'].unique()),
                            sex_id = list(template['sex_id'].unique()), 
                            age_group_id = list(template['age_group_id'].unique()))
    
    est = est[['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id', 'mean']]
    
    # Merge the template with the model estimates and return
    measure_link = est[['location_id', 'measure_id']].drop_duplicates()
    template = template.merge(measure_link, on = 'location_id', how = 'outer')
    template = pd.merge(template, est, on=['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id'], how = 'outer')
    
    return template




def get_model_metadata(model_version):
    # We're going to pull the fixed effects from the DisMod model and use them to extrapolate
    # These are the variables we want to get re: fixed effects
    varlist = ['measure_id', 'country_covariate_id', 'mean_effect']
    
    # Connect to the epi database, open the cursor, and execute the SQL query
    db = pymysql.connect(host = 'ADDRESS', user = 'USERNAME', password = 'PASSWORD', database = 'DATABASE') 
    
    with db:
        with db.cursor() as cursor:
            cursor.execute('SELECT ' + ', '.join(varlist) + ' FROM model_effect WHERE model_version_id = ' + str(model_version) + 
                           ' AND (country_covariate_id IS NOT NULL OR study_covariate_id IS NOT NULL)')

            # Fetch all rows of data, put them in a data frame and add column names
            model_data = pd.DataFrame(cursor.fetchall(), columns = varlist)
            measure_id = model_data['measure_id'][0]
            model_data.dropna(subset = ['country_covariate_id'], inplace=True)


    # Pull the model parameter file to get covariate transformation info (this doesn't seem to be stored in the database)        
    model_params = pd.read_csv(f'FILEPATH')
    model_params = model_params.query('parameter_type_id==7')[['measure_id', 'country_covariate_id', 'transform_type_id']]
    model_params['transform_type_id'] = model_params['transform_type_id'].astype(int)
    
    # Merge the model data and model_params
    model_data = model_data.merge(model_params, on = ['measure_id', 'country_covariate_id'], how = 'left')
    
    # Rename covariate_id variable to match what we'll pull with get_covariates function
    model_data['covariate_id'] = model_data['country_covariate_id'].astype(int)
    model_data = model_data.drop(columns = ['country_covariate_id']) 
    
    
    return model_data



def get_covariate_metadata(model_data):
    # Now we know what covariates were used in the model, let's get metadata for those covariates.
    # Steps below are essentially identical to the SQL query above
    # Get list of necessary covariates from the model_data 
    covar_str = ', '.join([str(x) for x in model_data['covariate_id']])
    covar_varlist = ['covariate_id', 'covariate_name_short', 'by_sex', 'by_age']
    
    # Connect to the shared database, open the cursor, and execute the SQL query
    db = pymysql.connect(host = 'ADDRESS', user = 'USERNAME', password = 'PASSWORD', database = 'DATABASE') 
    
    with db:
        with db.cursor() as cursor:
            cursor.execute('SELECT ' + ','.join(covar_varlist) + ' FROM covariate WHERE covariate_id IN (' + covar_str + ')')
    
            # Fetch all rows of data, put them in a data frame and add column names
            # TESTING: DELETE THIS LINE IF ALL WORKS covar_data = cursor.fetchall()
            covar_data = pd.DataFrame(cursor.fetchall(), columns = covar_varlist)
    
    return covar_data



def combine_covariate_estimates(template, model_data, covar_metadata, release):
    # Now that we have the covariate metadata, we can pull the covariate value.
    # Since the necessary shared function exists, we'll use that below instead of a SQL query
    
    # First merge the model data and covariate metadata (ensures we get a row for every combination of measure and covariate)
    covar_template = template.merge(model_data, on = 'measure_id', how = 'left').drop(columns = ['mean'])
    covar_template['covar_value'] = np.nan

    # Loop through the covariates
    for i in range(len(covar_metadata)):

        # Pull covariate and model metadata from covar_data and model_data, respectively, and move to individual objects
        id, name, by_sex, by_age = list(covar_metadata.iloc[i])

        # The id variables on which we'll merge will depend on whether or not the covariate is age- and sex-specific.
        # location and year are the minimum necessary merge variables; 
        # we'll add sex for sex-specific covariates, and age for age-specific covariates below.
        merge_vars = ['covariate_id', 'location_id', 'year_id']

        if by_sex==1:
            merge_vars.append('sex_id')

        if by_age==1:
            merge_vars.append('age_group_id')

        # Get_covariates returns variables we don't need here; 
        # We only want to keep the merge variables and the mean value of the covariate ('mean_value').
        keep = merge_vars[::]
        keep.append('mean_value')  

        # Pull the covariate estimates
        covar = get_covariate_estimates(covariate_id = int(id), location_id = list(template['location_id'].unique()), release_id = release)
        covar = covar[keep]


        # Rename 'mean_value' to the short name of the covariate
        covar.rename(columns = {'mean_value': 'covar_tmp'}, inplace = True)


        # Merge the covariate into the template
        covar_template = pd.merge(covar_template, covar, on = merge_vars, how = 'left')
        covar_template['covar_value'] = covar_template['covar_value'].fillna(covar_template['covar_tmp'])
        covar_template = covar_template.drop(columns = ['covar_tmp'])


    # Apply covariate transformations
    covar_template.loc[covar_template['transform_type_id']==1, 'covar_value'] = np.log(covar_template.loc[covar_template['transform_type_id']==1, 'covar_value'])
    covar_template.loc[covar_template['transform_type_id']==2, 'covar_value'] = logit(covar_template.loc[covar_template['transform_type_id']==2, 'covar_value'])
    covar_template.loc[covar_template['transform_type_id']==3, 'covar_value'] = covar_template.loc[covar_template['transform_type_id']==3, 'covar_value']**2
    covar_template.loc[covar_template['transform_type_id']==4, 'covar_value'] = np.sqrt(covar_template.loc[covar_template['transform_type_id']==4, 'covar_value'])
    covar_template.loc[covar_template['transform_type_id']==5, 'covar_value'] = 1000 * covar_template.loc[covar_template['transform_type_id']==5, 'covar_value']

    # Multiply covariate and coefficient
    covar_template['lincom'] = covar_template['covar_value'] * covar_template['mean_effect']
    
    template_merge_vars = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id']
   
    covar_template = covar_template.groupby(template_merge_vars)['lincom'].sum().reset_index()    
    
    template = template.merge(covar_template, on = template_merge_vars, how = 'left')
    return template



"""
Here we're going to define the function to actually do the interpolation and extrapolation.
It takes the template data frame and a list of reference years as inputs, 
where reference years are those between which you want to interpolate.

N.B. IMPORTANT!!! the data frame that you feed into this function should contain estimates for only a single
demographic group (i.e. a single location, age, sex combination) with one row per year.  The main function
handles this for you, but take care if you're going to build your own wrapper around this!

It will return a data frame with location, year, age, sex, and the filled time series of predictions

"""

def fill_time_series(df, ref_years):
    
    # fts_main is the main function that manages the data and calls the interpolation and extrpolation functions
    def fts_main(df, ref_years):
        # Determine year pairs between which we need to interpolate
        year_pairs = zip(ref_years, ref_years[1:])
        year_pairs = [pair for pair in year_pairs if pair[1] - pair[0] != 1]
        
        # Establish variables to be returned by interpolation/extrapolation code
        return_vars = ['age_group_id', 'location_id', 'sex_id', 'year_id', 'measure_id', 'ref_year', 'pred']
        
        df = df.reset_index()
        
        # If all input values are zeros fill the entire time series with zeros 
        if np.min(df['mean'])==0 and np.max(df['mean'])==0:
            df['pred'] = 0
            df['ref_year'] = df['year_id']
            preds = df[return_vars]
            status = pd.DataFrame({'lincom': ['zero fill'], 'xform': ['zero fill']})

        # Otherwise, do proper interpolation+extrapolation    
        else:            
            # Determine if we have a linear effect to use in the predictions; if not fill a lincom variable with zeros
            if 'lincom' not in df.keys():
                status_lincom = False    
                df['lincom'] = 0
            else:
                status_lincom = True


            # Determine the correct transformation for the measure (i.e. log for rates, logit for proportions)
            measure_id = df['measure_id'][0]

            # Don't transform continuous measure estimates
            if measure_id == 19:
                xform = None
            # Logit transform 0-1 bound measure estimates    
            elif int(measure_id) in [5, 17, 18]:
                xform = 'logit'
            # All other measures are 0-Inf and should be log transformed
            else:
                xform = 'log'

                # Make offset 10% of min non-zero value if min value is zero
                offset = np.min(df.query('mean > 0')['mean']) * 0.1 * (np.min(df['mean'])==0)  
                df['mean'] = df['mean'] + offset


            # Backcast the estimates
            back = covar_extrapolate(df, min(ref_years), return_vars, xform)

            # Interpolate the estimates
            inter = []
            for year_pair in year_pairs:
                result = covar_interpolate(df, year_pair, return_vars, xform)
                inter.append(result)

            preds = pd.concat([back, pd.concat(inter)])  

            # Remove the offset
            if xform=='log':
                preds['pred'] = preds['pred'] - offset
                preds.loc[preds['pred']<0, 'pred'] = 0

            status = pd.DataFrame({'lincom': [status_lincom], 'xform': [xform]})
            
        return preds, status
        
    
    # covar_interpolate will interpolate between two years; it takes the single-demographic template dataframe and list with two years 
    def covar_interpolate(df, year_pair, return_vars, xform = None):
        # Restrict the data to only relevant years and sort by year
        df = df.loc[df.year_id.between(*year_pair)]
        df = df.sort_values('year_id')
        
        # Find the log-transformed DisMod estimates for the reference years, and change between them
        if xform=='log':
            start, end = np.log(df['mean'].iloc[[0,-1]]) 
        elif xform=='logit':
            start, end = logit(df['mean'].iloc[[0,-1]]) 
        else:
            start, end = df['mean'].iloc[[0,-1]]
            
        change = end - start
        
        # Get the linear predictions, start and end values, and change between them
        preds = df['lincom']
        pred_start, pred_end = preds.iloc[[0,-1]]
        pred_change = pred_end - pred_start
        
        # Find the difference in the average annual rates of change in model estimates and linear predictions
        # If those two are different then we will get discontinuities in the predicitions between each interpolated period
        # we avoid that by shifting the overall slope of the linear predictions to match the model estimates
        # Think of this as maintaing the shape of the trend line in the linear predictions, fixing one end of the line to match
        # the corresponding model estimate value, and then rotating the trend line until it matches the model estimate at the
        # other end of the line.
        annual_change = (change - pred_change) / (len(preds)-1)
        shift = [float(x)*annual_change for x in range(len(preds))]
        
        # Make the final predictions here
        if xform=='log':
            df['pred'] = np.exp(start - pred_start + preds + shift)
        elif xform=='logit':
            df['pred'] = expit(start - pred_start + preds + shift)
        else:
            df['pred'] = np.exp(start - pred_start + preds + shift)
        
        df['ref_year'] = int(year_pair[0])
        
        return df[return_vars].iloc[:-1]
    
    
    
    def covar_extrapolate(df, ref_year, return_vars, xform = None, direction = 'backwards'): 
        # Determine if we're extrapolating backwards or foward in time and set up accordingly
        if direction=='backwards':
            df = df.loc[df.year_id <= ref_year]
            df = df.sort_values('year_id')
        elif direction=='forward':
            df = df.loc[df.year_id >= ref_year]
            df = df.sort_values('year_id', ascending=False)    
        else:
            print('direction argument must be either "forward" or "backwards"')
            return
        
        # Process here is really similar to interpolation but simplified, as we don't need
        # to shift the slope to match model estimates at both ends
        lincoms = df['lincom']
        lincom_ref = lincoms.iloc[-1]
        
        if xform=='log':
            ref = np.log(df['mean'].iloc[-1])
            df['pred'] = np.exp(lincoms + ref - lincom_ref)
        elif xform=='logit':
            ref = logit(df['mean'].iloc[-1])
            df['pred'] = expit(lincoms + ref - lincom_ref)
        else:
            ref = df['mean'].iloc[-1]
            df['pred'] = lincoms + ref - lincom_ref        
       
        df['ref_year'] = int(ref_year)
        
        if direction=='backwards':
            return df[return_vars].iloc[:-1]
        else:
            df = df.sort_values('year_id')
            return df[return_vars]
    
    return fts_main(df, ref_years)




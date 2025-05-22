import ast
import numpy as np
import pandas as pd
import polars as pl
import polars.selectors as cs
import pymysql 
import os 
import sys
import matplotlib.pyplot as plt

from scipy.special import logit, expit
from scipy.stats import norm
from db_queries import get_covariate_estimates, get_demographics, get_ids, get_location_metadata, get_population
from get_draws.api import get_draws

import fill_time_series as fill



def set_ids():   

    meids = {
        "intest_parent": 2523,
        "typhoid_prop": 23991,
        "paratyphoid_prop": 23992,
        "intest_high_burden": 10140,
        "intest_low_burden": 10139,
        "pr_para": 1252,
        "typhoid_inf_mod": 1249,
        "typhoid_inf_sev": 1250,
        "typhoid_abdom_sev": 1251,
        "typhoid_gastric_bleeding": 3134,
        "paratyphoid_inf_mild": 1253,
        "paratyphoid_inf_mod": 1254,
        "paratyphoid_inf_sev": 1255,
        "paratyphoid_abdom_mod": 1256,
        "ints_high_burden": 20291,
        "ints_low_burden": 20292,
        "ints_total_inf_sev": 19680,
        "ints_hiv_inf_sev": 28000,
        "ints_no_hiv_inf_sev": 196800,
        "ints_hiv_prop": 27540
    }

    cids = {
        "typhoid": 319,
        "paratyphoid": 320,
        "ints": 959,
        "ints_no_hiv": 959,
        "ints_hiv": 9999,
        "ints_total": 9959
    }
    return cids, meids 



if __name__ == '__main__':
    dx = False
    
    if not dx:
        loc = int(sys.argv[1])
        release = int(sys.argv[2])
        level_3 = sys.argv[3]
        sdi_coefs = sys.argv[4]
        ind_split = sys.argv[5]
    else:
        release = 16
        loc = 101
        level_3 = 'ints' #'ints' # ints or intest
        sdi_coefs = '{6:-1.1972930275724836}'
        ind_split = False

        
    input_dir  = f'FILEPATH'
    output_dir = f'FILEPATH'

    cod_demog = get_demographics(gbd_team="cod", release_id = release)
    epi_demog = get_demographics(gbd_team="epi", release_id = release)
    
    if sdi_coefs != None:
        sdi_coefs = ast.literal_eval(sdi_coefs)   

    cids, meids = set_ids()




def main(loc):
    print(f'Processing estimates for location {loc}')
    
    # We apply custom split to Indian subnationals with typhoid -- determine if location is in IND
    if level_3 == 'intest' and ind_split:
        loc_meta = get_location_metadata(35, release_id = release)
        is_ind = (loc in loc_meta[loc_meta.path_to_top_parent.str.contains(',163,')]['location_id'].tolist())

        if is_ind:
            in_loc = loc
            loc = 163
    else:
        is_ind = False
        
    
    # Determine if we are pulling from high or low burden DisMod model & if we are using natural hx or CODEm for cod
    # and set the MEID accoridngly
    is_high_burden, is_codem, codem_model_ids = get_source_categories(loc, level_3)
    meids['inc'] = int(np.where(is_high_burden == True, meids[f'{level_3}_high_burden'], meids[f'{level_3}_low_burden']))
       
    # Get the draws from DisMod models for total incidence (typhoid + paratyphoid) and the PR_typhoid models
    inc,  inc_model_version  = get_melted_draws(meids['inc'], 'incidence')
    
    model_versions = pd.DataFrame(data = {'parameter': ['inc'], 'tool': ['dismod'], 'model_version_id': [inc_model_version]})
    
    
    if level_3 == 'intest':
        # Interpolate incidence and PR_paratyphoid draws
        inc     = interpolate_draws(inc, meids['inc'], inc_model_version, release, 'incidence', 'log', 
                                use_covars = True)
    
        # Create df with PR_paratyphoid draws, and append PR_typhoid draws, calculate as 1 - PR_paratyhoid
        pr_sdi_coefs = {18:0}
        pr_para, pr_para_model_version = get_melted_draws(meids['pr_para'], 'pr_cause')
        pr_para = interpolate_draws(pr_para, meids['pr_para'], pr_para_model_version, release, 'pr_cause', 
                                    'logit', use_covars = False, sdi_coefs = pr_sdi_coefs)
        cause_split = pd.concat([pr_para, pr_para], keys = ['paratyphoid', 'typhoid'], names = (['cause', 'row_id'])).reset_index().drop(columns = 'row_id')
        cause_split.loc[cause_split.cause == 'typhoid', 'pr_cause'] = 1 - cause_split['pr_cause']
        
        model_versions = pd.concat([model_versions, pd.DataFrame(data = {'parameter': ['pr_para'], 'tool': ['dismod'], 'model_version_id': [pr_para_model_version ]})])
        del pr_para
    else:
        # Interpolate incidence
        inc     = interpolate_draws(inc, meids['inc'], inc_model_version, release, 'incidence', 'log', 
                                 use_covars = False, sdi_coefs = sdi_coefs) 
        
       # Perform HIV splits here
        hiv_pafs = get_hiv_pafs(inc, loc, release)
        cause_split = pd.concat([hiv_pafs, hiv_pafs], keys = ['ints_hiv', 'ints_no_hiv'], names = (['cause', 'row_id'])).reset_index().drop(columns = 'row_id')
        cause_split.loc[cause_split.cause == 'ints_no_hiv', 'pr_cause'] = 1 - cause_split['pr_cause']
        cause_split.loc[cause_split.cause == 'ints_total', 'pr_cause'] = 1 
        
    # Merge incidence and serotype/HIV splits 
    inc = inc.merge(cause_split, on = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'], how = 'outer')  
    #del cause_split
    
    # Apply serovar split 
    inc['incidence'] = inc.incidence * inc.pr_cause 
    inc = inc.drop(columns = 'pr_cause')
    
    # Apply age-restrictions
    inc.loc[inc.age_group_id == 2, 'incidence'] = 0                   # given incubation period typhoid not possible in first week of life
    inc.loc[inc.age_group_id == 3, 'incidence'] = inc['incidence']/2  # given incubation period only half of this age group is at risk

    
    ######## ADD THIS COVERSION TO AN EARLIER FUNCTION ###
    inc = pl.from_pandas(inc)
    cause_split = pl.from_pandas(cause_split)
        
    # There's a custom process for estimating Indian subnationals -- run this if loc is India
    if level_3 == 'intest' and is_ind:
        print('Performing IND split')
        inc = split_india(inc, in_loc)
        loc = in_loc
    else:
        print('Not performing IND split')

        

    # Produce cod draws
    if is_codem:
        if level_3 == 'intest':
            cod_draws = pd.concat([get_cod_draws(cids['typhoid'], codem_model_ids[0]),
                                   get_cod_draws(cids['typhoid'], codem_model_ids[1]),
                                   get_cod_draws(cids['paratyphoid'], codem_model_ids[2]),
                                   get_cod_draws(cids['paratyphoid'], codem_model_ids[3])])
        else:
          
            cod_draws = pd.concat([get_cod_draws(cids['ints_no_hiv'], codem_model_ids[0]),
                                   get_cod_draws(cids['ints_no_hiv'], codem_model_ids[1])])
        
       
            
        cod_draws = pl.from_pandas(cod_draws)
        model_versions = pd.concat([model_versions, pd.DataFrame(data = {'parameter': ['cod']*len(codem_model_ids), 
                                                                        'tool': ['codem']*len(codem_model_ids), 
                                                                        'model_version_id': codem_model_ids})])

    else:   
        cf_draws = get_case_fatality_draws(loc, level_3)
        cod_draws = calc_natural_hx(inc, cf_draws)
       
        del cf_draws
   
    # Calculate total of hiv-attributable and not hiv-attributable for iNTS here
    if level_3 == 'ints':
        cod_total = cod_draws.drop('cause_id').group_by(['location_id', 'year_id', 'age_group_id', 'sex_id']).agg([cs.starts_with('draw_').sum()])
        cod_total = cod_total.with_columns(cause_id = pl.lit(cids['ints_total']))
        cod_draws = pl.concat([cod_draws, cod_total], how = 'diagonal_relaxed')
 
        inc_total = inc.drop('cause').group_by(['location_id', 'year_id', 'age_group_id', 'sex_id', 'draw']).agg(pl.sum('incidence'))
        inc_total = inc_total.with_columns(cause = pl.lit('ints_total'))
        inc = pl.concat([inc, inc_total], how = 'diagonal_relaxed')
        inc = inc.drop('measure_id')
    else:
        inc_total = inc.drop('cause').group_by(['location_id', 'year_id', 'age_group_id', 'sex_id', 'draw']).agg(pl.sum('incidence'))
        inc_total = inc_total.with_columns(cause = pl.lit('intest'))
        inc = pl.concat([inc, inc_total], how = 'diagonal_relaxed')
        inc = inc.drop('measure_id', strict = False)
 
 
    # Export CoD draws
    export_cod(cod_draws)
    
    # Perform sequela splits
    if level_3 == 'ints':
        epi_draws = split_sequelae(inc, cod = cod_draws, cod_sequela = 'inf_sev', prop = cause_split)
    else:
        epi_draws = split_sequelae(inc, prop = cause_split)

  
    ### Export the non-fatal draws
    export_epi(epi_draws)

    ### Export input model version information (for record-keeping purposes)
    model_versions.to_csv(os.path.join(output_dir, 'model_info', f'{loc}.csv')) 
    print('..Done.')
    
 




def get_latest_dr_codem_models(cause_id, sex_id, release, loc):
    varlist = ['model_version_id', 'cause_id', 'sex_id', 'date_inserted']
    db = pymysql.connect(host = 'ADDRESS', user = 'USERNAME', password = 'PASSWORD', database = 'DATABASE') 
    
    with db:
        with db.cursor() as cursor:
            cursor.execute(f'SELECT {", ".join(varlist)} FROM model_version \
            WHERE cause_id = {cause_id} AND sex_id = {sex_id} AND model_version_type_id = 2 AND status IN (1,2) AND release_id = {release}') 
    
            # Fetch all rows of data, put them in a data frame and add column names
            cod_models = pd.DataFrame(cursor.fetchall(), columns = varlist)
    
            latest = cod_models.groupby(['cause_id', 'sex_id'])['model_version_id'].max().item()

        # Determine if the target location is included in the model
        with db.cursor() as cursor:
            cursor.execute(f'SELECT location_id FROM model WHERE model_version_id = {latest}') 
    
            # Fetch all rows of data, put them in a data frame
            cod_model_locations = pd.DataFrame(cursor.fetchall())

            loc_in_codem =  loc in cod_model_locations.iloc[:, 0].unique()
    
    return latest, loc_in_codem



"""
Context:
We have seperate DisMod models for high and low/moderate burden regions.
Similarly, we have seperate sources/methods for mortality estimates: 
for data rich locations (CoD team definition) we estimate mortality using
a CODEm model; while for other locations we estimate mortality using a 
natural history approach (i.e. mortality = incidence * case fatality).

Purpose:
Determine which DisMod model to use for non-fatal estimates and which approach
to use for fatal estimates

Arguments:
loc = int; location_id for the location for which you need to know the model category
level_3 = str; the level 3 cause (intest for typhoid and paratyphoid; ints for iNTS)

Expected variables in containing environment:
release = int; the release_id

Dependencies:
get_location_metadata from the db_queries library
pandas

Returns: 
is_high_burden: bool; if True use high burden DisMod model; if False use low burden model
is_codem: bool; if True get CoD estimates from CODEm; if False use natural hx model
"""

def get_source_categories(loc, level_3):
    # Determine if the location is high or low burden (we use different DisMod models for each and need to know which to pull estimates from)
    # Category is based on super-region, with three exceptions: 
    #  1) Haiti (114) is treated as high-burden since it is epidemiologically more like W.Africa than the Carribean;
    #  2) Sri Lanka (17) is treated as low-burden as it is an outlier in S Asia with regard to enteric infections;
    #  3) East Asia is treated as low burden since we have reliable VR data feeding into the low burden models from several E Asian locations
        
    loc_meta = get_location_metadata(35, release_id = release).query(f'location_id == {loc}')      
      
    # Determine if the location is data rich (we'll use CODEm estimates for mortality if data rich, and natural hx approach otherwise)
    dr_locs = get_location_metadata(43, release_id=release).query("parent_id == 44640")
    
    
    is_high_burden = ((((loc_meta.super_region_id.isin([4, 158, 166, 137])) & (loc_meta.region_id != 5)) \
                       & ((level_3 != 'intest') | (loc_meta.location_id != 17))) \
                      | ((level_3 == 'intest') & (loc_meta.location_id == 114))).bool() 
    print(f'..This location is classified as high burden = {is_high_burden}')
    
    is_codem = (loc in dr_locs.location_id.tolist()) & (is_high_burden == False)
    
    # If it's listed as data rich and is not high burden, check to make sure location was included
    # in the CODEm model
    if is_codem:
        if level_3 == 'intest':
            codem_model_info = [get_latest_dr_codem_models(cids['typhoid'], 1, release, loc), 
                                get_latest_dr_codem_models(cids['typhoid'], 2, release, loc),
                                get_latest_dr_codem_models(cids['paratyphoid'], 1, release, loc), 
                                get_latest_dr_codem_models(cids['paratyphoid'], 2, release, loc)]

        else:
            codem_model_info = [get_latest_dr_codem_models(cids['ints_no_hiv'], 1, release, loc), 
                                get_latest_dr_codem_models(cids['ints_no_hiv'], 2, release, loc)]
            
        
        codem_model_ids, loc_in_codem = zip(*codem_model_info)
        is_codem = is_codem & min(loc_in_codem) 
    else:
        codem_model_ids = -9999
        
    print(f'..This location gets CoD draws from CODEm = {is_codem}')
    
    return is_high_burden, is_codem, codem_model_ids


"""
Context:
We need to pull DisMod estimates and reshape to long (much easier and faster
to work with)

Purpose:
Pull DisMod model draws, reshape to long

Arguments:
meid = int; Modelable entity for which you want to pull draws
value_name = str; the name you'd like to assign to the variable
    containing estimated values

Expected variables in containing environment:
release = int; the release_id
loc = int; location_id for the location for which you need to know the model category
epi_demog = dict; dictionary returned by get_demographics for epi

Dependencies:
get_draws from the get_draws library
pandas

Returns: 
long = pd.df; dataframe of draws
model_version = int; the model version for which draws were pulled

To do: consider recoding with Polars instead of Pandas for speed and consistency
"""

def get_melted_draws(meid, value_name):
    print(f'..Pulling DisMod draws for {value_name}')

    # Get the draws from the database
    df = get_draws("modelable_entity_id", meid, source = "epi", location_id = loc, 
                   age_group_id = epi_demog['age_group_id'], sex_id = epi_demog['sex_id'], 
                   release_id = release, status = "best")
    
    # Extract the model version id -- we'll return this and use later to ensure that we pull the correct interpolation file
    model_version = int(df['model_version_id'].mode())

    # Reshape to long (from a variable for every draw to a draw variable and a value variable
    draw_vars = df.filter(regex="draw_").columns.tolist()
    id_vars = ['age_group_id', 'sex_id', 'year_id', 'location_id']

    long = pd.melt(df, id_vars = id_vars, value_vars = draw_vars, 
                   value_name = value_name, var_name = "draw")
    
    # We want to merge to the ref_year of the interpolated trend, so renaming variable here
    long = long.rename(columns = {'year_id': 'ref_year'})
    
    return long, model_version



"""
Arguments:
- cid = int; Cause ID for which you want to pull draws
- version_id = int; CODEm model version id
- min_age = int; age_group_id for which we estimate no deaths

Expected variables in containing environment:
- release = int; the release_id
- loc = int; location_id for the location for which you need to know the model category
- cod_demog = dict; dictionary returned by get_demographics for CoD

Dependencies:
- get_draws from the get_draws library
- pandas

Returns: 
- df = pd.df; dataframe of draws

To do:
- consider recoding with Polars instead of Pandas for speed and consistency
"""


def get_cod_draws(cid, version_id, min_age = 3):
    print(f'..Pulling CoD draws from CODEm model {version_id}')

    # Get the draws from the database
    df = get_draws("cause_id", cid, source = "codem", location_id = loc, 
                   age_group_id = cod_demog['age_group_id'], 
                   release_id = release, version_id = version_id)
        
    # Produce lists of desired draw and index varibles
    draw_vars = df.filter(regex="draw_").columns.tolist()
    id_vars = ['age_group_id', 'sex_id', 'year_id', 'location_id', 'cause_id']
    
    
    # Get_draws returns no data for restricted ages, but uploader requires them.
    # Here we find missing age groups and add them if it's a restricted age group
    # We throw a warning otherwise
    missing_ages = list(set(cod_demog['age_group_id']) - set(df.age_group_id))

    for age in missing_ages:
        if age <= min_age:
            tmp = df[df.age_group_id == 10]
            tmp['age_group_id'] = age
            df = pd.concat([df, tmp], ignore_index = True)
        else:
            print(f'WARNING: missing CODEm draws for age group {age}')
        
    # Convert counts to rates
    for draw_var in draw_vars:
        df[draw_var] = df[draw_var] / df['population']
        df.loc[df.age_group_id <= min_age, draw_var] = 0  # applying age restriction
    
    df = df[id_vars + draw_vars]
     
    return df



def interpolate_draws(df, meid, model_version, release, value_name, xform = None, use_covars = True, sdi_coefs = sdi_coefs):
    # Read in the trend file with the full interpolated time series of point estimates
    trend, status = fill.fill_dismod_trend(meid, model_version, release, loc, use_covars = use_covars, sdi_coefs = sdi_coefs)
    #trend = pd.read_csv(os.path.join(input_dir, f'trend_{meid}_{model_version}.csv'))
    
    # Retain only the current location
    trend = trend.loc[trend.location_id==loc, ]

    # Find the mean of the draws by each demographic
    mean_of_draws = df.groupby(['location_id', 'ref_year', 'age_group_id', 'sex_id'])[value_name].mean().reset_index(name='mean_of_draws')
                     
    # Merge everything together    
    df = df.merge(mean_of_draws, on = ['location_id', 'ref_year', 'age_group_id', 'sex_id'], how= 'outer') \
           .merge(trend, on = ['location_id', 'ref_year', 'age_group_id', 'sex_id'], how = 'outer')

    # Transform, shift, and back-trasform the draws
    if xform == 'log':
        print(f'..Performing log interpolation of {value_name}')
        df[value_name] = np.exp(np.log(df[value_name]) + np.log(df['pred']) - np.log(df['mean_of_draws']))
    elif xform == 'logit':
        print(f'..Performing logit interpolation of {value_name}')
        df[value_name] = expit(logit(df[value_name]) + logit(df['pred']) - logit(df['mean_of_draws']))
    else:
        print(f'..Performing linear interpolation of {value_name}')
        df[value_name] = df[value_name] + df['pred'] - df['mean_of_draws']

    # Keep and return only necessary columns    
    df = df.drop(columns = ['mean_of_draws', 'mean', 'pred', 'ref_year'])    
    return df


def get_case_fatality_draws(loc, level_3):
    print('..Getting case fatality draws')
    
    if level_3 == 'intest':
        # Case fatality varies based on income category.  
        # For most locations, pull income category from the database;
        # for locations with no value in database, hard code here   

        if loc in [8, 369, 374, 413]:
            income = "Upper middle income"
        elif loc in [320]:
            income = "High income, nonOECD"
        else:  
            loc_meta = get_location_metadata(35, release_id = release)
            country_id = int(loc_meta.loc[loc_meta.location_id==loc]['path_to_top_parent'].str.split(",").tolist()[0][3])

            # Connect to the shared database, open the cursor, and execute the SQL query
            db = pymysql.connect(host = "ADDRESS", user = "USERNAME", password = "PASSWORD", database = "DATABASE") 

            # Execute the SQL query and fetch all matching data
            with db:
                with db.cursor() as cursor:
                    cursor.execute(f"SELECT location_metadata_value AS income, location_metadata_version_id \
                                   FROM location_metadata_history WHERE location_metadata_type_id = 12 AND location_id = {country_id}")
                    covar_data = pd.DataFrame(cursor.fetchall(), columns = ['income', 'version'])

                # The query returns multiple verisons -- keep only the most recent
                income = covar_data.sort_values("version", ascending = False).iloc[0,0] 

        # Need to replace all spaces in income category with underscores to match file names
        income = income.replace(" ", "_").replace(",", "")

        # Read in the correct file and return the df
        cf_draws = pd.read_csv(os.path.join(input_dir, f'cfDrawsByIncomeAndAge_{income}.csv'))

    elif level_3 == 'ints':
        cf_draws = pd.read_csv(os.path.join(input_dir, 'cfrEstimates_2020.csv')).query(f'location_id == {loc}')
        cf_draws['key'] = 1

        cf_error = pd.DataFrame({'cf_error': np.random.normal(0, 1, 1000), 'draw': [f'draw_{i}' for i in range(1000)], 'key':1})
        cf_draws = cf_draws.merge(cf_error, on = 'key', how = 'left').drop(columns = 'key')
        
        cf_draws['case_fatality'] = expit(cf_draws['logitPred'] + (cf_draws['logitPredSeSm'] * cf_draws['cf_error']))
        
        cf_draws['cause']  = np.where(cf_draws['estPrHiv'] == 0, 'ints_no_hiv', 'ints_hiv')
        cf_draws = cf_draws[['location_id', 'year_id', 'age_group_id', 'draw', 'case_fatality', 'cause']]
    else:
        print(f'Can\'t pull case fatality estimates for "{level_3}". Value of level_3 must be either "intest" or "ints".')
    
    cf_draws = pl.from_pandas(cf_draws)
    return cf_draws




# HIV split
def get_hiv_pafs(df, loc, release):
    hiv_prev = get_covariate_estimates(49, location_id = loc, year_id = cod_demog['year_id'], age_group_id = cod_demog['age_group_id'], release_id = release) 
    hiv_prev = hiv_prev[['location_id', 'year_id', 'age_group_id', 'sex_id', 'mean_value', 'lower_value', 'upper_value']]
    hiv_prev = hiv_prev.rename(columns = {'mean_value':'hiv_prev_mean', 'lower_value':'hiv_prev_lower', 'upper_value':'hiv_prev_upper'})

    df = df.merge(hiv_prev, on = ['location_id', 'year_id', 'age_group_id', 'sex_id'], how = 'left')

    df['sigma'] = (df['hiv_prev_upper'] - df['hiv_prev_lower']) / ( 2 * norm.ppf(0.975))
    df['alpha'] = df['hiv_prev_mean'] * (df['hiv_prev_mean'] - df['hiv_prev_mean']**2 - df['sigma']**2) / df['sigma']**2
    df['beta'] = df['alpha'] * (1 - df['hiv_prev_mean']) / df['hiv_prev_mean']
    df.loc[(df['beta'] > 0.999e+8) | (df['beta'].isna()), 'alpha'] = df['hiv_prev_mean'] * 0.999e+8 
    df.loc[(df['beta'] > 0.999e+8) | (df['beta'].isna()), 'beta'] = 0.999e+8

    df['gammaA'] = np.random.gamma(df['alpha'])
    df['gammaB'] = np.random.gamma(df['beta'])
    df['hiv_prev'] = df['gammaA'] / (df['gammaA'] + df['gammaB'])

    rr = pd.read_csv(os.path.join(input_dir, 'hivRRs_2020.csv')).query(f'location_id == {loc}')
    rr_error = pd.DataFrame({'rr_error': np.random.normal(0, 1, 1000), 'draw': [f'draw_{i}' for i in range(1000)]})

    df = df.merge(rr, on = ['location_id', 'year_id', 'age_group_id', 'sex_id'], how = 'left') \
           .merge(rr_error, on = 'draw', how = 'left')

    df['rr'] = np.maximum(np.exp(df['rr_error'] * df['lnHivRrSe'] + df['lnHivRrMean']), 1)
    df['pr_cause'] = df['hiv_prev'] * (df['rr'] - 1) / (df['hiv_prev'] * (df['rr'] - 1) + 1)
    
    df = df[['location_id', 'year_id', 'age_group_id', 'sex_id', 'draw', 'pr_cause']]
    
    return df



def calc_natural_hx(df, cf_draws):
    print('..Calculating mortality using natural history approach')

    # Find common variables in incidence and case fatality data sets on which to merge
    merge_vars = list(set(df.columns) - (set(df.columns) - set(cf_draws.columns)))
    df = df.join(cf_draws, on = merge_vars, how = 'left')

    df = df.with_columns((pl.col('incidence') * pl.col('case_fatality')).alias('mortality'))
    df = df.with_columns(pl.when(pl.col('age_group_id') < 3).then(0).otherwise(pl.col('mortality')).alias('mortality'))
    
    cid_df = pl.from_dict(cids).transpose(include_header = True, header_name = 'cause', column_names = ['cause_id'])
    df = df.join(cid_df, on = 'cause', how = 'left')
    
    df = df.pivot(index = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'cause_id'], 
             on = 'draw', values = 'mortality') 
    
    return df



def calc_inc_from_cod(cod_draws, cf_draws):
    print('..Calculating incidence using reverse natural history approach')

    # Reshape to long (from a variable for every draw to a draw variable and a value variable
    draw_vars = df.filter(regex="draw_").columns.tolist()
    id_vars = ['age_group_id', 'sex_id', 'year_id', 'location_id']

    long = pd.melt(df, id_vars = id_vars, value_vars = draw_vars, 
                   value_name = value_name, var_name = "draw")
    
    # Find common variables in incidence and case fatality data sets on which to mergedf = inc
    merge_vars = list(set(df.columns) - (set(df.columns) - set(cf_draws.columns)))
    df = df.join(cf_draws, on = merge_vars, how = 'left')

    df = df.with_columns((pl.col('incidence') * pl.col('case_fatality')).alias('mortality'))
    df = df.with_columns(pl.when(pl.col('age_group_id') < 3).then(0).otherwise(pl.col('mortality')).alias('mortality'))
    
    cid_df = pl.from_dict(cids).transpose(include_header = True, header_name = 'cause', column_names = ['cause_id'])
    df = df.join(cid_df, on = 'cause', how = 'left')
    
    df = df.pivot(index = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'cause_id'], 
             on = 'draw', values = 'mortality') 
    
    return df



def export_cod(df):
    print('..Exporting CoD draws')
    
    for loc_cause, out_data in df.group_by(['location_id', 'cause_id']):
        out_data.write_csv(os.path.join(output_dir, str(loc_cause[1]), f"{loc_cause[0]}.csv"))



def split_india(df, in_loc):
    print("..Performing India subnational split")
    ind_split = pl.read_csv(os.path.join(input_dir, 'indiaSplit_ur_cod.csv'))

    print('....Getting populations')
    ind_pop = pl.from_pandas(get_population(location_id = list(np.unique(ind_split['location_id'])),  year_id = cod_demog['year_id'],
                             age_group_id = cod_demog['age_group_id'], sex_id = cod_demog['sex_id'], release_id = release).drop(columns = ['run_id']))

    ind_split = ind_split.join(ind_pop, on = 'location_id', how = 'left', validate = '1:m')

    print('....Joining incidence and split file')
    df = df.drop('location_id')
    df = df.join(ind_split, on = ['year_id', 'age_group_id', 'sex_id'], how='full', coalesce=True)

    print('....Calculating and adjusting cases')
    df = df.with_columns(
        (pl.col('incidence') * pl.col('population')).alias('cases'),
        (pl.col('incidence') * pl.col('population') * pl.col('rr')).alias('cases_adj'))

    totals = df.group_by(['year_id', 'draw']).agg([
        pl.sum('cases').alias('total_cases'), 
        pl.sum('cases_adj').alias('total_cases_adj')])

    # Keep only rows for the current location
    df = df.filter(pl.col('location_id') == in_loc)
    
    # Join the aggregated results back to the original DataFrame
    df = df.join(totals, on=['year_id', 'draw'], how='left')
    
    
    print('....Wrapping up subnational split')
    df = df.with_columns(((pl.col('cases_adj') / pl.col('population')) * (pl.col('total_cases') / pl.col('total_cases_adj'))).alias('incidence'))
    df = df.select(['age_group_id', 'sex_id', 'draw', 'incidence', 'year_id', 'cause', 'location_id'])
    
    return df

def split_sequelae(df, cod = None, cod_sequela = None, epi_years = True, value_name = 'incidence', prop = None, prop_value_name = 'pr_cause'):
    splits = pl.read_csv(os.path.join(input_dir, "sequela_splits.csv"))
    print('..Split file read successfullly')
    
    # Keep only epi years (default behaviour)
    if epi_years:
        df = df.filter(pl.col('year_id').is_in(epi_demog['year_id']))

    # Merge the sequela splits into the full df 
    df = df.join(splits, on = ['cause', 'draw'], how = 'left')

    # Do the math to perform sequela splits
    df = df.with_columns((pl.col(value_name) * pl.col('pr') * pl.col('duration')).alias('value'))
    
    
    # If a dataframe of proportions is included in arguments, prep that dataframe here
    if prop is not None:
        print("....Including proportional split estimates in epi output")
        
        if epi_years:
            prop = prop.filter(pl.col('year_id').is_in(epi_demog['year_id']))
         
        prop = prop.with_columns(pl.lit(18).alias('measure_id'), 
                                 pl.lit('prop').alias('state'))
        
        prop = prop.rename({prop_value_name: 'value'})
        df = pl.concat([df, prop], how = 'diagonal_relaxed')
                      
            
    # Need to reshape to wide to correctly format for uploader -- prepping variables first
    df = df.with_columns((pl.col('cause') + pl.lit("_") + pl.col('state')).alias('sequela'))

    # Perform the reshape -- will get n draw files where n is the number of draws (usually 1,000)
    df = df.pivot(index = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'sequela', 'measure_id'], 
                  on = 'draw', values = 'value')

    # If a COD dataframe is included in arguments, prep that dataframe here
    if cod is not None:
        print("....Including CoD estimates as CSMR in epi output")
        cid_df = pl.from_dict(cids).transpose(include_header = True, header_name = 'sequela', column_names = ['cause_id'])
        cid_df = cid_df.with_columns(pl.col('sequela') + pl.lit(f'_{cod_sequela}'))
        
        if epi_years:
            cod = cod.filter(pl.col('year_id').is_in(epi_demog['year_id']))
            
        cod = cod.join(cid_df, on = 'cause_id', how = 'left').drop('cause_id')
        cod = cod.with_columns(measure_id = pl.lit(15))
        
        df = pl.concat([df, cod], how = 'diagonal_relaxed')
        

        
    print('..Splits complete')
    return df



def export_epi(df):
    print('..Exporting non-fatal draws')
    
    # Keep only rows with sequela for which we have meids
    df = df.filter(pl.col('sequela').is_in(meids.keys()))
    
    # As per uploader requirements, we need to save a seperate file for each sequela 
    # We'll group the df by sequela, and then loop over those groups, prepping, and saving the file 
    for loc_seq, out_data in df.group_by(['location_id', 'sequela']):
        out_data = out_data.drop("sequela")
        out_data = out_data.with_columns(modelable_entity_id = pl.lit(meids[loc_seq[1]]))
        
        out_data.write_csv(os.path.join(output_dir, str(meids[loc_seq[1]]), f"{loc_seq[0]}.csv"))



if __name__ == '__main__':   
    main(loc)


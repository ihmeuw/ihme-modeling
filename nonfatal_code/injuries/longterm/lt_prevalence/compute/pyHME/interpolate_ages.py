"""This module contains functions related to imputing single-year-age-group 
values from GBD-age-group values."""
    
import pandas as pd

def get_permutations_of_demogs(demog_dict,ages=None):
    """
    Return a list of tuples such that each permutation of the various values 
    of demographic variables (iso3,year,sex) included in demog_dict shows up in 
    the list. If ages is passed as a list, these are included in the variables 
    to permute.
    """
    
    ix_values = []
    
    if 'iso3' in demog_dict.keys():
        for i in demog_dict['iso3']:
            for y in demog_dict['year']:
                for s in demog_dict['sex']:
                    if ages:
                        for a in ages:
                            ix_values.append((i,y,s,a))
                    else:
                        ix_values.append((i,y,s))
                        
    else:
        for y in demog_dict['year']:
            if ages:
                for a in ages:
                    ix_values.append((y,a))
            else:
                    ix_values.append((y,))
    return ix_values

def get_ipolated_age_df(demog_dict,ages,index_names = ['year','age']):
    """
    This function gives you a DataFrame of ages, indexed by year.
    """
    ix_values = get_permutations_of_demogs(demog_dict,ages=ages)
    index = pd.MultiIndex.from_tuples(ix_values,names=index_names)
    result = pd.DataFrame(index=index)
    result['age_mdpt'] = result.index.get_level_values('age')
    return result

def get_cases(df,old_prefix='inc',new_prefix='cases',to_cases=True):
    """Return a dataframe with columns for cases draws, starting with inc or mort 
    draws and pop"""
    result = df.copy()
    cols = result.filter(like=old_prefix).columns
    cases_cols = [i.replace(old_prefix,new_prefix) for i in cols]
    if to_cases:
        result[cases_cols] = result[cols].mul(result['pop'],axis=0)
    else:
        result[cases_cols] = result[cols].div(result['pop'],axis=0)
    return result
    
def map_single_age_to_age_group(age, age_grps_dict):
    """ Function that takes an individual single-year age and assigns it to an 
    age group. Will raise an error if you have any age == 0, since these 
    can't be deterministically mapped to a GBD age group."""
    assert age >= 1 or age in [0,0.1,0.01], """You have passed a single-year age of %g, which cannot be
                        deterministically mapped to a GBD age group.""" %age
    for i in age_grps_dict.keys():
        if age >= age_grps_dict[i][0] and age < age_grps_dict[i][1]: return i
    
    # this error will only happen if your age was unable to be mapped
    raise AssertionError("age %g was unable to be mapped" %age)
    
def map_age_to_age_grp(df,ages_dict):
    result = df.copy()
    result = result.reset_index(level='age')
    result['age_grp'] = result['age'].apply(map_single_age_to_age_group,args=[ages_dict])
    result = result.set_index('age_grp',append=True)
    return result
    
def squeeze_rates(s_age_df,age_grp_df,age_dict,prefix='inc',index_names=['year','age']):
    """Squeeze interpolated rates of s_age_df to the original rates by age group
    in age_grp_df"""

    # get cases for both dfs
    result = get_cases(s_age_df,old_prefix=prefix)
    orig_cases = get_cases(age_grp_df,old_prefix=prefix)
    
    # map midpoint ages in orig_cases back to correct age group name
    orig_cases = map_age_to_age_grp(orig_cases,age_dict)
    
    # get gbd age groups for each age in s_age
    result = map_age_to_age_grp(result,age_dict)
    
    # sum cases for each group and divide original GBD-age-group ages by 
    # collapsed single-year-age-group cases to get scaling ratio
    collapsed_cols = [i for i in result.filter(like='cases').columns]
    collapsed_cases = result[collapsed_cols].groupby(level=range(len(index_names))).sum()
    # calculate ratios, but make sure to change results to 0 when dividing by 0
    ratios = orig_cases.filter(like='cases').div(collapsed_cases.filter(like='cases'))
    ratios = ratios.rename(columns = lambda x: x.replace('cases','rat'))
    ratios = ratios.fillna(0.)
    
    # multiply incidences in s_age_df by this ratio
    result = result.join(ratios)
    rat_temp = result.filter(like='rat').rename(columns = lambda x: x.replace('rat',prefix))
    new_values = result.filter(like=prefix).mul(rat_temp)
    ages = result['age'].reset_index(drop=True)
    new_values = new_values.reset_index()
    result = new_values.join(ages).set_index(index_names).drop('age_grp',axis=1)

    return result
    

def ipolate_to_single_year_ages(df,prefix,pop_grp,pop_s_df,
                                age_grp_dict):
    """
    Inputs:
        DataFrame df: contains the columns you wish to interpolate. Indexed by 
            [year,age] (where age contains GBD age groups, including the 
            aggregated terminal age group (80+). Currently, the script cannot
            handle the disaggregated elderly age groups, which are just dropped 
            if they exist. This is b/c we don't have excess mortality or pop data
            for the disaggregated elderly groups.
        str prefix: prefix of columns you wish to interpolate (i.e. 'draw')
        Series pop_grp: contains populations for the GBD age-groups. B/c these 
            pops don't officially exist for the disaggregated older groups, this
            series will only contain the 80+ older group. MultiIndex organized 
            in the following way [year,age]
        Series pop_s: contains populations for the ages in result_ages, with
            MultiIndex organized in the following level [year,age]. The 
            ages here should be the midpoints of the single-year age groups you 
            want to interpolate. They should not contain <1 ages.
        dict age_grp_dict: maps GBD-age-group names to their start and end ages
            
    Outputs:
        DataFrame where the 'age' column now contains ages in result_ages and 
        the values of columns with prefix 'prefix' are the interpolated values. 
        Indexed by year.
    """
    # make age a column as well as index to facilitate mapping
    result = df.copy()
    result['age'] = result.index.get_level_values('age')
        
    # assert that pop_grp doesn't contain the disaggregated older age groups 
    # (will break code)
    # if they are present in the incidence data (though not in excess mort, pop, etc.)
    # map single-year populations to midpoint ages and 
    # drop both under 1 ages and terminal age group 
    # since contained in age_grp pops
    pop_s = pop_s_df.reset_index(level='age')
    pop_s = pop_s[(pop_s['age'] >= 1) & (pop_s['age'] != max(pop_s['age']))]
    pop_s['age'] = pop_s['age'] + .5
    pop_s = pop_s.set_index('age',append=True)
    
    # Get ages you want to interpolate
    result_ages = list(pop_s.index.levels[pop_s.index._get_level_number('age')])
    assert all([i >= 1 for i in result_ages])
    
    # get cols_to_ipolate
    cols_to_ipolate = df.filter(like=prefix).columns
    
    # get years from dataframe
    ix_values = {}
    ix_values['year'] = result.index.levels[result.index.names.index('year')]
    
    # keep under1 and terminalvalues for appending at end after interpolation
    ages_to_append = result[(result['age'] < 1) | (result['age'] == max(result['age']))].drop('age',axis=1)
    
    # merge on GBD_pops
    result = result.join(pop_grp,how='left')
    result = result.rename(columns={'pop':'pop_grp'})
    
    # change ages to midpoint age of age-interval
    result['age_mdpt'] = result['age'].map(
            lambda x: (age_grp_dict[x][0]+age_grp_dict[x][1]) / 2.)
    del result['age']
            
    # mark ages to drop after interpolation
    result['pre_interp'] = 1
    # add on ages that you want to interpolate
    result = result.append(get_ipolated_age_df(ix_values,result_ages))

    # merge on single-age-pops
    result = result.join(pop_s,how='left')
    result['pop_grp'][result['pre_interp'] != 1] = result['pop'][result['pre_interp'] != 1]
    del result['pop']
    result = result.rename(columns={'pop_grp':'pop'})
    
    
    ## INTERPOLATE
    s_age_values = pd.DataFrame()
    age_grp_values = pd.DataFrame()

    demog_permutations = get_permutations_of_demogs(ix_values)
    result = result.sort_index()
    
    for i in demog_permutations:
        df_temp = result.ix[i[0]]
        ipolated = df_temp[cols_to_ipolate].interpolate(method='values',axis=0,limit=0)
        df_temp = df_temp.drop(cols_to_ipolate,axis=1)
        df_temp = df_temp.join(ipolated)  
        
        # add back on index values
        df_temp['year'] = i[0]
        df_temp = df_temp.rename(columns={'age_mdpt':'age'}).set_index(['year','age'])
        
        # values from GBD age groups (for squeezing incident cases to)
        age_grp_temp = df_temp[(df_temp['pre_interp'] == 1) - 
                ((df_temp.index.get_level_values('age') < 1)) | 
                        (df_temp.index.get_level_values('age') == max(df_temp.index.get_level_values('age')))]
        
        # values for single-year age groups
        s_age_temp = df_temp[(df_temp['pre_interp'] != 1)]
        s_age_temp = s_age_temp.drop(['pre_interp'],axis=1)
        
        age_grp_values = age_grp_values.append(age_grp_temp)
        s_age_values = s_age_values.append(s_age_temp)

    # Re-set multi-index which was destroyed from pandas append command
    age_grp_values.index = pd.MultiIndex.from_tuples(age_grp_values.index,names=['year','age'])
    s_age_values.index = pd.MultiIndex.from_tuples(s_age_values.index,names=['year','age'])
    result = squeeze_rates(s_age_values,age_grp_values,age_dict=age_grp_dict,prefix=prefix)
    
    # change ages from midpoint to age group name (i.e. starting age)
    result = result.reset_index('age')
    result['age'] = result['age'] - .5
    result = result.set_index('age',append=True)
    
    # append under 1 and terminal age groups
    result = ages_to_append.append(result).sort_index()

    return result
    
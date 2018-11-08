import numpy as np
import pandas as pd


def standardize_years(db):
    '''Standardize years in the shocks database'''
    # Make sure that we have all the needed columns
    assert np.all([i in db.columns for i in ['year','date_start','date_end']])
    #############################  full year labeling ##########################
    valid_year_strings = [str(yr) for yr in range(1900,2018)]
    db['year_start'] = (db['date_start']
                         .apply(lambda x: str(x)[:4])
                         .apply(lambda x: float(x) if x in valid_year_strings 
                                                   else np.nan))
    print(db['year_start'].value_counts())
    # get end years
    db['year_end'] = (db['date_end']
                         .apply(lambda x: str(x)[:4])
                         .apply(lambda x: float(x) if x in valid_year_strings 
                                                   else np.nan))
    print(db['year_end'].value_counts())
    # fix one problematic row
    db.loc[db.year == '2001-2002', 'year_start'] = 2001
    db.loc[db.year == '2001-2002', 'year_end'] = 2002
    db.loc[db.year == '2001-2002', 'year'] = np.NaN

    db.loc[(db['year'].isnull()) & (db['year_start'].isnull()),
            'year'] = db.loc[(db['year'].isnull()) & (db['year_start'].isnull()),
                             'year_end']
    db.loc[(db['year'].isnull()) & (db['year_end'].isnull()),
            'year'] = db.loc[(db['year'].isnull()) & (db['year_end'].isnull()),
                             'year_start']

    db['mean_year_start_end'] = (db.loc[:,['year_start','year_end']]
                                   .mean(axis=1,skipna=False)
                                   .apply(np.floor))
    print(db['mean_year_start_end'].value_counts())
    db.loc[db['year'].isnull(),'year'] = db.loc[db['year'].isnull(),
                                                'mean_year_start_end']
    # Print some diagnostics
    print("Out of {} rows total, {} have missing 'year' field;".format(db.shape[0],
                                            db.loc[db['year'].isnull(),:].shape[0]))
    print("    {} rows have missing 'year_start' field;".format(
                                      db.loc[db['year_start'].isnull(),:].shape[0]))
    print("    {} rows have missing 'year_end' field.".format(
                                        db.loc[db['year_end'].isnull(),:].shape[0]))    
    db = db.drop(['year_start', 'year_end', 'mean_year_start_end'],
                 axis=1, errors='ignore')
    # Return the year-standardized database
    return db

def standardize_sexes_age_groups(db):
    '''Fill missing age and sex values'''
    db.sex_id = db.sex_id.fillna(3)
    db.age_group_id = db.age_group_id.fillna(22)
    if np.any(db['age_group_id'].apply(lambda x: type(x) is str)):
        db.loc[db.age_group_id == '85-90', 'age_group_id'] = 31
    db.age_group_id = db.age_group_id.astype(int)
    return db



def standardize_all_but_locs(db):

    db = standardize_years(db)
    # Standardize age groups and sexes
    db = standardize_sexes_age_groups(db)

    db.loc[(db.best.isnull()) & (db.low.notnull()), 'best'] = db['low'] 

    db['gbd_round_id'] = 5 
    db['data_type_id'] = 11 
    db['description'] = 'test GBD2016 data, year and age-group assignment.'

    db.index.name = 'shock_id'
    # Return the standardized dataframe
    return db
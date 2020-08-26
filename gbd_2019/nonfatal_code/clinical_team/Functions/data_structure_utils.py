


import pandas as pd


def test_name_switcher():

    return


def year_id_switcher(df):
    """
    Our process is moving from year start/end to year_id until we agg to 5 years
    this func should switch the cols, or if year id is present do nothing
    Params:
        df (pd.DataFrame):
            clinical data with a column identifying year
    """
    df_cols = df.columns
    good_years = ['year_start', 'year_end', 'year_id']
    
    year_cols = [y for y in df_cols if y in good_years]
    

    if 'year_id' in year_cols:
        drop_cols = [y for y in year_cols if y != 'year_id']
        df.drop(drop_cols, axis=1, inplace=True)
        return df

    if not year_cols:
        print("We can't recognize any potential year columns in the data, possible types are {}".\
                format(good_years))
        return df

    
    
    year_cols_present = set(year_cols).intersection({'year_start', 'year_end'})
    if not set(year_cols).symmetric_difference({'year_start', 'year_end'}):
        if (df['year_end'] != df['year_start']).all():
            print("Start and end values do not match. The data is aggregated in some way, "\
                "switch failed..")
            return df
        else:  
            df['year_id'] = df['year_start']
    else:
        df['year_id'] = df[list(year_cols_present)]
    df.drop(year_cols, axis=1, inplace=True)

    return df

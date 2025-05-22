# This script reads the intermediate files and assigns year & measure
# information before concatenating them into one large file.
import pandas as pd
import numpy as np
from pathlib import Path

repo = 'FILEPATH'

fp_list = ['FILEPATH'.glob('*.csv')]

# read in concatenated files
df_list = []
for fp in fp_list:
    df = pd.read_csv(fp)
    
    # the 2016 report contains 2016-2015 and the 2017 report
    # contains 2016-2017, so we usually keep the more recent account
    if '2016' in fp:
        df = df.loc[df['year'] == 2015]
    elif '2015' in fp:
        df = df.loc[df['year'] == 2014]
    elif '2014' in fp:
        df = df.loc[df['year'] == 2013]
    elif '2011' in fp:
        df = df.loc[df['year'] == 2010]
    elif '2010' in fp:
        df = df.loc[df['year'] == 2009]
    elif '2017' in fp:
        df = df.loc[df['year'] == 2016]
    elif '2018' in fp:
        df = df.loc[df['year'] == 2017]
    # elif '2019' in fp:
    #     df = df.loc[df['year'] == 2018] # add more as necessary!
    
    # assign a measure depending on filename
    if 'Prevalence' in fp:
        df['measure_id'] = 5
    elif 'Incidence' in fp:
        df['measure_id'] = 6
    elif 'INCEDENCE' in fp:
        df['measure_id'] = 6
    
    df_list.append(df)
total = pd.concat(df_list)

# drop duplicates and write
total.drop_duplicates(inplace=True)
total.to_csv(Path(repo).joinpath('FILEPATH/unformatted.csv'), index=False, encoding='utf-8-sig')
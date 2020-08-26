import pandas as pd
import os
import shutil
import sys

pop_dir = 'FILEPATH'
temp_dir = 'FILEPATH'
out_dir = 'FILEPATH/'

year = sys.argv[1]

os.makedirs(f'/FILEPATH/{year}/',exist_ok=True)
    
pop = pd.read_csv(pop_dir + f'FILEPATH{year}.csv')
for month in range(1,13):
    month = ('0' + str(month))[-2:]
    file = f'era5_{year}_{month}.csv'

    df = pd.read_csv(temp_dir + file)

    mg = pd.merge(df,pop)
    mg.drop('Unnamed: 0',axis=1,inplace=True)
    mg.to_csv(f'/FILEPATH/{year}/' + file,index=False)

df = pd.DataFrame()
for month in range(1,13):
    month = ('0' + str(month))[-2:]
    file = f'era5_{year}_{month}.csv'
    df = df.append(pd.read_csv(f'/FILEPATH/{year}/' + file))
df.to_csv(f'/FILEPATH/{year}/FILEPATH{year}.csv',index=False)
shutil.move(f'/FILEPATH/{year}/FILEPATH{year}.csv',out_dir + f'FILEPATH{year}.csv')

shutil.rmtree(f'/FILEPATH/{year}')
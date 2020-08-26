import xarray as xr
import pandas as pd
import numpy as np
import os
import sys
from math import exp
import shutil
import subprocess

year_month = sys.argv[1]
filename = f"era5_{year_month}.nc"

input_folder = 'FILEPATH'
output_folder = 'FILEPATH'

input_name = filename
output_name = filename.rstrip('.nc') + '.csv'

if not os.path.exists(input_folder + input_name):
    multi_slot = '-pe multi_slot 2'
    error = '-FILEPATH'
    output = 'FILEPATH'
    python = 'FILEPATH'
    dl_client = 'FILEPATH'

    qsub = f'qsub -b y -N dl{year_month} {multi_slot} {error} {output} {python} {dl_client} {year_month}'
    subprocess.call(qsub,shell=True)
    
    sys.exit()
    
def heat_index(temp,dewp):
    a = 1.0799
    b = .03755
    c = .0801
    return temp - a * exp(b * temp) *(1 - exp(c * (dewp - 14)))
heat_index = np.vectorize(heat_index)

ds = xr.open_dataset(input_folder + input_name)
df = ds.to_dataframe().reset_index()
del(ds)

df['date'] = df.time.dt.date.astype('datetime64[D]')
df.drop('time',axis=1,inplace=True)

df.columns = ['latitude','longitude','dewp','temp','date']

df.temp = df.temp - 273.15
df.dewp = df.dewp - 273.15

df['heat_index'] = heat_index(df.temp,df.dewp)

df = df.groupby(['latitude','longitude','date']).mean().reset_index()

df.to_csv(output_folder + output_name)

dest = 'FILEPATH'
shutil.move(output_folder + output_name,dest + output_name)
os.remove(input_folder + input_name)
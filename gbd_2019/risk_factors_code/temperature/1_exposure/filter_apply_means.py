import xarray as xr
import pandas as pd
import numpy as np
import os
import sys
from math import exp

# files = [0]
# for year in range(1979,2018):
    # for month in range(1,13):
        # files.append("usa_era5_" + str(year) + "_" + ("0" + str(month))[-2:] + ".nc")
        
# input_name = files[os.environ.get("SGE_TASK_ID")]

year = os.environ.get("SGE_TASK_ID")

files = []   
for month in range(1,13):
    files.append("usa_era5_" + str(year) + "_" + ("0" + str(month))[-2:] + ".nc")

def heat_index(temp,dewp):
    a = 1.0799
    b = .03755
    c = .0801
    return temp - a * exp(b * temp) *(1 - exp(c * (dewp - 14)))

heat_index = np.vectorize(heat_index)

input_folder = 'FILEPATH'
output_folder = 'FILEPATH'

def filter_apply_means(input_name):
    input_name = input_name
    output_name = input_name.strip('.nc') + '.csv'

    ds = xr.open_dataset(input_folder + input_name)
    df = ds.to_dataframe().reset_index()
    del(ds)

    df = df[
    (df.latitude.between(24,50) & df.longitude.between(235,294)) |
    (df.latitude.between(51,72) & df.longitude.between(172,230)) |
    (df.latitude.between(18,29) & df.longitude.between(181,206))
    ].reset_index(drop=True)

    df['date'] = df.time.dt.date.astype('datetime64[D]')
    df.drop('time',axis=1,inplace=True)

    df['temp'] = df.t2m - 273.15
    df['dewp'] = df.d2m - 273.15

    df.drop(['t2m','d2m'],axis=1,inplace=True)

    df['heat_index'] = heat_index(df.temp,df.dewp)

    df = df.groupby(['latitude','longitude','date']).mean().reset_index()

    df.to_csv(output_folder + output_name,index=False)

for file in files:
    filter_apply_means(file)
import pandas as pd
import cdsapi
import os
import sys
from argparse import ArgumentParser

jobid = int(os.getenv('SLURM_ARRAY_TASK_ID'))
config = pd.read_csv('config_20221031.csv')
product_type = config.loc[jobid, 'product_type']
variable = config.loc[jobid, 'variable']
year = str(config.loc[jobid, 'year'])

os.chdir("/FILEPATH/FOLDERNAME/")

c = cdsapi.Client()


c.retrieve(
    'reanalysis-era5-single-levels',
    {
        'product_type': product_type,
        'format': 'netcdf',
        'variable': [
            variable,
        ],
        'year': year,
        'month': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
        ],
        'day': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ],
        'time': [
            '00:00', '03:00', '06:00',
            '09:00', '12:00', '15:00',
            '18:00', '21:00',
        ],
    },
    'era5_'+product_type+'_'+variable+'_'+year+'.nc')

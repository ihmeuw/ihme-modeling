from time import sleep
from glob import glob
import pandas as pd
from db_queries import get_location_metadata
import subprocess
import os
from get_draws.api import get_draws

username = 'USERNAME'
ca_version = 'D4V5'



def make_new_hgb_file():
    '''
    This function writes a flat file of the draws for mean hemoglobin.
    Will only work with many slots.
    Run this function prior to anemia CA, any time that the mean hemoglobin model has been updated.
    '''
    hgb = get_draws('modelable_entity_id', 10487, 'epi', gbd_round_id=6, decomp_step='step4')
    hgb["hgb_mean"] = hgb[['draw_%s' % d for d in list(range(1000))]].mean(axis=1)
    hgb["mean_hgb"] = hgb["hgb_mean"]
    hgb = hgb.drop(['measure_id', 'modelable_entity_id', 'model_version_id'], axis=1)
    renames = {'draw_%s' % d: 'hgb_%s' % d for d in list(range(1000))}
    hgb.rename(columns=renames, inplace=True)
    hgb.to_hdf('FILEPATH',
               key='draws',
               mode='w',
               format='table',
               data_columns=['location_id',
                             'age_group_id',
                             'year_id',
                             'sex_id'
                             ]
		)

print("About to write file")

make_new_hgb_file()

print("File finished writting")

#if __name__ == "__main__":
#    parser = argparse.ArgumentParser()
#    args = parser.parse_args()
#    make_new_hgb_file()

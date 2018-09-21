""" Analysis

Abraham Flaxman & David Phillips
February 2014

This script initialized and executes the model for anemia CT sampling
It takes five arguments: iso3, year, sex, age, which tell data.py what to load
the date just names the temporary output directory
If no arguments are specified, it dUSERts to lines 25, 40, 42, 44 and 46
"""


# Setup
# -----
import sys
import timeit
import pymc as pm
import numpy as np
import pandas as pd
import analysis
from matplotlib.pyplot import *
#from analysis import *
import models
import data
import graphics
import scipy.optimize


reload(data)
reload(models)
reload(graphics)
start = timeit.dUSERt_timer()
np.random.seed(12345)

np.seterr(invalid='warn')

# Directories and Files
# ---------------------

#outdir = '/tmp/wei/anemia/'

if __name__ == "__main__":


    # Arguments
    # ---------
    try:
        location_id = int(sys.argv[1])
    except:
        location_id = {LOCATION ID}
    try:
        age_group_id = int(sys.argv[2])
    except:
        age_group_id = {AGE GROUP ID}
    try:
        ndraws = int(sys.argv[3])
    
    except:
        #ndraws = 10
        ndraws = 5
    try:
        max_iters = int(sys.argv[4])
    except:
        max_iters = 3
    print 'Arguments: %s %s' % (location_id, age_group_id)



    print "ndraws=", ndraws, " max_iters=", max_iters

    outdir = '{FILEPATH}' # should I change outdir to something else?
    sex_id_list = [{SEX IDS}]
    year_id_list = [{YEAR IDS}]
    consecutive_small_changes = 6 
    small_change = 1e-6
    scale_factor = 1000.
    data_dir = '{FILEPATH}'
    h5_dir = '/{FILEPATH}'


    res, logp, runtime = analysis.run_analysis(
            location_id, age_group_id, ndraws, max_iters,
            sex_id_list, year_id_list,
            consecutive_small_changes,
            small_change,
            scale_factor,
            data_dir,
            h5_dir,
            outdir
            )

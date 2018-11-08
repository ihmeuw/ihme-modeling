# -*- coding: utf-8 -*-
'''
Description: Runs each section of the nonfatal model for the acause-location_id pair 
'''


import utils.common_utils as utils
from c_models.e_nonfatal import (
    incidence_draws,
    survival,
    prevalence,
    adjust_and_finalize,
)
import pandas as pd
from sys import argv
import subprocess



def generate_inc_prev_estimates(acause, location_id, cnf_model_run_id, is_resubmission):
    ''' For the given acause and location_id, runs each stage of the model
    '''
    incidence_draws.generate_estimates(acause, location_id,
                                        cnf_model_run_id, is_resubmission)
    survival.generate_estimates(acause, location_id, cnf_model_run_id)
    prevalence.generate_estimates(acause, location_id)
    adjust_and_finalize.generate_estimates(acause, location_id)


if __name__ == "__main__":
    acause = argv[1]
    location_id = int(argv[2])
    cnf_model_run_id = int(argv[3])
    if len(argv) >= 5:
        is_resubmission = bool(int(argv[4]))
    else:
        is_resubmission = False
    generate_inc_prev_estimates(acause, location_id,
                                cnf_model_run_id, is_resubmission)

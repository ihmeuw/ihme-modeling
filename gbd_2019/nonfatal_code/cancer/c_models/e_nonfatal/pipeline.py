'''
Description: Runs each section of the nonfatal model for the acause-location_id pair 
How To Use:
'''


import cancer_estimation.py_utils.common_utils as utils
from cancer_estimation.c_models.e_nonfatal import (
    incidence_draws,
    survival,
    prevalence,
    adjust_and_finalize,
)
import pandas as pd
from sys import argv
import subprocess



def generate_inc_prev_estimates(acause, location_id, cnf_model_version_id, is_resubmission, faux_correct):
    ''' For the given acause and location_id, runs each stage of the model
    '''
    """
    incidence_draws.generate_estimates(acause, location_id,
                                        cnf_model_version_id, is_resubmission,
                                        faux_correct)
    """
    survival.generate_estimates(acause, location_id, cnf_model_version_id, faux_correct)
    prevalence.generate_estimates(acause, location_id, faux_correct)
    adjust_and_finalize.generate_estimates(acause, location_id, faux_correct)


if __name__ == "__main__":
    acause = argv[1]
    location_id = int(argv[2])
    cnf_model_version_id = int(argv[3])
    faux_correct = bool(argv[4])
    if len(argv) >= 6:
        is_resubmission = bool(int(argv[5]))
    else:
        is_resubmission = False
    generate_inc_prev_estimates(acause, location_id,
                                cnf_model_version_id, is_resubmission, faux_correct)

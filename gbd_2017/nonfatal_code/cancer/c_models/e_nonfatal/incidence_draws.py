'''
Description: Wrapper for the R processes that estimate incidence
'''

import utils.common_utils as utils
import c_models.e_nonfatal.nonfatal_dataset as nd
from sys import argv
from time import sleep
import os
import subprocess



def generate_estimates(acause, location_id, cnf_model_run_id, is_resubmission=False):
    ''' Runs a subprocess that passes all arguments to the R script that 
            calculates incidence draws
    '''
    r_script = utils.get_path("calculate_incidence", process="nonfatal_model")
    cmd = "bash {shl} {scr} {ac} {l} {id} {rs}".format(
                    shl=utils.get_cluster_setting("r_shell"), scr=r_script, ac=acause,
                    l=location_id, id=cnf_model_run_id, rs=int(is_resubmission))
    print(cmd)
    subprocess.call(cmd, shell=True)
    return(True)
    

if __name__ == "__main__":
    acause = argv[1]
    location_id = int(argv[2])
    cnf_model_run_id = int(argv[3])
    generate_estimates(acause, location_id, cnf_model_run_id)


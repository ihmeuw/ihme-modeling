import argparse
import sys
import time
import types
from datetime import datetime
from os.path import join
from save_results import save_results_cod


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cause_id", type=int,
                        help="The cause ID of that these draws will save")
    parser.add_argument("-p", "--run_id", type=str,
                        help="The run id of the draws/prioritization")

    cmd_args = parser.parse_args()
    cause_id = cmd_args.cause_id
    run_id = cmd_args.run_id

    print("**UPLOADING {} TO COD DB**".format(cause_id))
    
    if cause_id in [843]:
        input_dir = "FILEPATH"
    else:
        input_dir = "FILEPATH"
    

    save_results_cod(input_dir=input_dir.format(cause_id),
                     input_file_pattern="{location_id}.csv",
                     cause_id= cause_id,
                     description="desciption of run",
                     metric_id=1,
                     model_version_type_id=5,
                     mark_best=True,
                     decomp_step='iterative',
                     gbd_round_id=7)
    print("**COD DB UPLOAD WAS APPARENTLY SUCCESSFUL**")
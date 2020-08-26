
'''
Name of Script: save_worker.py
Description: Worker function to submit save_custom_results jobs.
Arguments: --meid (int)  - the model id number
           --desc (str)  - the description for the model
           --indir (str) - the directory that contains the .h5 files created by
                           the shared function split_epi_model
           Arguments are required although flagged as optional.
Output: split epi models for causes specified are uploaded to epi viz and
        marked 'best'
'''

import sys
from save_results import save_results_epi
import argparse
import pandas as pd
from cancer_estimation.py_utils import common_utils as utils
from cancer_estimation.c_models import epi_upload
from cancer_estimation.c_models.e_nonfatal import nonfatal_dataset as nd



def save_worker(meid, meas_ids, description, input_dir, cnf_run_id):
    print("saving {}...".format(description))
    d_step = utils.get_gbd_parameter('current_decomp_step')
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    estimation_yrs = [1990, 2000, 2017] # temporary for fauxcorrect
    try:
        success_df = save_results_epi(modelable_entity_id=meid,
                     description=description,
                     input_dir=input_dir,
                     measure_id=meas_ids,
                     mark_best=True,
                     n_draws=1000,
                     decomp_step = d_step,
                     gbd_round_id = gbd_id, 
                     input_file_pattern="{location_id}.h5")
    except:
        success_df = pd.DataFrame()
    return(success_df)


def main(meid, desc, indir, run_id, meas_id):
    this_step = nd.nonfatalDataset("split", meid)
    success_file = this_step.get_output_file('upload')
    print("Working on {} ({}) in {}".format(meid, desc, indir, run_id))
    success_df = save_worker(meid=meid, meas_ids=meas_id, 
                            description=desc, input_dir=indir, 
                             cnf_run_id=run_id)
    # Validate save and preserve record if successful
    if (len(success_df) > 0) and isinstance(success_df, pd.DataFrame):
       if 'model_version_id' in success_df.columns:
            model_id = success_df.at[0, 'model_version_id']
            epi_upload.update_upload_record(meid, run_id, model_id,
                                    cancer_model_type="split_custom_epi",success=1)
            success_df.to_csv(success_file, index=False)
            return(True)
    else:
        print("Error during split")
    return(False)



def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--meid',
                        type=int,
                        help='the modelable_entity id number')
    parser.add_argument('--cnf_run_id',
                        type=int,
                        help='run_id for the cnf model that is being split')
    parser.add_argument('--meas_id',
                        nargs='*',
                        type=int,
                        help='the measure_id(s) being uploaded')
    parser.add_argument('--desc',
                        type=str,
                        help='the description')
    parser.add_argument('--indir',
                        type=str,
                        help='input directory')
    args = parser.parse_args()
    return(args)


if __name__ == '__main__':
    args = parse_args()
    main(args.meid, args.desc, args.indir, args.cnf_run_id, args.meas_id)

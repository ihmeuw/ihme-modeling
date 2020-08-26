#! FILEPATH
# shebang forces use of specific environment
# -*- coding: utf-8 -*-
'''
Name of Script: save_worker.py
Description: Worker function to submit save_custom_results jobs.
Arguments: --target (int)- the cause_id of the model to upload
           --desc (str)  - the description for the model
           --indir (str) - the directory that contains the .h5 files created by
                           the shared function split_epi_model
           Arguments are required although flagged as optional.
Output: 
Contributors: NAME
'''
# shebang forces to use gbd environment
from save_results import save_results_cod
import argparse
from cancer_estimation.py_utils import common_utils as utils



def save_worker(target_id, description, input_dir):
    print("saving {}...".format(description))
    d_step = utils.get_gbd_parameter('current_decomp_step')
    years = list(range(1980, int(utils.get_gbd_parameter("max_year")) + 1))
    save_results_cod(input_dir=input_dir,
                     input_file_pattern='death_{location_id}.csv',
                     cause_id=target_id,
                     description=description,
                     sex_id=[1, 2],
                     metric_id=1,
                     year_id=years,
                     mark_best=True,
                     decomp_step = d_step)
    print("model saved.")


def main():
    args = parse_args()
    print("Working on {} ({}) in {}".format(args.target, args.desc, args.indir))
    save_folder = "{}/{}".format(args.indir, args.target)
    save_worker(args.target, args.desc, save_folder)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--target',
                        type=int,
                        help='the model id number')
    parser.add_argument('-d', '--desc',
                        type=str,
                        help='the description')
    parser.add_argument('-dir', '--indir',
                        type=str,
                        help='input directory')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()

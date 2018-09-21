# -*- coding: utf-8 -*-

"""
Description: Submits paralellized qsubs for splitting registry data and
               uploading the models to epi viz
             Future Development: Make epi_split_submitter.py take arguments to
               generalize the script to submit qsubs for any cause splits
Arguments:   None -- makes calls to split_worker.py and save_worker.py, which
               are worker scripts for the shared functions split_epi_model and
               save_custom_results, respectively
             The list of model ids, descriptions, and target ids are hard-coded
               in master.py

Outputs:    - Paralellized qsubs for calling split_epi_model and
                save_custom_results
            - Series of .h5 files in appropriate liver cancer folders (by meid)
            - Best models for prevalence of split liver cancer causes in epi
"""

import subprocess
import sys
import os
import getpass

sys.path.append(h + '/cancer_estimation/')

from utilities import *


def main():
    # get system information
    username = getpass.getuser()
    # Set paths
    nonfatal_path = get_path('nonfatal_model', 'storage')
    worker_folder = get_path('nonfatal_model', 'code_repo')
    worker_folder = worker_folder + '/subroutines'
    output_folder = nonfatal_path + '/GBD2016/liver_cancer_splits'
    error_dir = [FILEPATH] % (username)
    out_dir = [FILEPATH] % (username)
    save_worker_path = worker_folder + '/save_worker.py'
    split_worker_path = worker_folder + '/split_worker.py'

    # Set necessary modeling variables
    # Target model id arrays
    meid_dx_list = [1682, 1686, 1690, 1694]
    meid_ctrl_list = [x + 1 for x in meid_dx_list]
    meid_meta_list = [x + 2 for x in meid_dx_list]
    meid_term_list = [x + 3 for x in meid_dx_list]

    # List of source model ids with target model ids
    meid_pairs = [[1678, meid_dx_list], [1679, meid_ctrl_list],
                  [1680, meid_meta_list], [1681, meid_term_list]]

    # List of model ids to draw proportions from
    meid_prop_list = '2470 2471 2472 2473'    # string form for argparse

    # Descriptions for model ids
    meid_desc_list = ["liver cancer due to hepatitis B",
                      "liver cancer due to hepatitis C",
                      "liver cancer due to hepatitis alcohol",
                      "liver cancer due to hepatitis other"]

    # Configure qsub templates and variables
    split_slots = 6    # value from job profiling
    save_slots = 43    # value from job profiling

    # Qsub template for split_worker.py
    qsub_template = ("qsub -terse -N split_epi_model{jname} "
                     "-e {sgerror_dir} -o {sgeout_dir} "
                     "-pe multi_slot {split_slots} "
                     "-b y {path} --source {source_meid} "
                     "--targs {target_meids} --props {prop_meids} "
                     "--outdir {output_dir}")

    # Qsub template for save_worker.py
    qsub_n_template = ("qsub -hold_jid {hold_jids} -N save_results{jname} "
                       "-e {error_dir} -o {sgeout_dir} "
                       "-pe multi_slot {save_slots} "
                       "-b y {path} --meid {meid} --desc {description} "
                       "--indir {input_dir}")

    # Submit all qsubs in a loop, beginning with split_epi_model
    # Save_results qsubs will be held pending completion of their parent
    #   split_epi_model jobs
    for p, _ in enumerate(meid_pairs):

        # Turn the target array  into string for the argparse
        targ_str = ' '.join(str(m) for m in meid_pairs[p][1])

        qsub = qsub_template.format(jname=meid_pairs[p][0],
                                    sgerror_dir=error_dir,
                                    sgeout_dir=out_dir,
                                    split_slots=split_slots,
                                    path=split_worker_path,
                                    source_meid=meid_pairs[p][0],
                                    target_meids=targ_str,
                                    prop_meids=meid_prop_list,
                                    output_dir=output_folder)
        print qsub

        # Get the job number to hold on
        # Since terse option was specified, only the job number will be
        #   displayed when the qsub is submitted
        job_id = int(subprocess.check_output(qsub, shell=True))

        # Submit the qsub for save_custom_results jobs, holding for
        #   completion of its slpit_epi_model parent job
        for i, meid in enumerate(meid_pairs[p][1]):
            qsub_nested = qsub_n_template.format(hold_jids=job_id,
                                                 jname=meid,
                                                 error_dir=error_dir,
                                                 sgeout_dir=out_dir,
                                                 save_slots=save_slots,
                                                 path=save_worker_path,
                                                 meid=meid,
                                                 description=meid_desc_list[i],
                                                 input_dir=('%s/%d'
                                                            % (output_folder,
                                                               meid)))
            print qsub_nested
            subprocess.call(qsub_nested, shell=True)


if __name__ == '__main__':
    main()

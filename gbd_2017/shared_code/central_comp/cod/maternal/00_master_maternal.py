################################################################################
# Purpose: Master Script for Maternal Mortality
# Author: AUTHOR
#
################################################################################

import sys
import datetime
import pandas as pd
import subprocess
import logging

# ARGUMENTS ----------------------------------------------------------------- #

# define arguments
if len(sys.argv) > 1:
    # run in proportion space or MMR space?
    dep_map_type = sys.argv[1]

    # get the years for the current GBD round
    gbd_round = sys.argv[2]
    
    # define code directory
    code_directory = sys.argv[3]
else:
    dep_map_type = "mmr"
    gbd_round = 2017
    code_directory = "FILEPATH"


# SETUP ---------------------------------------------------------------------- #

# import custom functions
import maternal_fns
from db_tools.ezfuncs import query

# create directory for intermediate file outputs
current_date = maternal_fns.get_time()
current_date = 'DATE'

cluster_dir = maternal_fns.check_dir(
    'FILEPATH/%s' % current_date)

# create logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("maternal_custom.00_master_maternal")

# read in dependency map
dep_map = pd.read_csv("FILEPATH" % dep_map_type,
                      header=0).dropna(axis='columns', how='all')

# set all year values
yearvals = range(1980, gbd_round + 1)

#######################################################################################################################
# 01: SCALE FRACTIONS (Interpolation + Cause-Fraction Scaling)
#
# DisMod outputs cfs for every maternal sub-cause (except the maternal parent)
# but only for certain years. We first interpolate between years to get a
# full time series for our period of interest. We do this for sub-causes.
#
# Next, we proportionately scale the cause
# fractions so they sum to one across sub-causes. Timing scaling and
# interpolation is done in Step 3, after CodCorrect.
#######################################################################################################################

# check for dependencies for step 1
print(maternal_fns.check_dependencies(1, dep_map_type))

if maternal_fns.check_dependencies(1, dep_map_type):
    logger.info('Step 1: Interpolating and Scaling Cause Fractions')
    step_df = dep_map.ix[dep_map.step == 1]

# make output directories
for target_id in pd.unique(step_df.target_id):
    maternal_fns.check_dir('%s/%s' % (cluster_dir, target_id))

# I. Interpolation ------------------------------------------------------------ #

logger.info('Interpolating Sub-Cause Fractions')

if dep_map_type == "mmr":
    step_df_interp = step_df.loc[step_df.target_note.isin(['late maternal death'])]

    for index, row in step_df_interp.iterrows():
        
        # set directories for this DisMod ME_id
        dismod_me_id = row['source_id']
        interp_out_dir = maternal_fns.check_dir('%s/%s' % (
            cluster_dir, dismod_me_id))
            
        # run interpolation
        jobname = 'interp_dismod_%s' % (dismod_me_id)
        call = ('qsub -cwd -P proj_injuries '
                '-o FILEPATH '
                '-e FILEPATH '
                '-pe multi_slot 45 -N '
                '%s FILEPATH.sh FILEPATH.py '
                ' "%s" "%s" '
                % (jobname, dismod_me_id,
                    interp_out_dir))
        subprocess.call(call, shell=True)

    # II. Cause-fraction correction ------------------------------------------------- #

    # wait for interpolation to finish
    maternal_fns.wait('interp_dismod', 300)

    # run cause fraction scaling
    logger.info("Generating corrected cause fractions")
    for year in yearvals:
        jobname = 'dismod_cf_correct_%d' % year
        call = ('qsub -cwd -P proj_injuries '
                '-o FILEPATH '
                '-e FILEPATH '
                '-pe multi_slot 8 -N %s '
                'FILEPATH.sh FILEPATH.py '
                '"%s" "%s" "%s" "%s"'
                % (jobname, jobname, cluster_dir, year, dep_map_type))
        subprocess.call(call, shell=True)

    # wait for cause fraction scaling jobs to finish
    maternal_fns.wait('dismod_cf_correct', 300)

        # III. Save Results -------------------------------------------------------------- #
        # run save_results to upload all of these cause fractions to the epi db

    logger.info("Saving results for the cause fraction proportions to the epi database.")
    for target_id in pd.unique(step_df.target_id):
        in_dir = '%s/%s' % (cluster_dir, target_id)
        file_pattern = "{year_id}_2.h5"
        start_year = 1980
        end_year = gbd_round
        model_type = "epi"
        description = "Corrected cause fractions from maternal mortality with {} initial models".format(dep_map_type)
        
        jobname = 'cfs_save_results_%s' % target_id
        call = ('qsub -N %s -P proj_injuries '
                '-o FILEPATH '
                '-e FILEPATH '
                '-pe multi_slot 20 '
                '%s/FILEPATH.sh '
                '%s/FILEPATH.py '
                '"%s" "%s" "%s" "%s" "%s" "%s" "%s"' % (jobname, code_directory, code_directory,
                                                        target_id, in_dir, file_pattern,
                                                        start_year, end_year, model_type, description))
        print(call)
        subprocess.call(call, shell=True)

#######################################################################################################################
# 02: ADJUST PARENT MATERNAL CAUSE
#
# Most countries do not properly record late maternal deaths.
# USER identified the country/years that do properly report Late,
# but these are a small subset.
#
# For the places that report Late, their total maternal includes late.
# For those that don't report it, their total maternal completely ignores it,
# thus causing under-counting and over-counting. This step adjusts for this.
#######################################################################################################################

# wait for cause fractions to be saved to the database if it was run this round
maternal_fns.wait('cfs_save_results', 300)

print(maternal_fns.check_dependencies(2, dep_map_type))
if not maternal_fns.check_dependencies(2, dep_map_type):
    logger.info("Adjusting parent maternal cause.")
    step_df = dep_map.ix[dep_map.step == 2]
    
    # make output directories
    for target_id in pd.unique(step_df.target_id):
        maternal_fns.check_dir('%s/%s' % (cluster_dir, target_id))
    
    # I. Adjust Parent Maternal Envelope ------------------------------------------------- #
    # where is your pre-codcorrect, codem envelope?
    env_id = step_df.set_index('target_type').ix['cause', 'target_id']
    env_model_vers = maternal_fns.get_model_vers('cause', env_id, 2)
    
    jobname = "adjust_parent"
    out_dir = '%s/%s' % (cluster_dir, target_id)
    call = ('qsub -cwd -P proj_injuries '
            '-o FILEPATH '
            '-e FILEPATH '
            '-pe multi_slot 8 -N %s '
            'FILEPATH.sh FILEPATH.py "%s" "%s" "%s" "%s" '
            % (jobname, jobname, env_model_vers, out_dir, dep_map_type))
    subprocess.call(call, shell=True)
    
    # wait for parent adjustment to finish
    maternal_fns.wait('adjust_parent', 300)

    # II. Save Results to the Cod Database ---------------------------------------------- #
    # run save_results to upload late-corrected maternal env to codcorrect

    in_dir = '%s/%s' % (cluster_dir, env_id)
    file_pattern = 'late_corrected_maternal_envelope.h5'
    start_year = 1980
    end_year = gbd_round
    model_type = "cod"
    jobname = 'adjustment_save_results'
    description = ('Late corrected envelope from hybrid model %s'
                   % env_model_vers)
    call = ('qsub -N %s -P proj_injuries '
            '-o /FILEPATH '
            '-e /FILEPATH '
            '-pe multi_slot 50 %s/FILEPATH.sh '
            '%s/FILEPATH.py '
            '"%s" "%s" "%s" "%s" "%s" "%s" "%s"' % (jobname, code_directory, code_directory,
                                                    env_id, in_dir, file_pattern,
                                                    start_year, end_year, model_type, description))
    subprocess.call(call, shell=True)

    # Unmark Best for Hybrid CODEm Model ------------------------------------------------ #
    
    maternal_fns.wait('adjustment_save_results', 300)
    unmark_sql_statement = ('UPDATE model_version '
                            'SET best_end = "%s", is_best = 2 '
                            'WHERE '
                            'gbd_round_id = 5 AND '
                            'cause_id = %s AND '
                            'sex_id = 2 AND '
                            'best_start IS NOT NULL AND '
                            'best_end IS NULL AND '
                            'model_version_type_id = 3'
                            % (datetime.datetime.now(), env_id))
    try:
        unmark = query(unmark_sql_statement, conn_def='cod')
    except:
        print("caught the 'no rows returned' bug error of sqlalchemy")


##############################################################
# 03: GET FINAL DEATH COUNTS
#
# For each sub-cause, multiply the cause
# fractions from step 1 with the envelope from the late correction of CODEm,
# giving estimates of death counts for each subtype.
##############################################################

# wait for cause fractions to be saved to the database if it was run this round
if maternal_fns.check_dependencies(3, dep_map_type):
    logger.info("Getting final death counts for each sub-cause")
    step_df = dep_map.ix[dep_map.step == 3]

    # make output directories
    for target_id in pd.unique(step_df[step_df.source_type != 'cause'].target_id):
        maternal_fns.check_dir('%s/%s' % (cluster_dir, target_id))

    # where is your pre-codcorrect, late-corrected envelope?
        env_id = step_df.loc[step_df.source_type == "cause", 'source_id'].iloc[0]
        env_model_vers = maternal_fns.get_model_vers('cause', env_id)
        
    for index, row in step_df.iterrows():
        if row['source_type'] == 'modelable_entity':
            # get cfs from where save_results saved them after last step
            scaled_me_id = row['source_id']
            # set dir to save final death numbers to cluster_dir, by cause_ids
            out_dir = '%s/%s' % (cluster_dir, row['target_id'])
            
            # run calculation of final deaths by subtype
            jobname = 'final_deaths_%s' % row['target_id']
            call = ('qsub -cwd -P proj_injuries '
                    '-o FILEPATH '
                    '-e FILEPATH -pe multi_slot 10 '
                    '-N %s FILEPATH.sh FILEPATH.py '
                    '"%s" "%s" "%s" "%s" "%s"'
                    % (jobname, jobname, env_model_vers, scaled_me_id,
                       row['target_id'], out_dir))
            subprocess.call(call, shell=True)

    # wait for final deaths calculation to finish
    maternal_fns.wait('final_deaths', 300)

    # run save_results to upload deaths of subcauses to codcorrect
    for target_id in pd.unique(step_df[step_df.target_type == 'cause']
                               .target_id):
        in_dir = '%s/%s' % (cluster_dir, target_id)
        file_pattern = "FILEPATH" % target_id
        start_year = 1980
        end_year = gbd_round
        model_type = "cod"
        jobname = 'deaths_save_results_%s' % target_id
        description = ('Subcause deaths based on late_corrected env %s'
                       % env_model_vers)
        call = ('qsub -N %s -P proj_injuries '
                '-o FILEPATH '
                '-e FILEPATH '
                '-pe multi_slot 50 %s/FILEPATH.sh '
                '%s/FILEPATH.py '
                '"%s" "%s" "%s" "%s" "%s" "%s" "%s"' % (jobname, code_directory, code_directory,
                                                        target_id, in_dir, file_pattern,
                                                        start_year, end_year, model_type, description))
        subprocess.call(call, shell=True)
       

##############################################################
# 04: TIMINGS
# After codcorrect is run, 1) divide the late deaths by the all
# maternal envelope to get the post-central comp late cause fraction.
# 2) Interpolate the other three timings.
# 3) Then scale these all to one while freezing the late cf.
##############################################################

# what codcorrect version do you want to use as input for steps 3 and 4?
codcorrect_vers = maternal_fns.get_model_vers('codcorrect')
codcorrect_dir = 'FILEPATH' % codcorrect_vers

env_id = pd.unique(dep_map[dep_map.source_type == 'cause'].source_id)[0]

if maternal_fns.check_dependencies(4, dep_map_type):
    logger.info("On Step 4")
    step_df = dep_map[(dep_map.step == 4) &
                      (dep_map.source_id != 'codcorrect')]

    # make output directories
    for target_id in pd.unique(step_df.target_id):
        maternal_fns.check_dir('%s/%s' % (cluster_dir, target_id))

    ##############################################################
    # GETTING LATE CAUSE FRACTION FROM CoDCorrect
    ##############################################################
    logger.info("Getting cause fractions from CoDCorrect")
    late_id = pd.unique(
        dep_map[dep_map.source_type == 'codcorrect'].source_id)[1]

    out_dir = maternal_fns.check_dir('%s/%s' % (cluster_dir, late_id))

    # run getting late cause fractions from the dalynator
    for year in yearvals:
        jobname = 'codcorrect_late_%s' % year
        call = ('qsub -cwd -P proj_custom_models '
                '-o FILEPATH '
                '-e FILEPATH '
                '-pe multi_slot 20 -N %s '
                'FILEPATH.sh FILEPATH.py "%s" "%s" "%s" "%s"'
                % (jobname, year, env_id, late_id, out_dir))
        subprocess.call(call, shell=True)

    ##############################################################
    # INTERPOLATE TIMINGS (EXCEPT LATE)
    ##############################################################
    maternal_fns.wait('codcorrect_late', 300)

    logger.info("Interpolating timings")
    # set in and out directories for interpolation (but don't run for Late)
    for index, row in step_df[step_df.source_type != 'codcorrect'].iterrows():
        dismod_me_id = row['source_id']
        interp_out_dir = maternal_fns.check_dir('%s/%s' % (cluster_dir,
                                                           dismod_me_id))

        # iterate through start years of interpolation
        for start_year in interp_yearvals.keys():
            end_year = interp_yearvals[start_year]

            # run interpolation for timings
            jobname = 'timing_interp_dismod_%s_%s' % (
                dismod_me_id, start_year)
            call = ('qsub -cwd -P proj_custom_models '
                    '-o FILEPATH '
                    '-e FILEPATH '
                    '-pe multi_slot 10 -N %s '
                    'FILEPATH.sh FILEPATH.py "%s" "%s" '
                    '"%s" "%s"'
                    % (jobname, dismod_me_id,
                       interp_out_dir, start_year, end_year))
            subprocess.call(call, shell=True)

    ##############################################################
    # SCALE TIMINGS (WITH LATE FROZEN)
    ##############################################################
    maternal_fns.wait('timing_interp_dismod', 300)

    # run cause fraction scaling
    logger.info("Scaling timings, with late frozen")
    for year in yearvals:
        jobname = 'timing_dismod_cf_correct_%d' % year
        call = ('qsub -cwd -P proj_custom_models '
                '-o FILEPATH '
                '-e FILEPATH '
                '-pe multi_slot 10 -N %s '
                'FILEPATH.sh FILEPATH.py "%s" "%s" '
                '"%s" "%s"'
                % (jobname, jobname, cluster_dir, year, dep_map_type))
        subprocess.call(call, shell=True)

    # wait for cause fraction scaling to finish
    maternal_fns.wait('timing_dismod_cf_correct', 300)

    # run save_results to upload all of these timing cfs to the epi db
    for target_id in pd.unique(step_df.target_id):
        in_dir = '%s/%s' % (cluster_dir, target_id)
        file_pattern = "{year_id}_2.h5"
        start_year = 1980
        end_year = gbd_round
        model_type = "epi"
        jobname = 'timing_cfs_save_results_%s' % target_id
        description = "Scaled in maternal custom code for timings"
        call = ('qsub -N %s -P proj_custom_models '
                '-o /FILEPATH '
                '-e /FILEPATH '
                '-pe multi_slot 50 %s/FILEPATH.sh '
                '%s/FILEPATH.py '
                '"%s" "%s" "%s" "%s" "%s" "%s" "%s"' % (jobname, code_directory, code_directory,
                                                        target_id, in_dir, file_pattern,
                                                        start_year, end_year, model_type, description))
        subprocess.call(call, shell=True)
        

##############################################################
# 05: FINAL DEATHS BY TIMING
# Get scaled cause fractions from where save results saved
# them last step. Multiply by the codcorrected envelope.
# Upload final deaths by timings to epi
##############################################################
# wait for upload of scaled timing cfs to finish, if it was run this round
maternal_fns.wait('timing_cfs_save_results', 300)

if maternal_fns.check_dependencies(5, dep_map_type):
    logger.info("On Step 5")
    step_df = dep_map.ix[dep_map.step == 5]

    # make output directories
    for target_id in pd.unique(step_df.target_id):
        maternal_fns.check_dir('%s/%s' % (cluster_dir, target_id))

    years = "%s" % (" ".join([str(year) for year in range(1980, 2017)]))
    for index, row in step_df.iterrows():
        # get cfs from where save_results saved them after last step
        scaled_me_id = row['source_id']
        # set dir to save final death numbers to cluster_dir, by their me_ids
        out_dir = '%s/%s' % (cluster_dir, row['target_id'])

        # set the envelope to be the codcorrect envelope
        env_model_vers = None

        # run calculation of final deaths by timing
        jobname = 'timing_final_deaths_%s' % row['target_id']
        call = ('qsub -cwd -P proj_custom_models '
                '-o FILEPATH '
                '-e FILEPATH -pe multi_slot 10 -N %s '
                'FILEPATH.sh FILEPATH.py "%s" "%s" '
                '"%s" "%s" "%s"'
                % (jobname, jobname, env_model_vers, scaled_me_id,
                   row['target_id'], out_dir))
        subprocess.call(call, shell=True)

    # wait for final deaths to be calculated
    maternal_fns.wait('timing_final_deaths', 300)

    # call save_results to upload deaths of subtimings to epi
    for target_id in pd.unique(step_df.target_id):
        in_dir = '%s/%s' % (cluster_dir, target_id)
        file_pattern = "all_draws.h5"
        start_year = 1980
        end_year = gbd_round
        model_type = "epi"
        jobname = 'timing_deaths_save_results_%s' % target_id
        description = "Timing deaths from maternal custom code; post CodCorrect"
        call = ('qsub -N %s -P proj_custom_models '
                '-o FILEPATH'
                '-e FILEPATH'
                '-pe multi_slot 50 %s/FILEPATH.sh '
                '%s/FILEPATH.py '
                '"%s" "%s" "%s" "%s" "%s" "%s" "%s"' % (jobname, code_directory, code_directory,
                                                        target_id, in_dir, file_pattern,
                                                        start_year, end_year, model_type, description))
        subprocess.call(call, shell=True)

print 'Finished with everything!'

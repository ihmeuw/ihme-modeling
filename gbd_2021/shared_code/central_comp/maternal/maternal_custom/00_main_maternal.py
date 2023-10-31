"""
00_main_maternal takes in 4 positional arguments:
    dep_map_type (str): either 'mmr' or 'props' (you likely want 'mmr')
    gbd_round_id (int): like 6 for gbd 2019
    code_directory (str): 
        path to this codebase is living in the filesystem, ex:
            FILEPATH
    decomp_step_id (int): which decomp_step_id to run for

And does the following in three discrete steps:

    Step1 (01_scale_fractions.py):
        interpolating cause fractions for parent late maternal cause.
            runs chronos.interpolate on 
        scaling these cause fractions for maternal subcauses.
            uses get_draws for each of the 'source' me_ids, parallelized
            per year.
        saving the scaled maternal cause fractions.
            runs save_results_epi on all of the scaled me_ids to their target
            me_ids. parallelized by me_id (that isn't the parent)

    Step2 (02_adjust_parent.py):
        adjust main maternal deaths to codem envelope by:
            pulling codem envelope
            adjusting with dismod proportion model

    Step3 (03_final_deaths_by_subtype.py):
        For each sub-cause, multiply the cause
        fractions from step 1 with the envelope from the late correction of CODEm,
        giving estimates of death counts for each subtype.


The outcome is scaled maternal deaths in count space by etiology.
"""

import datetime
import logging
import os
import subprocess
import pathlib
from typing import List

import click
import pandas as pd
from db_tools.ezfuncs import get_session

import maternal_fns


@click.command()
@click.option('--dep-map-type', type=click.Choice(['mmr', 'props']), required=True,
              help='run in proportion space or MMR space')
@click.option('--gbd-round-id', type=click.INT, required=True,
              help='which gbd_round to run for; ex: 6 for 2019')
@click.option('--decomp-step-id', type=click.INT, required=True,
              help='decomp_step_id to run for')
@click.option('--code-directory', type=click.Path(),
              default=str(pathlib.Path(__file__).resolve().parent),
              help='directory of this codebase')
@click.option('-o', '--output-directory', type=click.STRING,
              default=maternal_fns.get_time())
@click.option('--pdb', is_flag=True, help='flag to post-mortem failed run')
def cli(**kwargs):
    post_mort_run = kwargs.pop('pdb')
    if post_mort_run:
        try:
            main(**kwargs)
        except Exception as err:
            print(f'Exception found, in namespace as "err":\n{err}')
            import pdb
            pdb.post_mortem()
    else:
        main(**kwargs)


def main(
    dep_map_type,
    gbd_round_id,
    code_directory,
    decomp_step_id,
    output_directory
):
    # establish directories
    os.chdir(code_directory)
    cluster_dir = maternal_fns.check_dir(
        'FILEPATH/%s' % output_directory)

    # create logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # read in dependency map
    dep_map = pd.read_csv(
        "./dependency_map_%s.csv" % dep_map_type,
        header=0
    ).dropna(axis='columns', how='all')

    # set all year values
    all_years = maternal_fns.get_all_years(gbd_round_id)
    # gbd2020 has a seprate epi_decomp_step
    epi_decomp_step_id = maternal_fns.get_epi_decomp_step(decomp_step_id)

    ###########################################################################
    # 01: SCALE FRACTIONS (Interpolation + Cause-Fraction Scaling)
    #
    # DisMod outputs cause fractions for every maternal sub-cause
    # (except the maternal parent)
    # but only for certain years. We first interpolate between years to get a
    # full time series for our period of interest. We do this for sub-causes.
    #
    # Next, we proportionately scale the cause
    # fractions so they sum to one across sub-causes. Timing scaling and
    # interpolation is done in Step 3, after CodCorrect.
    ###########################################################################

    if maternal_fns.check_dependencies(1, dep_map_type, decomp_step_id):
        logger.info('Step 1: Interpolating and Scaling Cause Fractions')
        maternal_split_step_1(
            dep_map=dep_map,
            epi_decomp_step_id=epi_decomp_step_id,
            logger=logger,
            cluster_dir=cluster_dir,
            code_directory=code_directory,
            all_years=all_years,
            dep_map_type=dep_map_type,
        )
    else:
        logger.info('Skipping Step 1 (Interpolating & Scaling Cause Fractions)')

    #############################################
    # 02: ADJUST PARENT MATERNAL CAUSE
    #
    # For the places that report late, their total maternal includes late.
    # For those that don't report it, their total maternal completely ignores it,
    # thus causing under-counting and over-counting. This step adjusts for this.
    #############################################

    # wait for cause fractions to be saved to the database if it was run this round
    maternal_fns.wait('cfs_save_results', 300)

    if not maternal_fns.check_dependencies(2, dep_map_type, decomp_step_id):
        logger.info('skipping step2')
    else:
        logger.info("Running Step 2: Adjusting parent maternal cause.")
        step_df = dep_map.loc[dep_map.step == 2]
        # make output directories
        for target_id in pd.unique(step_df.target_id):
            maternal_fns.check_dir('%s/%s' % (cluster_dir, target_id))
        # I. Adjust Parent Maternal Envelope -------------------------------------------------
        env_id = step_df.set_index('target_type').loc['cause', 'target_id']
        env_model_vers = maternal_fns.get_model_vers(
            'cause', decomp_step_id, env_id, 2)
        jobname = "adjust_parent"
        out_dir = '%s/%s' % (cluster_dir, target_id)
        call = ('qsub -cwd -P proj_centralcomp '
                '-o FILEPATH/maternal '
                '-e FILEPATH/maternal '
                '-l m_mem_free=50G,fthread=1,h_rt=10800 -q all.q -N %s '
                'cluster_shell.sh 02_adjust_parent.py "%s" "%s" "%s" "%s" "%s" "%s" '
                % (
                    jobname, jobname, env_model_vers, out_dir,
                    dep_map_type, decomp_step_id, cluster_dir))
        subprocess.call(call, shell=True)
        # wait for parent adjustment to finish
        maternal_fns.wait('adjust_parent', 300)
        # II. Save Results to the Cod Database ----------------------------------------------
        # run save_results to upload late-corrected maternal env to codcorrect
        in_dir = '%s/%s' % (cluster_dir, env_id)
        file_pattern = 'late_corrected_maternal_envelope.h5'
        start_year = all_years[0]
        end_year = all_years[-1]
        model_type = "cod"
        jobname = 'adjustment_save_results'
        description = ('Late corrected envelope from hybrid model %s' 
                    % env_model_vers)
        call = (
            f'qsub -N {jobname} -P proj_centralcomp '
            f'-o FILEPATH/maternal '
            f'-e FILEPATH/maternal '
            f'-l m_mem_free=70G,h_rt=10800,fthread=10 '
            f'-q all.q {code_directory}/cluster_shell.sh '
            f'{code_directory}/save_custom_results.py '
            f'--target-id {target_id} '
            f'--in-dir {in_dir} '
            f'--file-pattern {file_pattern} '
            f'--start-year {start_year} '
            f'--end-year {end_year} '
            f'--model-type {model_type} '
            f'--description "{description}" '
            f'--decomp-step-id {decomp_step_id} '
        )
        subprocess.call(call, shell=True)

        # Unmark Best for Hybrid CODEm Model ------------------------------------------------

        maternal_fns.wait('adjustment_save_results', 300)
        session = get_session('cod-save-results')
        unmark_sql_statement = (
            'UPDATE model_version '
            'SET best_end = "%s", is_best = 2 '
            'WHERE '
            'gbd_round_id = %s AND '
            'decomp_step_id = %s AND '
            'cause_id = %s AND '
            'sex_id = 2 AND '
            'best_start IS NOT NULL AND '
            'best_end IS NULL AND '
            'model_version_type_id = 3'
            % (
                datetime.datetime.now(),
                gbd_round_id,
                decomp_step_id,
                env_id))
        try:
            session.execute(unmark_sql_statement)
            session.commit()
        finally:
            session.close()

    ##############################################################
    # 03: GET FINAL DEATH COUNTS
    #
    # For each sub-cause, multiply the cause
    # fractions from step 1 with the envelope from the late correction of CODEm,
    # giving estimates of death counts for each subtype.
    ##############################################################

    # wait for cause fractions to be saved to the database if it was run this round
    if not maternal_fns.check_dependencies(3, dep_map_type, decomp_step_id):
        raise RuntimeError('step3 dependency error!')

    logger.info("Getting final death counts for each sub-cause")
    step_df = dep_map.loc[dep_map.step == 3]
    print(step_df)

    # make output directories
    for target_id in pd.unique(step_df[step_df.source_type != 'cause'].target_id):
        maternal_fns.check_dir('%s/%s' % (cluster_dir, target_id))

    env_id = step_df.loc[step_df.source_type == "cause", 'source_id'].iloc[0]
    env_model_vers = maternal_fns.get_model_vers(
        'cause', model_id=env_id, decomp_step_id=decomp_step_id)
    print(env_model_vers)

    for index, row in step_df.iterrows():
        if row['source_type'] == 'modelable_entity':
            scaled_me_id = row['source_id']
            out_dir = '%s/%s' % (cluster_dir, row['target_id'])
            jobname = 'final_deaths_%s' % row['target_id']
            call = ('qsub -cwd -P proj_centralcomp '
                    '-o FILEPATH/maternal '
                    '-e FILEPATH/maternal '
                    '-l m_mem_free=60G,h_rt=10800,fthread=1 -q all.q '
                    '-N %s cluster_shell.sh 03_final_deaths_by_subtype.py '
                    '"%s" "%s" "%s" "%s" "%s" "%s" "%s" '
                    % (
                        jobname, jobname, env_model_vers, scaled_me_id,
                        row['target_id'], out_dir, decomp_step_id,
                        cluster_dir))
            subprocess.call(call, shell=True)

    # wait for final deaths calculation to finish
    maternal_fns.wait('final_deaths', 300)

    # run save_results to upload deaths of subcauses to codcorrect
    for target_id in pd.unique(step_df[step_df.target_type == 'cause'].target_id):
        in_dir = '%s/%s' % (cluster_dir, target_id)
        file_pattern = "final_deaths_%s.h5" % target_id
        start_year = all_years[0]
        end_year = all_years[-1]
        model_type = "cod"
        jobname = 'deaths_save_results_%s' % target_id
        description = (
            'Subcause deaths based on late_corrected env %s' % env_model_vers)
        call = (f'qsub -N {jobname} -P proj_centralcomp '
                f'-o FILEPATH/maternal '
                f'-e FILEPATH/maternal '
                f'-l m_mem_free=70G,h_rt=10800,fthread=10 '
                f'-q all.q {code_directory}/cluster_shell.sh '
                f'{code_directory}/save_custom_results.py '
                f'--target-id {target_id} '
                f'--in-dir {in_dir} '
                f'--file-pattern {file_pattern} '
                f'--start-year {start_year} '
                f'--end-year {end_year} '
                f'--model-type {model_type} '
                f'--description "{description}" '
                f'--decomp-step-id {decomp_step_id} ')
        subprocess.call(call, shell=True)

    # wait for final deaths calculation to finish
    maternal_fns.wait('deaths_save_results', 300)
    print('that\'s all folks :)')


def maternal_split_step_1(
    dep_map: pd.DataFrame,
    epi_decomp_step_id: int,
    logger: logging.Logger,
    all_years: List[int],
    dep_map_type: str,
    cluster_dir: str,
    code_directory: str,
):
    """Maternal split interpolation, save_results."""
    # create step_df
    step_df = dep_map.loc[dep_map.step == 1]

    # make output directories
    for target_id in pd.unique(step_df.target_id):
        maternal_fns.check_dir('%s/%s' % (cluster_dir, target_id))

    # I. Interpolation ----------------------------------------------------- #

    if not (
        maternal_fns.check_dependencies(1, dep_map_type, epi_decomp_step_id)
    ):
        raise RuntimeError('first dependency check failure')

    logger.info('Interpolating Sub-Cause Fractions')
    step_df_interp = step_df.loc[step_df.target_note.isin(['late maternal death'])]

    for _, row in step_df_interp.iterrows():
        # set directories for this DisMod ME_id
        dismod_me_id = row['source_id']
        interp_out_dir = maternal_fns.check_dir('%s/%s' % (
            cluster_dir, dismod_me_id))
        # run interpolation
        jobname = 'interp_dismod_%s' % (dismod_me_id)
        call = ('qsub -cwd -P proj_centralcomp '
                '-o FILEPATH/maternal '
                '-e FILEPATH/maternal '
                '-l m_mem_free=100G,fthread=2,h_rt=10800 -q all.q -N '
                '{} cluster_shell.sh interp_dismod.py '
                ' "{}" "{}" "{}" ').format(
                    jobname, dismod_me_id,
                    epi_decomp_step_id, interp_out_dir
                )
        subprocess.call(call, shell=True)

    # II. Cause-fraction correction ------------------------------------------------- #

    # wait for interpolation to finish
    maternal_fns.wait('interp_dismod', 300)

    # run cause fraction scaling
    logger.info("Generating corrected cause fractions")
    for year in all_years:
        jobname = 'dismod_cf_correct_%d' % year
        call = ('qsub -cwd -P proj_centralcomp '
                '-o FILEPATH/maternal '
                '-e FILEPATH/maternal '
                '-l m_mem_free=20G,fthread=1,h_rt=10800 -q all.q -N %s '
                'cluster_shell.sh 01_scale_fractions.py '
                '"%s" "%s" "%s" "%s" "%s" '
                % (
                    jobname, jobname, cluster_dir,
                    year, dep_map_type, epi_decomp_step_id))
        subprocess.call(call, shell=True)

    # wait for cause fraction scaling jobs to finish
    maternal_fns.wait('dismod_cf_correct', 300)

    # III. Save Results -------------------------------------------------------------- #
    # run save_results to upload all of these cause fractions to the epi db

    logger.info("Saving results for the cause fraction proportions to the epi database.")
    for idx in step_df.index:
        source_id = step_df.loc[idx].source_id
        target_id = step_df.loc[idx].target_id

        in_dir = '%s/%s' % (cluster_dir, target_id)
        file_pattern = "{year_id}_2.h5"
        start_year = all_years[0]
        end_year = all_years[-1]
        model_type = "epi"
        description = (
            "Corrected cause fractions from maternal mortality "
            "with {} initial models".format(dep_map_type))

        jobname = 'cfs_save_results_%s' % target_id
        call = (
            f'qsub -N {jobname} -P proj_centralcomp '
            f'-o FILEPATH/maternal '
            f'-e FILEPATH/maternal '
            f'-l m_mem_free=70G,h_rt=10800,fthread=10 '
            f'-q all.q {code_directory}/cluster_shell.sh '
            f'{code_directory}/save_custom_results.py '
            f'--target-id {target_id} '
            f'--in-dir {in_dir} '
            f'--file-pattern {file_pattern} '
            f'--start-year {start_year} '
            f'--end-year {end_year} '
            f'--model-type {model_type} '
            f'--description "{description}" '
            f'--decomp-step-id {epi_decomp_step_id} '
            f'--parent-id {source_id}'
        )

        subprocess.call(call, shell=True)


if __name__ == '__main__':
    cli()

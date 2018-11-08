import argparse
from glob import glob
import os
import pandas as pd
from db_queries import get_location_metadata, get_population
from jobmon.workflow.python_task import PythonTask
import parameter_csv
from jobmon.workflow.workflow import Workflow
from gbd.constants import GBD_ROUND_ID, GBD_ROUND
from gbd.estimation_years import estimation_years_from_gbd_round_id as est_yr
from datetime import datetime


def sweep(dir_list, sweep_val):
    ###############################################
    #Attempt to create a directory, chomd the
    #directory, and then remove all files in the
    #directory if sweep_val=True
    ###############################################
    for direc in dir_list:
        if not os.path.exists(direc):
            os.mkdir(direc)
        os.chmod(direc, 0o777)
        files = glob('{}/*'.format(direc))
        if sweep_val:
            for file in files:
                os.remove(file)


def run_master(root_dir, envr, sweep_lt, sweep_yld, sweep_hale, prep_lt,
               prep_yld, calc_hale, summarize, upload_hale, n_draws,
               loc_set_id, year_id, yld_version, local, test_location,
               custom_lt, log_dir='DIRECTORY'):
    ###############################################
    #Start jobmon and launch different jobs. Also
    #set up directories, and run get_population
    #to cache pop for compile_yld file
    ###############################################
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)
    
    if local:
        out_dir = root_dir
    else:
        out_dir = 'DIRECTORY'

    parameter_csv.run_param(envr, yld_version, loc_set_id, year_id,
                            gbd_round_id=GBD_ROUND_ID)

    param_sheet = pd.read_csv('%s/inputs/parameters.csv' % root_dir)
    param_sheet = param_sheet.loc[param_sheet['status'] == 'best']

    hale_version = param_sheet['hale_version'].item()
    mort_version = param_sheet['mort_run'].item()
    print('HALE VERSION IS {}'.format(hale_version))
    print('MORT VERSION IS {}'.format(mort_version))
    print('YLD VERSION IS {}'.format(yld_version))

    prog_dir = '%s/v%s' % (out_dir, hale_version)
    draw_dir = '%s/draws' % prog_dir
    summ_dir = '%s/summaries' % prog_dir

    for direc in [prog_dir, draw_dir, summ_dir]:
        if not os.path.isdir(direc):
            os.mkdir(direc)
        os.chmod(direc, 0o777)

    if custom_lt is not None:
        lt_in = custom_lt
    else:
        lt_in = ("DIRECTORY")

    lt_tmp = '%s/lt' % draw_dir
    lt_dir = '%s/lt' % summ_dir
    yld_tmp = '%s/yld' % draw_dir
    yld_dir = '%s/yld' % summ_dir
    hale_tmp = '%s/results' % draw_dir
    hale_dir = '%s/results' % summ_dir

    sweep([lt_tmp, lt_dir], sweep_lt)
    sweep([yld_tmp, yld_dir], sweep_yld)
    sweep([hale_tmp, hale_dir], sweep_hale)

    err = glob('{}/*.e*'.format(log_dir))
    out = glob('{}/*.o*'.format(log_dir))
    ps = glob('{}/*.p*'.format(log_dir))
    for log in err + out + ps:
        os.remove(log)

    if test_location is not None:
        locations = [test_location]
    else:
        locations = []
        for location_set in loc_set_id:
            location_meta = get_location_metadata(
                                        location_set_id=location_set,
                                        gbd_round_id=GBD_ROUND_ID)
            location_meta = location_meta.loc[location_meta[
                    'location_id'] != 44620]
            locs = location_meta['location_id'].unique().tolist()
            locations = locations + locs
        locations = list(set(locations))

    year_draws = list(zip(year_id, n_draws))

    d_str = "[%m/%d/%Y %H:%M:%S]"
    wf = Workflow('HALE_{}'.format(datetime.now().strftime(d_str)),
                  project='proj_hale',
                  stderr=log_dir,
                  stdout=log_dir)

    print('Building DAG')
    if prep_lt:
        lt_task = {}
        for location in locations:
            for year, draws in year_draws:
                args = ['--lt_in', lt_in, '--lt_tmp', lt_tmp, '--location',
                        location, '--year', year, '--n_draws', draws]
                script = os.path.join(root_dir, '01_compile_lt.py')
                name = 'lt_{}_{}_prep'.format(location, year)
                lt_task[(location, year)] = PythonTask(
                        script=script, args=args, name=name, slots=4,
                        mem_free=8, max_attempts=3, tag='lt_prep')
                wf.add_task(lt_task[(location, year)])

    if prep_yld:
        population = get_population(location_id=locations, year_id=year_id,
                                    age_group_id='all', sex_id='all',
                                    gbd_round_id=GBD_ROUND_ID)
        population.drop('run_id', axis=1, inplace=True)
        population.set_index('location_id', inplace=True)
        population.to_csv('%s/inputs/pop.csv' % root_dir)

        yld_task = {}
        for location in locations:
            for year, draws in year_draws:
                args = ['--yld_tmp', yld_tmp, '--root_dir', root_dir,
                        '--location', location, '--yld_version', yld_version,
                        '--year', year, '--n_draws', draws]
                script = os.path.join(root_dir, '02_compile_yld.py')
                name = 'yld_{}_{}_prep'.format(location, year)
                yld_task[(location, year)] = PythonTask(
                        script=script, args=args, name=name, slots=4,
                        mem_free=8, max_attempts=3, tag='yld_prep')
                wf.add_task(yld_task[(location, year)])

    if calc_hale:
        hale_task = {}
        for location in locations:
            for year in year_id:
                if prep_yld and prep_lt:
                    upstream_tasks = [lt_task[(location, year)],
                                            yld_task[(location, year)]]
                elif prep_yld:
                    upstream_tasks = [yld_task[(location, year)]]
                elif prep_lt:
                    upstream_tasks = [lt_task[(location, year)]]
                else:
                    upstream_tasks = None
                args = ['--hale_tmp', hale_tmp, '--lt_tmp', lt_tmp,
                        '--yld_tmp', yld_tmp, '--location', location, '--year',
                        year]
                script = os.path.join(root_dir, '03_calc_hale.py')
                name = 'hale_{}_{}_calc'.format(location, year)
                hale_task[(location, year)] = PythonTask(
                        script=script, args=args, name=name, slots=4,
                        mem_free=8, max_attempts=3, tag='hale_calc',
                        upstream_tasks=upstream_tasks)
                wf.add_task(hale_task[(location, year)])

    if summarize:
        summary_task = {}
        for location in locations:
            if calc_hale:
                upstream_tasks = [hale_task[(location, year)] for year
                                            in year_id]
            else:
                upstream_tasks = None
            args = ['--lt_tmp', lt_tmp, '--lt_dir', lt_dir, '--yld_tmp',
                    yld_tmp, '--yld_dir', yld_dir, '--hale_tmp', hale_tmp,
                    '--hale_dir', hale_dir, '--location', location]
            script = os.path.join(root_dir, '04_calc_summaries.py')
            name = 'summary_{}_calc'.format(location)
            summary_task[location] = PythonTask(
                    script=script, args=args, name=name, slots=4,
                    mem_free=8, max_attempts=3, tag='summarize',
                    upstream_tasks=upstream_tasks)
            wf.add_task(summary_task[location])

    if upload_hale:
        if summarize:
            upstream_tasks = [summary_task[loc] for loc in locations]
        else:
            upstream_tasks = None
        args = ['--hale_version', hale_version, '--hale_dir', hale_dir,
                '--envr', envr]
        script = os.path.join(root_dir, '05_upload_hale.py')
        name = 'upload_hale'
        upload_task = PythonTask(
                script=script, args=args, name=name, slots=12,
                mem_free=24, max_attempts=3, tag='upload',
                upstream_tasks=upstream_tasks)
        wf.add_task(upload_task)

    print("executing workflow")
    integer_result = wf.execute()
    if integer_result:
        raise RuntimeError("Workflow failure")
    print("FINISHED")

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    run_years = est_yr(GBD_ROUND_ID) + [GBD_ROUND - 10]
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "-locs",
            "--loc_set_id",
            help="location set id to use",
            nargs='+',
            default=[35, 40],
            type=int)
    parser.add_argument(
            "-yrs",
            "--year_id",
            help="year_ids to run on",
            nargs='+',
            default=run_years,
            type=int)
    parser.add_argument(
            "-n",
            "--n_draws",
            help="number of YLD draws for each year",
            nargs='+',
            default=[1000 for yr in run_years],
            type=int)
    parser.add_argument(
            "-yld",
            "--yld_version",
            help="yld_version to use",
            default=0,
            type=int)
    parser.add_argument(
            "-e",
            "--environment",
            help="environment to upload to",
            default="prod",
            type=str)
    parser.add_argument(
            "-sl",
            "--sweep_lt",
            help="clear life table directory",
            action="store_true")
    parser.add_argument(
            "-sy",
            "--sweep_yld",
            help="clear YLD directory",
            action="store_true")
    parser.add_argument(
            "-sh",
            "--sweep_hale",
            help="clear HALE directory",
            action="store_true")
    parser.add_argument(
            "-nl",
            "--no_lt_prep",
            help="don't generate life table intermediates",
            action="store_false")
    parser.add_argument(
            "-ny",
            "--no_yld_prep",
            help="don't generate YLD intermediates",
            action="store_false")
    parser.add_argument(
            "-nh",
            "--no_hale_calc",
            help="don't calculate HALE",
            action="store_false")
    parser.add_argument(
            "-ns",
            "--no_summarize",
            help="don't summarize inputs and outputs",
            action="store_false")
    parser.add_argument(
            "-nu",
            "--no_upload",
            help="don't upload HALE",
            action="store_false")
    parser.add_argument(
            "-l",
            "--local",
            help="output files in code directory",
            action="store_true")
    parser.add_argument(
            "-loc",
            "--test_location",
            help="run on a single location, to test",
            default=None,
            type=int)
    parser.add_argument(
            "-clt",
            "--custom_lifetable",
            help="directory of custom life tables, if desired",
            default=None,
            type=str)
    
    args = parser.parse_args()
    loc_set_id = args.loc_set_id
    year_id = args.year_id
    n_draws = args.n_draws
    yld_version = args.yld_version
    envr = args.environment
    sweep_lt = args.sweep_lt
    sweep_yld = args.sweep_yld
    sweep_hale = args.sweep_hale
    prep_lt = args.no_lt_prep
    prep_yld = args.no_yld_prep
    calc_hale = args.no_hale_calc
    summarize = args.no_summarize
    upload_hale = args.no_upload
    local = args.local
    test_location = args.test_location
    custom_lt = args.custom_lifetable
    
    root_dir = os.path.dirname(os.path.abspath(__file__))

    run_master(root_dir, envr, sweep_lt, sweep_yld, sweep_hale, prep_lt,
               prep_yld, calc_hale, summarize, upload_hale, n_draws,
               loc_set_id, year_id, yld_version, local, test_location,
               custom_lt)

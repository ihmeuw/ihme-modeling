import argparse
from glob import glob
import os
import time
import pandas as pd
from jobmon import qmaster, central_job_monitor
from jobmon.schedulers import RetryScheduler
from jobmon.executors.sge_exec import SGEExecutor
from db_queries import get_location_metadata, get_population

def sweep(dir_list, sweep_val):
    for direc in dir_list:
        if not os.path.exists(direc):
            os.mkdir(direc)
        os.chmod(direc, 0o777)
        files = glob('{}/*'.format(direc))
        if sweep_val:
            for file in files:
                os.remove(file)

def create_params(q, root_dir, log_dir, envr, yld_version, loc_set_id):
    runfile = '{}/parameter_csv.py'.format(root_dir)
    params = ['--envr', envr, '--yld_vers', str(yld_version), '--loc_set',
              str(loc_set_id)]
    remote_job = q.create_job(runfile=runfile, jobname='parameter_csv',
                              parameters=params)
    q.queue_job(remote_job,
                slots=2,
                memory=4,
                project='proj_hale',
                stderr=log_dir,
                stdout=log_dir)

def lt_prep(q, root_dir, log_dir, location, lt_in, lt_tmp, lt_dir, ar,
            n_draws):
    runfile = '{}/01_compile_lt.py'.format(root_dir)
    params = ['--lt_in', lt_in, '--lt_tmp', lt_tmp, '--lt_dir', lt_dir,
              '--location', str(location), '--ar', str(ar), '--n_draws',
              str(n_draws)]
    remote_job = q.create_job(runfile=runfile,
                              jobname='lt_prep_{}'.format(location),
                              parameters=params)
    q.queue_job(remote_job,
                slots=4,
                memory=8,
                project='proj_hale',
                stderr=log_dir,
                stdout=log_dir)

def yld_prep(q, root_dir, log_dir, location, yld_tmp, yld_dir,
             yld_version, ar, n_draws):
    runfile = '{}/02_compile_yld.py'.format(root_dir)
    params = ['--yld_tmp', yld_tmp, '--yld_dir', yld_dir,
              '--root_dir', root_dir, '--location', str(location),
              '--yld_version', str(yld_version), '--ar', str(ar), '--n_draws',
              str(n_draws)]
    remote_job = q.create_job(runfile=runfile,
                              jobname='yld_prep_{}'.format(location),
                              parameters=params)
    q.queue_job(remote_job,
                slots=4,
                memory=8,
                project='proj_hale',
                stderr=log_dir,
                stdout=log_dir)

def hale_calc(q, root_dir, log_dir, location, hale_tmp, hale_dir, lt_tmp,
              yld_tmp):
    runfile = '{}/03_calc_hale.py'.format(root_dir)
    params = ['--hale_tmp', hale_tmp, '--hale_dir', hale_dir,
              '--lt_tmp', lt_tmp, '--yld_tmp', yld_tmp,
              '--location', str(location)]
    remote_job = q.create_job(runfile=runfile,
                              jobname='calc_hale_{}'.format(location),
                              parameters=params)
    q.queue_job(remote_job,
                slots=4,
                memory=8,
                project='proj_hale',
                stderr=log_dir,
                stdout=log_dir)

def hale_upload(q, root_dir, log_dir, hale_version, hale_dir, envr):
    runfile = '{}/04_upload_hale.py'.format(root_dir)
    params = ['--hale_version', str(hale_version), '--hale_dir', hale_dir,
              '--envr', envr, '--root_dir', root_dir]
    remote_job = q.create_job(runfile=runfile,
                              jobname='upload_hale',
                              parameters=params)
    q.queue_job(remote_job,
                slots=10,
                memory=20,
                project='proj_hale',
                stderr='{}/upload'.format(log_dir),
                stdout='{}/upload'.format(log_dir))

def run_master(root_dir, envr, sweep_lt, sweep_yld, sweep_hale, prep_lt,
               prep_yld, calc_hale, upload_hale, ar, n_draws, loc_set_id,
               yld_version, local, test_location, custom_lt,
               log_dir='/PATH'):
    #Start jobmon stuff
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)
    os.chmod(log_dir, 0o777)
    if os.path.isfile(log_dir + "/monitor_info.json"):
        os.remove(log_dir +  "/monitor_info.json")
    try:
        jm = central_job_monitor.CentralJobMonitor(log_dir, persistent=False)
        time.sleep(5)
    except Exception as e:
        raise RuntimeError("Couldnt launch central_job_monitor: {}".format(e))
    else:
        q = qmaster.JobQueue(log_dir, executor=SGEExecutor,
                             executor_params={'request_timeout': 6000},
                             scheduler=RetryScheduler)

        if local:
            out_dir = root_dir
        else:
            out_dir = '/PATH/{}'.format(envr)

        create_params(q, root_dir, log_dir, envr, yld_version, loc_set_id)
        q.block_till_done(poll_interval=60, stop_scheduler_when_done=False)

        param_sheet = pd.read_csv('%s/PATH/parameters.csv' % root_dir)
        param_sheet = param_sheet.loc[param_sheet['status'] == 'best']

        hale_version = param_sheet['hale_version'].item()

        prog_dir = '%s/%s' % (out_dir, hale_version)
        draw_dir = '%s/draws' % prog_dir
        summ_dir = '%s/summaries' % prog_dir

        for direc in [prog_dir, draw_dir, summ_dir]:
            if not os.path.isdir(direc):
                os.mkdir(direc)
            os.chmod(direc, 0o777)

        if custom_lt is not None:
            lt_in = custom_lt
        else:
            lt_in = ("/PATH")
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
            location_met = get_location_metadata(location_set_id=loc_set_id,
                                                 gbd_round_id=4)
            locations = location_met['location_id'].unique().tolist()

        if prep_lt:
            for location in locations:
                lt_prep(q, root_dir, log_dir, location, lt_in, lt_tmp, lt_dir,
                        ar, n_draws)

        if prep_yld:
            population = get_population(location_id=-1, year_id=-1,
                                        age_group_id=-1, sex_id=-1,
                                        location_set_id=loc_set_id)
            population.drop('process_version_map_id', axis=1, inplace=True)
            population.set_index('location_id', inplace=True)
            population.to_csv('%s/PATH/pop.csv' % root_dir)
            for location in locations:
                yld_prep(q, root_dir, log_dir, location, yld_tmp, yld_dir,
                         yld_version, ar, n_draws)

        q.block_till_done(poll_interval=60, stop_scheduler_when_done=False)
        if calc_hale:
            for location in locations:
                hale_calc(q, root_dir, log_dir, location, hale_tmp, hale_dir,
                          lt_tmp, yld_tmp)

        q.block_till_done(poll_interval=60, stop_scheduler_when_done=False)
        if upload_hale:
            hale_upload(q, root_dir, log_dir, hale_version, hale_dir, envr)


    finally:
        jm.generate_report()
        jm.stop_responder()
        jm.stop_publisher()

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "-locs",
            "--loc_set_id",
            help="location set id to use",
            dUSERt=45,
            type=int)
    parser.add_argument(
            "-yld",
            "--yld_version",
            help="yld_version to use",
            dUSERt=0,
            type=int)
    parser.add_argument(
            "-e",
            "--environment",
            help="environment to upload to",
            dUSERt="prod",
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
            "-nu",
            "--no_upload",
            help="don't upload HALE",
            action="store_false")
    parser.add_argument(
            "-ar",
            "--annual_results",
            help="annual results",
            action="store_true")
    parser.add_argument(
            "-n",
            "--n_draws",
            help="number of draws",
            dUSERt=1000,
            type=int)
    parser.add_argument(
            "-l",
            "--local",
            help="output files in code directory",
            action="store_true")
    parser.add_argument(
            "-loc",
            "--test_location",
            help="run on a single location, to test",
            dUSERt=None,
            type=int)
    parser.add_argument(
            "-clt",
            "--custom_lifetable",
            help="directory of custom life tables, if desired",
            dUSERt=None,
            type=str)

    args = parser.parse_args()
    loc_set_id = args.loc_set_id
    yld_version = args.yld_version
    envr = args.environment
    sweep_lt = args.sweep_lt
    sweep_yld = args.sweep_yld
    sweep_hale = args.sweep_hale
    prep_lt = args.no_lt_prep
    prep_yld = args.no_yld_prep
    calc_hale = args.no_hale_calc
    upload_hale = args.no_upload
    ar = args.annual_results
    n_draws = args.n_draws
    local = args.local
    test_location = args.test_location
    custom_lt = args.custom_lifetable

    root_dir = os.path.dirname(os.path.abspath(__file__))

    run_master(root_dir, envr, sweep_lt, sweep_yld, sweep_hale, prep_lt,
               prep_yld, calc_hale, upload_hale, ar, n_draws, loc_set_id,
               yld_version, local, test_location, custom_lt)

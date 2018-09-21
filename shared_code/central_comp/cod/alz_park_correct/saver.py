import argparse
import os
import cPickle
from jobmon import qmaster
from jobmon.schedulers import RetryScheduler
from jobmon.executors.sge_exec import SGEExecutor
from cause_mvid_info import get_cause_ids, pull_mvid

def save_old(q, vers, upload_dir, env, root_dir=None):
    ##########################################
    #Submit saving jobs for target and source
    #oldCorrect models after scaling is
    #complete. Return cause_child list for
    #parent child jobs as well.
    ##########################################
    if root_dir is None:
        root_dir = os.path.dirname(os.path.abspath(__file__))
    save_dir = '/FILEPATH'
    runfile = "%s/save_results_cod.py" % save_dir
    desc = 'oldC v%s' % vers

    hybrid_mvt = 3
    custom_mvt = 4
    hybrid_scale_input_mvt = 8
    hybrid_scaled_mvt = 9
    custom_scaled_mvt = 10
    sources, targets = get_cause_ids(cause_set)

    cause_child = []

    for target in targets:
        for sex in [1, 2]:
            children = pull_mvid(target, hybrid_mvt, sex=sex)
            cause_child.append((target, sex, custom_scaled_mvt, children))

        in_dir = ('%s/v%s/c%s' % (upload_dir, vers, int(target)))
        jobname = 'oldC_%s_%s_%s' % (vers, int(target), custom_scaled_mvt)

        params = [str(int(target)), '-in_dir', in_dir,
                  '-d', desc, '-mvt', str(custom_scaled_mvt), '-e', env, '-b']
        remote_job = q.create_job(runfile=runfile, jobname=jobname,
                                  parameters=params)
        q.queue_job(remote_job, slots=40, memory=80, project='proj_codcorrect',
                    stderr='/FILEPATH',
                    stdout='/FILEPATH')
        q.block_till_done(stop_scheduler_when_done=False)

    for source in sources:
        for sex in [1, 2]:
            source_models_codem = pull_mvid(source, hybrid_scale_input_mvt,
                                            sex=sex)
            source_models_dismod = pull_mvid(source, custom_mvt, sex=sex)
            children = source_models_codem + source_models_dismod
            cause_child.append((source, sex, hybrid_scaled_mvt, children))

        in_dir = ('%s/v%s/c%s' % (upload_dir, vers, int(source)))
        jobname = 'oldC_%s_%s_%s' % (vers, int(source), hybrid_scaled_mvt)

        params = [str(int(source)), '-in_dir', in_dir,
              '-d', desc, '-mvt', str(hybrid_scaled_mvt), '-e', env, '-b']
        remote_job = q.create_job(runfile=runfile, jobname=jobname,
                                  parameters=params)
        q.queue_job(remote_job, slots=40, memory=80, project='proj_codcorrect',
                    stderr='/FILEPATH',
                    stdout='/FILEPATH')
        q.block_till_done(stop_scheduler_when_done=False)

    return cause_child

def parent_child_adjust(q, cause_child, detail, vers, root_dir=None):
    ##########################################
    #Submit jobs to update parent and child
    #relationship in
    #cod.model_version_relation table
    ##########################################
    if root_dir is None:
        root_dir = os.path.dirname(os.path.abspath(__file__))
    runfile = '%s/parent_child.py' % root_dir

    for info in cause_child:
        cause_id = info[0]
        sex = info[1]
        mvt = info[2]
        children = info[3]

        print(cause_id, sex, mvt, children)

        jobname = 'oldC_parent_child_%s' % cause_id
        params = ['--cause_id', str(cause_id), '--children', str(children),
                  '--mvt', str(mvt), '--vers', str(vers), '--detail', detail,
                  '--sex', str(sex)]
        remote_job = q.create_job(runfile=runfile, jobname=jobname,
                                  parameters=params)
        q.queue_job(remote_job, slots=10, memory=20, project='proj_codcorrect',
                    stderr='/FILEPATH',
                    stdout='/FILEPATH')
        q.block_till_done(stop_scheduler_when_done=False)


def run_save(cause_set, upload_dir, log_dir, vers, detail, env):
    ###################################
    # Set constants, pull causes
    ###################################
    q = qmaster.JobQueue(log_dir, executor=SGEExecutor,
                         scheduler=RetryScheduler)

    ###################################
    # Save source and target causes
    # using save_results. Submit jobs
    # to update parent and child
    # relationship as well.
    ###################################

    cause_child = save_old(q, vers, upload_dir, env)
    cPickle.dump(cause_child, open('%s/v%s/FILEPATH/cause_child.p' % (
        upload_dir, vers), 'wb'))

    cause_child = cPickle.load(open('%s/v%s/FILEPATH/cause_child.p' % (
        upload_dir, vers), 'rb'))
    parent_child_adjust(q, cause_child, detail, vers)


if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--cause_set",
            help="cause_set",
            dUSERt=10,
            type=int)
    parser.add_argument(
            "--upload_dir",
            help="input directory",
            dUSERt="",
            type=str)
    parser.add_argument(
            "--log_dir",
            help="logging directory",
            dUSERt="/FILEPATH",
            type=str)
    parser.add_argument(
            "--vers",
            help="version",
            dUSERt=0,
            type=int)
    parser.add_argument(
            "--detail",
            help="more detailed info",
            dUSERt="",
            type=str)
    parser.add_argument(
            "--env",
            help="db env",
            dUSERt="prod",
            type=str)
    args = parser.parse_args()
    cause_set = args.cause_set
    upload_dir = args.upload_dir
    log_dir = args.log_dir
    vers = args.vers
    detail = args.detail
    env = args.env

    run_save(cause_set, upload_dir, log_dir, vers, detail, env)

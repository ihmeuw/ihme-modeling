import os
import json
import sys
import warnings
import numpy as np
import logging
import math

from importer import get_model_version
from jobmon import job, sge
import drill
import upload
from drill import Cascade, Cascade_loc
import file_check
from emr_calc.emr import dismod_emr


# MODULE LEVEL ATTRIBUTES
cfile = "%s/run_children.py" % drill.this_path
gfile = "%s/run_global.py" % drill.this_path
finfile = "%s/varnish.py" % drill.this_path

# Get configuration options
if os.path.isfile(os.path.join(drill.this_path, "../config.local")):
    settings = json.load(
        open(os.path.join(drill.this_path, "../config.local")))
else:
    settings = json.load(
        open(os.path.join(drill.this_path, "../config.dUSERt")))

# Set dUSERt file mask to readable-for all users
os.umask(0o0002)


# Disable warnings
def nowarn(message, category, filename, lineno, file=None, line=None):
    pass
warnings.showwarning = nowarn


# CUSTOM EXCEPTIONS
class InvalidSettings(Exception):
    pass


def update_run_time(mvid):
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query = """
        UPDATE epi.model_version
        SET model_version_run_start='%s'
        WHERE model_version_id=%s""" % (now, mvid)
    execute(settings['epi_conn_str'], query)


def run_world(year, cascade, drop_emr=False, reimport=False):
    cl = Cascade_loc(1, 0, year, cascade, timespan=50, reimport=reimport)
    if drop_emr:
        cl.gen_data(1, 0, drop_emr=True)
    cl.run_dismod()
    cl.summarize_posterior()
    cl.draw()
    cl.predict()
    return cascade


def execute(conn_str, query):
    import sqlalchemy
    eng = sqlalchemy.create_engine(conn_str)
    conn = eng.connect()
    conn.execute(query)
    conn.close()


class JobTreeBootstrapper(object):

    def __init__(self, mvid):
        self.mvid = mvid
        self.logdir = '{}/{}'.format(settings['log_dir'], self.mvid)
        self.mvm = get_model_version(mvid)
        self.run_cv = (self.mvm.cross_validate_id.values[0] == 1)
        self.meid = self.mvm.modelable_entity_id.values[0]

        if self.meid in [9422, 7695, 1175, 10352, 9309]:
            self.project = "proj_tb"
            self.is_tb = True
        else:
            self.project = "proj_dismod"
            self.is_tb = False

        try:
            os.makedirs(self.logdir)
        except:
            pass
        try:
            os.chmod(self.logdir, 0o775)
        except:
            pass

        if self.run_cv:
            cv_iters = range(11)
        else:
            cv_iters = [0]
            jids = []

        jids = []
        for it in cv_iters:
            jid = self.submit_jobtree(it)
            jids.append(jid)

        self.submit_varnish(jids)

    def submit_jobtree(self, cv_iter):
        """Submits a jobtree, which manages a given full/cross-validation
        run from global on down through the cascade"""
        jobname = 'dm_{}_G{}'.format(self.mvid, cv_iter)
        jid = sge.qsub(
                gfile,
                jobname,
                project=self.project,
                slots=20,
                memory=40,
                parameters=[self.mvid, '--submit_stage', 'jt', '--cv_iter',
                            cv_iter],
                conda_env='cascade_ode',
                prepend_to_path='strDir',
                stderr='{}/{}.error'.format(self.logdir, jobname))
        return jid

    def submit_varnish(self, hold_jids):
        """Submits a job that 'varnishes' this run, meaning it:
            1. Uploads fits
            2. Uploads adjusted data
            3. Computes fit statistics
            4. Uploads fit statistics
            5. Attempts to generate diagnostic plots
            5. Computes finals
            6. Uploads finals
            7. Updates the status of the model to finished
        """
        varn_jobname = 'dm_%s_varnish' % (self.mvid)
        varn_jid = sge.qsub(
                finfile,
                varn_jobname,
                project=self.project,
                slots=20,
                memory=40,
                parameters=[self.mvid],
                holds=hold_jids,
                conda_env='cascade_ode',
                prepend_to_path='strDir',
                stderr='%s/%s.error' % (self.logdir, varn_jobname))
        return varn_jid


class CascadeJobTree(object):

    def __init__(self, mvid, cv_iter_id):
        self.mvid = mvid
        self.cv_iter_id = cv_iter_id
        try:
            j = job.Job('%s/%s' % (settings['cascade_ode_out_dir'], mvid))
            j.start()
        except IOError as e:
            logging.exception(e)
        except Exception as e:
            logging.exception(e)

        self.cascade = Cascade(mvid, reimport=True, cv_iter=cv_iter_id)
        self.meid = (
            self.cascade.model_version_meta.modelable_entity_id.values[0])
        if self.meid in [9422, 7695, 1175, 10352, 9309]:
            self.project = "proj_tb"
            self.is_tb = True
        else:
            self.project = "proj_dismod"
            self.is_tb = False
        self.logdir = '{}/{}'.format(settings['log_dir'], self.mvid)
        self.finished = False
        self.has_csmr = 'mtspecific' in self.cascade.data.integrand.unique()

        if cv_iter_id == 0:
            self.jt_dir = "{}/".format(self.cascade.root_dir)
        else:
            self.jt_dir = "{}/{}".format(self.cascade.root_dir, cv_iter_id)

        self.rerun_num = self.get_rerun_num()
        ijs = self.incomplete_jobs()

        # Run global once
        self.run_jobtree_global()

        # Check if retry limit has been exceeded
        if self.rerun_num > 3:
            elog_file = '{}/error{}.log'.format(self.cascade.root_dir,
                                                self.cv_iter_id)

            with open(elog_file, 'w') as log:
                log.write('Model is incomplete after two attempted relaunches')
                for ij in ijs:
                    log.write(str(ij))
            sys.exit()

        # Submit all jobs and checking job, making sure varnish waits
        # for the checking job to complete
        varn_jobname = 'dm_%s_varnish' % (mvid)
        varn_job = sge.qstat(pattern=varn_jobname)
        varn_jid = int(varn_job.job_id.values[0])
        if len(ijs) > 0:
            jids = self.submit_cascade_jobs(ijs)
            pjid = self.resubmit_self_check(jids)
            sge.add_holds(varn_jid, pjid)

    def run_jobtree_global(self):
        """Sets up and runs the global 1 or more times, depending on the
        settings for EMR (which requires adjusted data from one run of the
        global model to calculate EMR to be fed into a second global model) and
        whether this is a cross validation instance. We will use cv_iter=0 to
        denote a 'full' model, whereas all non-zero cv_iters will be run on a
        randomly selected subset of the data"""

        csmr_cause_id = (
            self.cascade.model_version_meta.add_csmr_cause.values[0])
        if csmr_cause_id is None:
            csmr_cause_id = np.nan
        ccvid = self.cascade.model_version_meta.csmr_cod_output_version_id
        ccvid = ccvid.values[0]
        remdf = self.cascade.model_params.query(
                'parameter_type_id == 1 & measure_id == 7')
        if len(remdf) > 0:
            remdf = remdf[['parameter_type_id', 'measure_id', 'age_start',
                           'age_end', 'lower', 'mean', 'upper']]
        else:
            remdf = None
        if (self.rerun_num == 0 and self.cv_iter_id == 0 and
                (not np.isnan(csmr_cause_id) or self.has_csmr) and
                (not self.is_tb)):

            # Check whether there is a value constraint on EMR (in which case
            # we cannot compute EMR)
            emr_prior = self.cascade.model_params.query(
                'parameter_type_id == 1 & measure_id == 9')
            if len(emr_prior) == 1:
                zero_EMR_prior = (emr_prior.lower.squeeze() == 0 and
                                  emr_prior.upper.squeeze() == 0 and
                                  emr_prior.age_start.squeeze() == 0 and
                                  emr_prior.age_end.squeeze() >= 100)
                if zero_EMR_prior:
                    raise InvalidSettings("Cannot set a value prior of 0 for "
                                          "EMR for ages 0-100 while also "
                                          "triggering EMR calculation via "
                                          "cause/remission settings")

            upload.update_model_status(self.mvid, -1)
            commit_hash = sge.get_commit_hash(dir='%s/..' % drill.this_path)
            upload.set_commit_hash(self.mvid, commit_hash)

            # Use  CSMR data from codcorrect if requested, otherwise
            # use the user-provided data
            if np.isnan(csmr_cause_id):
                csmr_type = "custom"
            else:
                csmr_type = "cod"

            # Run the world once for emr calculation
            update_run_time(self.mvid)
            run_world(2000, self.cascade, drop_emr=True)
            dismod_emr(self.mvid, envr='prod', remission_df=remdf,
                       csmr_type=csmr_type)

            # ... then re-import the cascade and re-run the world
            update_run_time(self.mvid)
            self.cascade = Cascade(self.mvid,
                                   reimport=True,
                                   cv_iter=self.cv_iter_id)
            run_world(2000, self.cascade, reimport=True)

        elif self.rerun_num == 0 and self.cv_iter_id == 0:
            update_run_time(self.mvid)
            upload.update_model_status(self.mvid, -1)
            run_world(2000, self.cascade)

        elif self.rerun_num == 0:
            update_run_time(self.mvid)
            upload.update_model_status(self.mvid, -1)
            run_world(2000, self.cascade)

    def incomplete_jobs(self):
        return file_check.reruns(self.mvid,
                                 self.cascade.location_set_version_id,
                                 cv_iter=self.cv_iter_id)

    def get_rerun_num(self):
        retry_file = "{}/retries.txt".format(self.jt_dir)
        if not os.path.isfile(retry_file):
            with open(retry_file, 'w') as rf:
                rerun_num = 0
                rf.write("0")
        else:
            with open(retry_file, 'r+') as rf:
                rerun_num = int(rf.read())+1
                rf.seek(0)
                rf.write(str(rerun_num))
        return rerun_num

    def submit_cascade_jobs(self, incomplete_jobs):
        all_jids = []
        for sex in ['male', 'female']:
            def dependent_submit(location_id, hold_ids):
                node = self.cascade.loctree.get_node_by_id(location_id)
                num_children = len(node.children)
                if num_children == 0:
                    return 0
                else:
                    jids = []
                    for y in [1990, 1995, 2000, 2005, 2010, 2016]:
                        job_name = "dm_%s_%s_%s_%s_%s" % (self.mvid,
                                                          location_id,
                                                          sex[0],
                                                          str(y)[2:],
                                                          self.cv_iter_id)
                        if location_id == 1:
                            num_slots = 20
                        else:
                            num_slots = min(20, num_children*2)
                        path_to_conda_bin = (
                            'strDir')
                        if ((location_id, sex, y, self.cv_iter_id) in
                                incomplete_jobs):
                            params = [self.mvid, location_id, sex, y,
                                      self.cv_iter_id]
                            jid = sge.qsub(
                                    cfile,
                                    job_name,
                                    project=self.project,
                                    holds=hold_ids,
                                    slots=num_slots,
                                    memory=int(math.ceil(num_slots*2.5)),
                                    parameters=params,
                                    conda_env='cascade_ode',
                                    prepend_to_path=path_to_conda_bin,
                                    stderr='%s/%s.error' % (self.logdir,
                                                            job_name))
                            jids.append(jid)
                            all_jids.append(jid)
                    for c in node.children:
                        dependent_submit(c.id, jids)

            dependent_submit(1, [])
        return all_jids

    def resubmit_self_check(self, hold_jids):
        """Submits a job that checks that all child location-year-sex groups
        have run succesfully. If any have failed, it resubmits the below-global
        levels of the cascade (i.e. the submit_cascade function)"""
        jobname = 'dm_{}_G{}'.format(self.mvid, self.cv_iter_id)
        jid = sge.qsub(
                gfile,
                jobname,
                project=self.project,
                slots=20,
                memory=40,
                holds=hold_jids,
                parameters=[self.mvid, '--submit_stage', 'jt', '--cv_iter',
                            self.cv_iter_id],
                conda_env='cascade_ode',
                prepend_to_path='strDir',
                stderr='{}/{}.error'.format(self.logdir, jobname))
        return jid


def main():
    import argparse

    parser = argparse.ArgumentParser("Launch/relaunch the dismod cascade "
                                     "from the global level")
    parser.add_argument('mvid', type=int)
    parser.add_argument('--submit_stage', type=str, dUSERt='bootstrap')
    parser.add_argument('--cv_iter', type=int, dUSERt=0)

    args = parser.parse_args()
    submit_stage = args.submit_stage
    mvid = args.mvid
    cv_iter = args.cv_iter

    if submit_stage == 'bootstrap':
        JobTreeBootstrapper(mvid)
    else:
        CascadeJobTree(mvid, cv_iter)


if __name__ == "__main__":
    main()

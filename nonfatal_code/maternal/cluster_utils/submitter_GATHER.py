import os
import subprocess
import json
from datetime import datetime, time
import pandas as pd

this_path = os.path.dirname(os.path.abspath(__file__))

# Find all jobs matching a given pattern
def qstat(status=None, pattern=None):
    cmd = ["qstat", "-r"]
    if status is not None:
        cmd.extend(["-s", status])
    p1 = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    if pattern is None:
        p2 = subprocess.Popen(["grep", "Full jobname:", "-B1"], stdin=p1.stdout, stdout=subprocess.PIPE)
    else:
        p2 = subprocess.Popen(["grep", "Full jobname:[[:blank:]]*"+pattern+"$", "-B1"], stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
    output, err = p2.communicate()

    lines = output.splitlines()

    job_ids = []
    job_names = []
    job_statuses = []
    job_datetimes = []
    job_runtimes = []
    append_jobid = True
    append_jobname = False
    time_format = "%m/%d/%Y %H:%M:%S"
    now = datetime.now()
    for line in lines:

        if append_jobid==True:
            job_ids.append(line.split()[0])
            job_statuses.append(line.split()[4])
            job_date = line.split()[5]
            job_time = line.split()[6]
            job_datetime = datetime.strptime(" ".join([job_date, job_time]),
                time_format)
            job_runtimes.append(str(now-job_datetime))
            job_datetimes.append(datetime.strftime(job_datetime, time_format))
            append_jobid = False
            append_jobname = True
            continue

        if append_jobname==True:
            job_names.append(line.split()[2])
            append_jobname = False
            continue

        if line=='--':
            append_jobid = True

    num_jobs = len(job_ids)
    df = pd.DataFrame({'job_id':job_ids, 'name':job_names,
        'status':job_statuses, 'status_start':job_datetimes,
        'runtime':job_runtimes})
    return df[['job_id', 'name', 'status', 'status_start', 'runtime']]

def long_jobs(hour, min, sec, jobdf=None):
    if jobdf is None:
        jobdf = jobs_running()
    else:
        jobdf = jobdf.copy()
    jobdf['runtime'] = jobdf.runtime.apply(lambda x: datetime.strptime(x, "%H:%M:%S.%f").time())
    return jobdf[jobdf.runtime >= time(hour, min, sec)]

def current_slot_usage():
    p1 = subprocess.Popen(["qstat","-u",'"*"'], stdout=subprocess.PIPE)
    p2 = subprocess.Popen([ 'awk',
        "NR>2{a[$4]+=$9}END{ for (i in a) print  i,a[i] }"],
        stdin=p1.stdout,
        stdout=subprocess.PIPE)
    output, err = p2.communicate()
    p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
    p2.stdout.close()
    return output, err

def get_holds(jid):
    p1 = subprocess.Popen(["qstat", "-j", str(jid)], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["grep", "jid_predecessor_list:"], stdin=p1.stdout, stdout=subprocess.PIPE)
    p3 = subprocess.Popen([ 'awk', '{print $2}'], stdin=p2.stdout, stdout=subprocess.PIPE)

    output, err = p3.communicate()

    p1.stdout.close()
    p2.stdout.close()
    p3.stdout.close()

    return output.rstrip('\n')

def add_holds(jid, hold_jid):
    current_holds = get_holds(jid)
    if len(current_holds)>0:
        current_holds = ",".join([current_holds, str(hold_jid)])
    return subprocess.check_output(['qalter', str(jid), '-hold_jid', current_holds])

def resubmit_job(job_id):
    return subprocess.check_output(['qmod', '-r', str(job_id)])

# Submit jobs
def submit_job(runfile, jobname, parameters=[], project=None, slots=4, memory=10, hold_pattern=None, holds=None, jobtype='python', stdout=None, stderr=None, supress_trailing_space=False):

    # Set CPU and memory
    submission_params = ["qsub", "-pe", "multi_slot", str(slots), "-l", "mem_free=%sg" % memory]
    if project is not None:
        submission_params.extend(["-P", project])

    # Set holds, if requested
    if hold_pattern!=None:
        holds = jobs_running(hold_pattern)[1]

    if holds!=None and len(holds)>0:
         if isinstance(holds, (list,tuple)):
             submission_params.extend(['-hold_jid', ",".join(holds)])
         else:
             submission_params.extend(['-hold_jid', str(holds)])

    # Set job name
    submission_params.extend(["-N", jobname])

    if stdout is not None:
        submission_params.extend(['-o', stdout])
    if stderr is not None:
        submission_params.extend(['-e', stderr])

    # Convert all parameters to strings
    assert isinstance(parameters, (list,tuple)), "'parameters' must be a list or a tuple."
    parameters = [ str(p) for p in parameters ]

    # Define script to run and pass parameters
    shellfile = "%s/submit_master.sh" % (this_path)
    submission_params.append(shellfile)
    if jobtype=="python":
        submission_params.append("{FILEPATH}")
        submission_params.append(runfile.strip())
    elif jobtype=="stata":
        submission_params.append("{FILEPATH}")
        submission_params.append(runfile.strip())
    elif jobtype=="R":
        submission_params.append("{FILEPATH}")
        submission_params.append("<")
        submission_params.append(runfile.strip())
        submission_params.append("--no-save")
        submission_params.append("--args")
    else:
        submission_params.append(jobtype)
        submission_params.append(runfile)

    # Creat full submission array
    submission_params.extend(parameters)

    # Deal with errant carriage-returns
    if not supress_trailing_space:
        submission_params.extend([" "])

    # Submit job
    submission_msg = subprocess.check_output(submission_params)
    print(submission_msg)
    jid = submission_msg.split()[2]

    return jid

def get_commit_hash(dir="."):
    cmd = ['git', '--git-dir=%s/.git' % dir, '--work-tree=%s',
            'rev-parse', 'HEAD']
    return subprocess.check_output(cmd).strip()

def get_branch(dir="."):
    cmd = ['git', '--git-dir=%s/.git' % dir, '--work-tree=%s',
            'rev-parse', '--abbrev-ref', 'HEAD']
    return subprocess.check_output(cmd).strip()

def git_dict(dir="."):
    branch = get_branch(dir)
    commit = get_commit_hash(dir)
    return {'branch': branch, 'commit': commit}

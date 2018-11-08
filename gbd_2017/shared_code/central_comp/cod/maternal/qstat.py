import subprocess
import pandas as pd

from datetime import datetime

def qstat_cluster(status=None, pattern=None, user=None, jids=None):
    """parse sge qstat information into DataFrame

    Args:
        status (string, optional): status filter to use when running qstat
            command
        pattern (string, optional): pattern filter to use when running qstat
            command
        user (string, optional): user filter to use when running qstat command
        jids (list, optional): list of job ids to use when running qstat
            command

    Returns:
        DataFrame of qstat return values
    """
    cmd = ["qstat", "-r"]
    if status is not None:
        cmd.extend(["-s", status])
    if user is not None:
        cmd.extend(["-u", user])

    p1 = subprocess.Popen(
        " ".join(cmd),
        stdout=subprocess.PIPE,
        shell=True)
    p2 = subprocess.Popen(
        ["grep", "Full jobname:", "-B1"],
        stdin=p1.stdout,
        stdout=subprocess.PIPE)

    p1.stdout.close()
    output, err = p2.communicate()
    p2.stdout.close()

    # Careful, python 2 vs 3 - bytes versus strings
    if not isinstance(output, str):
        output = output.decode('utf-8')

    lines = output.splitlines()

    job_ids = []
    job_users = []
    job_names = []
    job_slots = []
    job_statuses = []
    job_datetimes = []
    job_runtimes = []
    job_runtime_strs = []
    append_jobid = True
    append_jobname = False
    time_format = "%m/%d/%Y %H:%M:%S"
    now = datetime.now()
    for line in lines:

        if append_jobid is True:
            job_ids.append(int(line.split()[0]))
            job_users.append(line.split()[3])
            job_statuses.append(line.split()[4])
            job_date = line.split()[5]
            job_time = line.split()[6]
            try:
                job_slots.append(int(line.split()[8]))
            except:
                job_slots.append(int(line.split()[7]))
            job_datetime = datetime.strptime(
                " ".join([job_date, job_time]),
                time_format)
            job_runtimes.append((now - job_datetime).total_seconds())
            job_runtime_strs.append(str(now - job_datetime))
            job_datetimes.append(datetime.strftime(job_datetime, time_format))
            append_jobid = False
            append_jobname = True
            continue

        if append_jobname is True:
            job_names.append(line.split()[2])
            append_jobname = False
            continue

        if line == '--':
            append_jobid = True

    df = pd.DataFrame({
        'job_id': job_ids,
        'name': job_names,
        'user': job_users,
        'slots': job_slots,
        'status': job_statuses,
        'status_start': job_datetimes,
        'runtime': job_runtime_strs,
        'runtime_seconds': job_runtimes})
    if pattern is not None:
        df = df[df.name.str.contains(pattern)]
    if jids is not None:
        df = df[df.job_id.isin(jids)]
    return df[['job_id', 'name', 'slots', 'user', 'status', 'status_start',
               'runtime', 'runtime_seconds']]
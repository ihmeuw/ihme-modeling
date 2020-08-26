"""Interface to the dynamic resource manager (DRM), aka the scheduler."""
import atexit

try:
    from collections.abc import Sequence
except ImportError:
    from collections import Sequence
from datetime import datetime, time
import itertools
from functools import lru_cache
import logging
import os
import re
import subprocess
import types

import pandas as pd
import numpy as np

from jobmon.exceptions import SGENotAvailable

# Because the drmaa package needs this library in order to load.
DRMAA_PATH = "PATH"
DRMAA_IMPORT_ERROR = list()  # Store errors so unit tests still work.
if "DRMAA_LIBRARY_PATH" not in os.environ:
    try:
        os.environ["DRMAA_LIBRARY_PATH"] = DRMAA_PATH.format(
            os.environ["SGE_CLUSTER_NAME"])
        import drmaa
    except KeyError:
        DRMAA_IMPORT_ERROR.append("'SGE_CLUSTER_NAME' not set")
    except Exception as e:
        DRMAA_IMPORT_ERROR.append(str(e))
try:
    import drmaa
except ImportError as ie:
    DRMAA_IMPORT_ERROR.append(str(ie))
    drmaa = None
except OSError as oe:
    # The drmaa library throws OSError, not ImportError.
    DRMAA_IMPORT_ERROR.append(str(oe))
    drmaa = None


this_path = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

# Comes from object_name in `man sge_types`. Also, * excluded.
UGE_NAME_POLICY = re.compile(
    r"[.#\n\t\r /\\\[\]:'{}\|\(\)@%,*]|[\n\t\r /\\\[\]:'{}\|\(\)@%,*]")
STATA_BINARY = "PATH"
R_BINARY = "PATH"


def _drmaa_session():
    """
    Get the global DRMAA session or initialize it if this is the first call.
    Can set this by setting DRMAA_LIBRARY_PATH. This should be the complete
    path the the library with .so at the end.
    """
    if drmaa is None:
        raise SGENotAvailable(os.linesep.join(DRMAA_IMPORT_ERROR))
    if "session" not in vars(_drmaa_session):
        session = drmaa.Session()
        session.initialize()
        atexit.register(_drmaa_exit)
        _drmaa_session.session = session
    return _drmaa_session.session


def _drmaa_exit():
    _drmaa_session.session.exit()


def qstat(status=None, pattern=None, user=None, jids=None):
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
        ["grep", "Full jobname:", "-B1", "-A1"],
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
    job_hosts = []
    linetype = "job_summary"
    time_format = "%m/%d/%Y %H:%M:%S"
    now = datetime.now()
    for line in lines:

        if line == '--':
            linetype = "job_summary"
            continue

        if linetype == "job_summary":
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
            linetype = "job_name"
            continue

        if linetype == "job_name":
            job_names.append(line.split()[2])
            linetype = "job_host"
            continue

        if linetype == "job_host":
            if "Master Queue" not in line:
                job_hosts.append("")
                continue
            host = line.split()[2].split("@")[1]
            job_hosts.append(host)
            lintype = "--"
            continue

    df = pd.DataFrame({
        'job_id': job_ids,
        'hostname': job_hosts,
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
    return df[['job_id', 'hostname', 'name', 'slots', 'user', 'status',
               'status_start', 'runtime', 'runtime_seconds']]


def qstat_details(jids):
    """get more detailed qstat information

    Args:
        jids (list): list of jobs to get detailed qstat information from

    Returns:
        dictionary of detailed qstat values
    """
    jids = np.atleast_1d(jids)
    cmd = ["qstat", "-j", "%s" % ",".join([str(j) for j in jids])]

    def group_separator(line):
        delim = "".join(["=" for i in range(62)])
        return line == delim

    deets = subprocess.check_output(cmd).decode("utf-8")
    deets = deets.splitlines()
    jobid = 0
    jobdict = {}
    for key, group in itertools.groupby(deets, group_separator):
        for line in group:
            if group_separator(line):
                continue
            ws = line.split(":")
            k = ws[0].strip()
            k = re.sub(r'\s*1', '', k)  
            v = ":".join(ws[1:]).strip()
            if k == 'job_number':
                v = int(v)
                jobdict[v] = {}
                jobid = v
            jobdict[jobid][k] = v
    return jobdict


def convert_wallclock_to_seconds(wallclock_str):
    wc_list = wallclock_str.split(':')
    wallclock = (float(wc_list[-1]) + int(wc_list[-2]) * 60 +
                 int(wc_list[-3]) * 3600)  # seconds.milliseconds, minutes, hrs
    if len(wc_list) == 4:
        wallclock += (int(wc_list[-4]) * 86400)  # days
    elif len(wc_list) > 4:
        raise ValueError("Cant parse wallclock for logging. Contains more info"
                         " than days, hours, minutes, seconds, milliseconds")
    return wallclock


def qstat_usage(jids):
    """get usage details for list of jobs

    Args:
        jids (list): list of jobs to get usage details for

    Returns:
        Usage details.
    """
    jids = np.atleast_1d(jids)
    details = qstat_details(jids)
    usage = {}
    for jid, info in details.items():
        usage[jid] = {}
        usagestr = info['usage']
        parsus = {u.split("=")[0]: u.split("=")[1]
                  for u in usagestr.split(", ")}
        parsus['wallclock'] = convert_wallclock_to_seconds(parsus['wallclock'])
        usage[jid]['usage_str'] = usagestr
        usage[jid]['nodename'] = info['exec_host_list']
        usage[jid].update(parsus)
    return usage


def long_jobs(hour, min, sec, jobdf=None):
    """get list of jobs that have been running for longer than the specified
    time

    Args:
        hour (int): minimum hours of runtime for job filter
        min (int): minimum minutes of runtime for job filter
        sec (int): minimum seconds of runtime for job filter
        jobdf (DataFrame, optional): qstat DataFrame to filter. by default
            will get current qstat and filter it by (hour, min, sec). custom
            DataFrame may be provided by using jobdf

    Returns:
        DataFrame of jobs that have been running longer than the specified time
    """
    if jobdf is None:
        jobdf = qstat()
    else:
        jobdf = jobdf.copy()
    jobdf['runtime'] = jobdf.runtime.apply(
        lambda x: datetime.strptime(x, "%H:%M:%S.%f").time())
    return jobdf[jobdf.runtime >= time(hour, min, sec)]


def current_slot_usage(user=None):
    """number of slots used by current user

    Args:
        user (string): user name to check for usage

    Returns:
        integer number of slots in use by user
    """
    jobs = qstat(user=user, status="r")
    slots = jobs.slots.astype('int').sum()
    return slots


def get_holds(jid):
    """get all current holds for a given job

    Args:
        jid (int): job id to check for list of holds

    Returns:
        comma separated string of job id holds for a given string
    """
    p1 = subprocess.Popen(["qstat", "-j", str(jid)], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(
        ["grep", "jid_predecessor_list:"],
        stdin=p1.stdout,
        stdout=subprocess.PIPE)
    p3 = subprocess.Popen(
        ['awk', '{print $2}'],
        stdin=p2.stdout,
        stdout=subprocess.PIPE)
    p1.stdout.close()
    p2.stdout.close()
    output, err = p3.communicate()
    p3.stdout.close()
    return output.rstrip('\n')


def add_holds(jid, hold_jid):
    """add new hold to an existing job

    Args:
        jid (int): job id of job to add holds to
        hold_jid (int): job id of job to add as hold to jid

    Returns:
        standard output of qalter command
    """
    current_holds = get_holds(jid)
    if len(current_holds) > 0:
        current_holds = ",".join([current_holds, str(hold_jid)])
    stdout = subprocess.check_output(
        ['qalter', str(jid), '-hold_jid', current_holds])
    return stdout


def _suffix(path):
    """Suffix of a filesystem path."""
    if isinstance(path, str):
        name = path.split(os.sep)[-1]
        dot_location = name.rfind(".")
        if dot_location > 0:
            return name[dot_location:]
        else:
            return None
    else:
        # Assume it's a pathlib.Path object.
        return path.suffix


def qsub(
        runfile,
        jobname,
        queue,
        parameters=None,
        project=None,
        slots=4,
        memory=10,
        runtime=None,
        hold_pattern=None,
        holds=None,
        shfile=None,
        jobtype=None,
        stdout=None,
        stderr=None,
        prepend_to_path=None,
        conda_env=None,
        environment_variables={}):
    """Submits job to Grid Engine Queue.
    This function provides a convenient way to call scripts for
    R, Python, and Stata using the job_type parameter.
    If the job_type is None, then it executes whatever path
    is in run_file, which may be a shell script or a binary.
    If the parameters argument is a list of lists of parameters or a
    generator of lists of parameters, then this will submit multiple jobs.

    If a shell file is given as shfile, then this function will submit the
    shell file as a job but construct the arguments to that shell file
    using the given job_type.

    Args:
        runfile (string): absolute path of script or binary to run
        jobname (string): what to call the job
        parameters (tuple or list, optional): arguments to pass to run_file.
        project (string, option): What project to submit the job under. Default
            is ihme_general.
        slots (int, optional): How many slots to request for the job.
            approximate using 1 core == 1 slot, 1 slot = 2GB of RAM
        memory (int, optional): How much ram to request for the job.
        runtime (str, optional): How long to run as HH:MM:SS.
        hold_pattern (string, optional): looks up scheduled jobs with names
            matching the specified patten and sets them as holds for the
            requested job.
        holds (list, optional): explicit list of job ids to hold based on.
        shfile (string, optional): All arguments are sent to the given
            shell script to execute. This shell script is NOT copied to
            the execution host. This is equivalent to "-b y" on qsub.
        jobtype (string, optional): joint purpose argument for specifying what
            to pass into the shell_file. can be arbitrary string or one of the
            below options. default is 'python'

                'python':
                    Uses the default python on the path to execute run_file.
                'stata':
                    stata-mp -b do run_file
                'R':
                    R < runfile --no-save --args
                'shell':
                    For shell scripts. Add -shell y to the arguments.  This
                    also ensures SGE copies the shell script so that if you
                    modify it after submission the original submitted script is
                    what runs.
                'plain':
                    Don't use any interpreter, even if the runfile suffix
                    is known.
                None:
                    Look at runfile's suffix to pick interpreter, if it's
                    known.

        stdout (string, optional): where to pipe standard out to. default is
            /dev/null. Recognizes $HOME, $USER, $JOB_ID, $JOB_NAME, $HOSTNAME,
            and $TASK_ID.
        stderr (string, optional): where to pipe standard error to. default is
            /dev/null.  Recognizes $HOME, $USER, $JOB_ID, $JOB_NAME, $HOSTNAME,
            and $TASK_ID.
        prepend_to_path (string, optional): Copies the current shell's
            environment variables and prepends the given one. Without this,
            the PATH is typically very short (PATH).
        conda_env (string, optional): If the job_type is Python, this
            finds the Conda environment by with the given name on the
            submitting host. If conda_env
            is a rooted path (starts with "/"), then this uses the
            path as given.
        environment_variables (dict, optional): Dictionary of
            environment variables to pass to the qsub job context.

    Returns:
        job_id of submitted job
    """
    if slots <= 0:
        raise ValueError("Requested number of slots must be greater than "
                         "zero not {}".format(slots))

    if holds and hold_pattern:
        raise ValueError("Cannot have both 'holds' and 'hold_pattern' set")

    if len(str(jobname)) == 0:
        raise ValueError("Must supply jobname, was empty or null")

    if isinstance(parameters, str):
        raise ValueError("'parameters' cannot be a string, must be a list or "
                         "a tuple. Value passed='{}'".format(parameters))

    # Known suffix, has job_type, shfile
    # N             N             N      Run runfile.
    # Y             N             N      Find jobtype and run interpreter.
    # N             Y             N      Run given jobtype's interpreter.
    # Y             Y             N      Run given jobtype's interpreter
    # N             N             Y      Run shfile with runfile as arg.
    # Y             N             Y      Build args to shfile for found jobtype
    # N             Y             Y      Build args to shfile for jobtype.
    # Y             Y             Y      Use given jobtype and build args.
    if not jobtype:
        job_types = {
            ".py": "python",
            ".pyc": "python",
            ".do": "stata",
            ".sh": "shell",
            ".r": "R",
            ".R": "R"}
        jobtype = job_types.get(_suffix(runfile), "plain")

    # Set holds, if requested
    if hold_pattern is not None:
        holds = qstat(hold_pattern)[1]

    if isinstance(holds, str):
        holds = holds.replace(" ", "")
    elif isinstance(holds, Sequence):
        holds = ",".join([str(hold_job_id) for hold_job_id in holds])
    elif holds:
        logger.error("Holds not a string or list {}".format(holds))
        raise ValueError("Holds not a string or list {}".format(holds))
    else:
        pass  # No holds

    session = _drmaa_session()
    template = session.createJobTemplate()

    need_shell = shfile or jobtype == "shell" or (conda_env and
                                                  not os.path.isabs(conda_env))
    native = [
        "-P {}".format(project) if project else None,
        "-q {}".format(queue),
        "-l m_mem_free={!s}G".format(memory) if memory else None,
        "-l fthread={}".format(slots),
        "-l h_rt={}".format(runtime) if runtime else None,
        "-hold_jid {}".format(holds) if holds else None,
        # Because DRMAA defaults to -shell n, as opposed to qsub default.
        # And because it defaults to -b y.
        "-shell y" if need_shell else None,
        "-b n" if jobtype == "shell" else None
    ]
    template.nativeSpecification = " ".join(
        [str(native_arg) for native_arg in native if native_arg])
    logger.debug("qsub native {}".format(template.nativeSpecification))
    template.jobName = UGE_NAME_POLICY.sub("", jobname)
    template.outputPath = ":" + (stdout or "/dev/null")
    template.errorPath = ":" + (stderr or "/dev/null")

    runfile = os.path.expanduser(runfile)

    if prepend_to_path:
        path = "{}:{}".format(prepend_to_path, os.environ["PATH"])
        environment_variables["PATH"] = path

    if len(environment_variables) > 0:
        template.jobEnvironment = environment_variables
        logger.debug("qsub environment {}".format(template.jobEnvironment))

    single_job = True
    if parameters:
        if isinstance(parameters, types.GeneratorType):
            single_job = False
        elif isinstance(parameters[0], str):
            parameters = [parameters]
        elif isinstance(parameters[0], Sequence):
            single_job = False
        else:
            parameters = [parameters]
    else:
        parameters = [list()]

    job_ids = list()
    for params in parameters:
        str_params = [str(bare_arg).strip() for bare_arg in params]
        if shfile:
            qsub_args = [shfile]
        else:
            qsub_args = list()
        if jobtype == "python":
            if shfile:
                if conda_env:
                    qsub_args.append(conda_env)
                qsub_args.extend(["python", runfile])
                if str_params:
                    qsub_args.extend(str_params)
            else:
                if conda_env:
                    if os.path.isabs(conda_env):
                        # We can skip using a shell in qsub if we have a
                        # full path to a conda environment.
                        python_env_bin = os.path.join(conda_env, "bin/python")
                        qsub_args.extend([python_env_bin, runfile])
                    else:
                        # If there's no full path, we have to use a shell
                        # and activate the environment, but still no need
                        # for a shell script.
                        cmd = "source activate {} && python".format(conda_env)
                        qsub_args.extend(cmd.split())
                        qsub_args.append(runfile)
                else:
                    qsub_args.extend(["python", runfile])
                if str_params:
                    qsub_args.extend(str_params)
        elif jobtype == "stata":
            if shfile:
                qsub_args.extend([STATA_BINARY, runfile])
            else:
                qsub_args.extend([STATA_BINARY, "-b", "do", runfile])
            if str_params:
                qsub_args.extend(str_params)
        elif jobtype == "R":
            if shfile:
                qsub_args.extend([R_BINARY, runfile])
                if str_params:
                    qsub_args.extend(str_params)
            else:
                qsub_args.extend([R_BINARY, "--vanilla", "-f", runfile])
                if str_params:
                    # For R, arguments need to be a single entry in ARGV,
                    # so join them.
                    r_args = "--args {}".format(" ".join(str_params))
                    qsub_args.append(r_args)
        elif jobtype == "plain" or jobtype == "shell":
            qsub_args.append(runfile)
            if str_params:
                qsub_args.extend(str_params)
        else:
            raise ValueError("sge.qsub unknown job type {}".format(jobtype))
        logger.debug("qsub args {}".format(qsub_args))

        template.remoteCommand = qsub_args[0]
        if len(qsub_args) > 1:
            template.args = qsub_args[1:]

        job_ids.append(session.runJob(template))

    if single_job:
        return job_ids[0]
    else:
        return job_ids


@lru_cache(maxsize=None)
def max_run_time_on_queue(queue_name):
    qconf_command = subprocess.run(
        'which qconf',
        shell=True,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ).stdout.strip()
    qconf_key_value = subprocess.run(
        [qconf_command, '-sq', queue_name],
        shell=False,
        stdout=subprocess.PIPE,
        universal_newlines=True
    ).stdout
    return [x.split() for x in qconf_key_value.splitlines()
            if x.startswith('h_rt')][0][1]


def get_sec(time_str):
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)

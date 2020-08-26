
import subprocess
import os
import sys
import getpass
import time as time


###################################
#          utilities
###################################

class JIDError(Exception):
    pass


class WrongLang(Exception):
    pass


def get_jid(response):
    """
    This function takes qsub output and returns jid. Assumes qsub response
    of the form "Your job [jid] ([jname]) has been submitted" for regular jobs.
    Can handle array jobs as well.

    Parameters
    ----------
    response - str - the response from a subprocess call to qsub

    Returns - str - the jid of the submitted job
    -------

    """

    split_response = response.split()
    # Qsub response can have funky stuff added. But, JID should
    # directly follow "Your job".
    try:
        your_ind = split_response.index('Your')
        # job arrays will say "job-array"
        # regular jobs just say "job"
        if 'job' in split_response:
            job_ind = split_response.index('job')
        elif 'job-array' in split_response:
            job_ind = split_response.index('job-array')
        else:
            raise JIDError
    except ValueError:
        print("\nThe response was formatted differently than expected:\n\n{}\n".format(response))
        raise JIDError

    if job_ind - your_ind == 1:
        # "job" should directly follow "Your"
        jid = split_response[job_ind + 1]
    else:
        raise JIDError

    # If this is an array job, we want the parent jid not the array indicators.
    # remove everything after the period
    period_ind = jid.find(".")
    if period_ind != -1:  # I wish -1 evaluated to false ...
        jid = jid[:period_ind]

    return jid


def block_for_jid(jid):
    """Checks whether jid is currently running

    Parameters
    ----------
    jid - str - an SGE job id

    Returns - bool - True if jid is running, else False
    -------

    """

    response = subprocess_wraper('qstat')

    if jid in response:
        return True
    else:
        return False


def qsub(slots, mem, name, path_to_script, env='root', sge_flags=None, script_args=None, hold_jid=None,
         e=True, o=False, lang='python', project='proj_dex'):
    """
    
    Submits a job to SGE via qsub. Handles variable qsub flags and qsub flags; can handle array jobs.


    Parameters
    ----------
    slots - int or str - number of slots to request for job
    mem - int or str - RAM in gb to request for job
    name - str - name for job
    path_to_script - str - absolute path to script for the job to execute
    env - str - conda environment to use for the job
    sge_flags - list - flags and arguments, sequential, to pass to qsub
    script_args - list - flags and arguments, sequential, to pass to the script
    hold_jid - str - Comma separated list of job ids to hold this qsub on
    e - bool or str - if True, will save stderr to sge_output, if False, won't save, if str, must be path to save to
    o - bool or str - if True, will save stdout to sge_output, if False, won't save, if str, must be path to save to
    lang - str - {python, R} - language of script to submit
    project - str - SGE project designation for the submitted job

    Returns - str - JID of submitted job
    -------

    """

    # Assign wha should be lists, but don't want mutable defaults
    if sge_flags is None:
        sge_flags = []
    if script_args is None:
        script_args = []

    # Cast some things that could be ints

    if isinstance(slots, int):
        slots = str(slots)
    elif not isinstance(slots, str):
        raise TypeError("Specify slots as str or int")

    if isinstance(mem, int):
        mem = str(mem)
    elif not isinstance(mem, str):
        raise TypeError("Specify mem as str or int")

    # some tests, because I do some iterable adding
    if sge_flags is not None:
        assert isinstance(sge_flags, list)
    if script_args is not None:
        assert isinstance(script_args, list)
    if hold_jid is not None:
        assert isinstance(hold_jid, str)

    # Do I want to force a python environment?
    # Don't use R much but as it stands, we use no virtualenv
    # assert lang == "python" and env is not None, "Must specify conda environment for a python qsub"
    #if lang == "R":
        #assert env is None, "R doesn't use a conda environment"

    # Set iostream paths. By default error is saved because I use them a lot.
    if e:
        if e is True:
            e_path = FILEPATH
        else:
            e_path = e
    else:
        e_path = 'FILEPATH'
    if o:
        if o is True:
            o_path = FILEPATH
        else:
            o_path = o
    else:
        o_path = 'FILEPATH'

    # get the language straight. I never use the rshell right now.
    if lang == 'python':
        path_to_shell = os.path.dirname(
            os.path.realpath(__file__)) + '/python_shell.sh'
    elif lang == 'R':
        path_to_shell = os.path.dirname(
            os.path.realpath(__file__)) + '/r_shell.sh'
    else:
        raise WrongLang("Incorrect Language Provided.")
    # If a jid is passed to hold on add it to the flags
    if hold_jid is not None:
        sge_flags += ['-hold_jid', hold_jid]

    cmd = (['FILEPATH',
            '-V',  # export environment variables to job context
            '-pe',  # parallel environment
            'multi_slot', slots,  # pe option slots request
            '-l', 'mem_free={mem}G'.format(mem=mem),  # pe option mem request
            '-N', name,  # job name
            '-P', project,  # Project name
            '-e', e_path,  # path for std.err logs
            '-o', o_path] +  # path for std.out logs
           sge_flags +  # qsub flags, like -t for array jobs, or -hold_jid if it was passed
           [path_to_shell,  # shell script that will be executed on node
            path_to_script] +  # script you want run, will be run by the shell script
           script_args)  # args to be passed to the script

    #cmd = ' '.join(cmd)

    print("This is broken for python 2.7")
    #response = subprocess_wraper(cmd)

    #Print the response to stdout
    #print(response)
    return cmd #get_jid(cmd) # change this back to response


## in case sub process fails
def subprocess_wraper(text):
    ## in case sub process fails
    output = None
    while output is None:
        try:
            output = subprocess.getoutput(text)
        except:
            time.sleep(2)
            pass
    return output

## iterate through a list of jobs and hold
def holder(jid):
    for i in jid:
        hold=block_for_jid(i)
        while hold==True:
            time.sleep(.1)
            hold=block_for_jid(i)


def check_job_failure(jids):
    for i in jids:
        ## looks at job
        output = subprocess_wraper('qacct -j {}'.format(i)).split()
        ## check if qacct info has been collected.. if not wait till it is collected
        if 'exit_status' not in output:
            hold = False
            while hold == False:
                output = subprocess_wraper('qacct -j {}'.format(i)).split()
                hold = 'exit_status' in output
                time.sleep(1)
        ## code throwing error
        if int(output[output.index('exit_status') + 1]) != 0:  ## anything not 1 is bad
            filename = output[output.index('jobname') + 1]
            error_file = subprocess_wraper('cat ' + output[output.index('-e') + 1] + '/' + filename + '.e' + i)
            output_file = subprocess_wraper('cat ' + output[output.index('-o') + 1] + '/' + filename + '.o' + i)
            print('output file')
            print(output_file)
            print('error file')
            print(error_file)

            raise ValueError('Error in job id {}'.format(i))


## limit the number of jobs that are on the cluster
def job_limiter(model_start, job_limit=100):
    time.sleep(.1)
    output = subprocess_wraper('qstat')
    hold = len([x for x in output.split() if model_start in x]) > job_limit
    while hold == True:
        output = subprocess_wraper('qstat')
        hold = len([x for x in output.split() if model_start in x]) > job_limit
        time.sleep(1)  ## not too much


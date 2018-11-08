
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


def qsub():
    """
    

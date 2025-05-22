# -*- coding: utf-8 -*-
'''
Description: Loads functions for interacting with directories. At end of
    scripts, appends common cancer code folder to sys.path
Contents:
    find_nth_substring
    is_filepath
    ensure_dir
    get_cluster_setting
    get_path
    get_gbd_parameter
    get_root

Arguments: see individual functions
Output: get_path returns a string-formatted filepath. a 'process' must be
            specified for non-common paths (see function)
        get_path_listing returns a dictionary will all filepaths for the
            process requested (default ="common")
Contributors: INDIVIDUAL_NAME
'''

import os
import sys
import yaml
from time import sleep
import warnings
from getpass import getuser
from datetime import datetime
import argparse
from cancer_estimation.py_utils.helpers.clean_tree_helpers import clean_tree
import cancer_estimation.py_utils.response_validator as rv
try:
    from functools import lru_cache
except ImportError:
    # fallback to Python2 (for use if running on gbd environment)
    from functools32 import lru_cache


def check_gbd_parameters_file() -> None:
    ''' Triggers a response validator to ensure correct release_id, gbd_round_id,
        and other req. gbd_parameters variables are accurate
    '''
    # set key gbd parameters to display
    key_gbd_params = ['current_gbd_name', 'current_gbd_round', 
                      'current_decomp_step', 'estimation_years',
                      'max_year', 'min_year_cod', 'min_year_epi']
    # create message, concaneate 
    gbd_params_msg = "".join(
        ["{}: \033[92m{}\033[00m\n".format(param, get_gbd_parameter(param)) 
         for param in key_gbd_params])

    res = {"y":"Yes, proceed"}
    rv.response_validator(
        prompt = "\033[91mVerify that the following gbd settings are accurate before proceeding.\n"\
               "(enter q to quit): \033[00m\n{}".format(gbd_params_msg),
        correct_responses = res)
    return(None)


def display_timestamp():
    ''' Returns the current date and time formatted for insertion into a filepath
    '''
    return (datetime.today().strftime("%Y_%m_%d_%H%M"))

def display_datestamp(): 
    ''' Returns the current data formatted for insertion into a filepath
    '''
    return (datetime.today().strftime('%Y_%m_%d'))

def db_timestamp():
    ''' Returns the current date and time formatted for insertion into a filepath
    '''
    return (datetime.today().strftime('%Y-%m-%d %H:%M:%S'))


def human_sort(some_list):
    ''' Sort the given list in the way that humans expect.
            http://nedbatchelder.com/blog/200712/human_sorting.html
    '''
    def atoi(text): return int(text) if text.isdigit() else text

    def natural_keys(text): return [atoi(c) for c in re.split(r'(\d+)', text)]
    return(some_list.sort(key=natural_keys))


def find_nth_substring(haystack, needle, n):
    ''' returns the index of the nth "needle" substring of the "haystack" string
    '''
    ix = 0
    while ix >= 0 and n > 1:
        ix = haystack.find(needle, ix + len(needle))
        n -= 1
    return ix


def str2bool(v):
    ''' Accepts a string values and returns a boolean interpretation
    '''
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return(True)
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return(False)
    else:
        raise (TypeError('Boolean value expected.'))


class StrJoin(argparse.Action):
    ''' Argparse action class to join a list into a space-separate string.
            Returns list of length 1 if no spaces present
    '''

    def __call__(self, parser, namespace, values, option_string=None):
        # Test for space-separated list. Return values as list
        print(values)
        if ' ' in values:
            setattr(namespace, self.dest, ' '.join(values))
        else:
            setattr(namespace, self.dest, values)


def force_remove_from_list(whatever_list, to_remove):
    ''' Removes either single entries or a list of entries from whatever_list if they are
        present. Avoids value error if entries (or entry) are not present.
    '''
    if not isinstance(to_remove, list):
        to_remove = list(to_remove)
    for value in to_remove:
        try:
            whatever_list.remove(value)
        except ValueError:
            pass
    return (whatever_list)


def is_ascii(string):
    ''' useful if pandas cannot encode an output
    '''
    if all([ord(s) < 128 for s in string]):
        return True
    return False


def clean_directory_tree(path, filepattern='', extension=''):
    ''' recursively removes files matching the argument patterns
        see helpers.clean_tree for more information
    '''
    clean_tree(path, filepattern, extension)


def is_filepath(path):
    ''' Takes a path as input and determines if the path leads to a file
        or not.
    '''
    dir_path, possible_file = os.path.split(path)
    if "." in dir_path:
        raise ValueError("Please fix this path. Periods should not exist in"
                         " directory names: {}".format(path))
    # assumes that tail of path is a file if it contains a period
    if "." in possible_file:
        return (True)
    else:
        return (False)


def ensure_dir(somePath):
    ''' Accepts a directory string and ensures it's existence. Also accepts a
            filepath and ensures presence of the containing directory
    '''
    # Validate that the root exists
    which_slash = 2 if somePath[0] == "/" else 1
    root = somePath[:find_nth_substring(somePath, "/", which_slash)]
    assert os.path.exists(root), "Path sent with invalid root: {}".format(root)
    # Create directory tree for path
    if is_filepath(somePath):
        directory_path = os.path.dirname(somePath)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
    else:
        if not os.path.exists(somePath):
            os.makedirs(somePath)
    return()


def get_root(root_name):
    ''' returns the string-formatted filepath of the requested root, or
        returns a list of possible roots if the "list_roots" is passed
    '''
    roots = set_roots()
    if root_name in roots.keys():
        return(roots[root_name])
    elif root_name == "list roots":
        return(list(roots.keys()))
    else:
        return("incorrect root_name requested")


@lru_cache()
def set_roots():
    '''
    '''
    # get current gbd_round
    gbd_round_folder = get_gbd_parameter('current_gbd_name')

    # Set mapped-drive locations
    j_drive = _get_mapped_drive("j")
    j_temp = "{}/temp/registry/cancer/{}".format(j_drive, gbd_round_folder)
    scratch = "/share/cancer/{}".format(gbd_round_folder)
    workspace = scratch if 'linux' in sys.platform else j_temp
    roots = {
        "j": j_drive,
        "i": _get_mapped_drive("i"),
        "h": _get_mapped_drive("h"),
        "j_temp": j_temp,
        "scratch": scratch,
        "workspace": workspace,
        "code_repo": find_current_repo(),
        "storage": "{}/WORK/07_registry/cancer/{}/".format(j_drive, gbd_round_folder)
    }
    return(roots)


def set_current_repo():
    ''' Prepends currently active repo and it's references to the PYTHONPATH
    '''
    this_repo = find_current_repo()
    # Keep references to the current repo
    current_refs = [p for p in sys.path if this_repo in p]
    # # Remove paths of other & all cancer_estimation repos
    sys.path = [p for p in sys.path if 'cancer_estimation' not in p]
    # Add the current repo references to the beginning of the sys path
    sys.path = list(set([this_repo] + current_refs)) + sys.path
    return(None)


def find_current_repo():
    ''' Returns the repo location for the code that is currently running.
    '''
    try:
        this_dir = os.path.realpath(__file__).strip(".py")
    except Exception:
        this_dir = os.getcwd()
        assert '/cancer_estimation' in this_dir, \
            "ERROR: code run interactively must be run from a repo sub-directory"
    repo_len = len("/cancer_estimation") + 1
    end_repo_name = this_dir.find("/cancer_estimation")+repo_len
    code_repo = this_dir[:end_repo_name].rstrip(
        '/')  # slice and remove trailing slash
    return(code_repo)


def _get_mapped_drive(which_drive):
    '''
    '''
    assert which_drive in ['h', 'i', 'j'], "Invalid argument for which_drive"
    this_platform = sys.platform
    if this_platform.startswith("win"):
        return("{}:".format(which_drive.upper()))  # eg "H:"
    elif this_platform.startswith('linux'):
        if which_drive == "h":
            return("/homes/{}".format(getuser()))
        else:
            return("/home/{}".format(which_drive))
    elif this_platform.startswith('darwin'):
        drive_locs = {'h': "/Volumes/{}".format(getuser()),
                      'j': "/Volumes/snfs",
                      'i': '/Volumes/ihme'}
        return (drive_locs[which_drive])


def get_path(key, process="common", base_folder="storage"):
    ''' returns a string with the user-specified path associated with
        the passed key
        accepts an optional specified base to override generic paths
            (i.e. ["cancer_folder"])
        accepts an optional specified process key to link with
            process-specific paths
    '''
    # Loaded helper function inside get_path to avoid to cyclical reference
    from cancer_estimation.py_utils.helpers.get_path_helpers import get_path_listing
    # If the key is a root, return the root
    all_roots = get_root("list roots")
    if key in all_roots:
        return(get_root(key))

    # Verify that the requested root is acceptable
    acceptable_roots = [r for r in all_roots if len(r) > 1]
    if base_folder not in acceptable_roots:
        raise KeyError(
            "User Error. To use base_folder specification with a get_path key, "
            "the root must be one of the folowing: "
            "{} {}".format(os.linesep, acceptable_roots + ["process_paths"]) +
            "\nYou entered \"{}\"".format(base_folder)
        )
    # load the directory of paths and return the matching value, if present
    path_directory = get_path_listing(base_folder, process)
    if path_directory == None:
        sleep(8)
        path_directory = get_path_listing(base_folder, process)
        if path_directory == None:
            raise LookupError(
                "ERROR: Could not find path for  \"{}\", (base_folder={}, process={}). ] \n".format(
                    key, base_folder, process) +
                "Check cancer_paths file for typos or duplicate entries. " +
                "If no cancer_paths errors exist, " +
                "likely reasons are a bad cluster node or a connectivity issue"
            )
        else:
            pass
    if key not in path_directory.keys():
        raise KeyError(
            "The requested path, \"{}\", does not exist".format(key))
    requested_path = str(path_directory[key])
    if requested_path == "":
        raise KeyError(
            "The requested path, \"{}\", does not exist".format(key))
    return(requested_path)


def get_gbd_parameter(parameter_name):
    ''' retruns the gbd_parameter entry for the parameter_name.
        returns a dict of parameters if parameter_name = 'list'
    '''
    path_file = find_current_repo() + "/gbd_parameters.yaml"
    with open(path_file) as data_file:
        parameter_dict = yaml.safe_load(data_file)
    if parameter_name not in list(parameter_dict.keys()) + ['list']:
        raise LookupError("Could not find specified parameter, "
                          "{}, in list of parameters".format(parameter_name))
    elif parameter_name == 'list':
        return(list(parameter_dict.keys()))
    else:
        return(parameter_dict[parameter_name])


def get_cluster_setting(setting_key):
    ''' Returns a value from cluster_settings
    '''
    # Loaded helper function inside get_cluster_setting to avoid to cyclical reference
    from cancer_estimation.py_utils.helpers.get_path_helpers import substitute_roots
    path_file = get_path("cluster_settings")
    with open(path_file) as data_file:
        yaml_data = yaml.safe_load(data_file)
    if setting_key not in yaml_data.keys():
        sys.exit("ERROR: Incorrect cluster setting specified.",
                 "No matching process key for",
                 "\'{}\' in cluster_settings".format(setting_key))
    refs = substitute_roots(yaml_data)
    entry = refs[setting_key]
    return(entry)


def clear_hdf(hdf_file):
    ''' Close open hdf file to prevent conflict with other scripts using the
            same file
    '''
    import pandas as pd
    print("Generating submission subset...")
    if os.path.isfile(hdf_file):
        try:
            temp = pd.read_hdf(hdf_file)
            del temp
            os.remove(hdf_file)
        except:
            pass
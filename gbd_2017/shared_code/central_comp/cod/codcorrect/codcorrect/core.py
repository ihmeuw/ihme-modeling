try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest
import pandas as pd
import json
import getpass
import subprocess
import os


def read_json(file_path):
    json_data = open(file_path)
    data = json.load(json_data)
    json_data.close()
    return data


def write_json(json_dict, file_path):
    je = open(file_path, 'w')
    je.write(json.dumps(json_dict))
    je.close()


def get_credentials(key, credential_path=None):
    if credential_path is None:
        credential_path = ("/FILE/PATH/TO/db_credentials.json"
                           .format(getpass.getuser()))
    c = read_json(credential_path)
    return c[key]['user'], c[key]['password']


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks"""
    args = [iter(iterable)] * n
    return list(zip_longest(fillvalue=fillvalue, *args))


def get_code_vers():
    git_dir = '--git-dir=' + os.path.join(
        os.path.dirname(os.path.dirname(__file__)), '.git')
    work_tree = '--work-tree=' + os.path.dirname(os.path.dirname(__file__))
    code_vers = str(subprocess.check_output(
        ["git", git_dir, work_tree, "rev-parse", "HEAD"])).rstrip('\n')
    return code_vers


class Envelope(object):
    """ Holds the all-cause mortality envelope """

    def __init__(self, data, index_columns, data_columns):
        """ Return an object with the envelope data as well as
        the index, population, and data column names. """
        self.data = data
        self.index_columns = index_columns
        self.data_columns = data_columns

    def reshape_long(self):
        data = self.data.copy(deep=True)
        data = data[self.index_columns + self.data_columns]
        data = pd.melt(data, id_vars=self.index_columns,
                       var_name='draw', value_name='envelope')
        return data

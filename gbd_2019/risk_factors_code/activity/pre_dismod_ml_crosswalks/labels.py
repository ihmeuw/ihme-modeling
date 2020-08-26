import pandas as pd 
import sys
import configparser
from ml_crosswalk.drives import Drives
import os
import ast


config = configparser.ConfigParser()
config.read(Drives().h + 'FILEPATH/config.ini')


class SharedLabels:
    def __init__(self):
        self.as_dict = False

    def super_regions(self, as_dict=False):
        super_region_df = pd.read_csv(Drives().h + 'locations.csv')
        super_region_df = super_region_df[['location_id', 'super_region_id', 'super_region_name']].drop_duplicates()

        super_region_df = super_region_df.dropna()
        super_region_df.iloc[:, 0] = super_region_df.iloc[:, 0].astype('int')
        super_region_df.super_region_id = super_region_df.super_region_id.astype('int')

        if as_dict:
            super_region_dict = _convert_to_dict(super_region_df.drop('location_id', axis=1))
            return super_region_dict
        else:
            return super_region_df


def _convert_to_dict(df):
    """
    Converts a data frame to a dictionary.

    :param df: a Pandas DataFrame with two columns
    :return: a python dictionary
    """
    col_names = df.columns.tolist()
    dictionary = df.set_index([col_names[0]])[col_names[1]].to_dict()

    return dictionary


def get_save_dir(topic, estimand, estimator=None, version=None):
    """
    Creates a new directory if it does not exist

    :param topic:
    :param estimand:
    :param estimator:
    :param version:
    :return:

    """

    if topic == "mets":
        root_dir = "FILEPATH"
    elif topic == "lpa":
        root_dir = "FILEPATH"
    else:
        root_dir = "FILEPATH"

    if version:
        version = str(version).lower() + '/'
    else:
        print("No version given. Defaulting to \'original\'")
        version = 'original/'

    if topic in ["mets", "lpa"]:
        save_dir = root_dir + 'v{}'.format(version) + 'FILEPATH' + estimand.lower() + '/'
    else:
        save_dir = root_dir + 'version_{}'.format(version.replace(" ", "_")) + 'FILEPATH' + estimand.lower() + '/'

    if estimator:
        save_dir += 'with_' + estimator.lower() + '/'

    print('Using ' + save_dir + '\nas the saving directory.')
    
    if not os.path.exists(save_dir):
        try:
            os.makedirs(save_dir)
        except OSError:
            print("Oops, can't save to directory: " + save_dir)
            print("You have a root directory of: " + root_dir)

    return save_dir

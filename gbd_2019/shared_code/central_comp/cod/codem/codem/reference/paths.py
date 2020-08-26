import os
import subprocess
from codem.metadata.step_metadata import STEP_IDS
import logging

logger = logging.getLogger(__name__)


class FilePaths:
    BASE_DIR = 'FILEPATH'


class ModelPaths:
    def __init__(self, model_version_id, acause):
        self.BASE_DIR = FilePaths.BASE_DIR.format(model_version_id=model_version_id,
                                                  acause=acause)

        self.INPUT_DIR = os.path.join(self.BASE_DIR, 'inputs')
        os.makedirs(self.INPUT_DIR, exist_ok=True)

        self.DIAGNOSTICS_DIR = os.path.join(self.BASE_DIR, 'diagnostics')
        os.makedirs(self.DIAGNOSTICS_DIR, exist_ok=True)

        self.DATA_FRAME = self.input_dir('cod_data.csv')
        self.COVARIATES = self.input_dir('covariates_data.csv')
        self.ALL_DATA = self.input_dir('input_database_square.csv')
        self.PRIORS = self.input_dir('priors.csv')
        self.KO_DATA = self.input_dir('ko_data.csv')

        self.DRAWS = os.path.join(self.BASE_DIR, 'draws')
        os.makedirs(self.DRAWS, exist_ok=True)
        self.DRAW_FILE = os.path.join(self.DRAWS, 'deaths_{sex}.h5')

        self.COVARIATE_FILES = {
            'ln_rate': self.step_file(step_id=STEP_IDS['CovariateSelection'],
                                      filename='cv_selected_ln_rate.txt'),
            'lt_cf': self.step_file(step_id=STEP_IDS['CovariateSelection'],
                                    filename='cv_selected_lt_cf.txt')
        }

        self.COVARIATE_FILES_NO_SELECT = {
            'ln_rate': self.step_file(step_id=STEP_IDS['CovariateSelection'],
                                      filename='no_covariates_for_ln_rate.txt'),
            'lt_cf': self.step_file(step_id=STEP_IDS['CovariateSelection'],
                                    filename='no_covariates_for_lt_cf.txt')
        }

        self.JSON_FILES = {
            'linear': self.step_file(step_id=STEP_IDS['LinearModelBuilds'],
                                     filename='linear_model_json.txt'),
            'spacetime': self.step_file(step_id=STEP_IDS['LinearModelBuilds'],
                                        filename='space_time_json.txt')
        }

        self.JOB_METADATA = os.path.join(self.BASE_DIR, 'job_metadata.txt')

    def input_dir(self, filename):
        return os.path.join(self.INPUT_DIR, filename)

    def diagnostics_dir(self, filename):
        return os.path.join(self.DIAGNOSTICS_DIR, filename)

    def step_path(self, step_id):
        p = os.path.join(self.BASE_DIR, f'step_{str(step_id)}')
        os.makedirs(p, exist_ok=True)
        return p

    def step_file(self, step_id, filename):
        path = self.step_path(step_id)
        return os.path.join(path, filename)


def setup_dir(directory):
    os.makedirs(directory, exist_ok=True)
    subprocess.call('chmod 777 -R {}'.format('/'.join(directory.split('/'))), shell=True)
    return directory


def cleanup_files(model_version_id, acause):
    """
    Cleans up the files that are made in intermediate steps
    that we don't want to keep for any diagnostic purposes
    later. Will not delete if it's a path.

    :param model_version_id: (int)
    :param acause: (str)
    :return:
    """
    files_to_delete = {
        'ReadSpacetimeModels': ['st_models_linear'],
        'ApplySpacetimeSmoothing': ['st_models_spacetime'],
        'ApplyGPSmoothing': ['st_models_gp'],
        'ReadLinearModels': ['linear_models_linear'],
        'SpacetimePV': ['st_models_pv'],
        'LinearPV': ['linear_models_pv'],
        'OptimalPSI': ['ensemble_preds',
                       'all_st_predictions',
                       'all_linear_predictions'],
        'LinearDraws': ['linear_models_draws'],
        'GPRDraws': ['st_models_draws']
     }
    paths = ModelPaths(model_version_id=model_version_id, acause=acause)
    for d in files_to_delete:
        for f in files_to_delete[d]:
            file_path = paths.step_file(STEP_IDS[d], f'{f}.pickle')
            if os.path.exists(file_path):
                logger.info(f"Deleting {file_path}")
                os.unlink(file_path)

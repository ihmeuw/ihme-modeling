import logging
import os
import shutil

from hale.common import path_utils


def delete_temp_files(hale_version: int) -> None:
    """Removes temporary files at the end of a HALE run"""
    population_root = path_utils.get_population_root(hale_version)
    logging.info(f'Deleting cached population files from {population_root}')
    shutil.rmtree(population_root)

    logging.info(f'Deleting temporary files used for infiling')
    single_infile_path = path_utils.get_infile_path(hale_version, single=True)
    multi_infile_path = path_utils.get_infile_path(hale_version, single=False)
    os.remove(single_infile_path)
    os.remove(multi_infile_path)

import os
import logging
import gbd.constants as gbd
from dalynator.data_filter import AddColumnsFilter
from dalynator.data_source import SuperGopherDataSource, PipelineDataSource
from dataframe_io.exceptions import InvalidSpec

logger = logging.getLogger(__name__)

# como updated folder structure mid gbd 2016
possible_patterns = ['FILEPATH',
                     'FILEPATH']


def get_data_frame(epi_dir, turn_off_null_and_nan_check, location_id, year_id):
    """
    Reads data from h5 files, return yld data source in number space
    """

    # Get YLD data
    desired_index = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                     'cause_id', 'measure_id', 'metric_id']
    for pattern in possible_patterns:
        try:
            yld_source = PipelineDataSource(
                'yld pipeline',
                [SuperGopherDataSource(
                    'yld hf files',
                    {'file_pattern': pattern},
                    epi_dir,
                    turn_off_null_and_nan_check,
                    measure_id=[gbd.measures.YLD],
                    location_id=location_id,
                    year_id=year_id,
                    sex_id=[gbd.sex.MALE, gbd.sex.FEMALE]),
                 AddColumnsFilter(
                     {'measure_id': gbd.measures.YLD,
                      'metric_id': gbd.metrics.RATE})],
                desired_index=desired_index)
            yld_df = yld_source.get_data_frame()
            yld_df = yld_df.loc[yld_df['measure_id'] == gbd.measures.YLD]
            return yld_df
        except InvalidSpec:
            if pattern == possible_patterns[0]:
                pass
            else:
                raise


def get_como_folder_structure(path_up_to_version):
    '''
    This function takes a path up to the version
    '''
    old_method = os.path.join(path_up_to_version, "FILEPATH")
    new_method = os.path.join(path_up_to_version, "FILEPATH")

    if os.path.exists(old_method):
        return old_method

    if os.path.exists(new_method):
        return new_method

    raise RuntimeError("Expected one of 2 possible como paths. Neither exist. "
                       "{} or {}".format(old_method, new_method))

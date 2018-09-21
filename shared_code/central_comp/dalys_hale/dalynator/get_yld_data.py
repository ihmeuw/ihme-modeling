import os
import logging
import gbd.constants as gbd
from dalynator.data_filter import AddColumnsFilter
from dalynator.data_source import SuperGopherDataSource, PipelineDataSource
from transmogrifier.super_gopher import InvalidSpec

logger = logging.getLogger(__name__)

# como updated folder structure mid gbd 2016
possible_patterns = ['{location_id}/{measure_id}_{year_id}_{sex_id}.h5',
                     '{measure_id}_{location_id}_{year_id}_{sex_id}.h5']


def get_data_frame(location_id, year_id, epi_dir, turn_off_null_and_nan_check):
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
                    {'file_pattern': pattern,
                     'h5_tablename': 'draws'},
                    epi_dir,
                    turn_off_null_and_nan_check,
                    measure_id=gbd.measures.YLD,
                    location_id=location_id,
                    year_id=year_id,
                    sex_id=[gbd.sex.MALE, gbd.sex.FEMALE]),
                 AddColumnsFilter(
                    {'measure_id': gbd.measures.YLD,
                     'metric_id': gbd.metrics.RATE})
                 ])
            yld_df = yld_source.get_data_frame(desired_index)
            break
        except InvalidSpec:
            if pattern == possible_patterns[0]:
                pass
            else:
                raise

    yld_df = yld_df.loc[yld_df['measure_id'] == gbd.measures.YLD]

    return yld_df


def get_folder_structure(path_up_to_version):
    '''COMO changed how it made folders from version 197 onwards.
    This function takes a path up to the version
     and returns the full path up to
    either /draws/cause/total or /draws/cause/ depending on which path exists
    '''
    old_method = os.path.join(path_up_to_version, "draws/cause/total")
    new_method = os.path.join(path_up_to_version, "draws/cause/")

    if os.path.exists(old_method):
        return old_method

    if os.path.exists(new_method):
        return new_method

    raise RuntimeError("Expected one of 2 possible como paths. Neither exist. "
                       "{} or {}".format(old_method, new_method))

import logging
import gbd.constants as gbd
from dalynator.data_filter import AddColumnsFilter
from dalynator.data_source import SuperGopherDataSource, PipelineDataSource

logger = logging.getLogger(__name__)


def get_data_frame(daly_dir, turn_off_null_and_nan_check, location_id,
                   year_id):
    """
    Reads data from h5 files, return daly data source in number space
    """

    # Get YLL data
    desired_index = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                     'cause_id', 'measure_id', 'metric_id']
    daly_source = PipelineDataSource(
        'daly pipeline',
        [SuperGopherDataSource(
            'daly hf files',
            {'file_pattern': 'FILEPATH',
             'h5_tablename': 'draws'},
            daly_dir,
            turn_off_null_and_nan_check,
            measure_id=[gbd.measures.DALY],
            location_id=location_id,
            year_id=year_id,
            sex_id=[gbd.sex.MALE, gbd.sex.FEMALE]),
         AddColumnsFilter(
            {'measure_id': gbd.measures.DALY,
             'metric_id': gbd.metrics.NUMBER})],
        desired_index=desired_index)
    daly_df = daly_source.get_data_frame()
    return daly_df

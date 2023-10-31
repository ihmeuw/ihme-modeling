import logging
import gbd.constants as gbd
from dalynator.data_filter import AddColumnsFilter
from dalynator.data_source import SuperGopherDataSource, PipelineDataSource
from dalynator.app_common import SafeDict

logger = logging.getLogger(__name__)


def get_data_frame(cod_dir, cod_pattern,
                   turn_off_null_and_nan_check,
                   location_id, year_id):
    """
    Reads data from h5 files, return death data source in number space
    """

    # Get death data
    desired_index = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                     'cause_id', 'measure_id', 'metric_id']

    death_source = PipelineDataSource(
        'death data',
        [SuperGopherDataSource(
            'death hf file',
            {'file_pattern': cod_pattern,
             'h5_tablename': 'draws'},
            cod_dir,
            turn_off_null_and_nan_check,
            location_id=location_id,
            year_id=year_id,
            measure_id=gbd.measures.DEATH),
         AddColumnsFilter(
            {'measure_id': gbd.measures.DEATH,
             'metric_id': gbd.metrics.NUMBER})
         ], desired_index=desired_index)
    death_df = death_source.get_data_frame()

    return death_df

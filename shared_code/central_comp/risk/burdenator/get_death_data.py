import logging
import gbd.constants as gbd
from dalynator.data_filter import AddColumnsFilter
from dalynator.data_source import SuperGopherDataSource, PipelineDataSource

logger = logging.getLogger(__name__)


def get_data_frame(location_id, year_id, cod_dir, turn_off_null_and_nan_check):
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
            {'file_pattern': str(gbd.measures.DEATH) + '_{location_id}.h5',
             'h5_tablename': 'draws'},
            cod_dir,
            turn_off_null_and_nan_check,
            location_id=location_id,
            year_id=year_id),
         AddColumnsFilter(
            {'measure_id': gbd.measures.DEATH,
             'metric_id': gbd.metrics.NUMBER})
         ])
    death_df = death_source.get_data_frame(desired_index)

    return death_df

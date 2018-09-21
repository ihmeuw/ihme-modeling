import logging
import gbd.constants as gbd
from dalynator.data_filter import AddColumnsFilter
from dalynator.data_source import SuperGopherDataSource, PipelineDataSource

logger = logging.getLogger(__name__)


def get_data_frame(location_id, year_id, cod_dir, turn_off_null_and_nan_check):
    """
    Reads data from h5 files, return yll data source in number space
    """

    # Get YLL data
    desired_index = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                     'cause_id', 'measure_id', 'metric_id']
    yll_source = PipelineDataSource(
        'yll pipeline',
        [SuperGopherDataSource(
            'yll hf file',
            {'file_pattern': str(gbd.measures.YLL) + '_{location_id}.h5',
             'h5_tablename': 'draws'},
            cod_dir,
            turn_off_null_and_nan_check,
            location_id=location_id,
            year_id=year_id),
         AddColumnsFilter(
            {'measure_id': gbd.measures.YLL,
             'metric_id': gbd.metrics.NUMBER})
         ])
    yll_df = yll_source.get_data_frame(desired_index)

    return yll_df

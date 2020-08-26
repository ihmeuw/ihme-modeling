import logging
import os
import pandas as pd

import gbd.constants as gbd

from dalynator.constants import UMASK_PERMISSIONS
from dalynator.write_csv import df_to_csv
from dalynator.get_rei_type_id import get_rei_type_id_df
from dalynator.data_container import remove_unwanted_stars

os.umask(UMASK_PERMISSIONS)
logger = logging.getLogger(__name__)

RISK_REI_TYPE = 1
ETI_REI_TYPE = 2


def write_summaries_multi(location_id, start_year, end_year, csv_dir, df,
                          index_cols, write_out_star_ids, dual_upload):
    logger.debug("Entering write summaries multi_year")
    year_dir = "FILEPATH"

    write_columns_order = ['measure_id', 'year_start_id', 'year_end_id',
                           'location_id', 'sex_id', 'age_group_id', 'cause_id',
                           'metric_id', 'mean', 'upper', 'lower']
    if 'rei_id' in df.columns:
        # Add rei_id to write_columns_order
        cid_pos = write_columns_order.index('cause_id')
        write_columns_order.insert(cid_pos + 1, 'rei_id')
        # Merge on REI types
        rei_type_id_df = get_rei_type_id_df()
        df = pd.merge(df, rei_type_id_df, on='rei_id')

    remove_unwanted_stars(df, write_out_star_ids=write_out_star_ids)

    for my_measure_id in (gbd.measures.DEATH, gbd.measures.DALY,
                          gbd.measures.YLD, gbd.measures.YLL):
        this_df = df[df['measure_id'] == my_measure_id]

        if not this_df.empty:
            this_out_dir = 'FILEPATH'.format(d=csv_dir, m=my_measure_id,
                                                y=year_dir)
            if 'rei_id' in df.columns:
                # Write out risks
                out_file_name = "FILEPATH".format(
                    location_id, start_year, end_year)
                df_to_csv(this_df[this_df['rei_type_id'] == RISK_REI_TYPE],
                          index_cols, this_out_dir, out_file_name,
                          write_columns_order, dual_upload)
                # Write out etiologies
                out_file_name = "FILEPATH".format(
                    location_id, start_year, end_year)
                df_to_csv(this_df[this_df['rei_type_id'] == ETI_REI_TYPE],
                          index_cols, this_out_dir, out_file_name,
                          write_columns_order, dual_upload)
            else:
                out_file_name = "FILEPATH".format(
                    location_id, start_year, end_year)
                df_to_csv(this_df, index_cols, this_out_dir, out_file_name,
                          write_columns_order, dual_upload)

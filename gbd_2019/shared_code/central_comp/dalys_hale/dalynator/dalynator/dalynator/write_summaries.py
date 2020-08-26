import logging
import os
import pandas as pd
import gbd.constants as gbd

from dalynator.constants import UMASK_PERMISSIONS
from dalynator.data_container import remove_unwanted_star_id_column
from dalynator.write_csv import df_to_csv

from dalynator.get_rei_type_id import get_rei_type_id_df

os.umask(UMASK_PERMISSIONS)
logger = logging.getLogger(__name__)

RISK_REI_TYPE = 1
ETI_REI_TYPE = 2


def write_summaries(location_id, year_id, csv_dir, df, index_cols,
                    do_risk_aggr=False, write_out_star_ids=False,
                    dual_upload=False):
    # find none/undefined value in df, remove it from df
    if do_risk_aggr:
        write_columns_order = ['measure_id', 'year_id', 'location_id',
                               'sex_id', 'age_group_id', 'cause_id', 'rei_id',
                               'metric_id', 'mean', 'upper', 'lower']
    else:
        write_columns_order = [
            'measure_id', 'year_id', 'location_id', 'sex_id', 'age_group_id',
            'cause_id', 'metric_id', 'mean', 'upper', 'lower']

    write_columns_order = remove_unwanted_star_id_column(
        write_columns_order, write_out_star_ids )

    logger.debug("Entering write summaries")
    if do_risk_aggr:
        rei_type_id_df = get_rei_type_id_df()
        df = pd.merge(df, rei_type_id_df, on='rei_id')

        tmp_df = df
        measure_ids = [gbd.measures.DEATH, gbd.measures.YLL, gbd.measures.YLD,
                       gbd.measures.DALY]
        for measure_id in measure_ids:
            this_df = tmp_df[tmp_df['measure_id'] == measure_id]

            if not this_df.empty:
                logger.debug("rei non-zero {}".format(measure_id))
                this_out_dir = "FILEPATH".format(csv_dir, measure_id)
                logger.debug("this_out_dir={}".format(this_out_dir))

                out_file_name = (
                    "upload_risk_" + str(location_id) + "_" + str(year_id) +
                    ".csv")
                df_to_csv(
                    this_df[this_df['rei_type_id'] == RISK_REI_TYPE],
                    index_cols, this_out_dir, out_file_name,
                    write_columns_order, dual_upload)

                out_file_name = (
                    "upload_eti_" + str(location_id) + "_" + str(year_id) +
                    ".csv")
                df_to_csv(
                    this_df[this_df['rei_type_id'] == ETI_REI_TYPE],
                    index_cols, this_out_dir, out_file_name,
                    write_columns_order, dual_upload)
    else:
        tmp_df = df

        # Save mortality measure IDs
        mortality_measure_ids = [gbd.measures.DEATH, gbd.measures.YLL]
        for measure_id in mortality_measure_ids:
            this_df = tmp_df[tmp_df['measure_id'] == measure_id]
            if not this_df.empty:
                logger.debug(
                    "rei_id is 0, measures 1 & 4, " +
                    "measure == {}".format(measure_id))
                this_out_dir = "FILEPATH".format(csv_dir, measure_id)
                out_file_name = (
                    "FILEPATH")
                df_to_csv(
                    this_df, index_cols, this_out_dir, out_file_name,
                    write_columns_order, dual_upload)

        # Save DALY results
        for measure_id in [gbd.measures.DALY]:
            this_df = tmp_df[tmp_df['measure_id'] == measure_id]
            if not this_df.empty:
                logger.debug("measure 2, measure is {}".format(measure_id))
                this_out_dir = "FILEPATH".format(csv_dir, measure_id)
                logger.debug("this_out_dir={}".format(this_out_dir))
                out_file_name = (
                    "FILEPATH")
                df_to_csv(
                    this_df, index_cols, this_out_dir, out_file_name,
                    write_columns_order, dual_upload)

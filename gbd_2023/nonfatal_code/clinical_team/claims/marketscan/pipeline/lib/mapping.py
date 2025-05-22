"""
Maps the ICD-mart data to bundle_id using the clinical_mapping module. Removes any other
bundles that may have been introduced after mapping due to the many-to-many relationship
between ICD codes and bundles. Attaches a measure_id column and casts column data types which
are often changed after a pandas merge.
"""
import pandas as pd
from crosscutting_functions.mapping import clinical_mapping
from loguru import logger

from marketscan.pipeline.lib import io
from marketscan.schema import file_handlers as fh


def map_icd_mart(icd_mart_df: pd.DataFrame, bundle_id: int, map_version: int) -> pd.DataFrame:
    """Given a set of ICD mart data map it to bundle_id and return"""
    logger.info("Pre-Mapping: {:,} rows of data".format(len(icd_mart_df)))

    icd_mart_df = clinical_mapping.map_to_gbd_cause(
        df=icd_mart_df,
        input_type="cause_code",
        output_type="bundle",
        retain_active_bundles=False,
        map_version=map_version,
        write_unmapped=False,
        truncate_cause_codes=True,
        extract_pri_dx=False,
        prod=True,
        write_log=False,
        groupby_output=False,
    )
    logger.info("Post-Mapping: {:,} rows of data".format(len(icd_mart_df)))

    icd_mart_df = icd_mart_df.query(f"bundle_id == {bundle_id}")
    msg = (
        "An ICD code can map to multiple bundles, or no bundle at all. We have removed any "
        "rows of the ICD-mart data which do not map to our single bundle of interest "
        "There are now {:,} rows of data".format(len(icd_mart_df))
    )
    logger.info(msg)

    icd_mart_df = clinical_mapping.attach_measure_to_df(icd_mart_df, map_version=map_version)

    icd_mart_df = io.cast_col_dtypes(df=icd_mart_df, dtypes=fh.get_col_dtypes())
    return icd_mart_df
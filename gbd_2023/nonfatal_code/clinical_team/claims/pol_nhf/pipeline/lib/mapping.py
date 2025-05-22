import pandas as pd
from crosscutting_functions.mapping import clinical_mapping
from loguru import logger

from pol_nhf.utils.settings import PipelineAgeSettings


def map_to_bundle(df: pd.DataFrame, map_version: int, bundle_id: int) -> pd.DataFrame:
    pre_shape = df.shape[0]
    mapped_df = clinical_mapping.map_to_gbd_cause(
        df=df,
        input_type="cause_code",
        output_type="bundle",
        map_version=map_version,
        retain_active_bundles=False,
        truncate_cause_codes=True,
        extract_pri_dx=False,
        prod=True,
        write_log=False,
        groupby_output=False,
    )
    logger.info(f"Mapping row counts- \n\tPre: {pre_shape} \tPost:{mapped_df.shape[0]}")
    mapped_df = mapped_df[mapped_df.bundle_id == bundle_id]
    mapped_df = clinical_mapping.attach_measure_to_df(df=mapped_df, map_version=map_version)
    return mapped_df


def apply_bundle_age_sex_restrictions(df: pd.DataFrame, map_version: int) -> pd.DataFrame:
    pre_shape = df.shape[0]
    # Claims data is never binned so we are excluding the "binned" arg option for applying
    # age sex restrictions
    age_sets = {"age": "indv", "age_group_id": "age_group_id"}
    age_col = [col for col in df.columns if col in age_sets.keys()]
    if not age_col:
        raise ValueError("Missing age column. Cannot apply age sex restrictions")
    if len(age_col) > 1:
        raise ValueError("Age sex restrictions will fail with more than one age column")
    asr_df = clinical_mapping.apply_restrictions(
        df=df,
        age_set=age_sets[age_col[0]],
        cause_type="bundle",
        map_version=map_version,
        clinical_age_group_set_id=PipelineAgeSettings.clinical_age_group_id,
        prod=True,
        break_if_not_contig=False,
    )
    logger.info(f"Age Sex Restrict row counts- \n\tPre: {pre_shape} \tPost:{asr_df.shape[0]}")
    return asr_df

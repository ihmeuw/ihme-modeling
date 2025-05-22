import time

import pandas as pd
from crosscutting_functions.deduplication.dedup import ClinicalDedup
from loguru import logger


def run_deduplication(df: pd.DataFrame, map_version: int, estimate_id: int) -> pd.DataFrame:
    start = time.perf_counter()
    clincial_dedup = ClinicalDedup(
        enrollee_col="bene_id",
        service_start_col="admission_date",
        year_col="year_id",
        estimate_id=estimate_id,
        map_version=map_version,
    )
    df["bundle_id"] = df.bundle_id.astype(int)
    df = clincial_dedup.main(df=df)
    logger.info(f"Dedupped in {round((time.perf_counter() - start)/ 60, 2)} minutes")
    return df
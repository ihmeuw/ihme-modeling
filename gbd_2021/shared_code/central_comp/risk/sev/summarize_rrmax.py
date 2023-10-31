import os
from typing import List

import click
import numpy as np
import pandas as pd

from core_maths.summarize import get_summary
from db_queries import get_demographics
from gbd import constants as gbd_constants
from draw_sources.draw_sources import DrawSource
from ihme_dimensions import dimensionality, gbdize

RRMAX_COLS = [
    "measure_id",
    "rei_id",
    "cause_id",
    "age_group_id",
    "sex_id",
    "metric_id",
    "val",
    "lower",
    "upper",
]


def fill_age_sex(sum_df: pd.DataFrame, id_cols: List[str], gbd_round_id: int) -> pd.DataFrame:
    """Missingness with regard to the most-detailed demographics will be filled with NaNs"""
    demographics = get_demographics(gbd_team="epi", gbd_round_id=gbd_round_id)
    cols_to_fill = ["age_group_id", "sex_id"]

    index_cols = [col for col in id_cols if not col in cols_to_fill]
    index_dict = {tuple(index_cols): list(set(tuple(x) for x in sum_df[index_cols].values))}
    index_dict.update({col: demographics[col] for col in cols_to_fill})
    data_dict = {"data_cols": sum_df.columns.difference(id_cols).values.tolist()}

    dimensions = dimensionality.DataFrameDimensions(
        index_dict=index_dict, data_dict=data_dict
    )
    gbdizer = gbdize.GBDizeDataFrame(dimensions)
    expanded_df = gbdizer.fill_empty_indices(sum_df, np.nan)
    return expanded_df


@click.command()
@click.option(
    "--sev_version_id", required=True, type=int, help="Version of this SEV calculator run"
)
@click.option(
    "--rei_id", required=True, type=int, help="The risk for which to summarize RRmax"
)
@click.option("--gbd_round_id", required=True, type=int, help="The GBD round for this run")
def summarize_rrmax(sev_version_id: int, rei_id: int, gbd_round_id: int) -> None:
    """Read RRmax draws for a single risk, summarize, square on age and sex,
    and save to a summary csv file
    """
    rrmaxdir = f"FILEPATH/{sev_version_id}/rrmax/"
    outdir = f"{rrmaxdir}/summaries/"
    outdir_to_upload = f"{outdir}/upload"

    # Read RRmax draws for this risk
    source = DrawSource(params={"draw_dir": rrmaxdir, "file_pattern": "{rei_id}.csv"})
    df = source.content(filters={"rei_id": rei_id})
    draw_cols = [c for c in df if c.startswith("draw_")]

    # Summarize
    sum_df = get_summary(df, draw_cols)
    sum_df.rename(columns={"mean": "val"}, inplace=True)

    # Clean up columns
    sum_df["measure_id"] = gbd_constants.measures.RR_MAX
    sum_df["metric_id"] = gbd_constants.metrics.RATE
    sum_df = sum_df[RRMAX_COLS]
    id_cols = [c for c in sum_df if c.endswith("_id")]
    sum_df[id_cols] = sum_df[id_cols].astype(int)

    # First save summaries without NAs as NAs cannot be uploaded
    outfile_upload = os.path.join(outdir_to_upload, f"{rei_id}.csv")
    sum_df.to_csv(outfile_upload, index=False)
    os.chmod(outfile_upload, 0o775)

    # Make square on age and sex
    sum_df = fill_age_sex(sum_df, id_cols, gbd_round_id)

    # Save square summaries
    outfile = os.path.join(outdir, f"{rei_id}.csv")
    sum_df.to_csv(outfile, index=False)
    os.chmod(outfile, 0o775)


if __name__ == "__main__":
    summarize_rrmax()

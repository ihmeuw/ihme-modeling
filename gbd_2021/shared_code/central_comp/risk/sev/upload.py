from argparse import ArgumentParser, Namespace
import glob
import logging
import os
import subprocess
import sys
from typing import List, Optional

import pandas as pd
from sqlalchemy import orm

import gbd_outputs_versions
import db_tools_core
from db_tools import loaders
from gbd.constants import gbd_metadata_type, gbd_process, gbd_process_version_status, measures
from gbd_outputs_versions import GBDProcessVersion


CODE_DIR = os.path.dirname(os.path.abspath(__file__))
SINGLE_COLS = [
    "measure_id",
    "year_id",
    "location_id",
    "sex_id",
    "age_group_id",
    "rei_id",
    "metric_id",
]
MULTI_COLS = [
    "measure_id",
    "year_start_id",
    "year_end_id",
    "location_id",
    "sex_id",
    "age_group_id",
    "rei_id",
    "metric_id",
]
VAL_COLS = ["val", "lower", "upper"]

SEV_OUTPUT_DIR = "FILEPATH"

SEV: str = "sev"
RR_MAX: str = "rrmax"


def parse_args() -> Namespace:
    """Parse arguments."""
    parser = ArgumentParser()
    parser.add_argument("--version_id", type=str, help="Version of the SEV calculator run")
    parser.add_argument("--gbd_round_id", type=int, help="GBD round id")
    parser.add_argument("--decomp_step", type=str, help="Decomp step")
    parser.add_argument(
        "--pct_change",
        type=int,
        help=(
            "Boolean representing if results were generated for percent change years (1) "
            "or not (0) for this run"
        ),
    )
    parser.add_argument(
        "--measure",
        type=str,
        help="Measure to upload for. Either 'rrmax' or 'sev'",
        choices=[RR_MAX, SEV],
    )

    return parser.parse_args()


def get_included_age_groups(version_id: int, measure: str) -> List[int]:
    """Return all age groups included in the SEV/RR max summaries."""
    summary_pattern = (
        f"{SEV_OUTPUT_DIR}/{version_id}/summaries/single_year_*.csv"
        if measure == SEV
        else f"{SEV_OUTPUT_DIR}/{version_id}/rrmax/summaries/*.csv"
    )

    try:
        summaries = pd.read_csv(glob.glob(summary_pattern)[0])
    except Exception as e:
        print(e)
        raise
    return sorted(summaries.age_group_id.unique().tolist())


def get_code_version() -> str:
  return subprocess.check_output(
      [
          "git",
          f"--git-dir={CODE_DIR}/.git",
          f"--work-tree={CODE_DIR}/",
          "rev-parse",
          "HEAD",
      ]
  ).strip()


def upload_sevs(version_id: int, gbd_process_version_id: int, table_type: str) -> None:
    """Read in all SEV summary files, resave and upload."""
    summary_dir = f"{SEV_OUTPUT_DIR}/{version_id}/summaries"
    summary_files = glob.glob(f"{summary_dir}/{table_type}_*.csv")
    if not summary_files:
        raise RuntimeError(
            f"No summary files with pattern '{f'{summary_dir}/{table_type}_*.csv'}' found."
        )

    data = []
    for file in summary_files:
        data.append(pd.read_csv(file))

    data = pd.concat(data)
    data.fillna(0, inplace=True)
    id_cols = [c for c in data if c.endswith("_id")]
    data[id_cols] = data[id_cols].astype(int)
    table_name = f"output_sev_{table_type}_v{gbd_process_version_id}"
    data_dir = f"{summary_dir}/{table_type}"

    # Make directory if it doesn't already exist
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    if table_type == "single_year":
        data = data[SINGLE_COLS + VAL_COLS].sort_values(SINGLE_COLS)
        year_ids = data["year_id"].drop_duplicates().tolist()
        for yr in year_ids:
            file = os.path.join(data_dir, f"single_year_{yr}.csv")
            data.loc[data["year_id"] == yr].to_csv(file, index=False)
            os.chmod(file, 0o775)
    else:
        data = data[MULTI_COLS + VAL_COLS].sort_values(MULTI_COLS)
        year_ids = (
            data[["year_start_id", "year_end_id"]].drop_duplicates().to_dict("split")["data"]
        )
        for yr in year_ids:
            file = os.path.join(data_dir, f"multi_year_{yr[0]}_{yr[1]}.csv")
            data.loc[
                (data["year_start_id"] == yr[0]) & (data["year_end_id"] == yr[1])
            ].to_csv(file, index=False)
            os.chmod(file, 0o775)

    infiler = loaders.Infiles(
        table=table_name, schema="gbd", session=db_tools_core.get_session("gbd")
    )

    # Upload all files in summaries directory
    infiler.indir(
        path=data_dir,
        with_replace=False,
        commit=True,
        partial_commit=True,
        sort_files=True,
    )


def upload_rr_max(version_id: int, gbd_process_version_id: int) -> None:
    """Upload RR max summary files."""
    directory = f"{SEV_OUTPUT_DIR}/{version_id}/rrmax/summaries/upload"
    table_name = f"output_rr_max_single_year_v{gbd_process_version_id}"

    infiler = loaders.Infiles(
        table=table_name, schema="gbd", session=db_tools_core.get_session("gbd")
    )

    # Upload all files in summaries directory
    infiler.indir(
        path=directory,
        with_replace=False,
        commit=True,
        partial_commit=True,
        sort_files=True,
    )


def get_gbd_process_version_id(
    metadata_type_id: int,
    version_id: int,
    gbd_process_version_status_id: List[int],
    session: orm.Session
) -> Optional[int]:
    """Converts an 'internal' version (ie. SEVs 123) to the linked GBD process version.

    If no existing GBD process version is found matching the given parameters,
    returns None.
    """
    metadata_process_map = {
        gbd_metadata_type.COMO: gbd_process.EPI,
        gbd_metadata_type.CODCORRECT: gbd_process.COD,
        gbd_metadata_type.SEV: gbd_process.SEV,
        gbd_metadata_type.RR_MAX: gbd_process.RR_MAX,
    }
    return session.execute(
        """
        SELECT MAX(gbd_process_version_id)
        FROM gbd.gbd_process_version_metadata
        JOIN gbd.gbd_process_version USING(gbd_process_version_id)
        WHERE gbd_process_id = :gbd_process_id
        AND metadata_type_id= :metadata_type_id
        AND val = :version_id
        AND gbd_process_version_status_id IN :gbd_process_version_status_id
        """,
        params = {
            "gbd_process_id": metadata_process_map[metadata_type_id],
            "metadata_type_id": metadata_type_id,
            "version_id": version_id,
            "gbd_process_version_status_id": gbd_process_version_status_id
        },
    ).scalar_one()


def get_process_version(
    version_id: int, measure: str, gbd_round_id: int, decomp_step: str
) -> GBDProcessVersion:
    """Gets the GBD process version given an internal version id and the measure.

    If a process version already exists for the measure's internal version, that
    process version is returned. Otherwise, a new process version is created.
    """
    metadata = pd.read_csv(f"{SEV_OUTPUT_DIR}/{version_id}/version.csv")
    paf_version = metadata["paf_version"].iat[0]
    como_version = metadata["como_version"].iat[0]
    codcorrect_version = metadata["codcorrect_version"].iat[0]
    pop_version = metadata["pop_run_id"].iat[0]

    # Check if a process version for the GBD process has already been created.
    with db_tools_core.session_scope("gbd") as session:
        process_version_id = (
            get_gbd_process_version_id(
                gbd_metadata_type.SEV,
                version_id,
                [gbd_process_version_status.RUNNING],
                session,
            )
            if measure == SEV
            else get_gbd_process_version_id(
                gbd_metadata_type.RR_MAX,
                version_id,
                [gbd_process_version_status.RUNNING],
                session,
            )
        )

    if process_version_id:
        pv = GBDProcessVersion(process_version_id)
    elif measure == SEV:
        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_process.SEV,
            gbd_process_version_note=f"SEV v{version_id}",
            code_version=get_code_version(),
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            metadata={
                gbd_metadata_type.SEV: version_id,
                gbd_metadata_type.AGE_GROUP_IDS: {
                    measures.SEV: get_included_age_groups(version_id, measure)
                },
                gbd_metadata_type.CODCORRECT: codcorrect_version,
                gbd_metadata_type.COMO: como_version,
                gbd_metadata_type.POPULATION: pop_version,
                gbd_metadata_type.RISK: paf_version,
                gbd_metadata_type.RR_MAX: version_id,
            },
        )
    else:
        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_process.RR_MAX,
            gbd_process_version_note=f"RR max v{version_id}",
            code_version=get_code_version(),
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            metadata={
                gbd_metadata_type.RR_MAX: version_id,
                gbd_metadata_type.AGE_GROUP_IDS: {
                    measures.RR_MAX: get_included_age_groups(version_id, measure)
                },
                gbd_metadata_type.RISK: paf_version,
                gbd_metadata_type.POPULATION: pop_version,
            },
        )

    return pv


def main() -> None:
    """Run the upload process."""
    args = parse_args()

    # Set up GBD process version
    pv = get_process_version(
        args.version_id, args.measure, args.gbd_round_id, args.decomp_step
    )

    # Run upload depending on measure
    if args.measure == SEV:
        table_types = ["single_year", "multi_year"] if args.pct_change else ["single_year"]
        for table_type in table_types:
            upload_sevs(args.version_id, pv.gbd_process_version_id, table_type)

        logging.info(
            f"SEV upload complete for GBD process version {pv.gbd_process_version_id}"
        )
    else:
        upload_rr_max(args.version_id, pv.gbd_process_version_id)

        logging.info(
            f"RR max upload complete for GBD process version {pv.gbd_process_version_id}"
        )

    # Activate the GBD process version
    pv._update_status(gbd_process_version_status.ACTIVE)


if __name__ == "__main__":
    main()

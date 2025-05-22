import glob
import os
from typing import Optional

import pandas as pd

import db_tools_core
from db_tools import loaders
from gbd import conn_defs
from gbd.constants import gbd_metadata_type, gbd_process, measures
from gbd_outputs_versions import DBEnvironment, GBDProcessVersion, internal_to_process_version

import ihme_cc_sev_calculator
from ihme_cc_sev_calculator.lib import constants, parameters

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


def get_process_version_id(
    measure: str, params: parameters.Parameters, env: DBEnvironment = DBEnvironment.PROD
) -> Optional[int]:
    """Get process version ID for the given measure using SEV Calculator parameters.

    If one can't be found, returns None.
    """
    process_id = gbd_process.SEV if measure == constants.SEV else gbd_process.RR_MAX

    try:
        process_version_id = internal_to_process_version(
            params.version_id, process_id, env=env
        )
    except RuntimeError:
        process_version_id = None

    return process_version_id


def get_process_version(
    measure: str, params: parameters.Parameters, env: DBEnvironment = DBEnvironment.PROD
) -> GBDProcessVersion:
    """Gets the GBD process version given an internal version id and the measure.

    If a process version already exists for the measure's internal version, that
    process version is returned. Otherwise, a new process version is created.
    """
    # Check if a process version for the GBD process has already been created.
    # This can happen when a previous upload job failed after creating the process version.
    process_version_id = get_process_version_id(measure, params, env)

    if process_version_id:
        pv = GBDProcessVersion(process_version_id, env=env)
    elif measure == constants.SEV:
        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_process.SEV,
            gbd_process_version_note=f"SEV v{params.version_id}",
            code_version=ihme_cc_sev_calculator.__version__,
            release_id=params.release_id,
            metadata={
                gbd_metadata_type.SEV: params.version_id,
                gbd_metadata_type.CODCORRECT: params.codcorrect_version_id,
                gbd_metadata_type.COMO: params.como_version_id,
                gbd_metadata_type.RISK: params.paf_version_id,
                gbd_metadata_type.POPULATION: params.population_version_id,
                gbd_metadata_type.RR_MAX: params.version_id,
                gbd_metadata_type.AGE_GROUP_IDS: {
                    measures.SEV: params.age_group_ids + params.aggregate_age_group_ids
                },
                gbd_metadata_type.YEAR_IDS: params.year_ids,
                gbd_metadata_type.N_DRAWS: params.n_draws,
                gbd_metadata_type.LOCATION_SET_VERSION_ID: params.location_set_version_ids,
                gbd_metadata_type.CAUSE_SET_VERSION_ID: params.cause_set_version_ids,
                gbd_metadata_type.MEASURE_IDS: measures.SEV,
            }
            | (
                {
                    gbd_metadata_type.YEAR_START_IDS: [
                        i[0] for i in params.percent_change_years
                    ],
                    gbd_metadata_type.YEAR_END_IDS: [
                        i[1] for i in params.percent_change_years
                    ],
                }
                if params.percent_change
                else {}
            ),
            env=env,
        )
    else:
        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_process.RR_MAX,
            gbd_process_version_note=f"RR max v{params.version_id}",
            code_version=ihme_cc_sev_calculator.__version__,
            release_id=params.release_id,
            metadata={
                gbd_metadata_type.RR_MAX: params.version_id,
                gbd_metadata_type.AGE_GROUP_IDS: {measures.RR_MAX: params.age_group_ids},
                gbd_metadata_type.YEAR_IDS: params.year_ids,
                gbd_metadata_type.N_DRAWS: params.n_draws,
                gbd_metadata_type.LOCATION_SET_VERSION_ID: params.location_set_version_ids,
                gbd_metadata_type.CAUSE_SET_VERSION_ID: params.cause_set_version_ids,
                gbd_metadata_type.MEASURE_IDS: measures.RR_MAX,
            },
            env=env,
        )

    return pv


def upload_sevs(
    version_id: int, gbd_process_version_id: int, table_type: str, output_dir: str
) -> None:
    """Read in all SEV summary files, resave and upload.

    SEV summaries are originally written by location. All results are read in and rewritten,
    saving out by year to optimize for the expected primary key order of the upload table.

    Potential improvement to sort the files themselves in the correct upload order rather than
    needing an additional step to read them in and re-shard them before we upload.
    """
    summary_dir = constants.SEV_UPLOAD_DIR.format(root_dir=output_dir)
    summary_files = 
    if not summary_files:
        raise RuntimeError(
            "No summary files with pattern found."
        )

    data = []
    for file in summary_files:
        data.append(pd.read_csv(file))

    data = pd.concat(data)
    data.fillna(0, inplace=True)
    id_cols = [c for c in data if c.endswith("_id")]
    data[id_cols] = data[id_cols].astype(int)
    table_name = f"output_sev_{table_type}_v{gbd_process_version_id}"
    data_dir = 

    # Make directory if it doesn't already exist
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    # Rewrite summaries so that results across all risks for a single year are in one file
    if table_type == "single_year":
        data = data[SINGLE_COLS + VAL_COLS].sort_values(SINGLE_COLS)
        year_ids = data["year_id"].drop_duplicates().tolist()
        for yr in year_ids:
            file = 
            data.loc[data["year_id"] == yr].to_csv(file, index=False)
            os.chmod(file, 0o775)
    else:
        data = data[MULTI_COLS + VAL_COLS].sort_values(MULTI_COLS)
        year_ids = (
            data[["year_start_id", "year_end_id"]].drop_duplicates().to_dict("split")["data"]
        )
        for yr in year_ids:
            file = 
            data.loc[
                (data["year_start_id"] == yr[0]) & (data["year_end_id"] == yr[1])
            ].to_csv(file, index=False)
            os.chmod(file, 0o775)

    infiler = loaders.Infiles(
        table=table_name, schema="gbd", session=db_tools_core.get_session(conn_defs.GBD)
    )

    # Upload all files in summaries directory
    infiler.indir(
        path=data_dir, with_replace=False, commit=True, partial_commit=True, sort_files=True
    )


def upload_rr_max(version_id: int, gbd_process_version_id: int, output_dir: str) -> None:
    """Upload RR max summary files."""
    table_name = f"output_rr_max_single_year_v{gbd_process_version_id}"

    infiler = loaders.Infiles(
        table=table_name, schema="gbd", session=db_tools_core.get_session(conn_defs.GBD)
    )

    # Upload all files in summaries directory
    infiler.indir(
        path=constants.RR_MAX_UPLOAD_DIR.format(root_dir=output_dir),
        with_replace=False,
        commit=True,
        partial_commit=True,
        sort_files=True,
    )

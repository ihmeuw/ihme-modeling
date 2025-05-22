"""
All code that uploads results to databases is in this file.
Every function that uploads writes a logging message with
the word "Uploading", so that we can be sure test runs do
not upload results.
"""

import logging
import os
import pathlib
import subprocess
from contextlib import closing
from datetime import datetime

import sqlalchemy
from sqlalchemy.sql import text

from cascade_ode import __version__
from cascade_ode.legacy import db, io
from cascade_ode.legacy.settings import load as load_settings

# Set default file mask to readable-for all users
os.umask(0o0002)

# Path to this file
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
settings = load_settings()

RUNNING = -1
FAILED = 7


def skip_if_uploaded(func):
    def decorated(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except sqlalchemy.exc.IntegrityError:
            log = logging.getLogger(__name__)
            log.debug("Skipping file since already uploaded: {}".format(args))

    return decorated


@skip_if_uploaded
def upload_file(mvid, filepath, table, cols, conn=None):
    """Infile a csv"""
    log = logging.getLogger(__name__)
    log.debug(f"Uploading {filepath} to mvid {mvid}")
    load_data_query = f"""
        LOAD DATA INFILE '{filepath}'
        INTO TABLE {table}
        FIELDS
            TERMINATED BY ","
            OPTIONALLY ENCLOSED BY '"'
        LINES
            TERMINATED BY "\\n"
        IGNORE 1 LINES
        ({','.join(cols)})
        SET model_version_id={mvid}"""

    if not conn:
        eng = db.get_engine(conn_def="epi", env=settings["env_variables"]["ENVIRONMENT_NAME"])
        with closing(eng.connect()) as conn:
            res = conn.execute(load_data_query)
    else:
        res = conn.execute(load_data_query)

    return res


def set_commit_hash(mvid: int) -> None:
    """Update epi.model_version table with git commit hash"""
    git_dir = 
    cmd = ["git", f"--git-dir={git_dir}", "--work-tree=%s", "rev-parse", "HEAD"]
    try:
        commit_hash = subprocess.check_output(cmd).strip()  # noqa:S603
    except subprocess.CalledProcessError:
        commit_hash = __version__
    if type(commit_hash) == bytes:
        commit_hash = commit_hash.decode("ascii")

    update_stmt = text(
        """
        UPDATE epi.model_version
        SET code_version=:ch
        WHERE model_version_id=:mvid
        """
    )
    params = {"ch": commit_hash, "mvid": mvid}
    eng = db.get_engine(conn_def="epi", env=settings["env_variables"]["ENVIRONMENT_NAME"])
    with closing(eng.connect()) as conn:
        res = conn.execute(update_stmt, **params)
    return res


def consolidate_and_upload_model(mvid):
    """Concatenate all submodel datasets into large files,
    then infile csvs to epi.model_estimate_fit, epi.model_prior,
    epi.model_data_adj, and epi.model_effect"""
    log = logging.getLogger(__name__)

    root_dir = settings["cascade_ode_out_dir"]
    submodel_dir = 
    io.file_storage.consolidate_all_submodel_files(submodel_dir)
    io.file_storage.delete_unconsolidated_files(submodel_dir)
    files_to_upload = io.file_storage.export_csvs_to_upload(submodel_dir)
    fit_file = files_to_upload["posterior_upload"]
    prior_file = files_to_upload["prior_upload"]
    global_adj_file = files_to_upload["adj_data_upload"]
    global_effect_file = files_to_upload["effect_upload"]

    log.info("Done creating infile csvs")

    eng = db.get_engine(conn_def="epi", env=settings["env_variables"]["ENVIRONMENT_NAME"])

    with closing(eng.connect()) as conn:
        log.info("Uploading fit data")
        upload_file(
            mvid,
            fit_file,
            "epi.model_estimate_fit",
            [
                "location_id",
                "year_id",
                "sex_id",
                "age_group_id",
                "measure_id",
                "mean",
                "lower",
                "upper",
            ],
            conn,
        )

        log.info("Uploading prior data")
        upload_file(
            mvid,
            prior_file,
            "epi.model_prior",
            [
                "year_id",
                "location_id",
                "sex_id",
                "age_group_id",
                "age",
                "measure_id",
                "mean",
                "lower",
                "upper",
            ],
            conn,
        )

        log.info("Uploading adjusted data")
        upload_file(
            mvid,
            global_adj_file,
            "epi.model_data_adj",
            ["model_version_dismod_id", "sex_id", "year_id", "mean", "lower", "upper"],
            conn,
        )

        log.info("Uploading effects")
        # if we've uploaded this before, we won't get a Integrity error, so
        # lets check via counting rows
        query_str = (
            "select count(*) from epi.model_effect where "
            f"model_version_id = {mvid} group by model_version_id"
        )
        result = conn.execute(query_str).fetchone()
        row_count = 0 if not result else result[0]

        if row_count == 0:
            upload_file(
                mvid,
                global_effect_file,
                "epi.model_effect",
                [
                    "measure_id",
                    "parameter_type_id",
                    "cascade_level_id",
                    "study_covariate_id",
                    "country_covariate_id",
                    "asdr_cause",
                    "location_id",
                    "mean_effect",
                    "lower_effect",
                    "upper_effect",
                ],
                conn,
            )
        else:
            log.info("Skipping effects file since already uploaded")

        for filepath in files_to_upload.values():
            log.info(f"deleting infile csv {filepath}")
            filepath = pathlib.Path(filepath)
            filepath.unlink()


def upload_fit_stat(mvid):
    """Infile to epi.model_version_fit_stat"""
    mvfs_file = 

    upload_file(
        mvid,
        mvfs_file,
        "epi.model_version_fit_stat",
        ["measure_id", "fit_stat_id", "fit_stat_value"],
    )


def update_model_status(mvid, status):
    """Update epi.model_version_status for given model version id with
    model_version_status_id provided"""
    log = logging.getLogger(__name__)
    log.info(f"Uploading model status for {mvid} to {status}.")
    eng = db.get_engine(conn_def="epi", env=settings["env_variables"]["ENVIRONMENT_NAME"])
    update_statement = text(
        """
        UPDATE epi.model_version
        SET model_version_status_id=:status
        WHERE model_version_id=:mvid
        """
    )
    with closing(eng.connect()) as conn:
        conn.execute(update_statement, status=status, mvid=mvid)


def update_run_time(mvid):
    """Given model version id, updates model_version_run_start to time of
    function call. Uses ENVIRONMENT_NAME to determine which db to update"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = logging.getLogger(__name__)
    log.info(f"Uploading run time {now} for {mvid}.")
    query = """
        UPDATE epi.model_version
        SET model_version_run_start='%s'
        WHERE model_version_id=%s""" % (
        now,
        mvid,
    )
    eng = db.get_engine(conn_def="epi", env=settings["env_variables"]["ENVIRONMENT_NAME"])
    conn = eng.connect()
    conn.execute(query)
    conn.close()

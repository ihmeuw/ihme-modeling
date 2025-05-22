import logging
import os
import re
import shutil
import subprocess
from argparse import ArgumentParser, Namespace
from typing import Any, Dict, List, Union

import sqlalchemy

from gbd import constants as gbd_constants

from cascade_ode.legacy import db, drill, fit_stats, importer, upload
from cascade_ode.legacy.argument_parser import cascade_parser
from cascade_ode.legacy.constants import CLIArgs
from cascade_ode.legacy.demographics import Demographics
from cascade_ode.legacy.patch_io import setup_io_patches
from cascade_ode.legacy.run_all import prepare_directories
from cascade_ode.legacy.setup_logger import setup_logger
from cascade_ode.legacy.shared_functions import DismodSaveResults, get_location_metadata

# Set default file mask to readable-for all users
os.umask(0o0002)


def parse_args(args=None) -> Namespace:
    parser = get_arg_parser()
    return parser.parse_args(args=args)


def get_arg_parser() -> ArgumentParser:
    parser = cascade_parser(description="Upload model, plot, aggregate up hierarchy.")
    parser.add_argument(
        CLIArgs.MVID.FLAG, type=CLIArgs.MVID.TYPE, help=CLIArgs.MVID.DESCRIPTION
    )
    return parser


def main() -> None:
    """Set commit hash, upload model, try to write effects_plots PDFs,
    and aggregate model version draws up location hierarchy.
    """
    args = parse_args()
    mvid = args.mvid
    default_debug_level = -1
    dirs = prepare_directories(mvid, create_directories=False)
    logging_filepath = 
    setup_logger(logging_filepath, level=args.quiet - args.verbose + default_debug_level)

    log = logging.getLogger(__name__)
    log.info("Varnish started for mvid {}".format(mvid))
    setup_io_patches(no_upload=args.no_upload)

    try:
        upload.set_commit_hash(mvid)
        upload.consolidate_and_upload_model(mvid)

        outdir = 
        joutdir = 
        fit_df = fit_stats.write_fit_stats(mvid, outdir, joutdir)
        if fit_df is not None:
            try:
                upload.upload_fit_stat(mvid)
            except sqlalchemy.exc.IntegrityError:
                log.warning("fit stat already uploaded -- skipping")
        else:
            log.warning("No fit stats computed")

        # Write effect PDFs
        plotter = 
        plotter = os.path.realpath(plotter)

        demo = Demographics(model_version_id=mvid)
        try:
            subprocess.check_output(
                [
                    ,
                    plotter,
                    str(mvid),
                    joutdir,
                    drill.settings["cascade_ode_out_dir"],
                    str(max(demo.year_ids)),
                ],
                stderr=subprocess.STDOUT,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            log.exception("Error in effect plots")

        # Clean aggregations to ensure idempotentcy
        release_id = importer.get_model_version(mvid).release_id.unique()[0]
        clean_model_directory(outdir=outdir, release_id=release_id)

        # Launch final aggregations
        log.info("Starting Save Results")
        aggregate_model(mvid=mvid, demo=demo, no_upload=args.no_upload)
    except Exception:
        log.exception("Error in varnish")
        raise


def aggregate_model(
    mvid: int, demo: Demographics, no_upload: bool = False
) -> DismodSaveResults:
    """call save_results to create location aggregates,
    upload summaries to epi.model_estimate_final,
    mark model as finished"""
    agg_args = get_aggregation_arguments(mvid=mvid, demo=demo)

    dsr = DismodSaveResults(
        input_dir=agg_args["input_dir"],
        input_file_pattern=agg_args["input_file_pattern"],
        model_version_id=mvid,
        modelable_entity_id=agg_args["modelable_entity_id"],
        description=agg_args["description"],
        year_id=agg_args["year_id"],
        sex_id=agg_args["sex_id"],
        measure_id=agg_args["measure_id"],
        db_env=agg_args["db_env"],
        birth_prevalence=agg_args["birth_prevalence"],
        release_id=agg_args["release_id"],
    )
    if not no_upload:
        dsr.run()

    return dsr


def get_aggregation_arguments(mvid: int, demo: Demographics) -> Dict[str, Any]:
    casc = drill.Cascade(
        model_version_id=mvid, root_dir=drill.settings["cascade_ode_out_dir"], reimport=False
    )
    mvm = casc.model_version_meta
    db_env = str(drill.settings["env_variables"]["ENVIRONMENT_NAME"])

    agg_args = {}
    agg_args["input_dir"] = 
    agg_args["input_file_pattern"] = 
    agg_args["modelable_entity_id"] = int(mvm["modelable_entity_id"].iat[0])
    agg_args["description"] = mvm.description.iat[0]
    agg_args["year_id"] = [int(y) for y in demo.year_ids]
    agg_args["sex_id"] = [int(s) for s in demo.sex_ids]
    agg_args["measure_id"] = get_measures_from_casc(casc=casc)
    agg_args["db_env"] = db_env
    agg_args["birth_prevalence"] = bool(
        mvm.birth_prev.fillna(0).replace({0: False, 1: True}).iat[0]
    )
    agg_args["release_id"] = int(mvm.release_id.iat[0])

    return agg_args


def get_measures_from_casc(casc: drill.Cascade) -> Union[Any, List]:
    measure_only = casc.model_version_meta.measure_only
    if measure_only.notnull().all():
        return int(measure_only.iat[0])

    q = "select measure_id from shared.measure where measure in ('{}')".format(
        "', '".join(importer.integrand_pred)
    )
    df = db.execute_select(q, conn_def="epi")
    return sorted(df.measure_id.astype(int).tolist())


def clean_model_directory(outdir: str, release_id: int) -> None:
    """Removes past (maybe corrupt) .h5 aggregate files and summary directory
    for a given varnish job run.
    Args:
        outdir: full output directory of a cascade model
        release_id: release_id for which to get location metadata
    """
    draw_path = 
    # remove summary file
    if os.path.exists():
        shutil.rmtree()
    # remove aggregated draw files
    files_to_remove = get_files_to_remove(
        dir_list=os.listdir(draw_path), release_id=release_id
    )
    for file in files_to_remove:
        os.remove()


def get_files_to_remove(dir_list, release_id):
    """
    To make idempotent, find aggregate location files to delete
    """
    location_set_id = (
        35
        if release_id != gbd_constants.release.USRE
        else Demographics.USA_RE_LOCATION_SET_ID
    )
    loc_df = get_location_metadata(location_set_id=location_set_id, release_id=release_id)
    locs = loc_df.loc[loc_df.most_detailed != 1].location_id.tolist()
    location_substr = "|".join([str(l) for l in locs])
    regex = 
    files_to_remove = [f for f in dir_list if re.match(regex, f)]
    return files_to_remove


if __name__ == "__main__":
    main()

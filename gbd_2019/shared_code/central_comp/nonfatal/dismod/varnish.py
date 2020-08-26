import logging
import os
import re
import shutil
import subprocess

from cascade_ode import upload
from cascade_ode import fit_stats
from cascade_ode import drill
from cascade_ode.argument_parser import cascade_parser
from cascade_ode.patch_io import setup_io_patches
from cascade_ode.demographics import Demographics
from cascade_ode import importer
from cascade_ode import __version__
from cascade_ode.run_all import prepare_directories
from cascade_ode.setup_logger import setup_logger
from cascade_ode.sge import get_commit_hash
from cascade_ode.shared_functions import DismodSaveResults
from db_tools.ezfuncs import query
from db_queries import get_location_metadata
from gbd.decomp_step import decomp_step_from_decomp_step_id
import sqlalchemy

# Set default file mask to readable-for all users
os.umask(0o0002)


def parse_args(args=None):
    parser = cascade_parser("Upload model, plot, aggregate up hierarchy.")
    parser.add_argument('mvid', type=int)
    return parser.parse_args(args)


def main():
    '''Set commit hash, upload model, try to write effects_plots pdfs,
    aggregate model version draws up location hierarchy
    '''
    args = parse_args()
    mvid = args.mvid
    default_debug_level = -1
    dirs = prepare_directories(mvid, create_directories=False)
    logging_filepath = '%s/%s' % (
        dirs['model_logdir'], f'{args.mvid}_varnish.log')
    setup_logger(
        logging_filepath,
        level=args.quiet - args.verbose + default_debug_level)

    log = logging.getLogger(__name__)
    log.info("Varnish started for mvid {}".format(mvid))
    setup_io_patches(args.no_upload)

    try:
        try:
            commit_hash = get_commit_hash(dir='%s/..' % drill.this_path)
        except subprocess.CalledProcessError:
            # in site-packages, not git repo
            commit_hash = __version__

        upload.set_commit_hash(mvid, commit_hash)
        upload.upload_model(mvid)

        outdir = "%s/%s/full" % (
            drill.settings['cascade_ode_out_dir'],
            str(mvid))
        joutdir = "%s/%s" % (drill.settings['diag_out_dir'], mvid)
        fit_df = fit_stats.write_fit_stats(mvid, outdir, joutdir)
        if fit_df is not None:
            try:
                upload.upload_fit_stat(mvid)
            except sqlalchemy.exc.IntegrityError:
                log.warning("fit stat already uploaded -- skipping")
        else:
            log.warning("No fit stats computed")

        # Write effect PDFs
        plotter = "{}/effect_plots.r".format(drill.this_path)
        plotter = os.path.realpath(plotter)

        demo = Demographics(mvid)
        try:
            subprocess.check_output([
                "Rscript",
                plotter,
                str(mvid),
                joutdir,
                drill.settings['cascade_ode_out_dir'],
                str(max(demo.year_ids))],
                stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            log.exception("Error in effect plots")

        # Clean aggregations to ensure idempotentcy
        decomp_step = decomp_step_from_decomp_step_id(
            importer.get_model_version(mvid).decomp_step_id.unique()[0])
        clean_model_directory(outdir, demo.gbd_round_id, decomp_step)

        # Launch final aggregations
        log.info("Starting Save Results")
        aggregate_model(mvid, demo=demo, no_upload=args.no_upload)
    except Exception:
        log.exception("Error in varnish")
        raise


def aggregate_model(mvid, demo, no_upload=False):
    '''call save_results to create location aggregates,
    upload summaries to epi.model_estimate_final,
    mark model as finished'''
    agg_args = get_aggregation_arguments(mvid, demo)

    dsr = DismodSaveResults(
        input_dir=agg_args['input_dir'],
        input_file_pattern=agg_args['input_file_pattern'],
        model_version_id=mvid,
        modelable_entity_id=agg_args['modelable_entity_id'],
        description=agg_args['description'],
        year_id=agg_args['year_id'],
        sex_id=agg_args['sex_id'],
        measure_id=agg_args['measure_id'],
        db_env=agg_args['db_env'],
        gbd_round_id=agg_args['gbd_round_id'],
        birth_prevalence=agg_args['birth_prevalence'],
        decomp_step=agg_args['decomp_step'])
    if not no_upload:
        dsr.run()

    return dsr


def get_aggregation_arguments(mvid, demo):
    casc = drill.Cascade(
            mvid, root_dir=drill.settings['cascade_ode_out_dir'],
            reimport=False)
    mvm = casc.model_version_meta
    db_env = drill.settings['env_variables']['ENVIRONMENT_NAME']

    agg_args = {}
    agg_args['input_dir'] = os.path.join(casc.root_dir, 'draws')
    agg_args['input_file_pattern'] = '{location_id}_{year_id}_{sex_id}.h5'
    agg_args['modelable_entity_id'] = mvm.modelable_entity_id.iat[0]
    agg_args['description'] = mvm.description.iat[0]
    agg_args['year_id'] = demo.year_ids
    agg_args['sex_id'] = demo.sex_ids
    agg_args['measure_id'] = get_measures_from_casc(casc)
    agg_args['db_env'] = db_env
    agg_args['gbd_round_id'] = demo.gbd_round_id
    agg_args['birth_prevalence'] = mvm.birth_prev.fillna(0).replace(
            {0: False, 1: True}).iat[0]
    agg_args['decomp_step'] = mvm.decomp_step.iat[0]

    return agg_args


def get_measures_from_casc(casc):
    measure_only = casc.model_version_meta.measure_only
    if measure_only.notnull().all():
        return measure_only.iat[0]

    q = "select measure_id from shared.measure where measure in ('{}')".format(
            "', '".join(importer.integrand_pred))
    df = query(q, conn_def="epi")
    return sorted(df.measure_id.tolist())


def clean_model_directory(outdir, gbd_round_id, decomp_step):
    '''Removes past (maybe corrupt) .h5 aggregate files and summary directory
    for a given varnish job run.
    Args:
        outdir (str): full output directory of a cascade model
        gbd_round_id (int): gbd_round_id for which to get location metadata
    '''
    draw_path = os.path.join(outdir, 'draws')
    # remove summary file
    if os.path.exists(os.path.join(draw_path, 'summaries')):
        shutil.rmtree(os.path.join(draw_path, 'summaries'))
    # remove aggregated draw files
    files_to_remove = get_files_to_remove(
        os.listdir(draw_path), gbd_round_id, decomp_step)
    for file in files_to_remove:
        os.remove(os.path.join(draw_path, file))


def get_files_to_remove(dir_list, gbd_round_id, decomp_step):
    '''
    To make varnish.py idempotent, find aggregate location files to delete
    '''
    loc_df = get_location_metadata(
        location_set_id=35,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step)
    locs = loc_df.loc[loc_df.most_detailed != 1].location_id.tolist()
    location_substr = "|".join([str(l) for l in locs])
    regex = f"({location_substr})_.*.h5$"
    files_to_remove = [f for f in dir_list if re.match(regex, f)]
    return files_to_remove


if __name__ == '__main__':
    main()

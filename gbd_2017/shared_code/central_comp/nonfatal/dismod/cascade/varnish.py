import sys
import os
import subprocess
import logging

import upload
import fit_stats
import drill
from db_tools.ezfuncs import query
from cascade_ode.demographics import Demographics
from cascade_ode import importer
from cascade_ode import __version__
from setup_logger import setup_logger
from jobmon import sge
from save_results._save_results import DismodSaveResults

# Set default file mask to readable-for all users
os.umask(0o0002)

AGE_GROUP_SET_ID = 12


def main():
    '''Set commit hash, upload model, try to write effects_plots pdfs,
    aggregate model version draws up location hierarchy
    '''
    setup_logger()
    mvid = sys.argv[1]

    log = logging.getLogger(__name__)
    log.info("Varnish started for mvid {}".format(mvid))

    try:
        try:
            commit_hash = sge.get_commit_hash(dir='%s/..' % drill.this_path)
        except subprocess.CalledProcessError:
            # in site-packages, not git repo
            commit_hash = __version__
        upload.set_commit_hash(mvid, commit_hash)
        upload.upload_model(mvid)

        outdir = "%s/%s/full" % (
            drill.settings['cascade_ode_out_dir'],
            str(mvid))
        joutdir = "%s/%s" % (drill.settings['diag_out_dir'], mvid)
        fit_stats.write_fit_stats(mvid, outdir, joutdir)
        upload.upload_fit_stat(mvid)

        # Write effect PDFs
        plotter = "{}/effect_plots.r".format(drill.this_path)
        plotter = os.path.realpath(plotter)

        demo = Demographics()
        try:
            subprocess.check_output([
                "FILEPATH",
                plotter,
                str(mvid),
                joutdir,
                drill.settings['cascade_ode_out_dir'],
                str(max(demo.year_ids))],
                stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            log.exception("Error in effect plots")

        # Launch final aggregations
        log.info("Starting Save Results")
        aggregate_model(mvid, demo=demo)
    except Exception:
        log.exception("Error in varnish")
        raise


def aggregate_model(mvid, demo):
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
        birth_prevalence=agg_args['birth_prevalence'])
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
    agg_args['modelable_entity_id'] = mvm.modelable_entity_id.item()
    agg_args['description'] = mvm.description.item()
    agg_args['year_id'] = demo.year_ids
    agg_args['sex_id'] = demo.sex_ids
    agg_args['measure_id'] = get_measures_from_casc(casc)
    agg_args['db_env'] = db_env
    agg_args['gbd_round_id'] = demo.gbd_round_id
    agg_args['birth_prevalence'] = mvm.birth_prev.fillna(0).replace(
            {0: False, 1: True}).item()

    return agg_args


def get_measures_from_casc(casc):
    measure_only = casc.model_version_meta.measure_only
    if measure_only.notnull().all():
        return measure_only.item()

    q = "select measure_id from shared.measure where measure in ('{}')".format(
            "', '".join(importer.integrand_pred))
    df = query(q, conn_def="epi")
    return sorted(df.measure_id.tolist())


if __name__ == '__main__':
    main()

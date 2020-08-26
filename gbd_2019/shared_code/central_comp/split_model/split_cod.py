import functools
import glob
from multiprocessing import Pool
import os
import pandas as pd
import sys
import tempfile
import traceback

from cluster_utils.io import makedirs_safely
from core_maths.scale_split import merge_split
from db_tools.ezfuncs import query
from db_queries import get_location_metadata, get_cause_metadata, get_ids
from get_draws.api import get_draws
import gbd.constants as gbd
from gbd.decomp_step import decomp_step_id_from_decomp_step
from gbd.estimation_years import gbd_round_from_gbd_round_id
from test_support.profile_support import profile

from split_models.exceptions import IllegalSplitCoDArgument
from split_models.job_classes import SplitCoDSwarm
from split_models.validate import (
    validate_decomp_step_input,
    validate_ids
)

if sys.version_info > (3,):
    long = int

REPORTING_CAUSE_SET_ID, COMPUTATION_CAUSE_SET_ID = 3, 2

# Create list of valid cause_ids
VALID_CAUSE_IDS = get_ids(table='cause').cause_id.unique()
VALID_MEIDS = get_ids(table='modelable_entity').modelable_entity_id.unique()


@profile
def _launch_cod_splits(source_cause_id, target_cause_ids, target_meids,
                       prop_meas_id, gbd_round_id, decomp_step, output_dir,
                       project):
    """
    Split the given source_cause_id given target_meid proportions, saved
    to the target_cause_ids in output_dir.

    Arguments:
        source_cause_id (int): cause_id for the draws to be split
        target_cause_ids (intlist): list of cause ids that you want the new
            outputted subcauses to be identified by
        target_meids (intlist): list of proportion models' modelable_entity_ids
            that you want the source_cause_id to be split by, to make the
            target_cause_ids. Target_cause_ids and target_me_ids must be
            specified in the same order
        prop_meas_id (int): The measure_id that identifies the proportion
            in the target_meids to use for the split.
        gbd_round_id (int): the gbd_round_id for models being split.
        decomp_step (str): Specifies which decomposition step the returned
            estimates should be from. If using interpolate for GBD round 6 and
            above, must specify one of 'step1', 'step2', 'step3', 'step4',
            'step5', or 'iterative'.
        output_dir (str): directory where you want final results stored
        project (str): The SGE project to launch split_cod_model subjobs
            to using SplitCodSwarm.

    Returns:
        A list of tuples with each location_id paired with either 0, or an
                error message. This is then parsed in the central function
                draw_ops.split_cod_model into errors or success messages
    """

    # setup years, sex restrictions, most detailed locations, etc.

    if gbd_round_id >= 6:
        cause_set_id = COMPUTATION_CAUSE_SET_ID
    else:
        cause_set_id = REPORTING_CAUSE_SET_ID
    causes = get_cause_metadata(
        cause_set_id=cause_set_id,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step
    ).query("cause_id==@source_cause_id")
    sex_ids = []
    if causes['male'].item() != 0:
        sex_ids.append(1)
    if causes['female'].item() != 0:
        sex_ids.append(2)
    if not sex_ids:
        raise ValueError(
            "Source_cause_id {} is restricted for both males and females, "
            "according to cause metadata"
            .format(source_cause_id)
        )

    most_detailed_locs = list(
        get_location_metadata(
            35, gbd_round_id=gbd_round_id, decomp_step=decomp_step
        ).query('most_detailed==1').location_id.unique())
    meid_cause_map = dict(zip(target_meids, target_cause_ids))

    # run interpolating/extrapolating
    intermediate_dir = os.path.join(
        output_dir, 'intermediate_{}'.format(source_cause_id)
    )
    if not os.path.exists(intermediate_dir):
        makedirs_safely(intermediate_dir)

    swarm = SplitCoDSwarm(
        source_id=source_cause_id,
        proportion_ids=target_meids,
        proportion_measure_id=prop_meas_id,
        sex_ids=sex_ids,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        intermediate_dir=intermediate_dir,
        outdir=output_dir,
        project=project
    )
    swarm.add_interpolate_tasks()
    exit_code = swarm.run()
    if exit_code != 0:
        raise RuntimeError(
            "Interpolating CoD years failed. Check logs in {}."
            .format(output_dir)
        )

    # run splitting
    for cid in target_cause_ids:
        cid_dir = os.path.join(output_dir, str(cid))
        if not os.path.exists(cid_dir):
            makedirs_safely(cid_dir)
    file_list = glob.glob(os.path.join('{}/*.h5'.format(intermediate_dir)))

    # read in draws for source cause
    source = _get_draws(source_cause_id, gbd_round_id, decomp_step, sex_ids)
    # create a temporary directory to store all the draws from the source cause
    tmpdir = tempfile.TemporaryDirectory(dir=output_dir)
    # save source cause draws to temporary directory
    source.to_hdf(
        os.path.join(tmpdir.name, 'source_cause_draws.h5'),
        key='draws', mode='w', format='table',
        data_columns=['location_id', 'year_id', 'sex_id', 'age_group_id']
    )

    run_splits = functools.partial(_parallel_merge_split,
                                   meid_cause_map,
                                   file_list,
                                   output_dir,
                                   tmpdir)
    pool = Pool(30)
    res = pool.map(run_splits, most_detailed_locs)
    pool.close()
    pool.join()
    # clean up tempdir
    tmpdir.cleanup()
    return res


def _get_draws(source_cause_id, gbd_round_id, decomp_step, sex_ids):
    """Read in all the draws from the source cause id for each sex."""
    cd = []
    for sex in sex_ids:
        version_id = best_versions(source_cause_id, gbd_round_id, sex,
                                   decomp_step)
        cd.append(get_draws(gbd_id_type='cause_id', gbd_id=source_cause_id,
                            source='codem', gbd_round_id=gbd_round_id,
                            decomp_step=decomp_step, version_id=version_id))
    cd = pd.concat(cd)
    return cd


def best_versions(cause_id, gbd_round_id, sex_id, decomp_step):
    decomp_step_id = decomp_step_id_from_decomp_step(decomp_step, gbd_round_id)
    q = """
        SELECT model_version_id 
        FROM cod.model_version
        WHERE cause_id={cid} AND 
              sex_id = {sex} AND 
              gbd_round_id={gbd} AND
              is_best = 1 AND
              model_version_type_id IN (3, 4) AND
              decomp_step_id = {decomp}
        """.format(cid=cause_id, sex=sex_id, gbd=gbd_round_id,
                   decomp=decomp_step_id)
    res = query(q, conn_def='cod')
    mvids = res.model_version_id.tolist()
    bad_mvids_msg = ("Error: Returned more than one model_version_id: "
                     "{mvids} for cause_id: {cid}. Cannot split model."
                     .format(mvids=mvids, cid=cause_id))
    if len(mvids) != 1:
        raise RuntimeError(bad_mvids_msg)
    return mvids[0]


@profile
def _parallel_merge_split(meid_cause_map, interp_files, output_dir, tmpdir,
                          location_id):
    try:
        epi_draw = []
        for f in interp_files:
            epi_draw.append(pd.read_hdf(f, 'draws',
                            where=["location_id=={}".format(location_id)]))
        epi_draws = pd.concat(epi_draw)

        cd = pd.read_hdf(
            os.path.join(tmpdir.name, 'source_cause_draws.h5'), 'draws',
            where=['location_id=={}'.format(location_id)]
        )

        draw_cols = [col for col in cd.columns if 'draw_' in col]
        epi_draws = epi_draws[
            epi_draws['age_group_id'].isin(cd['age_group_id'].unique())]

        # these columns are not needed and cause maths.merge_split to break
        drop_cols = ['measure_id', 'model_version_id', 'metric_id']
        cd.drop(drop_cols, axis=1, inplace=True, errors='ignore')
        epi_draws.drop(drop_cols, axis=1, inplace=True, errors='ignore')

        cout = merge_split(
            cd, epi_draws,
            ['year_id', 'age_group_id', 'sex_id', 'location_id'],
            draw_cols)

        cout = cout.merge(
            cd[['year_id', 'age_group_id', 'sex_id', 'location_id',
                'envelope']],
            how='left')
        cout['cause_id'] = cout['modelable_entity_id']
        cout['cause_id'] = cout['cause_id'].replace(meid_cause_map)
        cout['measure_id'] = 1
        for cid in cout.cause_id.unique():
            cid_dir = '{}/{}'.format(output_dir, int(cid))
            cid_dir = cid_dir.replace("\r", "")
            if not os.path.exists(cid_dir):
                makedirs_safely(cid_dir)
            fn = '{}/death_{}.csv'.format(cid_dir, location_id)
            cout.query('cause_id=={}'.format(cid)).to_csv(fn, index=False)
        return location_id, 0
    except Exception:
        tb_str = traceback.format_exc()
        return location_id, tb_str


@profile
def split_cod_model(source_cause_id,
                    target_cause_ids,
                    target_meids,
                    project,
                    prop_meas_id=18,
                    decomp_step=None,
                    gbd_round_id=gbd.GBD_ROUND_ID,
                    output_dir=None):

    """Returns a dataframe containing only the name of the out_dir where this
    function will save your new draws.

    Arguments:
        source_cause_id (int): the cause_id to be split into target_cause_ids.

        target_cause_ids (intlist): the cause_ids that should be produced as a
            result of the split. These should be provided in the same order as
            the target_meids which define the split proportions.

        target_meids (intlist): the modelable_entity_ids containing the
            proportion models by which to split the source_cause_id. These
            should be provided in the same order as the target_cause_ids.
        
        project (str): the team-specific SGE cluster project to launch the
            subjobs of the split_cod_model swarm to.

        prop_meas_id (int): The measure_id that identifies the proportion in
            the target_meids to use for the split. Defaults to measure_id 18,
            proportion.

        decomp_step (str): Specifies which decomposition step the returned
            estimates should be from. Default to None. "Allowed values are
            None, iterative', 'step1', 'step2', 'step3', 'step4', and 'step5'
            depending on the value of gbd_round_id."

        gbd_round_id (int): the gbd_round_id for models being split. Defaults
            to current GBD round id.

        output_dir (str): place where you want new draws to be saved.

    Returns:
        Pandas.DataFrame:
        The output directory where either the draws or an errors logfile
        can be found.

    Raises:
        IllegalSplitCoDArgument: If the source_cause_id, any of the
            target_cause_ids or target_meids are invalid, or if the lists
            of target causes and target meids are not one-to-one.
        RuntimeError: If any under-the-hood errors thrown directing the
            user to the log file.
    """
    # Validate all incoming arguments.
    validate_decomp_step_input(decomp_step, gbd_round_id)

    # Validate source cause_id
    validate_ids(source_cause_id, VALID_CAUSE_IDS, 'source_cause_id')
    # Validate target_cause_ids
    validate_ids(target_cause_ids, VALID_CAUSE_IDS, 'target_cause_ids')

    # Validate modelable_entity_ids
    validate_ids(target_meids, VALID_MEIDS, 'proportion_meids')

    if len(target_cause_ids) != len(target_meids):
        raise IllegalSplitCoDArgument(
            "target_cause_ids and target_meids lists must represent a 1-to-1 "
            "mapping of cause_ids to modelable_entity_ids. Received: {t_ids} "
            "target ids and {p_ids} proportion ids."
            .format(t_ids=len(target_cause_ids), p_ids=len(target_meids))
        )

    if not output_dir:
        output_dir = 'FILEPATH'

    if not os.path.exists(output_dir):
        makedirs_safely(output_dir)

    res = _launch_cod_splits(source_cause_id,
                             target_cause_ids,
                             target_meids,
                             prop_meas_id,
                             gbd_round_id,
                             decomp_step,
                             output_dir,
                             project)
    errors = [r for r in res if r[1] != 0]

    if len(errors) == 0:
        return pd.DataFrame({'output_dir': output_dir}, index=[0])
    else:
        logfile = '{}/{}_errors.log'.format(
            output_dir, str(source_cause_id))
        with open(logfile, 'w') as f:
            estr = "\n".join([str(r) for r in errors])
            f.write(estr)

        return pd.DataFrame({'error_dir': logfile}, index=[0])

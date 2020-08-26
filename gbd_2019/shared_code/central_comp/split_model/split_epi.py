from multiprocessing import Pool
import os
import pandas as pd

from cluster_utils.io import makedirs_safely
from core_maths.scale_split import merge_split
from db_queries import get_best_model_versions
import gbd.constants as gbd
from gbd.estimation_years import gbd_round_from_gbd_round_id
from get_draws.api import get_draws
from hierarchies import dbtrees
from test_support.profile_support import profile

from split_models.exceptions import IllegalSplitEpiArgument
from split_models.validate import (
    validate_decomp_step_input,
    validate_measure_id,
    validate_meids,
    validate_requested_measures,
    validate_source_meid,
    validate_split_measure_ids
)


@profile
def best_version(modelable_entity_id, gbd_round_id, decomp_step):
    res = get_best_model_versions(
        entity='modelable_entity',
        ids=modelable_entity_id,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        status='best'
    )
    return res.model_version_id.tolist()


@profile
def filet(source_meid, target_prop_map, location_id, split_meas_ids,
          prop_meas_id, gbd_round_id, mvid_map, source_mvid, decomp_step,
          n_draws, downsample):
    """
    Splits the draws for source_meid to the target meids given in
    target_prop_map by the proportions estimated in the prop_meids. The split
    is applied to all GBD years associated with the given gbd_round_id for the
    specified location_id. The 'best' version of the meids will be used by
    default.

    Arguments:
        source_meid (int): meid for the draws to be split.

        target_prop_map (dict): dictionary whose keys are the target meids and
            whose values are the meids for the corresponding proportion models.

        location_id (int): location_id to operate on.

        split_meas_ids (list of ints): The measure_ids from source_meid to be
            split.

        prop_meas_id (int): The measure_id that identifies the proportion in
            prop_meids to use for the split.

        gbd_round_id (int): the gbd_round_id for models being split.

        mvid_map (dict): relationship of target MEs to proportion MEs.

        source_mvid (int): source model version id.

        decomp_step (str): Decomposition step. Allowed values are None,
            'iterative', 'step1', 'step2', 'step3', 'step4', and 'step5'
            depending on the value of gbd_round_id.

        n_draws (Optional[int])

        downsample (Optional[bool])

    Returns:
        A DataFrame containing the draws for the target meids
    """
    splits = []
    props = []

    for key in target_prop_map:
        if mvid_map is not None:
            version_id = mvid_map[target_prop_map[key]]
        else:
            version_id = None
        this_props = get_draws(
            gbd_id_type='modelable_entity_id',
            gbd_id=target_prop_map[key],
            source='epi',
            measure_id=prop_meas_id,
            location_id=location_id,
            version_id=version_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            n_draws=n_draws,
            downsample=downsample
        )
        props.append(this_props)

    props = pd.concat(props)
    props = props.reset_index(drop=True)
    props['target_modelable_entity_id'] = (
        props.modelable_entity_id.replace(
            {v: k for k, v in target_prop_map.items()}))
    props_drawcols = [col for col in props.columns if 'draw_' in col]
    if source_mvid is not None:
        version_id = source_mvid
    else:
        version_id = None
    for measure_id in split_meas_ids:
        source = get_draws(
            gbd_id_type='modelable_entity_id',
            gbd_id=[source_meid],
            source='epi',
            measure_id=measure_id,
            location_id=location_id,
            version_id=version_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step
        )
        source_drawcols = [col for col in source.columns if 'draw_' in col]
        props['measure_id'] = measure_id
        props = props[props.age_group_id.isin(source.age_group_id.unique())]
        props = props[props.sex_id.isin(source.sex_id.unique())]

        # These columns are not needed and break maths.merge_split
        drop_cols = ['modelable_entity_id', 'model_version_id', 'metric_id']
        source.drop(drop_cols, axis=1, inplace=True, errors='ignore')

        if len(source) > 0 and len(props) > 0:
            if len(target_prop_map) > 1:
                force_scale = True
            else:
                force_scale = False

            if len(props_drawcols) != len(source_drawcols) :
                raise ValueError("props and source drawcols are different lengths")

            split = merge_split(
                source,
                props,
                ['year_id', 'age_group_id', 'sex_id', 'location_id',
                 'measure_id'],
                props_drawcols,
                force_scale=force_scale)
            splits.append(split)
        else:
            pass
    splits = pd.concat(splits)
    splits = splits[[
        'location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id',
        'target_modelable_entity_id'] + props_drawcols]
    splits.rename(
        columns={'target_modelable_entity_id': 'modelable_entity_id'},
        inplace=True)
    return splits


@profile
def split_n_write(args):
    """Wrapper for multiprocessed splits"""
    (source, targets, loc, split_meas_ids, prop_meas_id, output_dir,
     gbd_round_id, mvid_map, source_mvid, decomp_step, n_draws,
     downsample) = args
    try:
        res = filet(
            source,
            targets,
            loc,
            split_meas_ids,
            prop_meas_id,
            gbd_round_id,
            mvid_map,
            source_mvid,
            decomp_step,
            n_draws,
            downsample
        )
        drawcols = [col for col in res.columns if 'draw_' in col]
        idxcols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                   'measure_id']
        for meid in res.modelable_entity_id.unique():
            meid_dir = '{}/{}'.format(output_dir, meid)
            meid_dir = meid_dir.replace("\r", "")
            try:
                os.makedirs(meid_dir)
            except:
                pass
            fn = '{}/{}.h5'.format(meid_dir, loc)
            tw = res.query("modelable_entity_id=={}".format(meid)
                           )[idxcols + drawcols]
            tw.to_hdf(fn, 'draws', mode='w', format='table',
                      data_columns=idxcols)
        return loc, 0
    except Exception as e:
        return loc, str(e)


@profile
def launch_epi_splits(source_meid, target_meids, proportion_meids,
                      split_measure_ids, proportion_measure_id, output_dir,
                      gbd_round_id, decomp_step):
    """Split the given source_meid by the prop_meid proportions,
    saving the results in the appropriate target_meid directories in output_dir

    Arguments:
        source_meid (int): The meid to be split.

        target_meids (list of ints): The identifiers that should be used to
            tagging and saving outputs.

        proportion_meids (list of ints): The meids for the proportion models
            that should be used to split the source_meid. This should be the
            same length as target_meids, and is order dependent (i.e. the
            result of applying the first listed proportion_meid will be tagged
            and saved using the first listed target_meid).

        split_measure_ids (list of ints): The measure_ids from source_meid to
            be split.

        proportion_measure_id (int): The measure_id that identifies the
            proportion in proportion_meids to use for the split.

        output_dir(str): directory where you want final results stored

        gbd_round_id (int): the gbd_round_id for models being split.

        decomp_step (str): Decomposition step. Allowed values are None,
            'iterative', 'step1', 'step2', 'step3', 'step4', and 'step5'
            depending on the value of gbd_round_id.

    Returns:
        A list of tuples indicating: (location_id, success (0) or failure
        (string with error details))
    """
    meme_map = dict(zip(target_meids, proportion_meids))

    # Get model versions. Because we are multiprocessing, we want to get the
    # model version ahead of calling epi.draws. This avoids repeated database
    # queries for the same values
    try:
        source_mvid = best_version(modelable_entity_id=source_meid,
                                   gbd_round_id=gbd_round_id,
                                   decomp_step=decomp_step)[0]
    except Exception as e:
        raise ValueError(
            "No best model for modelable_entity_id: {me} in gbd_round_id: "
            "{gr} for decomp_step: {ds}. Error: {e}"
            .format(me=source_meid, gr=gbd_round_id, ds=decomp_step, e=e)
        )
    mvid_map = {}
    for prop in proportion_meids:
        try:
            mvid = best_version(modelable_entity_id=prop,
                                gbd_round_id=gbd_round_id,
                                decomp_step=decomp_step)[0]
        except Exception as e:
            raise ValueError(
                "No best model for modelable_entity_id: {me} in gbd_round_id: "
                "{gr}. Error: {e}".format(me=prop, gr=gbd_round_id, e=e))
        mvid_map[prop] = mvid

    # get location_ids using dbtrees
    lt = dbtrees.loctree(location_set_id=35, gbd_round_id=gbd_round_id)
    leaf_ids = [l.id for l in lt.leaves()]

    tiny_draw_df = _dummy_draw_call(
        source_meid, source_mvid, gbd_round_id, decomp_step
    )
    n_draws, downsample = _infer_sampling_args(tiny_draw_df.columns)
    validate_requested_measures(
        split_measure_ids,
        tiny_draw_df.measure_id.unique().tolist()
    )

    params = []
    for lid in leaf_ids:
        params.append(
            (source_meid, meme_map, lid, split_measure_ids,
             proportion_measure_id, output_dir, gbd_round_id, mvid_map,
             source_mvid, decomp_step, n_draws, downsample)
        )

    pool = Pool(30)
    res = pool.map(split_n_write, params)
    pool.close()
    return res


def _dummy_draw_call(
        source_meid,
        source_mvid,
        gbd_round_id,
        decomp_step
):
    """
    Query a small set of draws to help us infer our n_draws and sampling
    arguments.

    Arguments:
        source_meid (int): the parent me_id
        source_mvid (int): the model version associated with the best model for
            our source_meid.
        gbd_round_id (int)
        decomp_step (str)

    Returns:
        pd.DataFrame
    """
    return get_draws(
        source='epi',
        gbd_id_type='modelable_entity_id',
        gbd_id=source_meid,
        version_id=source_mvid,
        location_id=1,
        sex_id=[1, 2],
        year_id=2000,
        age_group_id=22,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step
    )


def _infer_sampling_args(columns):
    """
    Given a list of columns from a dataframe, infer the number of draws and
    whether downsampling will be necessary when calling the proportion
    modelable entities.

    Arguments:
        columns (List[String]): The columns from our sample dataframe.

    Returns:
        tuple of n_draws Optional[int] and downsample Optional[bool]
    """
    n_draws = len([col for col in columns if col.startswith('draw_')])
    downsample = True if n_draws < 1000 else None
    n_draws = n_draws if downsample else None
    return n_draws, downsample


@profile
def split_epi_model(
        source_meid,
        target_meids,
        prop_meids,
        split_measure_ids=[5, 6],
        prop_meas_id=18,
        gbd_round_id=gbd.GBD_ROUND_ID,
        decomp_step=None,
        output_dir=None):
    """
    Splits modelable entity draws based on proportion models. Outputs are new
    draw files for each proportion model, tagged with the target_meid.

    Arguments:
        source_meid(int):  The id of the me to be split.

        target_meids(intlist): A list of modelable entity ids that will be
            used to tag the new draws. Essentially, these are the me_ids for
            the newly created models.

        prop_meids (intlist): A list of modelable entity ids corresponding
            to the proportion models used to do the split. Note: target_meids
            and proportion_meids need to have 1-1 parity with each other and
            are order dependent.

            Example: target_meids = [2891, 2892, 2893, 2894]
                 proportion_meids = [1922, 1920, 1921, 1923]

            Proportion model 1922 will be used to create model 2891,
            proportion model 1920 will be used to create model 2892 and so on.

        split_measure_ids(int or intlist, optional): A list of measure_ids from
            the source model, to be split according to the proportion models.
            Default is [5, 6], prevalence and incidence.

        prop_meas_id(int, optional): The measure id used in the proportion
        models. Default is 18, proportion.

        gbd_round_id(int, optional): The gbd_round_id for the proportion models
            being used. This argument is used to retrieve the best model for a
            given round. Default is the current GBD round.

        decomp_step (str): Decomposition step. Allowed values are None,
            'iterative', 'step1', 'step2', 'step3', 'step4', and 'step5'
            depending on the value of gbd_round_id.

        output_dir(str, optional): The directory where the draw files are
            created. Subdirectories are created for each target modelable
            entity id, and files are tagged by location id.
            Example: output_dir/2891/35.h5 is a draw file for newly created
            model 2891, location_id 35.

    Returns:
        Pandas.DataFrame:
            The output directory where either the draws or an errors logfile
            can be found.

    Raises:
        IllegalSplitEpiArgument: If source_meid and proportion_measure_id are
            not integers. If target_meids and prop_meids are not lists of ints
            or split_measure_ids is not an int or list of ints. If target_meids
            and prop_meids do not have the same length.

        ValueError: If the decomp_step argument is invalid.
    """

    # Validate Arguments
    validate_decomp_step_input(decomp_step, gbd_round_id)

    validate_source_meid(source_meid)
    validate_measure_id(prop_meas_id)
    validate_meids(target_meids, 'Target')
    validate_meids(prop_meids, 'Proportion')
    validate_split_measure_ids(split_measure_ids)

    if len(target_meids) != len(prop_meids):
        raise IllegalSplitEpiArgument(
            "Target modelable_entity_ids and proportion modelable_entity_ids "
            "lists must represent a 1-to-1 mapping of modelable_entity_ids. "
            "Received: {t_ids} target ids and {p_ids} proportion ids."
            .format(t_ids=len(target_meids), p_ids=len(prop_meids))
        )

    if output_dir is None:
        output_dir = 'FILEPATH'

    try:
        makedirs_safely(output_dir)
    except Exception:
        pass

    res = launch_epi_splits(
        source_meid,
        target_meids,
        prop_meids,
        split_measure_ids,
        prop_meas_id,
        output_dir,
        gbd_round_id,
        decomp_step)

    errors = [r for r in res if r[1] != 0]

    if len(errors) == 0:
        return pd.DataFrame({'output_dir': output_dir}, index=[0])
    else:
        logfile = '{}/{}_errors.log'.format(
            output_dir, str(source_meid))
        with open(logfile, 'w') as f:
            estr = "\n".join([str(r) for r in errors])
            f.write(estr)

        return pd.DataFrame({'error_dir': logfile}, index=[0])

import os
import argparse
from multiprocessing import Pool
from functools import partial

import pandas as pd

import gbd.constants as gbd

from core_maths.scale_split import scale
from dataframe_io.pusher import SuperPusher
from dataframe_io.io_control.h5_io import H5IO
from epic.util.common import (
    get_best_model_version_and_decomp_step,
    group_and_downsample
)
from epic.util.exceptions import NoBestVersionError
from get_draws.sources.epi import Epi
from get_draws.transforms.automagic import automagic_age_sex_agg
from ihme_dimensions.gbdize import get_age_group_set
from rules.enums import ResearchAreas, Rules, Tools
from rules.RulesManager import RulesManager


code_dir = os.path.dirname(os.path.realpath(__file__))
MAP_FILE = os.path.join(code_dir, "data", "map_pre_pos_mes.csv")
SOURCE_TARGET_FILE = os.path.join(code_dir, "data", "source_target_maps.csv")
stage_key = {
    'epi': 'epi',
    'id': 'id_noepi',
    'blind': 'blind_noepi_noid'}


def squeeze_draws(to_squeeze, envelope, scale_up, n_draws):
    drawcols = [f'draw_{i}' for i in range(n_draws)]
    for draw in drawcols:
        if (to_squeeze[draw].sum() > envelope[draw]) or scale_up:
            to_squeeze = scale(
                to_squeeze,
                draw,
                scalar=envelope[draw])
    return to_squeeze


def squeeze_age_group(age_group_id, unsqueezed, env_dict, n_draws):
    drawcols = [f'draw_{i}' for i in range(n_draws)]
    try:
        # Get envelope
        sqzd = unsqueezed[unsqueezed['age_group_id'] == age_group_id]
        sqzd["locked"] = False
        for imp in ['blind', 'epi', 'id_bord', 'id_mild', 'id_mod', 'id_sev',
                    'id_prof']:
            print(f'Running {age_group_id} {imp}')

            if imp == 'epi':
                env_frac = 0.95
                scale_up = False
            elif 'id' in imp:
                env_frac = 0.95
                scale_up = False
            else:
                env_frac = 1
                scale_up = True

            # Squeeze to envelope if exceeded
            imp_bin = (sqzd[f'i_{imp}'] == 1)
            imp_prev = sqzd[imp_bin]
            other_prev = sqzd[~imp_bin]

            squeezable = imp_prev[~(imp_prev.locked) &
                                  (imp_prev['squeeze'] == "yes")]
            locked = imp_prev[(imp_prev.locked) |
                              (imp_prev['squeeze'] == "no")]

            env = env_dict[imp].copy()
            env = env[env.age_group_id.astype(float).astype(int) ==
                      int(float(age_group_id))]
            env = env[drawcols].squeeze() * env_frac - locked[drawcols].sum()
            env = env.clip(lower=0)
            squeezable = squeeze_draws(squeezable, env, scale_up, n_draws)
            squeezable["locked"] = True
            sqzd = pd.concat([other_prev, locked, squeezable])

        return sqzd
    except Exception as e:
        return ('error', e)

###################################
# Prepare envelopes
###################################


def create_env(
        location_id: int,
        year_id: int,
        sex_id: int,
        decomp_step: str,
        gbd_round_id: int,
        n_draws: int,
        output_dir: str
):
    env_ids = {
        'epi': 2403,
        'blind': 9805,
        'id_bord': 9423,
        'id_mild': 9424,
        'id_mod': 9425,
        'id_sev': 9426,
        'id_prof': 9427}
    envelope_dict = {}
    for envlab, _id in env_ids.items():
        print(_id)
        mvid, dstep = (
            get_best_model_version_and_decomp_step(output_dir, _id)
        )
        print(mvid, dstep)
        draw_source = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=_id,
            model_version_id=mvid,
            gbd_round_id=gbd_round_id,
            decomp_step=dstep
        )
        draw_source.remove_transform(automagic_age_sex_agg)
        if n_draws < 1000:
            draw_source.add_transform(group_and_downsample, n_draws)
        env = draw_source.content(
            filters={"location_id": location_id,
                     "year_id": year_id,
                     "sex_id": sex_id,
                     "measure_id": gbd.measures.PREVALENCE})
        envelope_dict[envlab] = env
    return envelope_dict


###################################
# Get unsqueezed data
###################################
def get_unsqueezed(
        sequelae_map: pd.DataFrame,
        location_id: int,
        year_id: int,
        sex_id: int,
        decomp_step: str,
        gbd_round_id: int,
        n_draws: int,
        output_dir: str
) -> pd.DataFrame:
    drawcols = [f'draw_{i}' for i in range(n_draws)]
    # Get all causes with epilepsy, ID, and blindness
    unsqueezed = []
    for idx, seqrow in sequelae_map.iterrows():
        me_id = int(seqrow[['me_id']])
        print(me_id)
        try:
            mvid, dstep = (
                get_best_model_version_and_decomp_step(output_dir, me_id)
            )
            draw_source = Epi.create_modelable_entity_draw_source(
                n_workers=1,
                modelable_entity_id=me_id,
                model_version_id=mvid,
                gbd_round_id=gbd_round_id,
                decomp_step=dstep
            )
            draw_source.remove_transform(automagic_age_sex_agg)
            if n_draws < 1000:
                draw_source.add_transform(group_and_downsample, n_draws)
            df = draw_source.content(
                filters={"location_id": location_id,
                         "year_id": year_id,
                         "sex_id": sex_id,
                         "measure_id": gbd.measures.PREVALENCE})
            df['me_id'] = me_id
            unsqueezed.append(df)
        except NoBestVersionError:
            print('Failed retrieving {}. Filling with zeros'.format(me_id))
            df = unsqueezed[0].copy()
            df['me_id'] = me_id
            df.loc[:, drawcols] = 0
            unsqueezed.append(df)

    unsqueezed = pd.concat(unsqueezed, sort=True)
    unsqueezed = unsqueezed[['me_id', 'location_id', 'year_id', 'age_group_id',
                             'sex_id'] + drawcols]
    unsqueezed = unsqueezed.merge(sequelae_map, on='me_id')

    age_range = get_age_group_set(12)["age_group_id"].tolist()
    unsqueezed = unsqueezed[unsqueezed['age_group_id'].isin(age_range)]

    return unsqueezed


##################################
# Write to files
##################################
def write_squeezed(output_dir, sqzd, location_id, year_id, sex_id, map_file):

    tmap = pd.read_csv(map_file)
    for me_id, df in sqzd.groupby(['me_id']):

        t_meid = tmap.query(f'modelable_entity_id_source == {me_id}')
        t_meid = t_meid['modelable_entity_id_target'].squeeze()
        try:
            t_meid = int(t_meid)
        except Exception:
            pass
        if not isinstance(t_meid, int):
            continue
        print(f'Writing squeezed {t_meid} to file')
        df['location_id'] = int(float(location_id))
        df['year_id'] = int(float(year_id))
        df['sex_id'] = int(float(sex_id))
        df['measure_id'] = gbd.measures.PREVALENCE
        df['age_group_id'] = df.age_group_id.astype(float).astype(int)
        df["modelable_entity_id"] = t_meid

        pusher = SuperPusher(
            spec={'file_pattern': ("{modelable_entity_id}/{location_id}/"
                                   "{measure_id}_{year_id}_{sex_id}.h5"),
                  'h5_tablename': 'draws'},
            directory=output_dir)
        pusher.push(df, append=False)


##################################
# Allocate residuals
##################################
def allocate_residuals(
        output_dir: str,
        usqzd: pd.DataFrame,
        sqzd: pd.DataFrame,
        location_id: int,
        year_id: int,
        sex_id: int,
        map_file: str,
        decomp_step: str,
        gbd_round_id: int,
        n_draws: int
) -> pd.DataFrame:
    drawcols = [f'draw_{i}' for i in range(n_draws)]
    tmap = pd.read_csv(map_file)

    resids = usqzd.merge(
        sqzd,
        on=['location_id', 'year_id', 'age_group_id', 'sex_id', 'me_id'],
        suffixes=('.usqzd', '.sqzd'))
    resids = resids[resids['resid_target_me.usqzd'].notnull()]

    dscols = [f'draw_{d}.sqzd' for d in range(n_draws)]
    ducols = [f'draw_{d}.usqzd' for d in range(n_draws)]
    toalloc = resids[ducols].values - resids[dscols].values
    toalloc = toalloc.clip(min=0)
    resids = resids.join(pd.DataFrame(
        data=toalloc, index=resids.index, columns=drawcols))
    resids = resids[['location_id', 'year_id', 'age_group_id', 'sex_id',
                     'resid_target_me.usqzd'] + drawcols]
    resids.rename(
        columns={'resid_target_me.usqzd': 'resid_target_me'},
        inplace=True)
    resids = resids.groupby(['resid_target_me', 'age_group_id']).sum()
    resids = resids.reset_index()
    resids = resids[['resid_target_me', 'age_group_id'] + drawcols]
    resids['resid_target_me'] = resids['resid_target_me'].astype(int)


    for me_id, resid_df in resids.groupby('resid_target_me'):
        t_meid = tmap.query(f'modelable_entity_id_source == {me_id}')
        t_meid = t_meid.modelable_entity_id_target.squeeze()
        try:
            t_meid = int(t_meid)
        except Exception:
            pass
        present = True
        try:
            mvid, dstep = (
                get_best_model_version_and_decomp_step(output_dir, me_id)
            )
            draw_source = Epi.create_modelable_entity_draw_source(
                n_workers=1,
                modelable_entity_id=me_id,
                model_version_id=mvid,
                gbd_round_id=gbd_round_id,
                decomp_step=dstep
            )
            draw_source.remove_transform(automagic_age_sex_agg)
            if n_draws < 1000:
                draw_source.add_transform(group_and_downsample, n_draws)
            t_df = draw_source.content(
                filters={"location_id": location_id,
                         "year_id": year_id,
                         "sex_id": sex_id,
                         "measure_id": gbd.measures.PREVALENCE})
        except NoBestVersionError:
            present = False
        if present:
            t_df = t_df.merge(
                resid_df, on='age_group_id', suffixes=('#base', '#resid'))
            newvals = (
                t_df.filter(like="#base").values +
                t_df.filter(like="#resid").values)
            t_df = t_df.join(pd.DataFrame(
                data=newvals, index=t_df.index, columns=drawcols))

            print(f'Writing residual {t_meid} to file')
            t_df['location_id'] = int(float(location_id))
            t_df['year_id'] = int(float(year_id))
            t_df['sex_id'] = int(float(sex_id))
            t_df['measure_id'] = gbd.measures.PREVALENCE
            t_df['age_group_id'] = t_df.age_group_id.astype(float).astype(int)
            t_df["modelable_entity_id"] = t_meid
            t_df = t_df[['location_id', 'year_id', 'age_group_id', "sex_id",
                         "modelable_entity_id", "measure_id"] + drawcols]
            pusher = SuperPusher(
                spec={'file_pattern': ("{modelable_entity_id}/{location_id}/"
                                       "{measure_id}_{year_id}_{sex_id}.h5"),
                      'h5_tablename': 'draws'},
                directory=output_dir)
            pusher.push(t_df, append=False)
        else:
            print(f'ME ID {me_id} missing')

    return resids


###########################################
# Determine the remainder of the envelopes
###########################################
def calc_env_remainders(envelope_dict, sqzd, n_draws):
    drawcols = [f'draw_{i}' for i in range(n_draws)]
    remains = {}
    for key in envelope_dict:
        allocd = sqzd[sqzd[f'i_{key}'] == 1]
        allocd = allocd.groupby('age_group_id').sum().reset_index()
        remain = envelope_dict[key].merge(
            allocd,
            on='age_group_id',
            suffixes=('.env', '.alloc'))
        decols = [f'draw_{d}.env' for d in range(n_draws)]
        dacols = [f'draw_{d}.alloc' for d in range(n_draws)]
        toalloc = remain[decols].values - remain[dacols].values
        toalloc = toalloc.clip(min=0)
        remain = remain.join(pd.DataFrame(
            data=toalloc, index=remain.index, columns=drawcols))
        remain = remain[['age_group_id'] + drawcols]
        remains[key] = remain.copy()
    return remains


def run_squeeze(
        output_dir: str,
        location_id: int,
        year_id: int,
        sex_id: int,
        decomp_step: str,
        gbd_round_id: int = gbd.GBD_ROUND_ID,
        n_draws: int = 1000
    ):
    drawcols = [f'draw_{i}' for i in range(n_draws)]

    ###################################
    # Prepare envelopes
    ###################################
    sequelae_map = pd.read_csv(SOURCE_TARGET_FILE)
    envelope_dict = create_env(
        location_id,
        year_id,
        sex_id,
        decomp_step,
        gbd_round_id,
        n_draws,
        output_dir
    )

    ###################################
    # Prepare unsqueezed prevalence
    ###################################
    # Load map of sequelae and their targets
    unsqueezed = get_unsqueezed(
        sequelae_map,
        location_id,
        year_id,
        sex_id,
        decomp_step,
        gbd_round_id,
        n_draws,
        output_dir
    )
    unsqueezed.loc[:, drawcols] = unsqueezed.loc[:, drawcols].clip(lower=0)

    ###################################
    # SQUEEZE
    ###################################
    # Parallelize the squeezing
    pool = Pool(20)
    ages = list(pd.unique(unsqueezed['age_group_id']))
    partial_squeeze = partial(
        squeeze_age_group,
        unsqueezed=unsqueezed,
        env_dict=envelope_dict,
        n_draws=n_draws)
    squeezed = pool.map(partial_squeeze, ages, chunksize=1)
    pool.close()
    pool.join()
    squeezed = pd.concat(squeezed)
    squeezed = squeezed.groupby(['location_id', 'year_id', 'age_group_id',
                                 'sex_id', 'me_id']).sum()
    squeezed = squeezed.reset_index()

    ##################################
    # Write to files
    ##################################
    write_squeezed(output_dir, squeezed, location_id, year_id, sex_id, MAP_FILE)

    ##################################
    # Allocate residuals
    ##################################
    allocate_residuals(
        output_dir,
        unsqueezed,
        squeezed,
        location_id,
        year_id,
        sex_id,
        MAP_FILE,
        decomp_step,
        gbd_round_id,
        n_draws
    )

    ###########################################
    # Determine the remainder of the envelopes
    ###########################################
    remains = calc_env_remainders(envelope_dict, squeezed, n_draws)

    remain_map = {'id_bord': 2000,
                  'id_mild': 1999,
                  'id_mod': 2001,
                  'id_sev': 2002,
                  'id_prof': 2003}
    for key, meid in remain_map.items():
        print(f'Writing remainder {meid} to file')
        try:
            meid = int(meid)
        except Exception:
            pass
        df = remains[key]
        df['location_id'] = int(float(location_id))
        df['year_id'] = int(float(year_id))
        df['sex_id'] = int(float(sex_id))
        df['measure_id'] = gbd.measures.PREVALENCE
        df['age_group_id'] = df.age_group_id.astype(float).astype(int)
        df["modelable_entity_id"] = meid
        pusher = SuperPusher(
            spec={'file_pattern': ("{modelable_entity_id}/{location_id}/"
                                   "{measure_id}_{year_id}_{sex_id}.h5"),
                  'h5_tablename': 'draws'},
            directory=output_dir)
        pusher.push(df[['location_id', 'year_id', 'age_group_id', "sex_id",
                        "modelable_entity_id", "measure_id"] + drawcols],
                    append=False)

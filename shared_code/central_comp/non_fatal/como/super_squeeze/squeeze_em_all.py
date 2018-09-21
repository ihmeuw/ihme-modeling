import argparse
import pandas as pd
from multiprocessing import Pool
from transmogrifier import gopher
import os
import errno
from functools import partial
import core.scale_funcs as sf
import pkg_resources

###################################
# Prepare envelopes
###################################
def create_env(location_id, year_id, sex_id):
    env_ids = {
        'epi': 2403,
        'blind': 9805,
        'id_bord': 9423,
        'id_mild': 9424,
        'id_mod': 9425,
        'id_sev': 9426,
        'id_prof': 9427}
    envelope_dict = {}
    for envlab, id in env_ids.iteritems():
        env = gopher.draws(
            {'modelable_entity_ids': [id]},
            'dismod',
            location_ids=location_id,
            year_ids=year_id,
            sex_ids=sex_id,
            measure_ids=5)
        envelope_dict[envlab] = env.copy()
    return envelope_dict


###################################
# Get unsqueezed data
###################################
def get_unsqueezed(sequelae_map, drawcols, location_id, year_id, sex_id):
    # Get all causes with epilepsy, ID, and blindness
    unsqueezed = []
    for idx, seqrow in sequelae_map.iterrows():
        me_id = int(seqrow[['me_id']])
        a = seqrow['acause']

        try:
            gbd_ids = {'modelable_entity_ids': [me_id]}
            df = gopher.draws(
                gbd_ids,
                'dismod',
                location_ids=location_id,
                year_ids=year_id,
                sex_ids=sex_id,
                measure_ids=5)
            df['me_id'] = me_id
            unsqueezed.append(df)
        except:
            print('Failed retrieving %s. Filling with zeros' %
                  (a))
            df = unsqueezed[0].copy()
            df['me_id'] = me_id
            df.ix[:, drawcols] = 0
            unsqueezed.append(df)

    unsqueezed = pd.concat(unsqueezed)
    unsqueezed = unsqueezed[['me_id', 'location_id', 'year_id', 'age_group_id',
                             'sex_id'] + drawcols]
    unsqueezed = unsqueezed.merge(sequelae_map, on='me_id')
    age_range = range(2, 21) + [30, 31, 32, 235]
    unsqueezed = unsqueezed[unsqueezed['age_group_id'].isin(age_range)]

    return unsqueezed


##################################
# Write to files
##################################
def write_squeezed(sqzd, location_id, year_id, sex_id, map_file, drawcols):

    tmap = pd.read_csv(map_file)
    for me_id, df in sqzd.groupby(['me_id']):

        t_meid = tmap.query('modelable_entity_id_source == %s' % me_id)
        t_meid = t_meid['modelable_entity_id_target'].squeeze()
        try:
            t_meid = int(t_meid)
        except:
            pass
        if not isinstance(t_meid, int):
            continue
        print('Writing squeezed %s to file' % t_meid)
        drawsdir = "/FILEPATH"
        fn = "%s/%s_%s_%s.h5" % (drawsdir, location_id, year_id, sex_id)
        if not os.path.exists(drawsdir):
            os.makedirs(drawsdir)
        df['location_id'] = int(float(location_id))
        df['year_id'] = int(float(year_id))
        df['sex_id'] = int(float(sex_id))
        df['measure_id'] = 5
        df['age_group_id'] = df.age_group_id.astype(float).astype(int)
        datacols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id']
        df[datacols + drawcols].to_hdf(
            fn,
            'draws',
            mode='w',
            format='table',
            data_columns=datacols)


##################################
# Allocate residuals
##################################
def allocate_residuals(usqzd, sqzd, location_id, year_id, sex_id, map_file,
                       drawcols):
    tmap = pd.read_csv(map_file)

    resids = usqzd.merge(
        sqzd,
        on=['location_id', 'year_id', 'age_group_id', 'sex_id', 'me_id'],
        suffixes=('.usqzd', '.sqzd'))
    resids = resids[resids['resid_target_me.usqzd'].notnull()]

    dscols = ['draw_%s.sqzd' % d for d in range(1000)]
    ducols = ['draw_%s.usqzd' % d for d in range(1000)]
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

    for me_id, resid_df in resids.groupby('resid_target_me'):
        t_meid = tmap.query('modelable_entity_id_source == %s' % me_id)
        t_meid = t_meid.modelable_entity_id_target.squeeze()
        try:
            t_meid = int(t_meid)
        except:
            pass
        present = True
        try:
            gbd_ids = {'modelable_entity_ids': [me_id]}
            t_df = gopher.draws(
                gbd_ids,
                'dismod',
                location_ids=location_id,
                year_ids=year_id,
                sex_ids=sex_id,
                measure_ids=5)
        except ValueError:
            present = False
        if present:
            t_df = t_df.merge(
                resid_df, on='age_group_id', suffixes=('#base', '#resid'))
            newvals = (
                t_df.filter(like="#base").values +
                t_df.filter(like="#resid").values)
            t_df = t_df.join(pd.DataFrame(
                data=newvals, index=t_df.index, columns=drawcols))

            print('Writing residual %s to file' % t_meid)
            drawsdir = "/FILEPATH"
            fn = "%s/%s_%s_%s.h5" % (drawsdir, location_id, year_id, sex_id)
            try:
                os.makedirs(drawsdir)
            except OSError  as e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
            t_df['location_id'] = int(float(location_id))
            t_df['year_id'] = int(float(year_id))
            t_df['sex_id'] = int(float(sex_id))
            t_df['measure_id'] = 5
            t_df['age_group_id'] = t_df.age_group_id.astype(float).astype(int)
            datacols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                        'measure_id']
            t_df[datacols + drawcols].to_hdf(
                fn,
                'draws',
                mode='w',
                format='table',
                data_columns=datacols)
        else:
            print('ME ID %s missing' % me_id)

    return resids


###########################################
# Determine the remainder of the envelopes
###########################################
def calc_env_remainders(envelope_dict, sqzd, drawcols):
    remains = {}
    for key in envelope_dict:

        allocd = sqzd[sqzd['i_%s' % key] == 1]
        allocd = allocd.groupby('age_group_id').sum().reset_index()
        remain = envelope_dict[key].merge(
            allocd,
            on='age_group_id',
            suffixes=('.env', '.alloc'))
        decols = ['draw_%s.env' % d for d in range(1000)]
        dacols = ['draw_%s.alloc' % d for d in range(1000)]
        toalloc = remain[decols].values - remain[dacols].values
        toalloc = toalloc.clip(min=0)
        remain = remain.join(pd.DataFrame(
            data=toalloc, index=remain.index, columns=drawcols))
        remain = remain[['age_group_id'] + drawcols]
        remains[key] = remain.copy()
    return remains


def run_squeeze(location_id, year_id, sex_id):
    ###################################
    # Prepare variables
    ###################################
    drawcols = ['draw_' + str(i) for i in range(1000)]
    MAP_FILE = pkg_resources.resource_filename('super_squeeze',
                                               'data/map_pre_pos_mes.csv')
    SOURCE_TARGET_FILE = pkg_resources.resource_filename('super_squeeze',
                                                         'data/source_target_maps.csv')

    ###################################
    # Prepare envelopes
    ###################################
    sequelae_map = pd.read_csv(SOURCE_TARGET_FILE)
    envelope_dict = create_env(location_id, year_id, sex_id)

    ###################################
    # Prepare unsqueezed prevalence
    ###################################
    # Load map of sequelae and their targets
    unsqueezed = get_unsqueezed(sequelae_map, drawcols, location_id, year_id,
                                sex_id)
    unsqueezed.ix[:, drawcols] = unsqueezed.ix[:, drawcols].clip(lower=0)

    ###################################
    # SQUEEZE
    ###################################
    # Parallelize the squeezing
    pool = Pool(20)
    ages = list(pd.unique(unsqueezed['age_group_id']))
    partial_squeeze = partial(
        sf.squeeze_age_group,
        unsqueezed=unsqueezed,
        env_dict=envelope_dict)
    squeezed = pool.map(partial_squeeze, ages, chunksize=1)
    pool.close()
    pool.join()
    errors = [e for e in squeezed if isinstance(e, tuple)]
    squeezed = pd.concat(squeezed)
    squeezed = squeezed.groupby(['location_id', 'year_id', 'age_group_id',
                                 'sex_id', 'me_id']).sum()
    squeezed = squeezed.reset_index()

    ##################################
    # Write to files
    ##################################
    write_squeezed(squeezed, location_id, year_id, sex_id, MAP_FILE, drawcols)

    ##################################
    # Allocate residuals
    ##################################
    resids = allocate_residuals(unsqueezed, squeezed, location_id, year_id,
                                sex_id, MAP_FILE, drawcols)

    ###########################################
    # Determine the remainder of the envelopes
    ###########################################
    remains = calc_env_remainders(envelope_dict, squeezed, drawcols)

    remain_map = {'id_bord': 2000,
                  'id_mild': 1999,
                  'id_mod': 2001,
                  'id_sev': 2002,
                  'id_prof': 2003}
    for key, meid in remain_map.iteritems():
        print('Writing remainder %s to file' % meid)
        drawsdir = "/PATH"
        fn = "%s/%s_%s_%s.h5" % (drawsdir, location_id, year_id, sex_id)
        try:
            meid = int(meid)
        except:
            pass
        if not os.path.exists(drawsdir):
            os.makedirs(drawsdir)
        df = remains[key]
        df['location_id'] = int(float(location_id))
        df['year_id'] = int(float(year_id))
        df['sex_id'] = int(float(sex_id))
        df['measure_id'] = 5
        df['age_group_id'] = df.age_group_id.astype(float).astype(int)
        datacols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id']
        df[datacols + drawcols].to_hdf(
            fn,
            'draws',
            mode='w',
            format='table',
            data_columns=datacols)

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--location_id",
        help="location_id",
        dUSERt=7,
        type=int)  # DUSERt is North Korea
    parser.add_argument(
        "--year_id",
        help="year",
        dUSERt=2010,
        type=int)
    parser.add_argument(
        "--sex_id",
        help="sex",
        dUSERt=1,
        type=int)  # DUSERt is male
    args = parser.parse_args()
    location_id = args.location_id
    year_id = args.year_id
    sex_id = args.sex_id

    run_squeeze(location_id, year_id, sex_id)

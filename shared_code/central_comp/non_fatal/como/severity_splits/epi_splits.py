from hierarchies import dbtrees
from transmogrifier import gopher, maths
import pandas as pd
import numpy as np
import os
from itertools import cycle
from adding_machine.db import EpiDB
from multiprocessing import Queue, Process
from db_queries.core import location, demographics
from db_tools.ezfuncs import query


sentinel = None
year_map = {2015: 3,
            2016: 4}


class SevSplitter(object):

    def __init__(self, parent_meid,
                 prop_drawfile=None,
                 gbd_round=2016):
        self.parent_meid = parent_meid
        self.gbd_round = gbd_round
        self.lt = dbtrees.loctree(
            None, location_set_id=35, gbd_round=gbd_round)
        self.ags = self.age_groups()
        self.child_meids = self.get_children()
        if prop_drawfile is None:
            self.props = self.get_split_proportions()
            self.props = self.gen_proportion_draws()
        else:
            self.props = pd.read_csv(prop_drawfile)

    def get_children(self):
        q = """
            SELECT DISTINCT parent_meid, child_meid
            FROM severity_splits.split_proportion
            JOIN severity_splits.split_version sv
            USING(split_version_id)
            WHERE sv.is_best=1 AND parent_meid={meid}
            AND sv.gbd_round_id={gbd}
            """.format(meid=self.parent_meid, gbd=year_map[self.gbd_round])
        return query(q, conn_def='sev_splits')

    def get_split_proportions(self):
        q = """
            SELECT split_version_id, parent_meid,
                child_meid, draw_generation_seed, location_id, measure_id,
                year_start, year_end, age_start, age_end, sex_id, mean,
                lower, upper
            FROM severity_splits.split_proportion sp
            JOIN severity_splits.split_version sv
                USING(split_version_id)
            WHERE sv.is_best=1
            AND sp.parent_meid={meid}
            AND gbd_round_id={gbd}
            """.format(meid=self.parent_meid, gbd=year_map[self.gbd_round])
        return query(q, conn_def='sev_splits')

    def gen_proportion_draws(self):
        return self.draw_beta()

    def draw_beta(self):
        seed = self.props.draw_generation_seed.unique()[0]
        np.random.seed(seed)
        sd = (self.props['upper'] - self.props['lower']) / (2 * 1.96)
        sample_size = self.props['mean'] * (1 - self.props['mean']) / sd**2
        alpha = self.props['mean'] * sample_size
        alpha = alpha.replace({0: np.nan})
        beta = (1 - self.props['mean']) * sample_size
        beta = beta.replace({0: np.nan})
        draws = np.random.beta(alpha, beta, size=(1000, len(alpha)))
        draws = pd.DataFrame(
            draws.T,
            index=self.props.index,
            columns=['draw_%s' % i for i in range(1000)])
        draws = draws.fillna({
            'draw_%s' % i: self.props['mean'] for i in range(1000)})
        return self.props.join(draws)

    def draw_normal():
        pass

    def age_groups(self):
        q = """
            SELECT age_group_id, age_group_years_start, age_group_years_end
            FROM shared.age_group
            JOIN shared.age_group_set_list USING(age_group_id)
            WHERE age_group_set_id=12"""
        return query(q, conn_def='epi')

    def gbdize_proportions(self, location_id):
        valid_locids = (
            [location_id] +
            [node.id for node in self.lt.get_node_by_id(
                location_id).ancestors()])
        gbdprops = []
        for m in [5, 6]:
            # select one measure at a time
            ms_props = self.props.query('measure_id == {}'.format(m))
            for lid in valid_locids:
                lprops = ms_props.query('location_id == {}'.format(lid))
                if len(lprops) > 0:
                    break

            # Expand sexes
            ss_props = lprops.query('sex_id != 3')
            bs_props = lprops.query('sex_id == 3')
            ss_props = ss_props.append(bs_props.replace({'sex_id': {3: 1}}))
            ss_props = ss_props.append(bs_props.replace({'sex_id': {3: 2}}))
            lprops = ss_props
            # Expand age_groups and years

            year_set = demographics.Year("epi", year_map[self.gbd_round]).ids
            for y in year_set:
                for _, ag in self.ags.iterrows():
                    for s in [1, 2]:
                        quer = ("(age_start <= {ast}) & "
                                "(age_end >= {ae}) & "
                                "(year_start <= {ys}) & "
                                "(year_end >= {ye}) & "
                                "(sex_id == {s})".format(
                                    ast=ag['age_group_years_start'],
                                    ae=min(100, ag['age_group_years_end']),
                                    ys=y,
                                    ye=y,
                                    s=s))
                        ya_props = lprops.query(quer)
                        assert len(ya_props) == len(self.child_meids), """
                            Proportions must be unique to a location_id,
                            year_id, age_group_id, measure_id,
                            sex combination"""
                        ya_props = ya_props.assign(
                            age_group_id=ag['age_group_id'])
                        ya_props = ya_props.assign(year_id=y)
                        ya_props = ya_props.assign(location_id=location_id)
                        ya_props = ya_props.assign(measure_id=m)
                        gbdprops.append(ya_props)
        gbdprops = pd.concat(gbdprops)
        return gbdprops.reset_index(drop=True)


def split_location(location_id, gbd_round):
    draws = gopher.draws(
        {'modelable_entity_ids': [ss.parent_meid]},
        source='dismod',
        location_ids=location_id,
        measure_ids=[5, 6],
        gbd_round_id=year_map[gbd_round])
    draws['measure_id'] = draws.measure_id.astype(int)
    gprops = ss.gbdize_proportions(location_id)
    gprops = gprops[gprops.measure_id.isin(draws.measure_id.unique())]
    gprops = gprops[gprops.age_group_id.isin(draws.age_group_id.unique())]
    gprops = gprops[gprops.sex_id.isin(draws.sex_id.unique())]
    dcs = ['draw_%s' % i for i in range(1000)]
    splits = maths.merge_split(
        draws,
        gprops,
        group_cols=['location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id'],
        value_cols=dcs)
    splits = splits.assign(modelable_entity_id=splits['child_meid'])
    return splits


def split_locationq(inqueue, oq_meta, gbd_round):
    for location_id in iter(inqueue.get, sentinel):
        try:
            print 'Splitting %s' % location_id
            splits = split_location(location_id, gbd_round)
            for child_meid in splits.child_meid.unique():
                oq = [v['outqueue'] for k, v in oq_meta.iteritems()
                      if child_meid in v['child_meids']][0]
                oq.put(splits[splits.child_meid == child_meid])
        except Exception as e:
            print 'Error splitting %s: %s' % (location_id, e)
            for oqid, oq in oq_meta.iteritems():
                for meid in oq['child_meids']:
                    oq['outqueue'].put(
                        'Something wrong in %s %s' % (
                            meid, location_id))
    print 'Got sentinel. Exiting.'


def write_me(nwait, nmeids, outqueue, mvid_q, gbd_round):
    if envi == 'dev':
        db = EpiDB('epi-save-results-dev')
    elif envi == 'prod':
        db = EpiDB('epi-save-results')
    get_count = 0
    me_mv = {}
    loc_set_version = location.active_location_set_version(
        35, year_map[gbd_round])
    while get_count < nwait:
        try:
            df = outqueue.get()
            meid = df.modelable_entity_id.unique()[0]
            if meid not in me_mv:
                print 'Creating mvid for meid %s' % (meid)
                mvid = db.create_model_version(
                    meid,
                    'Central severity split: parent %s' % ss.parent_meid,
                    loc_set_version, gbd_round=gbd_round)
                print 'Created mvid %s for meid %s' % (mvid, meid)
                me_mv[meid] = mvid
            else:
                mvid = me_mv[meid]
            outdir = 'FILEPATH'.format(
                envi=envi, mvid=mvid)
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            outfile = (
                outdir + '/FILEPATH.h5')
            for col in [
                    'location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id']:
                df[col] = df[col].astype(int)
            df = df[[
                'location_id', 'year_id', 'age_group_id', 'sex_id',
                'measure_id'] + ['draw_%s' % i for i in range(1000)]]
            store = pd.HDFStore(outfile)
            store.append(
                'draws',
                df,
                data_columns=[
                    'measure_id', 'location_id', 'year_id',
                    'age_group_id', 'sex_id'],
                index=False)
            store.close()
            get_count += 1
        except Exception as e:
            print str(e)
            get_count += 1

    try:
        for meid, mvid in me_mv.iteritems():
            outdir = 'FILEPATH'.format(
                envi=envi, mvid=mvid)
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            outfile = outdir + '/FILEPATH.h5'
            store = pd.HDFStore(outfile)
            print 'Creating index for mv %s for meid %s' % (mvid, meid)
            store.create_table_index(
                'draws',
                columns=[
                    'measure_id', 'location_id', 'year_id',
                    'age_group_id', 'sex_id'],
                optlevel=9,
                kind='full')
            store.close()
            print 'Closing file for mv %s for meid %s' % (mvid, meid)
            mvid_q.put(mvid)
            nmeids = nmeids - 1
    except Exception as e:
        print 'Hit a writing error %s' % e
        for i in range(nmeids):
            mvid_q.put((500, str(e)))


def split_me(parent_meid, env='dev', prop_drawfile=None, gbd_round=2016):
    global ss, envi
    envi = env

    # Create instance of SevSplitter object for given parent_meid
    ss = SevSplitter(parent_meid,
                     prop_drawfile=prop_drawfile,
                     gbd_round=gbd_round)

    # location list from SS object
    locs = [l.id for l in ss.lt.leaves()]
    nlocs = len(locs)

    # Create 2 queue objects
    inqueue = Queue()
    mvid_queue = Queue()

    # writers are minimum between 4 or the number of child_meids
    num_writers = min(4, len(ss.child_meids.child_meid))

    # a nested dictionary linking writer number to child_meids to the outque
    oq_meta = {i: {'child_meids': [], 'outqueue': None}
               for i in range(num_writers)}

    # Returns a number from 1 - 4 indefinitely, in order.
    # Cycles through writers
    wo_pool = cycle(range(num_writers))
    for child_meid in ss.child_meids.child_meid:
        oq_meta[wo_pool.next()]['child_meids'].append(child_meid)

    # print git_info
    for n in range(num_writers):
        outqueue = Queue()
        nmeids = len(oq_meta[n]['child_meids'])
        writer = Process(
            target=write_me,
            args=(
                nlocs * nmeids, nmeids,
                outqueue, mvid_queue, gbd_round))
        writer.start()
        oq_meta[n]['outqueue'] = outqueue

    num_processes = 20
    split_jobs = []
    for i in range(num_processes):
        p = Process(target=split_locationq, args=(inqueue, oq_meta, gbd_round))
        split_jobs.append(p)
        p.start()
    for l in locs:
        inqueue.put(l)
    for l in range(num_processes):
        inqueue.put(sentinel)
    mvids = []
    while len(mvids) < len(ss.child_meids.child_meid):
        mvid = mvid_queue.get()
        print mvid
        mvids.append(mvid)
        print len(mvids), len(ss.child_meids.child_meid)
    return mvids


def write_split_props(
        meid_list, gbd_round=2016, out_dir='FILEPATH'):

    for meid in meid_list:
        ss = SevSplitter(meid, gbd_round=gbd_round)
        draws_dir = out_dir + '/{meid}/'.format(meid=meid)
        if not os.path.exists(draws_dir):
            os.makedirs(draws_dir)
        outfile = draws_dir + 'FILEPATH.h5'.format(meid=meid)
        ss.props.to_hdf(outfile, 'draws')

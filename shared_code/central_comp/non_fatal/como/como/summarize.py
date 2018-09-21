import os
import errno
from multiprocessing import Pool

import numpy as np

from transmogrifier import super_gopher
from adding_machine import summarizers
from adding_machine.summarizers import Globals


def summ_lvl_meas(args):
    drawdir, outdir, location_id, measure_id, year_ids = args
    try:
        os.makedirs(outdir)
        os.chmod(outdir, 0o775)
        os.chmod(os.path.join(outdir, '..'), 0o775)
        os.chmod(os.path.join(outdir, '..', '..'), 0o775)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    try:
        sg = super_gopher.SuperGopher({
            'file_pattern': '{location_id}/{measure_id}_{year_id}_{sex_id}.h5',
            'h5_tablename': 'draws'},
            drawdir)
        print('Combining summaries {} {}...'.format(drawdir, measure_id))
        summ, csumm = summarizers.summarize_location(
            location_id,
            drawdir,
            sg,
            years=year_ids,
            change_intervals=None,
            combine_sexes=True,
            force_age=False,
            calc_counts=True,
            draw_filters={'measure_id': measure_id},
            gbd_compare_ags=True)
        if 'cause' in drawdir:
            summ = summ[[
                'location_id', 'year_id', 'age_group_id', 'sex_id',
                'measure_id', 'metric_id', 'cause_id', 'mean', 'lower',
                'upper']]
            summ = summ.sort_values([
                'measure_id', 'year_id', 'location_id', 'sex_id',
                'age_group_id', 'cause_id', 'metric_id'])
        elif 'sequela' in drawdir:
            summ = summ[[
                'location_id', 'year_id', 'age_group_id', 'sex_id',
                'measure_id', 'metric_id', 'sequela_id', 'mean', 'lower',
                'upper']]
            summ = summ.sort_values([
                'measure_id', 'year_id', 'location_id', 'sex_id',
                'age_group_id', 'sequela_id', 'metric_id'])
        elif 'impairment' in drawdir or 'injuries' in drawdir:
            summ = summ[[
                'location_id', 'year_id', 'age_group_id', 'sex_id',
                'measure_id', 'metric_id', 'rei_id', 'cause_id', 'mean',
                'lower', 'upper']]
            summ = summ.sort_values([
                'measure_id', 'year_id', 'location_id', 'sex_id',
                'age_group_id', 'rei_id', 'cause_id', 'metric_id'])
        summfile = "{}/{}_{}_single_year.csv".format(
            outdir, measure_id, location_id)
        print('Writing to file...')
        summ = summ[summ['mean'].notnull()]
        summ.to_csv(summfile, index=False)
        os.chmod(summfile, 0o775)
        if len(csumm) > 0:
            if 'cause' in drawdir:
                csumm = csumm[[
                    'location_id', 'year_start_id', 'year_end_id',
                    'age_group_id', 'sex_id', 'measure_id', 'cause_id',
                    'metric_id', 'median', 'lower', 'upper']]
                csumm = csumm.sort_values([
                    'measure_id', 'year_start_id', 'year_end_id',
                    'location_id', 'sex_id', 'age_group_id', 'cause_id',
                    'metric_id'])
            elif 'sequela' in drawdir:
                csumm = csumm[[
                    'location_id', 'year_start_id', 'year_end_id',
                    'age_group_id', 'sex_id', 'measure_id', 'sequela_id',
                    'metric_id', 'median', 'lower', 'upper']]
                csumm = csumm.sort_values([
                    'measure_id', 'year_start_id', 'year_end_id',
                    'location_id', 'sex_id', 'age_group_id', 'sequela_id',
                    'metric_id'])
            elif 'impairment' in drawdir or 'injuries' in drawdir:
                csumm = csumm[[
                    'location_id', 'year_start_id', 'year_end_id',
                    'age_group_id', 'sex_id', 'measure_id', 'rei_id',
                    'cause_id', 'metric_id', 'median', 'lower', 'upper']]
                csumm = csumm.sort_values([
                    'measure_id', 'year_start_id', 'year_end_id',
                    'location_id', 'sex_id', 'age_group_id', 'rei_id',
                    'cause_id', 'metric_id'])
            csummfile = "{}/{}_{}_multi_year.csv".format(
                outdir, measure_id, location_id)
            csumm = csumm[
                (csumm['median'].notnull()) & np.isfinite(csumm['median']) &
                (csumm['lower'].notnull()) & np.isfinite(csumm['lower']) &
                (csumm['upper'].notnull()) & np.isfinite(csumm['upper'])]
            csumm.to_csv(csummfile, index=False)
            os.chmod(csummfile, 0o775)
    except Exception as e:
        print(e)


def summ(cv, location_id, component):
    drawdir = os.path.join(cv.como_dir, 'draws', component)
    summdir = os.path.join(cv.como_dir, 'summaries', component)
    Globals.aw = summarizers.get_age_weights()
    Globals.pop = summarizers.get_pop(
        {'location_id': location_id, "sex_id": [1, 2, 3]})
    arglist = [(drawdir, summdir, location_id, measure_id,
                cv.dimensions.index_dim.levels.year_id)
               for measure_id in cv.dimensions.index_dim.levels.measure_id]
    pool = Pool(3)
    pool.map(summ_lvl_meas, arglist, chunksize=1)
    pool.close()
    pool.join()

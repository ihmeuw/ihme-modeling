import os
from multiprocessing import Pool

from db_tools import ezfuncs


def upload_cause_year_summaries(como_dir, process_id, location_id, measure_id):
    eng = ezfuncs.get_engine(conn_def="como-gbd")
    for tn in ['single_year', 'multi_year']:
        try:
            if tn == 'single_year':
                cols = ",".join([
                    'location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id', 'metric_id', 'cause_id', 'val', 'lower',
                    'upper'])
            elif tn == 'multi_year':
                cols = ",".join([
                    'location_id', 'year_start_id', 'year_end_id',
                    'age_group_id', 'sex_id', 'measure_id', 'cause_id',
                    'metric_id', 'val', 'lower', 'upper'])

            summdir = os.path.join(como_dir, 'summaries', "cause")
            summary_file = os.path.join(
                summdir,
                "%s_%s_%s.csv" % (measure_id, location_id, tn))
            ldstr = """
                LOAD DATA INFILE '{sf}'
                INTO TABLE gbd.output_epi_{tn}_v{pid}
                FIELDS
                    TERMINATED BY ","
                    OPTIONALLY ENCLOSED BY '"'
                LINES
                    TERMINATED BY "\\n"
                IGNORE 1 LINES
                    ({cols})""".format(
                sf=summary_file, pid=process_id, tn=tn,
                cols=cols)
            res = eng.execute(ldstr)
            print 'Uploaded %s %s %s %s' % (
                location_id, measure_id, tn)
        except Exception as e:
            print e
            res = None
    return res


def ucys(args):
    upload_cause_year_summaries(*args)


def upload_sequela_year_summaries(como_dir, process_id, location_id,
                                  measure_id):
    eng = ezfuncs.get_engine(conn_def="como-gbd")
    for tn in ['single_year', 'multi_year']:
        try:
            if tn == 'single_year':
                cols = ",".join([
                    'location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id', 'metric_id', 'sequela_id', 'val', 'lower',
                    'upper'])
            elif tn == 'multi_year':
                cols = ",".join([
                    'location_id', 'year_start_id', 'year_end_id',
                    'age_group_id', 'sex_id', 'measure_id', 'sequela_id',
                    'metric_id', 'val', 'lower', 'upper'])

            summdir = os.path.join(como_dir, 'summaries', "sequela")
            summary_file = os.path.join(
                summdir,
                "%s_%s_%s.csv" % (measure_id, location_id, tn))
            ldstr = """
                LOAD DATA INFILE '{sf}'
                INTO TABLE gbd.output_sequela_{tn}_v{pid}
                FIELDS
                    TERMINATED BY ","
                    OPTIONALLY ENCLOSED BY '"'
                LINES
                    TERMINATED BY "\\n"
                IGNORE 1 LINES
                    ({cols})""".format(
                sf=summary_file, pid=process_id, tn=tn, cols=cols)
            res = eng.execute(ldstr)
            print 'Uploaded %s %s %s %s' % (
                location_id, measure_id, tn)
        except Exception as e:
            print e
            res = None
    return res


def usys(args):
    upload_sequela_year_summaries(*args)


def upload_rei_year_summaries(como_dir, process_id, location_id, measure_id):
    eng = ezfuncs.get_engine(conn_def="como-gbd")
    for tn in ['single_year', 'multi_year']:
        try:
            if tn == 'single_year':
                cols = ",".join([
                    'location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id', 'metric_id', 'rei_id', 'cause_id', 'val',
                    'lower', 'upper'])
            elif tn == 'multi_year':
                cols = ",".join([
                    'location_id', 'year_start_id', 'year_end_id',
                    'age_group_id', 'sex_id', 'measure_id', 'rei_id',
                    'cause_id', 'metric_id', 'val', 'lower', 'upper'])

            summdir = os.path.join(como_dir, 'summaries', "impairment")
            summary_file = os.path.join(
                summdir,
                "%s_%s_%s.csv" % (measure_id, location_id, tn))
            ldstr = """
                LOAD DATA INFILE '{sf}'
                INTO TABLE gbd.output_impairment_{tn}_v{pid}
                FIELDS
                    TERMINATED BY ","
                    OPTIONALLY ENCLOSED BY '"'
                LINES
                    TERMINATED BY "\\n"
                IGNORE 1 LINES
                    ({cols})""".format(
                sf=summary_file, pid=process_id, tn=tn, cols=cols)
            res = eng.execute(ldstr)
            print 'Uploaded %s %s %s %s' % (
                location_id, measure_id, tn)
        except Exception as e:
            print e
            res = None
    return res


def urys(args):
    upload_rei_year_summaries(*args)


def upload_inj_year_summaries(como_dir, process_id, location_id, measure_id):
    eng = ezfuncs.get_engine(conn_def="como-gbd")
    for tn in ['single_year', 'multi_year']:
        try:
            if tn == 'single_year':
                cols = ",".join([
                    'location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id', 'metric_id', 'rei_id', 'cause_id', 'val',
                    'lower', 'upper'])
            elif tn == 'multi_year':
                cols = ",".join([
                    'location_id', 'year_start_id', 'year_end_id',
                    'age_group_id', 'sex_id', 'measure_id', 'rei_id',
                    'cause_id', 'metric_id', 'val', 'lower', 'upper'])

            summdir = os.path.join(como_dir, 'summaries', "injuries")
            summary_file = os.path.join(
                summdir,
                "%s_%s_%s.csv" % (measure_id, location_id, tn))
            ldstr = """
                LOAD DATA INFILE '{sf}'
                INTO TABLE gbd.output_injury_{tn}_v{pid}
                FIELDS
                    TERMINATED BY ","
                    OPTIONALLY ENCLOSED BY '"'
                LINES
                    TERMINATED BY "\\n"
                IGNORE 1 LINES
                    ({cols})""".format(
                sf=summary_file, pid=process_id, tn=tn, cols=cols)
            res = eng.execute(ldstr)
            print 'Uploaded %s %s %s %s' % (
                location_id, measure_id, tn)
        except Exception as e:
            print e
            res = None
    return res


def uiys(args):
    upload_inj_year_summaries(*args)


def upload_cause_summaries(cv, location_id):

    process_id = cv.gbd_process_version_id
    pool = Pool(9)
    args = [(cv.como_dir, process_id, l, m)
            for l in location_id
            for m in [3, 5, 6]]
    pool.map(ucys, args)
    pool.close()
    pool.join()


def upload_sequela_summaries(cv, location_id):
    process_id = cv.gbd_process_version_id
    pool = Pool(9)
    args = [(cv.como_dir, process_id, l, m)
            for l in location_id
            for m in [3, 5, 6]]
    pool.map(usys, args)
    pool.close()
    pool.join()


def upload_rei_summaries(cv, location_id):
    process_id = cv.gbd_process_version_id
    pool = Pool(9)
    args = [(cv.como_dir, process_id, l, m)
            for l in location_id
            for m in [3, 5, 6]]
    pool.map(urys, args)
    pool.close()
    pool.join()


def upload_inj_summaries(cv, location_id):
    process_id = cv.gbd_process_version_id
    pool = Pool(9)
    args = [(cv.como_dir, process_id, l, m)
            for l in location_id
            for m in [3]]
    pool.map(uiys, args)
    pool.close()
    pool.join()

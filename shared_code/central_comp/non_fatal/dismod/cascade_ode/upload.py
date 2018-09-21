from glob import glob
import sqlalchemy
import pandas as pd
import os
import json
import logging

# Set dUSERt file mask to readable-for all users
os.umask(0o0002)

# Path to this file
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
if os.path.isfile(os.path.join(this_path, "../config.local")):
    settings = json.load(open(os.path.join(this_path, "../config.local")))
else:
    settings = json.load(open(os.path.join(this_path, "../config.dUSERt")))

def upload_file(mvid, filepath, table, cols):
    logging.info('Uploading %s to mvid %s' % (filepath, mvid))
    load_data_query = """
        LOAD DATA INFILE '{fp}'
        INTO TABLE {table}
        FIELDS
            TERMINATED BY ","
            OPTIONALLY ENCLOSED BY '"'
        LINES
            TERMINATED BY "\\n"
        IGNORE 1 LINES
        ({cols})
        SET model_version_id={mvid}""".format(
            fp=filepath, table=table, cols=",".join(cols), mvid=mvid)
    eng = sqlalchemy.create_engine(settings['epi_conn_str'])
    conn = eng.connect()
    res = conn.execute(load_data_query)
    conn.close()
    return res

def set_commit_hash(mvid, ch):
    update_stmt = """
        UPDATE epi.model_version
        SET code_version='{ch}'
        WHERE model_version_id={mvid}""".format(
            ch=ch, mvid=mvid)
    eng = sqlalchemy.create_engine(settings['epi_conn_str'])
    conn = eng.connect()
    res = conn.execute(update_stmt)
    conn.close()
    return res

def copy_model_version(mvid, description=""):
    eng = sqlalchemy.create_engine(settings['epi_conn_str'])
    mvdf = pd.read_sql("""
        SELECT modelable_entity_id, description, code_version,
            location_set_version_id, model_version_status_id, drill,
            drill_location, drill_year, drill_sex, add_csmr_cause,
            csmr_cod_output_version_id, csmr_mortality_output_version_id,
            birth_prev, measure_only, age_mesh, timespan, data_likelihood,
            prior_likelihood, sample_interval, num_sample, random_seed,
            integrate_step, integrate_method, fix_cov, fix_sex, fix_year,
            keep_re, cv_global, cv_super, cv_region, external,
            cascade_version_id, cross_validate_id
        FROM epi.model_version
        WHERE model_version_id=%s""" % mvid, eng)
    desc = 'Pandas copy of %s (%s)' % (mvid, description)
    mvdf['description'] = desc
    mvdf.to_sql('model_version', eng, if_exists='append', index=False)
    new_mvid = pd.read_sql("""
        SELECT model_version_id FROM epi.model_version
        WHERE description='%s'""" % desc, eng).values[0][0]
    return new_mvid

def copy_model_parameters(mvid, new_mvid):
    assert mvid!=new_mvid, "New and old model versions can't be the same"
    eng = sqlalchemy.create_engine(settings['epi_conn_str'])
    mpdf = pd.read_sql("""
        SELECT * FROM epi.model_parameter
        WHERE model_version_id=%s""" % mvid, eng)
    mpdf['model_version_id'] = new_mvid
    mpdf.drop('model_parameter_id', axis=1, inplace=True)
    mpdf.to_sql('model_parameter', eng, if_exists='append', index=False)
    return mpdf

def upload_final(mvid):
    searchdir = '%s/%s/full/summaries' % (settings['cascade_ode_out_dir'], mvid)
    summ_files = glob('%s/*.csv' % searchdir)

    for i, sf in enumerate(summ_files):
        logging.info('Final file #:%s' % i)
        upload_file(mvid, sf,
            'epi.model_estimate_final',
            [   'location_id','year_id','age_group_id','sex_id','measure_id',
                'mean','@dummy','lower','upper'])


def upload_model(mvid):
    import os

    matches = []
    prior_matches = []
    searchdir = '%s/%s/full/locations' % (settings['cascade_ode_out_dir'], mvid)
    locdirs = glob('%s/*' % searchdir)
    for ld in locdirs:
        for sex in ['male', 'female', 'both']:
            for year in [1990, 1995, 2000, 2005, 2010, 2016]:
                outfile = '%s/outputs/%s/%s/model_estimate_fit.csv' % (ld, sex, year)
                if os.path.isfile(outfile):
                    matches.append(outfile)
                priorfile = '%s/inputs/%s/%s/model_prior.csv' % (ld, sex, year)
                if os.path.isfile(priorfile):
                    prior_matches.append(priorfile)

    for i, m in enumerate(matches):
        logging.info('Fit file #:%s' % i)
        try:
            upload_file(mvid,
                        m,
                        'epi.model_estimate_fit',
                        ['location_id', 'year_id', 'sex_id', 'age_group_id',
                         'measure_id', 'mean', 'lower', 'upper'])
        except Exception as e:
            logging.exception(e)

    for i, m in enumerate(prior_matches):
        logging.info('Prior file #:%s' % i)
        try:
            upload_file(mvid,
                        m,
                        'epi.model_prior',
                        ['year_id', 'location_id', 'sex_id', 'age_group_id',
                         'age', 'measure_id', 'mean', 'lower', 'upper'])
        except Exception as e:
            logging.exception(e)

    try:
        logging.info('Uploading adjusted data')
        adj_data_file = ('%s/%s/full/locations/1/outputs/both/2000/'
                         'model_data_adj.csv' % (
                             settings['cascade_ode_out_dir'], mvid))
        upload_file(mvid, adj_data_file, 'epi.model_data_adj',
                    ['model_version_dismod_id', 'sex_id', 'year_id', 'mean',
                     'lower', 'upper'])
    except Exception as e:
        logging.exception(e)

    try:
        logging.info('Uploading effects')
        me_file = ('%s/%s/full/locations/1/outputs/both/2000/'
                   'model_effect.csv' % (
                       settings['cascade_ode_out_dir'], mvid))
        upload_file(mvid, me_file, 'epi.model_effect',
                    ['measure_id', 'parameter_type_id', 'cascade_level_id',
                     'study_covariate_id', 'country_covariate_id',
                     'asdr_cause', 'location_id', 'mean_effect',
                     'lower_effect', 'upper_effect'])
    except Exception as e:
        logging.exception(e)


def upload_fit_stat(mvid):
    mvfs_file = '%s/%s/full/model_version_fit_stat.csv' % (
            settings['cascade_ode_out_dir'], mvid)

    upload_file(mvid, mvfs_file,
        'epi.model_version_fit_stat',
        ['measure_id', 'fit_stat_id', 'fit_stat_value'])

def update_model_status(mvid, status):
    eng = sqlalchemy.create_engine(settings['epi_conn_str'])
    conn = eng.connect()
    conn.execute("""
        UPDATE epi.model_version SET model_version_status_id=%s
        WHERE model_version_id=%s """ % (status, mvid))
    conn.close()

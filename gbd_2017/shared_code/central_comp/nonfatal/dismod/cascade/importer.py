import logging
import math
import numpy as np
import os
import pandas as pd

from cascade_ode import upload
from cascade_ode import db
from cascade_ode import sanitize
from cascade_ode.demographics import Demographics
from db_queries.get_envelope import get_envelope
from db_queries.get_population import get_population
from db_queries.get_covariate_estimates import get_covariate_estimates
from hierarchies.dbtrees import loctree
from gbd.gbd_round import gbd_round_from_gbd_round_id
from adding_machine import summarizers

from settings import load as load_settings

# Set default file mask to readable-for all users
os.umask(0o0002)

# Get this filepath
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
settings = load_settings()

# mapping from parameter_type_id to a short name for the prior_type
inf = float('inf')

likelihood_map = {
    1: 'gaussian',
    2: 'laplace',
    3: 'log_gaussian',
    4: 'log_laplace',
    5: 'log_gaussian'}

integrand_rate_map = {
    'incidence': 'iota',
    'remission': 'rho',
    'mtexcess': 'chi',
    'mtother': 'omega'}

integrand_pred = [
    'incidence',
    'remission',
    'prevalence',
    'mtexcess',
    'mtall',
    'relrisk',
    'mtstandard',
    'mtwith',
    'mtspecific',
    'mtother'
]

# ----------------------------------------------------------------------------


def gen_locmap(location_set_version_id):
    '''Given a location set version id, return a dataframe denomormalized
    on superregion, region, subregion (ie national), and most detailed
    location (ie atom)
    '''

    demo = Demographics()
    lt = loctree(location_set_version_id=location_set_version_id,
                 gbd_round_id=demo.gbd_round_id)
    lmap = lt.flatten()
    lmap.drop('level_0', axis=1, inplace=True)
    lmap.rename(columns={
        'level_1': 'super',
        'level_2': 'region',
        'level_3': 'subreg',
        'level_4': 'atom',
        'leaf_node': 'location_id'}, inplace=True)
    lmap = lmap.append(lmap.ix[lmap.atom.notnull(), [
                       'super', 'region', 'subreg']].drop_duplicates())
    lmap.ix[lmap.location_id.isnull(
    ), 'location_id'] = lmap.ix[lmap.location_id.isnull(), 'subreg']
    lmap = lmap.append(lmap[['super', 'region']].drop_duplicates())
    lmap.ix[lmap.location_id.isnull(
    ), 'location_id'] = lmap.ix[lmap.location_id.isnull(), 'region']
    lmap = lmap.append(lmap[['super']].drop_duplicates())
    lmap.ix[lmap.location_id.isnull(
    ), 'location_id'] = lmap.ix[lmap.location_id.isnull(), 'super']

    for c in ['atom', 'subreg', 'region', 'super']:
        lmap[c] = lmap[c].apply(lambda x: "{:.0f}".format(x))
        lmap.ix[lmap[c] == 'nan', c] = 'none'

    return lmap


def get_model_version(mvid):
    '''Given a model version id, return a dataframe of settings/metadata.
    Uses ENVIRONMENT_NAME environment variable to determin prod/dev db.
    '''
    query = """
        SELECT * FROM epi.model_version
        JOIN epi.modelable_entity USING(modelable_entity_id)
        JOIN epi.integration_type it ON integrate_method=it.integration_type_id
        LEFT JOIN (SELECT measure_id, measure as integrand_only
            FROM shared.measure) m ON model_version.measure_only=m.measure_id
        WHERE model_version_id=%s """ % (mvid)
    df = execute_select(query)
    return df


def execute_select(query, conn_def='epi'):
    '''Run a sql query, return a dataframe. Uses ENVIRONMENT_NAME environment
    variable to determine prod/dev database server.

    Args:
        query(str): sql statement to execute
        conn_def(str, 'epi'): one of 'epi' or 'cod'

    Returns:
        Pandas Dataframe
    '''
    engine = db.get_engine(conn_def=conn_def,
                           env=settings['env_variables']['ENVIRONMENT_NAME'])
    conn = engine.connect()
    df = pd.read_sql(query, conn.connection)
    conn.close()
    return df


class Importer(object):

    def __init__(self, model_version_id,
                 root_dir=os.path.expanduser(settings['cascade_ode_out_dir'])):
        '''
        The Importer reads input data from the epi database, dismod settings
        from the epi database, and COD CSMR data from the mortality database.

        It does some transformation and validation on this data and writes
        results to various csvs for the particular model version given. It will
        write directories that don't exist.

        Uses ENVIRONMENT_NAME environment variable to determine prod/dev epi
        database server. 

        Args:
            model_version_id(int): model version id
            root_dir(str): Folder path up to where model_version_id folder
                exists (will make if doesn't exist)
        '''

        log = logging.getLogger(__name__)
        log.info("Starting importer")
        self.root_dir = os.path.join(root_dir, str(model_version_id))
        try:
            os.makedirs(self.root_dir)
        except Exception:
            log.info("Root dir {} already exists".format(self.root_dir))

        try:
            os.chmod(self.root_dir, 0o775)
        except Exception:
            log.exception(
                "Could not chmod root dir {}".format(self.root_dir))

        self.mvid = model_version_id
        self.model_params = self.get_model_parameters()
        self.model_version_meta = get_model_version(self.mvid)
        self.covariate_data = pd.DataFrame()
        self.ages = self.get_age_ranges()
        if self.model_version_meta.integrand_only.isnull().any():
            self.single_param = None
            self.integrand_pred = [
                'incidence',
                'remission',
                'prevalence',
                'mtexcess',
                'mtall',
                'relrisk',
                'mtstandard',
                'mtwith',
                'mtspecific',
                'mtother'
            ]

        else:
            self.single_param = (
                self.model_version_meta.integrand_only.values[0])
            self.model_params['measure'] = self.model_params.measure.replace({
                self.single_param: 'mtother'})
            self.integrand_pred = ['mtother']

        self.lmap = gen_locmap(
            self.model_version_meta.location_set_version_id.values[0])
        self.age_mesh = self.get_age_mesh()
        log.info("Reading input data")
        self.data = self.get_t3_input_data()
        log.info("Done reading input data")

    @staticmethod
    def execute_proc(proc, args, conn_def='epi'):
        ''' Execute a stored prodcedure.'''
        engine = db.get_engine(
            conn_def=conn_def,
            env=settings['env_variables']['ENVIRONMENT_NAME'])
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            cursor.callproc(proc, args)
            cursor.close()
            conn.commit()
        finally:
            conn.close()

    def recalc_SE(self, meas_value, standard_error, sample_size, lower_ci,
                  upper_ci):
        '''
        Applied to every row of input data. Will recalculate standard
        error if SE not provided.

        Returns:
            Float

        Raises:
            ValueError if not enogh information to calculate standard error
        '''
        if standard_error > 0.0:
            return standard_error

        if sample_size > 0.0:
            counts = meas_value * sample_size
            std_5 = math.sqrt((5.0 / sample_size) / sample_size)
            if counts >= 5.0:
                return math.sqrt(meas_value / sample_size)
            std_0 = 1.0 / sample_size
            return ((5.0 - counts) * std_0 + counts * std_5) / 5.0

        if upper_ci is not None and lower_ci is not None:
            return (upper_ci - lower_ci) / (2. * 1.96)

        raise ValueError(
            "Insufficient information to calculate standard error")

    def attach_study_covariates(self, df):
        '''
        Takes a dataframe with a column called "study_covariates" that
        is a space separated list of covariate ids. Will read matching
        covariates from covariates database and add a new column per covariate
        (ie denormalize). It drops original "study_covariates" column.

        New study covariate columns are prefixed with 'x_s_', then short
        str identifier named study_covariate

        Returns:
            Pandas dataframe
        '''
        log = logging.getLogger(__name__)
        dmdf = df.copy()
        sclists = df.study_covariates.str.strip()
        sclists = sclists.apply(lambda x: str(x).split(" "))

        scovs = reduce(lambda x, y: x.union(set(y)), sclists, set())
        try:
            scovs.remove('None')
        except Exception as e:
            log.exception(e)
        try:
            scovs.remove('')
        except Exception as e:
            log.exception(e)
        scovs = list(scovs)
        scov_str = ",".join(scovs)

        # Retrieve study covariate names and ids
        query = """
            SELECT study_covariate_id, study_covariate
            FROM epi.study_covariate
            WHERE study_covariate_id IN (%s) """ % (scov_str)

        if len(scovs) > 0:
            scov_names = execute_select(query)

            for i, row in scov_names.iterrows():
                v_zeros_ones = (
                    [1 if str(row["study_covariate_id"]) in r else 0 for
                     r in sclists])
                dmdf['x_s_' + row["study_covariate"]] = v_zeros_ones

        dmdf.drop('study_covariates', axis=1, inplace=True)
        return dmdf

    def promote_dm_t2_to_t3(self):
        '''
        Running a dismod model takes a snapshot of the input data, stored in
        epi database tier 2 table, and moves it to tier 3 table.

        Raises:
            ValueError if model has already been promoted
        '''
        t3_dm_rows = execute_select("""
            SELECT * FROM epi.t3_model_version_dismod
            WHERE model_version_id={}
            LIMIT 1""".format(self.mvid))
        if len(t3_dm_rows) > 0:
            raise ValueError(
                "data for model_version {} has already been promoted "
                "to t3".format(self.mvid))
        t3_sc_rows = execute_select(
            """SELECT * FROM epi.t3_model_version_study_covariate
            WHERE model_version_id={}
            LIMIT 1""".format(self.mvid))
        if len(t3_sc_rows) > 0:
            raise ValueError(
                "study covs for model_version {} has already been promoted "
                "to t3".format(self.mvid))

        # Load t2 to t3
        Importer.execute_proc("load_t3_model_version_dismod", [self.mvid])
        Importer.execute_proc("load_t3_model_version_study_covariate",
                              [self.mvid])

    def get_age_ranges(self):
        '''Get a dataframe mapping age_group_id to age_group_years_start and
        years_end'''
        aq = """
                SELECT age_group_id, age_group_years_start, age_group_years_end
                FROM shared.age_group"""
        return execute_select(aq, 'cod')

    def promote_asdr_t2_to_t3(self):
        '''
        Take a snapshot of age specific death rate from cod database,
        write in root of model version directory, and infile to
        epi.t3_model_version_asdr.

        Returns:
            pandas dataframe

        Raises:
            ValueError if asdr has already been promoted
        '''
        # Check whether this model has already been loaded, and if so
        # throw an error
        t3_csmr_rows = execute_select("""
            SELECT * FROM epi.t3_model_version_asdr
            WHERE model_version_id={}
            LIMIT 1""".format(self.mvid))
        if len(t3_csmr_rows) > 0:
            raise ValueError(
                "asdr for model_version {} has already been promoted "
                "to t3".format(self.mvid))

        # Download from T2 to file (can't cross-insert from different DB
        # servers)

        if self.single_param is None:
            demo = Demographics()
            mo_vid = self.model_version_meta.csmr_mortality_output_version_id
            mo_vid = mo_vid.values[0]
            asdr = get_envelope(location_id=-1,
                                location_set_id=demo.LOCATION_SET_ID,
                                age_group_id=demo.mortality_age_grid,
                                year_id=demo.mortality_years,
                                sex_id=[1, 2],
                                with_hiv=1,
                                gbd_round_id=demo.gbd_round_id)
            if asdr.empty:
                raise RuntimeError(
                    "all cause mortality envelope empty")
            pop = get_population(location_id=-1,
                                 location_set_id=demo.LOCATION_SET_ID,
                                 age_group_id=demo.mortality_age_grid,
                                 year_id=demo.mortality_years,
                                 sex_id=[1, 2],
                                 gbd_round_id=demo.gbd_round_id)

            asdr = asdr.merge(
                pop, on=['location_id', 'year_id', 'age_group_id', 'sex_id'])
            asdr['meas_value'] = asdr['mean'] / asdr['population']
            asdr['meas_lower'] = asdr['lower'] / asdr['population']
            asdr['meas_upper'] = asdr['upper'] / asdr['population']
            asdr = asdr.merge(self.ages, on='age_group_id')
            asdr.rename(columns={'year_id': 'time_lower',
                                 'sex_id': 'x_sex',
                                 'age_group_years_start': 'age_lower',
                                 'age_group_years_end': 'age_upper'},
                        inplace=True)
            asdr['age_upper'] = asdr.age_upper.clip(upper=100)

            df = asdr[['location_id', 'time_lower', 'age_group_id', 'x_sex',
                       'age_lower', 'age_upper', 'meas_value', 'meas_lower',
                       'meas_upper']]

            df = df[df.meas_value.notnull()]

            # Write to disk and upload to T3 table
            asdr_file = "{rd}/asdr.csv".format(rd=self.root_dir)
            df.to_csv(asdr_file, index=False)

            upload.upload_file(self.mvid, asdr_file, 't3_model_version_asdr',
                               ['location_id', 'year_id', 'age_group_id',
                                'sex_id', '@dummy', '@dummy', 'mean', 'lower',
                                'upper'])
        else:
            df = pd.DataFrame()
        return df

    def promote_csmr_t2_to_t3(self):
        '''
        If this model_version metadata says to add csmr data,
        take a snapshot of cause specific mortality rate from cod database,
        write in root of model version directory, and infile to
        epi.t3_model_version_csmr.

        Returns:
            pandas dataframe

        Raises:
            ValueError if csmr has already been promoted
        '''
        # Check whether this model has already been loaded, and if so
        # throw an error
        if self.model_version_meta.add_csmr_cause.isnull().squeeze():
            return pd.DataFrame()
        else:
            mpm = self.model_params
            exclude_params = mpm.ix[mpm.parameter_type_id == 17, 'measure']
            if 'mtspecific' in list(exclude_params):
                return pd.DataFrame()
            else:
                t3_csmr_rows = execute_select("""
                    SELECT * FROM epi.t3_model_version_csmr
                    WHERE model_version_id={}
                    LIMIT 1""".format(self.mvid))
                if len(t3_csmr_rows) > 0:
                    raise ValueError(
                        "csmr for model_version {} has already been promoted "
                        "to t3".format(self.mvid))

                # Download from T2 to file (can't cross-insert from different
                # DB servers)
                csmr_cause_id = (
                    self.model_version_meta.add_csmr_cause.squeeze())
                cc_vid = self.model_version_meta.csmr_cod_output_version_id
                cc_vid = cc_vid.squeeze()
                demo = Demographics()
                query = """
                    SELECT
                        cc.location_id,
                        cc.year_id AS time_lower,
                        cc.sex_id AS x_sex,
                        mean_death,
                        lower_death,
                        upper_death,
                        cc.age_group_id,
                        age_group_years_start AS age_lower,
                        age_group_years_end AS age_upper
                    FROM cod.output as cc FORCE INDEX (output_pk)
                    JOIN shared.age_group USING(age_group_id)
                    WHERE output_version_id = {cc_vid}
                    AND cc.age_group_id in ({ages})
                    AND cc.year_id IN ({years})
                    AND cc.sex_id IN (1,2)
                    AND cause_id={cid}""".format(
                        cc_vid=cc_vid,
                        cid=csmr_cause_id,
                        ages=", ".join(map(str, demo.mortality_age_grid)),
                        years=", ".join(map(str, demo.mortality_years)))
                df = execute_select(query, 'cod')

                pop = get_population(location_id=-1,
                                     location_set_id=demo.LOCATION_SET_ID,
                                     age_group_id=demo.mortality_age_grid,
                                     year_id=demo.mortality_years,
                                     sex_id=[1, 2],
                                     gbd_round_id=demo.gbd_round_id)
                df = df.merge(pop,
                              right_on=['location_id', 'year_id',
                                        'age_group_id', 'sex_id'],
                              left_on=['location_id', 'time_lower',
                                       'age_group_id', 'x_sex'])
                df['meas_value'] = df['mean_death'] / df['population']
                df['meas_lower'] = df['lower_death'] / df['population']
                df['meas_upper'] = df['upper_death'] / df['population']
                df = df[['location_id', 'time_lower', 'age_group_id', 'x_sex',
                         'age_lower', 'age_upper', 'meas_value', 'meas_lower',
                         'meas_upper']]
                if len(df) == 0:
                    raise ValueError("There was a problem with the selected "
                                     "CodCorrect version ({}) + csmr_cause_id "
                                     "({})".format(cc_vid, csmr_cause_id))

                # Write to disk and upload to T3 table
                csmr_file = "{rd}/csmr.csv".format(rd=self.root_dir)
                df.to_csv(csmr_file, index=False)

                upload.upload_file(self.mvid,
                                   csmr_file,
                                   't3_model_version_csmr',
                                   ['location_id', 'year_id', 'age_group_id',
                                    'sex_id', '@dummy', '@dummy', 'mean',
                                    'lower', 'upper'])
                return df

    def query_t3_dismod_data(self):
        '''Return a pandas dataframe of tier 3 dismod input data
        '''
        query = """
            SELECT
                model_version_dismod_id as data_id,
                nid,
                location_id,
                sm.measure as integrand,
                mean,
                year_start,
                year_end,
                age_start,
                age_end,
                lower,
                upper,
                sample_size,
                standard_error,
                sex_id,
                TRIM(GROUP_CONCAT(
                    CASE
                        WHEN study_covariate_id IS NOT NULL
                            THEN study_covariate_id
                        ELSE
                            ''
                    END
                    SEPARATOR ' ')) AS study_covariates
            FROM
                epi.model_version mv
            INNER JOIN
                epi.t3_model_version_dismod t3_dm
                    # ON mv.model_version_id = t3_dm.model_version_id
                    USING(model_version_id)
            LEFT JOIN
                epi.t3_model_version_study_covariate t3_sc
                    ON t3_dm.model_version_id = t3_sc.model_version_id
                    AND t3_dm.bundle_id = t3_sc.bundle_id
                    AND t3_dm.seq = t3_sc.seq
            LEFT JOIN
                shared.measure sm ON (t3_dm.measure_id = sm.measure_id)
            WHERE mv.model_version_id = %s
            AND t3_dm.outlier_type_id = 0
            GROUP BY t3_dm.model_version_dismod_id""" % (self.mvid)
        df = execute_select(query)
        return df

    def query_t3_emr_data(self):
        '''Return a pandas dataframe of tier 3 excess mortality data
        '''
        query = """
            SELECT
                model_version_dismod_id as data_id,
                location_id,
                year_start as time_lower,
                age_start as age_lower,
                age_end as age_upper,
                sex_id as x_sex,
                mean as meas_value,
                lower as meas_lower,
                upper as meas_upper,
                standard_error
            FROM
                epi.t3_model_version_emr t3_emr
            WHERE model_version_id = {}
            AND t3_emr.outlier_type_id = 0""".format(self.mvid)
        df = execute_select(query)
        if len(df) == 0:
            return pd.DataFrame()
        else:
            df['integrand'] = 'mtexcess'
            df['data_id'] = -1 * df['data_id']
            return df

    def query_t3_csmr_data(self):
        ''' Return a dataframe of tier 3 cause specific mortality rates'''
        query = """
            SELECT
                location_id,
                year_id as time_lower,
                age_group_id,
                sex_id as x_sex,
                mean as meas_value,
                lower as meas_lower,
                upper as meas_upper
            FROM
                epi.t3_model_version_csmr t3_csmr
            WHERE model_version_id = {}""".format(self.mvid)
        df = execute_select(query)
        if len(df) == 0:
            return pd.DataFrame()
        else:
            df['integrand'] = 'mtspecific'
            df = df.merge(self.ages)
            df.rename(columns={'age_group_years_start': 'age_lower',
                               'age_group_years_end': 'age_upper'},
                      inplace=True)
            df['age_upper'] = df.age_upper.clip(upper=100)
            return df

    def query_t3_asdr_data(self):
        ''' Return a dataframe of tier3 age specific death rates'''
        query = """
            SELECT
                location_id,
                year_id as time_lower,
                age_group_id,
                sex_id as x_sex,
                mean as meas_value,
                lower as meas_lower,
                upper as meas_upper
            FROM
                epi.t3_model_version_asdr t3_asdr
            WHERE model_version_id = {}""".format(self.mvid)
        df = execute_select(query)
        if len(df) == 0:
            return pd.DataFrame()
        else:
            df['integrand'] = 'mtall'
            df = df.merge(self.ages)
            df.rename(columns={'age_group_years_start': 'age_lower',
                               'age_group_years_end': 'age_upper'},
                      inplace=True)
            df['age_upper'] = df.age_upper.clip(upper=100)
            return df

    def convert_location_ids(self, df):
        """Convert location_id to flattened atom/subreg/region/super"""
        dmdf = df.merge(self.lmap, on='location_id')
        return dmdf

    def map_t3mt_to_dismod(self, df, integrand):
        log = logging.getLogger(__name__)

        if len(df) == 0:
            return pd.DataFrame()

        mtdf = df.copy()

        # Calculate stdevs
        mtdf['meas_stdev'] = (mtdf.meas_upper - mtdf.meas_lower) / (2.0 * 1.96)
        mtdf['meas_stdev'] = mtdf.meas_stdev.replace({0: 1e-9})
        mtdf.drop(['meas_lower', 'meas_upper'], axis=1, inplace=True)

        # Reformat age and sex values
        mtdf['age_upper'] = mtdf.age_upper.fillna(100)
        try:
            mtdf.drop('age_group_id', axis=1, inplace=True)
        except Exception as e:
            log.exception(e)

        mtdf['x_sex'] = mtdf.x_sex.replace({1: 0.5,
                                            2: -0.5,
                                            3: 0.0})

        mtdf['time_upper'] = mtdf['time_lower'] + 1
        mtdf['year_id'] = mtdf['time_lower']
        mtdf['hold_out'] = 0
        mtdf['x_local'] = 1
        mtdf['a_data_id'] = -999999999
        mtdf['integrand'] = integrand
        mtdf['data_like'] = likelihood_map[
            self.model_version_meta.data_likelihood.values[0]]

        mtdf = self.convert_location_ids(mtdf)

        mtdf['nid'] = -999999999
        return mtdf

    def map_t3dm_to_dismod(self, df):
        '''Transform t3 epi input data to a format for dismod'''
        renames = {
            'data_id': 'a_data_id',
            'mean': 'meas_value',
            'year_start': 'time_lower',
            'year_end': 'time_upper',
            'age_start': 'age_lower',
            'age_end': 'age_upper',
            'standard_error': 'meas_stdev',
            'sex_id': 'x_sex'}
        dmdf = df.rename(columns=renames)
        dmdf['x_sex'] = dmdf.x_sex.replace({1: 0.5,
                                            2: -0.5,
                                            3: 0.0})

        dmdf['x_local'] = 1

        dmdf['hold_out'] = 0

        dmdf['meas_stdev'] = dmdf.apply(lambda x: self.recalc_SE(
            x['meas_value'], x['meas_stdev'], x['sample_size'],
            x['lower'], x['upper']), axis=1)
        dmdf = dmdf.drop(['lower', 'upper', 'sample_size'], axis=1)

        dmdf = self.convert_location_ids(dmdf)

        # Use the mid-year as the year_id for merging country covariates
        dmdf['year_id'] = (
            (dmdf.time_upper + dmdf.time_lower) / 2).astype('int')

        return dmdf

    def set_data_likelihood(self, df):
        '''Take a dataframe and use model version metadata's data_likelihood
        field to add a 'data_like' column
        '''
        dmdf = df.copy()
        dmdf['data_like'] = likelihood_map[
            self.model_version_meta.data_likelihood.values[0]]
        return dmdf

    def exlude_data(self, df):
        '''Drop measures from dataframe, depending on model parameters'''
        mpm = self.model_params
        exclude_params = mpm.ix[mpm.parameter_type_id == 17, 'measure']
        return df[~df.integrand.isin(exclude_params)]

    def restrict_single_param(self, df):
        # Scope to single parameter if requested
        if self.single_param is not None:
            dmdf = df[df.integrand == self.single_param]
            dmdf['integrand'] = dmdf.integrand.replace(
                {self.single_param: 'mtother'})
        else:
            dmdf = df[df.integrand.isin(integrand_pred)]
        return dmdf

    def correct_demographer_notation(self, df):
        dmdf = df.copy()
        dmdf.ix[(dmdf.age_lower >= 1) &
                (dmdf.age_upper < 100), 'age_upper'] = (
                    dmdf.ix[(dmdf.age_lower >= 1) &
                            (dmdf.age_upper < 100), 'age_upper'] + 1)
        return dmdf

    def get_t3_input_data(self):
        '''Main method for getting final formatted input data to dismod.
        Reads promoted tier 3 input data from database, transforms to match
        dismod expectations, adds study covariates, sets data likelihood.

        Returns:
            pandas dataframe'''
        log = logging.getLogger(__name__)
        try:
            self.promote_dm_t2_to_t3()
        except ValueError as e:
            log.exception(e)
        data = self.query_t3_dismod_data()
        data = self.map_t3dm_to_dismod(data)
        data = self.set_data_likelihood(data)
        data = self.attach_study_covariates(data)

        user_requested_csmr = (
            self.model_version_meta.add_csmr_cause.notnull().squeeze())
        csmr_in_data = 'mtspecific' in data.integrand.unique()
        if user_requested_csmr and csmr_in_data:
            raise ValueError("Cannot both provide custom CSMR data and "
                             "data from CodCorrect")

        try:
            csmr_data = self.promote_csmr_t2_to_t3()
        except ValueError as e:
            log.exception(e)
            csmr_data = self.query_t3_csmr_data()
        csmr_data = self.map_t3mt_to_dismod(csmr_data, 'mtspecific')
        try:
            asdr_data = self.promote_asdr_t2_to_t3()
        except ValueError as e:
            log.exception(e)
            asdr_data = self.query_t3_asdr_data()
        asdr_data = self.map_t3mt_to_dismod(asdr_data, 'mtall')
        emr_data = self.query_t3_emr_data()
        emr_data = self.map_t3mt_to_dismod(emr_data, 'mtexcess')
        data = pd.concat([data, csmr_data, asdr_data, emr_data])

        data = self.exlude_data(data)
        data = self.restrict_single_param(data)
        data = self.correct_demographer_notation(data)
        return data

    def measured_integrands(self):
        '''Returns list of unique integrands in input data'''
        return list(self.data.integrand.drop_duplicates())

    def measured_locations(self):
        '''Returns dictionary mapping location type (atom/subreg/region/super)
        to possibly empty list. If list is not empty, that location type is in
        the input data'''
        data = self.data
        if self.single_param is None:
            data = data[data.integrand != 'mtall']
        mls = {}
        for ltype in ['atom', 'subreg', 'region', 'super']:
            mls[ltype] = list(
                data.ix[data[ltype] != 'none', ltype].drop_duplicates())
        return mls

    @sanitize.convert_to_ascii
    def get_model_parameters(self):
        '''Return dataframe of model parameters'''
        query = """
            SELECT * FROM epi.model_parameter
            LEFT JOIN shared.measure USING(measure_id)
            LEFT JOIN epi.parameter_type USING(parameter_type_id)
            LEFT JOIN epi.study_covariate USING(study_covariate_id)
            WHERE model_version_id=%s """ % (self.mvid)
        df = execute_select(query)
        df.drop(['date_inserted', 'inserted_by', 'last_updated',
                 'last_updated_by', 'last_updated_action'], axis=1,
                inplace=True)

        df['mean'] = df['mean'].fillna((df.upper + df.lower) / 2.0)
        df['std'] = df['std'].fillna(inf)

        return df

    def get_integrand_data(self, input_data, integrand):
        '''subset input_data dataframe to just that integrand'''
        return input_data[input_data.integrand == integrand]

    def assign_eta(self):
        model_param_meta = self.model_params
        data = self.data

        db_etas = model_param_meta[model_param_meta.parameter_type_id == 13]

        eta_priors = []
        for integrand in self.integrand_pred:
            if integrand in db_etas.measure.values:
                # check if eta has a value for this integrand in
                # epi.model_parameters
                eta = db_etas.ix[
                    db_etas.measure == integrand, "mean"].values[0]
            elif integrand in data.integrand.values:
                # otherwise check if there is any data for this integrand
                if len(data[
                        (data.integrand == integrand) &
                        (data.meas_value != 0)]) > 0:
                    eta = 1e-2 * data.ix[(
                        (data.integrand == integrand) &
                        (data.meas_value != 0)), "meas_value"].median()
                else:
                    eta = 1e-5
            else:
                # otherwise use a default value
                eta = 1e-5

            eta = "{:.5g}".format(eta)
            eta_priors.append(eta)

        eta_df = pd.DataFrame({'integrand': self.integrand_pred,
                               'eta': eta_priors})
        return eta_df

    def get_age_mesh(self, use_default=False):
        '''Return a dataframe of age_groups and age_group metadata.

        Args:
            use_default(bool, False): If False, use model version's metdata to
                create age mesh. If True, use default mortality age grid
        default age mesh'''
        # parent age grid
        demo = Demographics()
        if use_default:
            age_group_ids = demo.mortality_age_grid
            age_group_ids = [str(a) for a in age_group_ids]
            age_group_id_str = ",".join(age_group_ids)
            query = """
                SELECT * FROM shared.age_group
                WHERE age_group_id IN (%s) """ % (age_group_id_str)
            age_mesh = execute_select(query)
            age_mesh.rename(columns={
                'age_group_years_start': 'age_start',
                'age_group_years_end': 'age_end'}, inplace=True)
            age_mesh['age_end'] = age_mesh.age_end.clip(upper=100)
            age_mesh = age_mesh.sort_values('age_start')
            age_mesh = (age_mesh['age_start'] + age_mesh['age_end']) / 2.0
        else:
            age_mesh = self.model_version_meta.age_mesh
            age_mesh = age_mesh.values[0].split(" ")
        age_mesh = ["{:.5g}".format(float(a)) for a in age_mesh]

        return age_mesh

    def get_integrand_bounds(self):
        '''Return a dataframe of integrand bounds, based on model version
        metadata and parameters
        '''
        model_version_meta = self.model_version_meta
        mpm = self.model_params

        integrand_df = self.assign_eta()
        integrand_df['min_cv_world2sup'] = (
            model_version_meta['cv_global'].values[0])
        integrand_df['min_cv_sup2reg'] = (
            model_version_meta['cv_super'].values[0])
        integrand_df['min_cv_reg2sub'] = (
            model_version_meta['cv_region'].values[0])

        if 'cv_subreg' in model_version_meta.columns:
            integrand_df['min_cv_sub2atom'] = (
                model_version_meta['cv_subreg'].values[0])
        else:
            integrand_df['min_cv_sub2atom'] = (
                model_version_meta['cv_region'].values[0])

        age_mesh = self.get_age_mesh(use_default=True)
        age_mesh = " ".join(age_mesh)

        integrand_df = propagate_mincv_bounds(mpm, integrand_df)

        integrand_df = integrand_df.replace({0: 0.01})
        integrand_df['parent_age_grid'] = age_mesh

        return integrand_df

    def attach_country_covariate(self, covariate_id, integrand,
                                 asdr_cause_id=None):
        '''
        Add study covariate column to self.data. Possibly transform covariate
        depending on model parameters. If asdr_cause_id not none, merge
        on log transformed cause specific age standardized rate as a covariate.

        '''
        log = logging.getLogger(__name__)
        demo = Demographics()
        if asdr_cause_id is not None:
            cc_vid = (
                self.model_version_meta.csmr_cod_output_version_id.squeeze())
            query = """
                SELECT o.location_id, o.year_id, o.age_group_id, o.sex_id,
                    mean_death as mean_value
                FROM cod.output as o
                JOIN shared.age_group USING(age_group_id)
                WHERE output_version_id=%s
                AND o.age_group_id=27
                AND o.sex_id IN (1, 2)
                AND cause_id=%s""" % (cc_vid, asdr_cause_id)

            covname = 'lnasdr_%s' % asdr_cause_id
            covdata = execute_select(query, 'cod')
            covdata['mean_value'] = np.log(covdata.mean_value)
            colname = 'raw_c_{}'.format(covname)
            log.info("Adding ASDR column {}".format(colname))
        else:
            covnq = """
                SELECT covariate_name_short FROM shared.covariate
                WHERE covariate_id=%s """ % (covariate_id)
            covname = execute_select(covnq).values[0][0]
            log.info("Adding country covariate column {}".format(covname))
            covdata = get_covariate_estimates(
                covariate_id=covariate_id,
                gbd_round_id=demo.gbd_round_id,
                status='best')
            covdata = covdata[['location_id', 'year_id', 'age_group_id',
                               'sex_id', 'mean_value']]

            transform_type_id = (
                self.model_params[
                    (self.model_params.country_covariate_id == covariate_id) &
                    (self.model_params.measure == integrand)][
                        'transform_type_id'].squeeze())

            if transform_type_id == 1:
                covdata['mean_value'] = np.log(covdata.mean_value)
            elif transform_type_id == 2:
                covdata['mean_value'] = (
                    np.log(covdata.mean_value / (1 - covdata.mean_value)))
            elif transform_type_id == 3:
                covdata['mean_value'] = covdata.mean_value**2
            elif transform_type_id == 4:
                covdata['mean_value'] = np.sqrt(covdata.mean_value)
            elif transform_type_id == 5:
                covdata['mean_value'] = covdata.mean_value * 1000

            colname = 'raw_c_{}_{}'.format(int(transform_type_id), covname)

        lsvid = self.model_version_meta.location_set_version_id.values[0]
        lt = loctree(location_set_version_id=lsvid,
                     gbd_round_id=demo.gbd_round_id)
        leaves = [l.id for l in lt.leaves()]
        covdata = covdata[covdata.location_id.isin(leaves)]

        if 3 not in covdata.sex_id.unique():
            bothdata = (
                covdata[['location_id', 'year_id', 'mean_value']].groupby(
                    ['location_id', 'year_id']).mean().reset_index())
            bothdata['sex_id'] = 3
            covdata = covdata.append(bothdata)
        for s in [1, 2]:
            if s not in covdata.sex_id.unique():
                sexdata = covdata[covdata.sex_id == 3]
                sexdata['sex_id'] = s
                covdata = covdata.append(sexdata)

        covdata.rename(columns={'mean_value': colname}, inplace=True)
        covdata['x_sex'] = covdata.sex_id.replace({
            1: 0.5, 2: -0.5, 3: 0})
        covdata = covdata[['location_id', 'year_id', 'x_sex', colname]]

        covdata = covdata.groupby(['location_id', 'year_id', 'x_sex'],
                                  as_index=False).mean()
        if colname not in self.data.columns:
            self.data = self.data.merge(covdata,
                                        on=['location_id', 'year_id', 'x_sex'],
                                        how='left')
        if len(self.covariate_data) == 0:
            self.covariate_data = covdata
        else:
            if colname not in self.covariate_data.columns:
                self.covariate_data = self.covariate_data.merge(
                    covdata, on=['location_id', 'year_id', 'x_sex'])

        return covname, covdata

    def get_effect_priors(self):
        log = logging.getLogger(__name__)
        mpm = self.model_params
        mis = self.measured_integrands()
        if self.single_param is None:
            try:
                mis.remove('mtall')
            except Exception:
                log.exception(
                    "All cause mortality no longer part of the data file?")
            try:
                mis.remove('mtother')
            except Exception:
                log.exception(
                    "mtother in the effects file causing problems?")
        else:
            mis = ['mtother']
        mlsets = self.measured_locations()

        #####################################################
        # DEFAULTS
        #####################################################
        # Set location gamma defaults
        ep = []
        for integrand in self.integrand_pred:
            for loc_lvl in ['super', 'region', 'subreg']:
                defaults = {
                    'integrand': integrand,
                    'effect': 'gamma',
                    'name': loc_lvl,
                    'lower': 1.0,
                    'upper': 1.0,
                    'mean': 1.0,
                    'std': inf}
                ep.append(defaults)

        # Location random effects
        for integrand in self.integrand_pred:
            for loc_lvl, mls in mlsets.iteritems():
                if loc_lvl == 'atom':
                    pass
                else:
                    for n in ['none', 'cycle']:
                        cyc_none = {
                            'integrand': integrand,
                            'effect': loc_lvl,
                            'name': n,
                            'lower': 0,
                            'upper': 0,
                            'mean': 0,
                            'std': inf}
                        ep.append(cyc_none)
                    if integrand in self.integrand_pred:
                        for ml in mls:
                            add_csmr_cause = (
                                self.model_version_meta['add_csmr_cause'])
                            if ((add_csmr_cause.isnull().values[0]) &
                                (integrand == 'mtspecific') &
                                    (loc_lvl != 'super')):
                                defaults = {
                                    'integrand': integrand,
                                    'effect': loc_lvl,
                                    'name': ml,
                                    'lower': 0,
                                    'upper': 0,
                                    'mean': 0,
                                    'std': inf}
                            else:
                                defaults = {
                                    'integrand': integrand,
                                    'effect': loc_lvl,
                                    'name': ml,
                                    'lower': -2,
                                    'upper': 2,
                                    'mean': 0,
                                    'std': 1}
                            ep.append(defaults)

        # Beta sex priors
        for integrand in self.integrand_pred:
            defaults = {
                'integrand': integrand,
                'effect': 'beta',
                'name': 'x_sex',
                'lower': -2.0,
                'upper': 2.0,
                'mean': 0.0,
                'std': inf}
            ep.append(defaults)

        # Zeta priors
        for integrand in mis:
            defaults = {
                'integrand': integrand,
                'effect': 'zeta',
                'name': 'x_local',
                'lower': 0.0,
                'upper': 0.5,
                'mean': 0.0,
                'std': inf}
            ep.append(defaults)

        #####################################################
        # DB SETTINGS
        #####################################################
        # Apply other priors as specified in the database
        """
        1   Value prior
        2   Slope prior
        5   Study-level covariate fixed effect on integrand value (x-cov)
        6   Study-level covariate fixed effect on integrand variance (z-cov)
        7   Country-level covariate fixed effect
        8   ln-ASDR covariate fixed effect
        9   Location random effect
        10  Smoothness (xi)
        11  Heterogeneity (zeta1)
        12  Offset in the log normal transformation of rate smoothing (kappa)
        13  Offset in the log normal transformation of data fitting (eta)
        14  Multiplier of standard deviation for country random effects (gamma)
        15  Multiplier of standard deviation for region random effects (gamma)
        16  Multiplier of standard deviation for super region random effects
            (gamma)
        17  Exclude data for parameter
        18  Exclude prior data
        19  Min CV by integrand
        """

        # Country covariates
        ccovs = mpm[mpm.parameter_type_id == 7]
        asdr_cause_ids = []
        if 8 in mpm.parameter_type_id.unique():
            for asdr_cause_id in self.model_params[
                    self.model_params.parameter_type_id == 8][
                        'asdr_cause'].unique():
                asdr_cause_ids.append(asdr_cause_id)
            ccovs = ccovs.append(mpm[mpm.parameter_type_id == 8])

        for i, row in ccovs[ccovs.country_covariate_id.notnull()].iterrows():
            ccov_id = row['country_covariate_id']
            integrand = row['measure']
            transform = row['transform_type_id']

            ccov_name, ccov_data = self.attach_country_covariate(
                ccov_id, integrand=integrand)
            if integrand in self.integrand_pred:
                ccov = {
                    'integrand': integrand,
                    'effect': 'beta',
                    'name': 'x_c_{}_{}'.format(int(transform), ccov_name),
                    'lower': row['lower'],
                    'upper': row['upper'],
                    'mean': row['mean'],
                    'std': inf}
                ep.append(ccov)

            log.info("Setting effect prior for {}".format(ccov_name))

        for i, row in ccovs[ccovs.asdr_cause.isin(asdr_cause_ids)].iterrows():
            asdr_cause_id = row['asdr_cause']
            integrand = row['measure']

            ccov_name, ccov_data = self.attach_country_covariate(
                -1, integrand, asdr_cause_id)
            if integrand in self.integrand_pred:
                ccov = {
                    'integrand': integrand,
                    'effect': 'beta',
                    'name': 'x_c_{}'.format(ccov_name),
                    'lower': row['lower'],
                    'upper': row['upper'],
                    'mean': row['mean'],
                    'std': inf}
                ep.append(ccov)

            log.info("Setting effect prior for {}".format(ccov_name))

        # Study xcovs
        xcovs = mpm[mpm.parameter_type_id == 5]
        xcovs['mean'] = xcovs['mean'].fillna((xcovs.lower + xcovs.upper) / 2.)
        if len(xcovs) > 0:
            for i, row in xcovs.iterrows():
                integrand = row['measure']
                if integrand in self.integrand_pred:
                    name = 'x_s_%s' % (row['study_covariate'])
                    xcov = {
                        'integrand': integrand,
                        'effect': 'beta',
                        'lower': row['lower'],
                        'upper': row['upper'],
                        'mean': row['mean'],
                        'std': inf}
                    if name == 'x_s_sex':
                        xcov['name'] = 'x_sex'
                        ep = [i
                              if not (
                                  i['name'] == 'x_sex' and
                                  i['effect'] == 'beta' and
                                  i['integrand'] == integrand)
                              else xcov for i in ep]
                    else:
                        xcov['name'] = name
                        ep.append(xcov)

                    if name not in self.data.columns:
                        self.data[name] = 0

        # Study zcovs
        zcovs = mpm[mpm.parameter_type_id == 6]
        zcovs['mean'] = zcovs['mean'].fillna((zcovs.lower + zcovs.upper) / 2.)
        if len(zcovs) > 0:
            for i, row in zcovs.iterrows():
                integrand = row['measure']
                if integrand in self.integrand_pred:
                    integrand = row['measure']
                    name = 'x_s_%s' % (row['study_covariate'])
                    zcov = {
                        'integrand': integrand,
                        'effect': 'zeta',
                        'name': name,
                        'lower': row['lower'],
                        'upper': row['upper'],
                        'mean': row['mean'],
                        'std': inf}
                    if name == 'x_s_sex':
                        zcov['name'] = 'x_sex'
                    else:
                        zcov['name'] = name
                    ep.append(zcov)

                    if name not in self.data.columns:
                        self.data[name] = 0

        ep = pd.DataFrame(ep)
        self.study_covariates = list(self.data.filter(like='x_s_').columns)

        # Override location random effects
        loc_res = mpm[mpm.parameter_type_id == 9]
        loc_res['location_id'] = loc_res.location_id.astype('float')
        for i, row in loc_res.iterrows():
            integrand = row['measure']
            conditions = (
                (ep.integrand == integrand) &
                (~ep.name.isin(['none', 'cycle'])) &
                (ep.effect.isin(['subreg', 'region', 'super'])))
            if math.isnan(row['location_id']):
                this_conditions = conditions
            else:
                location_id = "{:.0f}".format(int(row['location_id']))
                this_conditions = conditions & (ep.name == location_id)
            ep.ix[this_conditions, "mean"] = row['mean']
            ep.ix[this_conditions, "lower"] = row['lower']
            ep.ix[this_conditions, "upper"] = row['upper']
            ep.ix[this_conditions, "std"] = row['std']

        # Override zeta
        zeta1s = mpm[mpm.parameter_type_id == 11]
        zeta1s['mean'] = zeta1s['mean'].fillna(
            (zeta1s.lower + zeta1s.upper) / 2.)
        for i, row in zeta1s.iterrows():
            integrand = row['measure']
            conditions = (ep.integrand == integrand) & (ep.name == 'x_local')
            ep.ix[conditions, 'mean'] = row['mean']
            ep.ix[conditions, 'lower'] = row['lower']
            ep.ix[conditions, 'upper'] = row['upper']

        # Override location gamma defaults if specified in DB
        subreg_gamma = mpm[(mpm.parameter_type_id == 14) &
                           (mpm.cascade_level_id == 4)]
        reg_gamma = mpm[(mpm.parameter_type_id == 14) &
                        (mpm.cascade_level_id == 3)]
        super_gamma = mpm[(mpm.parameter_type_id == 14) &
                          (mpm.cascade_level_id == 2)]
        for val in ['mean', 'lower', 'upper']:
            for i, row in subreg_gamma.iterrows():
                conditions = ((ep.integrand == row['measure']) &
                              (ep.effect == 'gamma') &
                              (ep.name.isin(['subreg'])))
                if row['measure'] in self.integrand_pred:
                    ep.ix[conditions, val] = subreg_gamma[val].values[0]
            for i, row in reg_gamma.iterrows():
                conditions = ((ep.integrand == row['measure']) &
                              (ep.effect == 'gamma') &
                              (ep.name.isin(['region'])))
                if row['measure'] in self.integrand_pred:
                    ep.ix[conditions, val] = reg_gamma[val].values[0]
            for i, row in super_gamma.iterrows():
                conditions = ((ep.integrand == row['measure']) &
                              (ep.effect == 'gamma') &
                              (ep.name.isin(['super'])))
                if row['measure'] in self.integrand_pred:
                    ep.ix[conditions, val] = super_gamma[val].values[0]

        lmap = gen_locmap(
            self.model_version_meta.location_set_version_id.values[0])
        if len(self.covariate_data) > 0:
            ccovs = list(
                set(self.covariate_data.columns) -
                set(['location_id', 'year_id', 'x_sex']))
            self.covariate_data = self.covariate_data.merge(
                lmap, on='location_id', how='left')

            subregcovs = self.covariate_data[
                self.covariate_data.atom != 'none'].groupby(
                    ['subreg', 'year_id', 'x_sex'])[ccovs].mean().reset_index()
            subregcovs.rename(columns={'subreg': 'location_id'}, inplace=True)
            regcovs = self.covariate_data.groupby(
                ['region', 'year_id', 'x_sex'])[ccovs].mean().reset_index()
            regcovs.rename(columns={'region': 'location_id'}, inplace=True)
            supcovs = self.covariate_data.groupby(
                ['super', 'year_id', 'x_sex'])[ccovs].mean().reset_index()
            supcovs.rename(columns={'super': 'location_id'}, inplace=True)
            gcovs = self.covariate_data.groupby(
                ['year_id', 'x_sex'])[ccovs].mean().reset_index()
            gcovs['location_id'] = '1'
            self.covariate_data = pd.concat([
                self.covariate_data, subregcovs, regcovs, supcovs, gcovs])
            self.covariate_data['location_id'] = (
                self.covariate_data.location_id.astype(int))

        return ep

    def get_rate_prior(self):
        ages_num = [float(a) for a in self.age_mesh]
        ages_str = self.age_mesh

        slope_defaults = pd.DataFrame({
            'age': ages_str,
            'lower': -inf,
            'upper': inf,
            'mean': 0.0,
            'std': inf})
        slope_defaults = slope_defaults[:-1]

        val_defaults = {}
        val_defaults['iota'] = pd.DataFrame({
            'age': ages_str,
            'lower': 0.0,
            'upper': 10.0,
            'mean': 5.0,
            'std': inf})
        val_defaults['rho'] = pd.DataFrame({
            'age': ages_str,
            'lower': 0.0,
            'upper': 10.0,
            'mean': 5.0,
            'std': inf})
        val_defaults['chi'] = pd.DataFrame({
            'age': ages_str,
            'lower': 0.0,
            'upper': 10.0,
            'mean': 5.0,
            'std': inf})
        if self.single_param in ['proportion', 'prevalence']:
            val_defaults['omega'] = pd.DataFrame({
                'age': ages_str,
                'lower': 0.0,
                'upper': 1.0,
                'mean': 0.5,
                'std': inf})
        elif self.single_param == 'continuous':
            val_defaults['omega'] = pd.DataFrame({
                'age': ages_str,
                'lower': 0.0,
                'upper': inf,
                'mean': 1,
                'std': inf})
        else:
            val_defaults['omega'] = pd.DataFrame({
                'age': ages_str,
                'lower': 0.0,
                'upper': 10.0,
                'mean': 5.0,
                'std': inf})

        rp_df = []
        for t in ['iota', 'rho', 'chi', 'omega']:
            type_slope = slope_defaults.copy()
            type_slope['type'] = 'd' + t
            rp_df.append(type_slope)
            type_val = val_defaults[t].copy()
            type_val['type'] = t
            rp_df.append(type_val)
        rp_df = pd.concat(rp_df)

        # Override defaults if specified in the database
        mpm = self.model_params

        val_pri = mpm[mpm.parameter_type_id == 1]
        if len(val_pri) > 0:
            val_pri['mean'] = val_pri['mean'].fillna(
                (val_pri.lower + val_pri.upper) / 2.0)
            val_pri['lower'] = val_pri.lower.fillna(val_pri['mean'])
            val_pri['upper'] = val_pri.upper.fillna(val_pri['mean'])

        slope_pri = mpm[mpm.parameter_type_id == 2]
        if self.single_param is not None:
            val_pri['measure'] = 'mtother'
            slope_pri['measure'] = 'mtother'
        if len(slope_pri) > 0:
            slope_pri['mean'] = slope_pri['mean'].fillna(
                (slope_pri.lower + slope_pri.upper) / 2.0)
            slope_pri['lower'] = slope_pri.lower.fillna(slope_pri['mean'])
            slope_pri['upper'] = slope_pri.upper.fillna(slope_pri['mean'])

        rp_df['age'] = rp_df.age.astype('float')
        for integrand, rate in integrand_rate_map.iteritems():
            ivp = val_pri[val_pri.measure == integrand]
            isp = slope_pri[slope_pri.measure == integrand]
            for a in ages_num:
                ivp_mlu = ivp.ix[(ivp.age_start <= a) & (
                    ivp.age_end >= a), ['mean', 'lower', 'upper']]
                isp_m = isp.ix[(isp.age_start <= a) &
                               (isp.age_end >= a), 'mean']

                if len(ivp_mlu) > 0:
                    rp_df.ix[(rp_df.type == rate) & (rp_df.age == a),
                             "lower"] = ivp_mlu['lower'].values[0]
                    rp_df.ix[(rp_df.type == rate) & (rp_df.age == a),
                             "upper"] = ivp_mlu['upper'].values[0]
                    rp_df.ix[(rp_df.type == rate) & (rp_df.age == a),
                             "mean"] = ivp_mlu['mean'].values[0]
                if len(isp_m) > 0:
                    if isp_m.values[0] == 1:
                        rp_df.ix[(rp_df.type == 'd' + rate) &
                                 (rp_df.age == a), "lower"] = 0
                        rp_df.ix[(rp_df.type == 'd' + rate) &
                                 (rp_df.age == a), "upper"] = inf
                    elif isp_m.values[0] == -1:
                        rp_df.ix[(rp_df.type == 'd' + rate) &
                                 (rp_df.age == a), "lower"] = -inf
                        rp_df.ix[(rp_df.type == 'd' + rate) &
                                 (rp_df.age == a), "upper"] = 0
                    else:
                        rp_df.ix[(rp_df.type == 'd' + rate) &
                                 (rp_df.age == a), "lower"] = -inf
                        rp_df.ix[(rp_df.type == 'd' + rate) &
                                 (rp_df.age == a), "upper"] = inf

        # Enforce slope priors
        for integrand, rate in integrand_rate_map.iteritems():
            for i, a in enumerate(ages_num[:-1]):
                next_age = ages_num[i + 1]
                this_mean = rp_df.ix[(rp_df.type == rate) & (rp_df.age == a),
                                     "mean"].values[0]
                next_mean = rp_df.ix[
                    (rp_df.type == rate) & (rp_df.age == next_age),
                    "mean"].values[0]

                if rp_df.ix[(rp_df.type == 'd' + rate) & (rp_df.age == a),
                            "lower"].values[0] == 0:
                    next_mean = np.clip(next_mean, a_min=this_mean,
                                        a_max=inf)
                elif rp_df.ix[(rp_df.type == 'd' + rate) & (rp_df.age == a),
                              "upper"].values[0] == 0:
                    next_mean = np.clip(next_mean, a_min=-inf,
                                        a_max=this_mean)

                rp_df.ix[(rp_df.type == rate) & (rp_df.age == next_age),
                         "mean"] = next_mean

        if self.single_param is not None:
            rp_df.ix[~rp_df.type.isin(['omega', 'domega']), 'mean'] = 0
            rp_df.ix[~rp_df.type.isin(['omega', 'domega']), 'lower'] = 0
            rp_df.ix[~rp_df.type.isin(['omega', 'domega']), 'upper'] = 0
            rp_df.ix[~rp_df.type.isin(['omega', 'domega']), 'std'] = inf

            if self.single_param == 'proportion':
                o_low = rp_df.ix[rp_df.type == 'omega', 'lower']
                o_upp = rp_df.ix[rp_df.type == 'omega', 'upper']
                rp_df.ix[rp_df.type == 'omega', 'lower'] = o_low.clip(lower=0)
                rp_df.ix[rp_df.type == 'omega', 'upper'] = o_upp.clip(upper=1)

        return rp_df

    def get_simple_prior(self):
        """
        Generate the simple smoothness priors. Use the integrand to
        greek map in the rate prior to map the database 'smoothness'
        overrides. 
        """
        def_p_zero = pd.DataFrame([{
            'name': 'p_zero',
            'lower': 0,
            'upper': 0,
            'mean': 0,
            'std': inf}])

        if self.single_param is not None:
            def_omega = pd.DataFrame([{
                'name': 'xi_omega',
                'lower': 0.3,
                'upper': 0.3,
                'mean': 0.3,
                'std': inf}])
        else:
            def_omega = pd.DataFrame([{
                'name': 'xi_omega',
                'lower': 1,
                'upper': 1,
                'mean': 1,
                'std': inf}])

        def_other = pd.DataFrame({
            'name': ['xi_iota', 'xi_rho', 'xi_chi'],
            'lower': 0.3,
            'upper': 0.3,
            'mean': 0.3,
            'std': inf})

        sp = pd.concat([def_p_zero, def_omega, def_other])

        mpm = self.model_params
        smooth_df = mpm[mpm.parameter_type_id == 10]
        if self.single_param is not None:
            smooth_df['measure'] = 'mtother'
        for integrand in integrand_rate_map.keys():
            if integrand in smooth_df.measure.values:
                lower = smooth_df.ix[smooth_df.measure ==
                                     integrand, "lower"].values[0]
                upper = smooth_df.ix[smooth_df.measure ==
                                     integrand, "upper"].values[0]
                mean = (lower + upper) / 2.0
                rate = integrand_rate_map[integrand]

                sp.ix[sp.name == 'xi_' + rate, "lower"] = lower
                sp.ix[sp.name == 'xi_' + rate, "upper"] = upper
                sp.ix[sp.name == 'xi_' + rate, "mean"] = mean

        if self.model_version_meta['birth_prev'].values[0] == 1:
            sp.ix[sp.name == 'p_zero', "lower"] = 0
            sp.ix[sp.name == 'p_zero', "upper"] = 1
            sp.ix[sp.name == 'p_zero', "mean"] = 0.01

        return sp[['name', 'lower', 'upper', 'mean', 'std']]

    def get_value_prior(self):
        """ Most come from user inputs, and the kappas
        should come from the integrand file """
        etas = self.assign_eta()
        mvm = self.model_version_meta

        value_params = {}
        for integrand, rate in integrand_rate_map.iteritems():
            if integrand in etas.integrand.values:
                kappa = etas.ix[etas.integrand == integrand, "eta"].values[0]
                value_params["kappa_" + rate] = kappa
            else:
                value_params["kappa_" + rate] = 1e-5
        value_params['kappa_omega'] = 1e-5

        for i, eta_row in etas.iterrows():
            value_params['eta_%s' % eta_row['integrand']] = eta_row['eta']

        value_params['sample_interval'] = mvm.sample_interval.values[0]
        value_params['num_sample'] = mvm.num_sample.values[0]
        value_params['integrate_step'] = mvm.integrate_step.values[0]
        value_params['random_seed'] = mvm.random_seed.values[0]
        value_params['integrate_method'] = mvm.integration_type.values[0]
        value_params['integrate_method'] = (
            value_params['integrate_method'].lower().replace(" ", "_"))
        value_params['watch_interval'] = value_params['num_sample'] / 100
        value_params['data_like'] = 'in_data_file'
        value_params['prior_like'] = likelihood_map[
            self.model_version_meta.prior_likelihood.values[0]]

        value_params = pd.DataFrame({
            'name': value_params.keys(),
            'value': value_params.values()})

        return value_params

    def get_study_cov_ids(self):
        '''Get dataframe mapping study covariate id to study_covariate short
        string
        '''
        query = """
            SELECT study_covariate_id, study_covariate
            FROM epi.study_covariate"""
        df = execute_select(query, 'epi')
        return df

    def get_country_cov_ids(self):
        '''Get dataframe mapping study covariate id to covariate_name_short
        string
        '''
        query = """
            SELECT covariate_id, covariate_name_short
            FROM shared.covariate"""
        df = execute_select(query, 'epi')
        return df

    def get_measure_ids(self):
        '''Get dataframe mapping measure_id to measure'''
        query = """
            SELECT measure_id, measure
            FROM shared.measure"""
        df = execute_select(query, 'epi')
        return df

    def get_age_weights(self):
        '''Get dataframe mapping age group id to age standardization weights
        '''
        demo = Demographics()
        gbd_round = int(gbd_round_from_gbd_round_id(demo.gbd_round_id))
        df = summarizers.get_age_weights(gbd_round=gbd_round)
        df = df.rename(columns={"age_group_weight_value": "weight"})
        return df


def propagate_mincv_bounds(mpm, df):
    '''
    For each integrand, if there is an integrand specific global->SR
    min cv setting, override the general global->SR setting

    Repeat that for global->SR down to subregion->atom, for each integrand.


    Arguments:
        mpm(pandas.dataframe): model version parameter dataframe,
            from model_parameter.csv (or Importer.model_params).
            This contains the integrand specific cv settings, if
            any.

        df(pandas.dataframe): dataframe of all integrands with
            general mincv settings. Columns are
            ['integrand', 'min_cv_world2sup', 'min_cv_sup2reg',
            'min_cv_reg2sub', 'min_cv_sub2atom']

    Returns:
        df, with possibly updated cv values
    '''
    MIN_CV_ID = 19

    lvl_map = {
        1: 'min_cv_world2sup',
        2: 'min_cv_sup2reg',
        3: 'min_cv_reg2sub',
        4: 'min_cv_sub2atom'}

    mpm = _standardize_lvl_5_mincv(mpm.copy())
    for integrand in df.integrand:
        integrand_mask = df.integrand == integrand
        for lvl in sorted(lvl_map.keys()):
            min_cv_override = mpm[(mpm.parameter_type_id == MIN_CV_ID) &
                                  (mpm.cascade_level_id == lvl) &
                                  (mpm.measure == integrand)]
            if min_cv_override.empty:
                # Case 1
                # no integrand specific setting specified
                continue

            if len(min_cv_override) > 1:
                # Case 2
                # More than one setting; this should never happen
                raise RuntimeError((
                    "More than one integrand specific min cv setting"
                    "found {}".format(min_cv_override.to_dict('list'))))

            # Case 3
            # need to update this loc lvl min cv setting
            min_cv_override = min_cv_override['mean'].item()
            lvl_label = lvl_map[lvl]
            df.loc[integrand_mask, [lvl_label]] = min_cv_override
    return df


def _standardize_lvl_5_mincv(mpm):
    '''
    The min cv settings allow for global down to subnational,
    but because each level affects the priors of the level below,
    subnational should be invalid. The behavior is for
    a subnational setting to be treated as synonymous
    with country level.

    This function mutates the mpm dataframe of model parameters
    by looking for subnational mincv settings and labeling them
    as national. If national settings already exist and they disagree
    with the subnational, it raises a RuntimeError.

    Inputs:
        mpm(pandas.dataframe): model level parameters, from
        importer.model_params

    Returns:
        mpm, possibly mutated
    '''
    subnat_level_id = 5
    nat_level_id = 4
    MIN_CV_ID = 19
    subnat_mincv_mask = ((mpm.parameter_type_id == MIN_CV_ID) &
                         (mpm.cascade_level_id == subnat_level_id))
    subnat_mincv_settings = mpm[subnat_mincv_mask]

    if subnat_mincv_settings.empty:
        return mpm

    # check for conflicting nat/subnat settings
    for integrand in subnat_mincv_settings.measure:
        nat_mincv_settings = mpm[(mpm.parameter_type_id == MIN_CV_ID) &
                                 (mpm.cascade_level_id == nat_level_id) &
                                 (mpm.measure == integrand)]
        if nat_mincv_settings.empty:
            continue

        subnat_value = subnat_mincv_settings.query(
            "measure == @integrand")['mean'].item()
        nat_value = nat_mincv_settings['mean'].item()
        if subnat_value != nat_value:
            err_msg = ("Got conflicting national and subnational mincv "
                       "settings for integrand {}. National: {}; Subnational: "
                       "{}. The integrand-specific mincv settings affect the "
                       "priors of the location level below the location "
                       "specified in the dropdown, so subnational is an "
                       "invalid option. The cascade treats the subnational "
                       "option as synonymous with the national option, but "
                       "in this case the two settings disagree.".format(
                        integrand, str(nat_value), str(subnat_value)))
            raise RuntimeError(err_msg)

    # assign subnat settings to national, now that we know that either
    # national was not specified, or they are the same value.
    mpm.loc[subnat_mincv_mask, ["cascade_level_id"]] = nat_level_id

    mpm = mpm.drop_duplicates()
    return mpm

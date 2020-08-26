import pandas as pd
import numpy as np
import re
from db_tools.ezfuncs import get_engine, query
from db_tools.registry import cleanup_registry
from cod_prep.utils.delete_package_version import delete_package_version
from datetime import datetime


class RegressionUploader():

    """
    Built to handle one upload of one new version of a single shared package

    note on code structure:Can't first prep all the tables, then upload them.
    critical values are populated through the MySQL auto-incrementers on
    some of the SQL tables, which then need to be joined into prepped data.
    """

    def __init__(self, df, package_id, package_version_description,
                 wgt_group_cols,
                 conn_def='engine',
                 package_type_id=3):

        self.input_data = df
        self.package_id = package_id
        self.package_version_description = package_version_description
        self.package_type_id = package_type_id
        self.wgt_group_cols = wgt_group_cols
        self.conn_def = conn_def
        self.tables_uploaded = {}
        self.farthest_step = "Nowhere"

        req_cols = ['wgt_group_name', 'target_codes', 'wgt'] + wgt_group_cols
        missing_cols = []
        for req_col in req_cols:
            if req_col not in df.columns:
                missing_cols.append(req_col)
        assert len(missing_cols) == 0, \
            "Missing required columns {}".format(missing_cols)

        assert np.allclose(
            df.groupby('wgt_group_name')['wgt'].sum(), 1
        ), "Weights do not add to 1 across wgt group name"

        check_package_id = """
            SELECT * FROM rdp_sharedpackage WHERE shared_package_id = {}
        """.format(package_id)
        check_package_id = query(check_package_id, conn_def=conn_def)
        assert len(check_package_id) == 1, \
            "Did not find exactly 1 package id entry " \
            "for {}: \n{}".format(package_id, check_package_id)

    def run_table_upload(self, step_number, step_name, prep_function,
                         table_name, conn):
        """Run a table upload and track it's name and output"""

    def upload_to_db(self, user_input=True):
        """uploads the package_id to the database

        Assumes input_data global contains
        meta data and weights for the given package id. Saves farthest_step,
        list of dataframes containing uploaded tables.

        this method ensures the correct order of operations for the upload.

        KEY ASSUMPTIONS
            - You have one cause group per target
            - You do not have any OR clauses in your weight group logic

        [OPEN CONNECTION]
        [BEGIN TRANSACTION]
        1. add a new version using the given package id and description
        2. add new target cause groups to the database based on the targets
           in the input data (WILL ASSUME ONE CAUSE GROUP PER TARGET)
            a. add the actual targets using a 1:1 mapping to the new cause
               group ids created in the database (essentially, each target gets
               a random cause group id)
        3. add new weight groups to the database based on input data,
           associated with the version id in (1) in the database
            a. add new weight group logic sets that bridge between weight
               groups and weight group logic using version id from (1) to find
               weight group ids created in (3) (WILL ASSUME THERE ARE NO
               'OR' LOGIC CLAUSES IN THE WEIGHT GROUP)
            b. add new weight group logic using version id
               from (1) to find weight group logic sets created in (3a)
        4. map the input data to newly created weight group ids and cause
           group ids using the cause group names and the
           weight group names in the input data, and using the version id
           created in (1) to find both. Then upload to weights
           table. Do all of this with version id from (1)
        5. mark version metadata so that new version is best
        [COMMIT CHANGES]
        [CLOSE CONNECTION]

        """
        name_function_order = [
            ('1', 'Versions table', self.prep_version,
             'rdp_sharedpackageversion'),
            ('2', 'Cause groups table', self.prep_cause_groups,
             'rdp_sharedcausegroup'),
            ('2a', 'Targets table', self.prep_targets, 'rdp_sharedtarget'),
            ('3', 'Weight groups table', self.prep_weight_groups,
             'rdp_sharedwgtgroup'),
            ('3a', 'Weight logic set table', self.prep_weight_group_logic_set,
             'rdp_sharedwgtgrouplogicset'),
            ('3b', 'Weight logic table', self.prep_weight_group_logic,
             'rdp_sharedwgtgrouplogic'),
            ('4', 'Weights table', self.prep_weights, 'rdp_sharedwgt')
        ]
        # rows_expected = printExpectedRowAdditions(do_print=user_input)
        self.farthest_step = 'Nowhere'
        self.tables_uploaded = {}
        # start a transaction - if anything fails from this point out,
        # rollback all changes
        engine = get_engine(self.conn_def)
        conn = engine.connect()
        trans = conn.begin()
        try:
            for step_number, step_name, prep_function, table_name in \
                    name_function_order:

                print(
                    "[{t}] ({no}): {nm}".format(
                        t=str(datetime.now()), no=step_number, nm=step_name
                    )
                )
                # prep the data to upload
                df = prep_function()
                # upload it to the given table name
                RegressionUploader.upload_table_to_db(
                    df, table_name, conn
                )
                if step_number == '1':
                    self.new_version_id = self.get_new_pvid()
                    print("VERSION ID: {}".format(self.new_version_id))
                # add to appended tables
                self.tables_uploaded[step_number] = df
                self.farthest_step = step_name

            print 'uploading...'
            continue_upload = 'unknown'
            if user_input:
                while continue_upload != 'Y' and continue_upload != 'N':
                    continue_upload = raw_input(
                        'Should the new version be accepted?'
                        'Check that everything looks right above [Y/N]'
                    )
                    if continue_upload == 'Y':
                        print(
                            'Ok. Setting the old version'
                            'to old and the new version {n} to'
                            'best status'.format(n=self.new_version_id)
                        )
                        self.switch_best_flag(conn)
                        self.farthest_step = 'Flag switch'
                    elif continue_upload == 'N':
                        print(
                            'Got it. Check that out and in the '
                            'meantime I\'ll rollback everything '
                            'that was just uploaded.'
                        )
                        self.rollback_everything(engine)
                    else:
                        print(
                            'I\'m dumb and didnt understand your '
                            'input of \'{u}\'. Press either \'Y\' '
                            'or \'N\'. I\'ll keep asking \\    '
                            '                until either the end of '
                            'time or you give me a good '
                            'answer.'.format(u=continue_upload)
                        )
            else:
                # just check that
                # if rows uploaded equals rows expected
                if False:
                    self.switch_best_flag(conn)
                else:
                    # print(rows_expected)
                    # print(rows_uploaded)
                    print(
                        'rows uploaded didnt equal rows expected. '
                        'rolling back.'
                    )
                    self.rollback_everything(engine)
                    raise
            trans.commit()
            conn.close()
        except Exception, e:
            trans.rollback()
            conn.close()
            print(
                "ROLLING BACK: Got an {et}: {m}".format(et=type(e), m=str(e))
            )
            self.rollback_everything(engine)
            raise(e)
        finally:
            print("Final clean-up of registry...")
            cleanup_registry()
        self.farthest_step = 'Completion'
        print('\nDONE.')

    def prep_version(self):
        '''Prepare an upload to the shared package version table'''
        print("In prep version function")
        newv = pd.DataFrame(index=[0])
        newv['shared_package_version_description'] = \
            self.package_version_description
        newv['package_type_id'] = self.package_type_id
        newv['shared_package_id'] = self.package_id
        newv['shared_package_version_status_id'] = 4
        return newv

    def get_new_pvid(self):
        """Get the id associated with the new version"""
        q = '''
            SELECT shared_package_version_id
            FROM rdp_sharedpackage rsp
            INNER JOIN rdp_sharedpackageversion rspv
                USING(shared_package_id)
            WHERE
                shared_package_id = {pid}
                AND shared_package_version_status_id = 4
                AND shared_package_version_description = "{d}"
        '''.format(pid=self.package_id, d=self.package_version_description)
        pvid = query(q, conn_def=self.conn_def)
        error_text = """
            Did not find exactly one version for package_id {p}, description
            {d}:

            {df}
        """.format(p=self.package_id, d=self.package_version_description,
                   df=pvid)
        assert len(pvid) == 1, error_text
        return pvid.iloc[0, 0]

    def prep_cause_groups(self):
        df = self.input_data[['target_codes']]
        df = df.drop_duplicates()
        df['shared_package_version_id'] = self.new_version_id

        return df[['shared_package_version_id']]

    def prep_targets(self):

        q = """
            SELECT *
            FROM rdp_sharedcausegroup
            WHERE shared_package_version_id = {}
        """.format(self.new_version_id)
        cg_ids = query(q, conn_def=self.conn_def)

        df = self.input_data[['target_codes']]
        df = df.drop_duplicates().reset_index().drop('index', axis=1)

        # leverage same index of these two to combine
        # columns (if length is same, index will be same; surprises should
        # be caught in assertion)
        df = pd.concat([cg_ids, df], axis=1)
        null_vals = df[df['shared_group_id'].isnull()]
        if len(null_vals) > 0:
            raise AssertionError(
                """
                Found null values of shared_group_id:

                {}
                """.format(null_vals)
            )

        # fill in fluff
        df['error'] = 0
        df['warning'] = 0
        df['validated'] = 0

        return df[
            ['target_codes', 'error', 'warning',
             'validated', 'shared_group_id']
        ]

    def prep_weight_groups(self):
        '''Prep list of weight group names'''
        df = self.input_data[['wgt_group_name']].drop_duplicates()
        df['shared_package_version_id'] = self.new_version_id
        return df

    def prep_weight_group_logic_set(self):
        """Make a bunch of weight group logic set ids"""
        q = """
            SELECT shared_wgt_group_id
            FROM rdp_sharedwgtgroup
            WHERE shared_package_version_id = {}
        """.format(self.new_version_id)
        df = query(q, conn_def=self.conn_def)
        return df

    def prep_weight_group_logic(self):
        """Do the only hard part - create all the weight group logic

        This won't do the right thing if you have multiple logic sets per
        weight group name
        """
        q = """
            SELECT
                shared_wgt_group_logic_set_id, wgt_group_name
            FROM rdp_sharedwgtgrouplogicset
            INNER JOIN rdp_sharedwgtgroup USING(shared_wgt_group_id)
            WHERE shared_package_version_id = {}
        """.format(self.new_version_id)
        ls_ids = query(q, conn_def=self.conn_def)

        df = self.create_weight_group_logic()

        df = df.merge(ls_ids, on='wgt_group_name', how='right')
        # make sure variable column never missing
        assert df['variable'].notnull().all()
        # make sure weight group names match before and after
        assert set(df.wgt_group_name.unique()) == \
            set(ls_ids.wgt_group_name.unique())

        df = df[
            ['variable', 'operator', 'value',
             'shared_wgt_group_logic_set_id']]
        return df

    def prep_weights(self):
        '''Prep the actual weights'''
        cause_groups = self.get_cause_groups()[
            ['shared_group_id', 'target_codes']]
        weight_groups = self.get_weight_groups()[
            ['shared_wgt_group_id', 'wgt_group_name']]
        df = self.input_data.merge(
            cause_groups, on='target_codes',
            how='outer', indicator=True
        )
        assert (df['_merge'] == "both").all()
        df = df.drop('_merge', axis=1)
        df = df.merge(
            weight_groups, on='wgt_group_name',
            how='outer', indicator=True
        )
        assert (df['_merge'] == "both").all()
        df = df.drop('_merge', axis=1)

        df = df[['wgt', 'shared_group_id', 'shared_wgt_group_id']]

        # make sure weights add up to 1, still
        assert np.allclose(df.groupby('shared_wgt_group_id')['wgt'].sum(), 1)

        return df

    def get_weight_groups(self):
        q = '''
            SELECT *
            FROM rdp_sharedwgtgroup
            WHERE shared_package_version_id={pvid}
        '''.format(pvid=self.new_version_id)
        return query(q, conn_def=self.conn_def)

    def get_cause_groups(self):
        q = '''
            SELECT *
            FROM rdp_sharedcausegroup
            INNER JOIN rdp_sharedtarget
                USING(shared_group_id)
            WHERE shared_package_version_id={pvid}
        '''.format(pvid=self.new_version_id)
        return query(q, conn_def=self.conn_def)

    def create_weight_group_logic(self):
        '''return a dataset with new weight logic from the input data

        Does the following:
            * determines if these are region or country weights
            * creates the weight group name from the location, sex, age, year
            * reshapes such that the variable name(location/sex/age) and
                variable value are long
            * infers the operator based on assumed structure of the data
            * cleans the value to make it a number if needed

        This method is highly dependent on input data structure, but is meant
        to fail with a
        helpful message if the structure is unexpected.
        '''
        # set variables that will have ranges
        range_vars = ['age', 'year_id']

        # collapse to just weight groups
        df = self.input_data[
            ['wgt_group_name'] + self.wgt_group_cols
        ].drop_duplicates()

        # reshape long
        df = pd.melt(
            df,
            id_vars='wgt_group_name',
            value_vars=self.wgt_group_cols,
            var_name='variable',
            value_name='value'
        )
        df['num'] = 1
        df_tmp = df[
            (df['variable'].isin(range_vars)) &
            (df['value'].str.contains('-'))
        ]
        df_tmp['num'] = 2
        df = df.append(df_tmp)
        df['operator'] = df.apply(
            lambda x: self.infer_operator(x, range_vars), axis=1)
        df['value'] = df.apply(
            lambda x: self.infer_value(x), axis=1)
        df = df[[
            'wgt_group_name', 'variable', 'operator', 'value']]
        assert len(df) == len(df.drop_duplicates()), \
            'duplicates in weight logic result'
        return df.sort_values(by='wgt_group_name')

    def infer_operator(self, x, range_vars):
        if x['variable'] not in range_vars:
            return '=='
        else:
            if '<' in x['value']:
                return '<'
            elif '-' in x['value'] and x['num'] == 2:
                return '<='
            elif '+' in x['value'] or ('-' in x['value'] and x['num'] == 1):
                return '>='
            else:
                assert False, 'cant understand the range value {}'.format(x)
                return 'FAIL'

    def infer_value(self, x):
        if x['variable'] == 'region':
            return "'{}'".format(x['value'])
        elif x['variable'] == 'country':
            # may have to do some cleaning here in the future
            return "'{}'".format(x['value'])
        elif x['variable'] == 'sex':
            if x['value'] == 'Female':
                return 2
            else:
                assert x['value'] == 'Male', 'unrecognized sex: {s}'.format(
                    s=x['value'])
                return 1
        elif x['variable'] in ['year_id', 'age']:
            match_num = re.compile('[0-9\.]+')
            nums_in_x = match_num.findall(x['value'])
            if x['num'] == 2:
                return nums_in_x[1]
            else:
                assert x['num'] == 1, \
                    'found num other than 1 or 2: {n}'.format(n=x['num'])
                return nums_in_x[0]
        else:
            raise AssertionError('unrecognized variable: {v}'.format(
                v=x['variable']))

    @staticmethod
    def upload_table_to_db(df, tbl, conn):
        '''
            Unfortunately (seriously so unfortunately) pandas doesnt support
            nested queries in df.to_sql() So thats whats up with all the
            rollback functions. Putting a transaction around this does nothing
            because the to_sql is its own transaction that it creates
            and you can't create one for it.
        '''
        df.to_sql(tbl, conn.engine, if_exists='append', index=False)
        print("Successfully uploaded {} rows to {}".format(len(df), tbl))

    def mark_old_versions(self, conn, end_date):
        """Mark other versions old"""
        mark_status = '''
            UPDATE rdp_sharedpackageversion
            SET shared_package_version_status_id=1
            WHERE
                shared_package_id = {pid}
                AND shared_package_version_id != {npvid}
        '''.format(pid=self.package_id, npvid=self.new_version_id)
        mark_end_date = '''
            UPDATE rdp_sharedpackageversion
            SET end_date = '{ed}'
            WHERE
                shared_package_id = {pid}
                AND shared_package_version_id != {npvid}
        '''.format(pid=self.package_id, npvid=self.new_version_id, ed=end_date)

        res = conn.execute(mark_status)
        print("Updated status to 1 for {} rows".format(res.rowcount))
        res = conn.execute(mark_end_date)
        print("Updated end date to {} for {} rows".format(
            end_date, res.rowcount))

    def mark_new_version(self, conn, start_date):
        """Mark the new package version metadata"""
        mark_status = '''
            UPDATE rdp_sharedpackageversion
            SET shared_package_version_status_id=2
            WHERE
                shared_package_version_id = {npvid}
        '''.format(npvid=self.new_version_id)
        mark_start_date = '''
            UPDATE rdp_sharedpackageversion
            SET start_date = '{sd}'
            WHERE
                shared_package_version_id = {npvid}
        '''.format(npvid=self.new_version_id, sd=start_date)

        res = conn.execute(mark_status)
        print("Updated status to 2 for {} rows".format(res.rowcount))
        res = conn.execute(mark_start_date)
        print(
            "Updated start date to {} for {} rows".format(
                start_date, res.rowcount)
        )

    def switch_best_flag(self, conn):

        # standardized time for end date of old versions and start date of new
        curr_time = str(datetime.now())

        # mark old versions
        self.mark_old_versions(conn, curr_time)

        # mark new version
        self.mark_new_version(conn, curr_time)

    def print_expected_row_additions(self, do_print=1):
        """Print number of expected rows to add"""
        input_data = self.input_data.copy()
        step1 = 1
        step2 = len(input_data.target_codes.unique())
        step2a = len(input_data.target_codes.unique())

        if 'country' in input_data.columns:
            unique_wgt_groups = input_data[[
                'country', 'sex', 'age']].drop_duplicates()
        elif 'region' in input_data.columns:
            unique_wgt_groups = input_data[[
                'region', 'sex', 'age']].drop_duplicates()
        else:
            assert False, 'dont recognize this input data structure'
        step3 = len(unique_wgt_groups)
        step3a = len(unique_wgt_groups)
        # adds one row per sex, because there is one sex logic value
        # for each weight logic set
        sex_logic_count = len(unique_wgt_groups)
        # adds one row per country, for same reason
        loc_logic_count = len(unique_wgt_groups)
        # adds one row per age unless the age contains '-', then two
        age_btwn_count = len(
            unique_wgt_groups[unique_wgt_groups.age.str.contains('-')])
        age_notbtwn_count = len(unique_wgt_groups) - age_btwn_count
        age_logic_count = 2 * age_btwn_count + age_notbtwn_count
        logic_count = age_logic_count + loc_logic_count + sex_logic_count
        step3b = logic_count
        step4 = len(input_data)
        if do_print == 1:
            print('\nIt is possible from the input data alone to infer the '
                  'number of rows that can be inserted.')
            print('Pay attention to the numbers below to see that the upload '
                  'was successful in doing what is expected.')
            print('[step number]: expected rows inserted')
            print('[1]:', step1, 'rows')
            print('[2]:', step2, 'rows')
            print('[2a]:', step2a, 'rows')

            print('[3]:', step3, 'rows')
            print('[3a]:', step3a, 'rows')
            print('[3b]:', step3b, 'rows')
            print('[4]:', step4, 'rows')
            print('==============================')
            print('now make sure all the inserts align with those numbers. '
                  'call rollbackBadPvid(your new package version id) if not.')
            print('')
        return {
            '1': step1,
            '2': step2,
            '2a': step2a,
            '3': step3,
            '3a': step3a,
            '3b': step3b,
            '4': step4
        }

    def rollback_everything(self, engine):
        delete_package_version(self.new_version_id, engine,
                               conn_def=self.conn_def)

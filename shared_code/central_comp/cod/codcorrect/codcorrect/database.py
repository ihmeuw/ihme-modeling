import os
import pandas as pd
import getpass
import subprocess
import datetime

from db_queries.get_location_metadata import get_location_metadata
from db_tools.ezfuncs import query, get_session
from db_tools.query_tools import exec_query
from db_tools.loaders import Inserts, Infiles


def get_best_model_version(gbd_round):
    """ Get the list of best models for a given GBD round except for shock
        aggregator models
    """
    sql_statement = """SELECT
                        gbd_round,
                        model_version_id,
                        cause_id,
                        sex_id,
                        model_version_type_id,
                        is_best
                       FROM
                        cod.model_version
                       JOIN shared.cause_set_version
                       USING (cause_set_version_id)
                       WHERE
                        is_best = 1 AND
                        gbd_round = {gbd_round} AND
                        model_version_type_id IN (0, 1, 2, 3, 4, 6, 10);
                    """.format(gbd_round=gbd_round)
    result_df = query(sql_statement, conn_def='cod')
    # certain best models with a mvt of 10 come out of OldCorrect and are
    # duplicates for the same cause with a mvt of 3. Keep the OldCorrect ones
    mvt_10 = result_df.ix[result_df['model_version_type_id'] == 10,
                          'cause_id'].unique()
    oldc = result_df.ix[(result_df.cause_id.isin(mvt_10) &
                        (result_df.model_version_type_id != 3))]
    result_df = result_df.ix[~result_df.cause_id.isin(mvt_10)]
    return pd.concat([result_df, oldc]).reset_index(drop=True)


def get_best_shock_models(gbd_round):
    """
    Get list of models for a given GBD round used in the shock aggregator """
    sql_statement = """ SELECT
                            gr.gbd_round,
                            mv.model_version_id,
                            mv.cause_id,
                            mv.sex_id,
                            mv.model_version_type_id,
                            mv.is_best
                        FROM
                            cod.shock_version sv
                        JOIN
                            cod.shock_version_model_version svmv
                        USING (shock_version_id)
                        JOIN
                            cod.model_version mv USING (model_version_id)
                        JOIN
                            shared.gbd_round gr
                            ON (gr.gbd_round_id=mv.gbd_round_id)
                        WHERE
                            shock_version_status_id = 1 AND
                            gr.gbd_round = {gbd_round};
                    """.format(gbd_round=gbd_round)
    result_df = query(sql_statement, conn_def='cod')
    return result_df


def get_best_envelope_version(gbd_round):
    """ Get best envelope version """
    sql_statement = """ SELECT
                            run_id
                        FROM
                            mortality.process_version mv
                        JOIN
                            shared.gbd_round gr
                            USING(gbd_round_id)
                        WHERE
                            process_id = 12
                            AND gbd_round = {round}
                            AND status_id = 5;""".format(round=gbd_round)
    result_df = query(sql_statement, conn_def='mortality')
    if len(result_df) > 1:
        exception_text = ('This returned more than 1 envelope version: '
                          '({returned_ids})'.format(returned_ids=", ".join(
                              str(v) for v in result_df['run_id']
                              .drop_duplicates().to_list())))
        raise LookupError(exception_text)
    elif len(result_df) < 1:
        raise LookupError("No envelope versions returned")
    return result_df.ix[0, 'run_id']


def get_cause_hierarchy_version(cause_set_id, gbd_round):
    """ Get the IDs associated with best version of a cause set

    This function will return the following integers:
        cause_set_version_id
        cause_metadata_version_id
    """
    sql_statement = """SELECT
    cause_set_version_id, cause_metadata_version_id
    FROM
        shared.cause_set_version_active csva
    JOIN
        shared.cause_set_version csv USING (cause_set_version_id)
    WHERE
        csv.gbd_round = {round}
    AND csva.cause_set_id = {id}""".format(round=gbd_round, id=cause_set_id)
    result_df = query(sql_statement, conn_def='cod')

    if len(result_df) > 1:
        exception_text = ('This returned more than 1 cause_set_version_id '
                          '({returned_ids})').format(returned_ids=", ".join(
                              str(v) for v in result_df['cause_set_version_id']
                              .drop_duplicates().tolist()))
        raise LookupError(exception_text)
    elif len(result_df) < 1:
        raise LookupError("No cause set versions returned")
    return (result_df.ix[0, 'cause_set_version_id'],
            result_df.ix[0, 'cause_metadata_version_id'])


def get_cause_hierarchy(cause_set_version_id):
    ''' Return a DataFrame containing cause hierarchy table

        Arguments: cause set version id
        Returns: DataFrame
    '''

    sql_statement = """ SELECT
                          cause_id, acause, level, parent_id, sort_order,
                          most_detailed
                        FROM
                          shared.cause_hierarchy_history
                        WHERE
                          cause_set_version_id = {cause_set_version_id};
                    """.format(cause_set_version_id=cause_set_version_id)
    result_df = query(sql_statement, conn_def='cod')
    return result_df


def get_cause_metadata(cause_metadata_version_id):
    ''' Returns a dict containing cause ids as keys and cause metadata as a
        nested dict

        Arguments: cause metadata version id
        Returns: nested dict
    '''
    sql_statement = """
                    SELECT
                      cause_id, cause_metadata_type, cause_metadata_value
                    FROM
                      shared.cause_metadata_history
                    JOIN
                      shared.cause_metadata_type USING (cause_metadata_type_id)
                    WHERE
                      cause_metadata_version_id = {cause_metadata_vid};
                    """.format(cause_metadata_vid=cause_metadata_version_id)
    result_df = query(sql_statement, conn_def='cod')
    result_dict = {}
    for i in result_df.index:
        id = result_df.ix[i, 'cause_id']
        metadata_type = result_df.ix[i, 'cause_metadata_type']
        metadata_value = result_df.ix[i, 'cause_metadata_value']
        if id not in result_dict:
            result_dict[id] = {}
        result_dict[id][metadata_type] = metadata_value
    return result_dict


def get_location_hierarchy(location_set_id):
    result_df = get_location_metadata(location_set_id)
    result_df = result_df[['location_id', 'parent_id', 'level', 'is_estimate',
                           'most_detailed', 'sort_order']]
    return result_df


def get_age_weights():
    sql_query = """
        SELECT
            age_group_id,
            age_group_weight_value
        FROM
            shared.age_group_weight agw
        JOIN
            shared.gbd_round USING (gbd_round_id)
        WHERE
            gbd_round = 2016
        AND
          age_group_weight_description = 'IHME standard age weight';
          """
    age_standard_data = query(sql_query, conn_def='cod', dispose=True)
    return age_standard_data


def get_age_dict():
    agg_sql_query = """
        SELECT
            age_group_id, age_group_years_start AS age_start,
            age_group_years_end AS age_end
        FROM
            shared.age_group_set_list
        INNER JOIN
            shared.age_group USING (age_group_id)
        WHERE
            age_group_set_id = 16
            and is_aggregate=1
            and age_group_id != 27
            and age_group_id != 235;
            """
    agg_ages = query(agg_sql_query, conn_def='cod', dispose=True)
    age_dict = {}
    for age in agg_ages.age_group_id.unique():
        age_start = int(agg_ages.ix[agg_ages['age_group_id'] == age,
                        'age_start'])
        age_end = int(agg_ages.ix[agg_ages['age_group_id'] == age, 'age_end'])
        sql_query = """
        SELECT
            age_group_id
        FROM
            shared.age_group_set_list
        INNER JOIN
            shared.age_group ag USING (age_group_id)
        WHERE
            age_group_set_id = 16
            AND age_group_years_start >= {start}
            AND age_group_years_end <= {end}
            AND (is_aggregate = 0 OR age_group_id = 235)
        """.format(start=age_start, end=age_end)
        disagg_ages_df = query(sql_query, conn_def='cod', dispose=True)
        disagg_ages = disagg_ages_df.age_group_id.unique().tolist()
        age_dict[age] = disagg_ages
    return age_dict


def get_spacetime_restrictions(gbd_round):
    sql_query = """
        SELECT
            rv.cause_id,
            r.location_id,
            r.year_id
        FROM
            codcorrect.spacetime_restriction r
        JOIN
            codcorrect.spacetime_restriction_version rv
                USING (restriction_version_id)
        WHERE
            rv.is_best = 1 AND
            rv.gbd_round = {};""".format(gbd_round)
    spacetime_restriction_data = query(sql_query, conn_def='codcorrect')
    return spacetime_restriction_data


def is_existing_output_version(output_version_id, conn_def):
    # Check to see if row exists
    sql_statement = """ SELECT
                            output_version_id
                        FROM
                            cod.output_version
                        WHERE
                            output_version_id = {}""".format(output_version_id)
    result = query(sql_statement, conn_def=conn_def)
    if len(result) == 0:
        return False
    else:
        return True


def new_CoDCorrect_version():
    sql_statement = """ SELECT
                            MAX(output_version_id) as max_id
                        FROM
                            cod.output_version
                        """
    result = query(sql_statement, conn_def='cod').ix[0, 'max_id']
    return result + 1


def create_new_output_version_row(output_version_id, description,
                                  envelope_version_id, conn_def, status=2):
    if not is_existing_output_version(output_version_id, conn_def):
        # Create a row to input
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ins_df = pd.DataFrame({'output_version_id': output_version_id,
                               'description': description,
                               'code_version': 4,
                               'env_version': envelope_version_id,
                               'status': status, 'is_best': 0,
                               'date_inserted': now,
                               'inserted_by': getpass.getuser(),
                               'last_updated': now,
                               'last_updated_by': getpass.getuser(),
                               'last_updated_action': "INSERT"},
                              columns=['output_version_id', 'description',
                                       'code_version', 'env_version', 'status',
                                       'is_best',
                                       'date_inserted', 'inserted_by',
                                       'last_updated', 'last_updated_by',
                                       'last_updated_action'],
                              index=[0])
        # Insert row
        ins = Inserts(table='output_version', schema='cod', insert_df=ins_df)
        sesh = get_session(conn_def)
        ins.insert(sesh, commit=True)


def create_new_compare_version(gbd_round_id, description, conn_def, status):
    # Create a row to input
    ins_df = pd.DataFrame([{'gbd_round_id': gbd_round_id,
                            'compare_version_description': description,
                            'compare_version_status_id': status}])
    ins = Inserts(table='compare_version', schema='gbd', insert_df=ins_df)
    sesh = get_session(conn_def)
    ins.insert(sesh, commit=True)
    # pull down the new compare version
    compare_vers_query = ('SELECT MAX(compare_version_id) as compare_version '
                          'FROM gbd.compare_version '
                          'WHERE gbd_round_id={round} '
                          'AND compare_version_status_id={status}'
                          .format(round=gbd_round_id, status=status))
    return query(compare_vers_query,
                 conn_def=conn_def).ix[0, 'compare_version']


def create_new_process_version(gbd_round_id, gbd_process_version_note,
                               compare_version_id, conn_def):
    git_dir = '--git-dir=' + os.path.join(
        os.path.dirname(os.path.dirname(__file__)), '.git')
    work_tree = '--work-tree=' + os.path.dirname(os.path.dirname(__file__))
    code_vers = str(subprocess.check_output(["git", git_dir, work_tree,
                                             "rev-parse", "HEAD"])
                    ).rstrip('\n')
    make_process_vers = ('CALL gbd.new_gbd_process_version({round}, 3, '
                         '"{note}", "{code_vers}", {comp_vers}, NULL)'
                         .format(round=gbd_round_id,
                                 note=gbd_process_version_note,
                                 code_vers=code_vers,
                                 comp_vers=compare_version_id))
    process_vers = query(make_process_vers, conn_def=conn_def)
    return int(process_vers.ix[0, 'v_return_string'].split()[2]
               .replace('"', "").replace(",", ""))


def add_metadata_to_process_version(process_version_id, output_version_id,
                                    lifetable_version_id, envelope_version_id,
                                    pop_version_id, conn_def):
    sesh = get_session(conn_def)
    query = ('INSERT INTO gbd.gbd_process_version_metadata ('
             'gbd_process_version_id, metadata_type_id, val) '
             'VALUES '
             '({pv_id}, 1, "{ov_id}"), '
             '({pv_id}, 3, "{ltv_id}"), '
             '({pv_id}, 6, "{env_id}"), '
             '({pv_id}, 7, "{popv_id}")').format(
                 pv_id=process_version_id, ov_id=output_version_id,
                 ltv_id=lifetable_version_id, env_id=envelope_version_id,
                 popv_id=pop_version_id)
    exec_query(query, session=sesh, close=True)


def add_process_version_to_compare_version_output(process_version_id,
                                                  compare_version_id,
                                                  conn_def):
    sesh = get_session(conn_def)
    add_process_vers_query = ('CALL gbd.add_process_version_to_compare_version'
                              '_output ({}, {})'.format(compare_version_id,
                                                        process_version_id))
    exec_query(add_process_vers_query, session=sesh, close=True)


def wipe_diagnostics():
    # Create a connection
    sesh = get_session("codcorrect")
    unmark_best = ('UPDATE codcorrect.diagnostic_version SET is_best = 0 '
                   'WHERE is_best = 2;')
    exec_query(unmark_best, session=sesh, close=True)
    unmark_best = ('UPDATE codcorrect.diagnostic_version SET is_best = 2 '
                   'WHERE is_best = 1;')
    exec_query(unmark_best, session=sesh, close=True)


def new_diagnostic_version(output_version_id):
    # Create values to insert
    ins_df = pd.DataFrame([{'output_version_id': output_version_id,
                            'is_best': 1}])
    # Insert row
    ins = Inserts(table='diagnostic_version', schema='codcorrect',
                  insert_df=ins_df)
    sesh = get_session("codcorrect")
    ins.insert(sesh, commit=True)


def upload_diagnostics(parent_dir):
    sesh = get_session('codcorrect')
    inf = Infiles(table='diagnostic', schema='codcorrect', session=sesh)
    inf.infile(path='FILEPATH.csv', with_replace=True, commit=True)


def upload_cod_summaries(directories, conn_def):
    sesh = get_session(conn_def)
    exec_query("set unique_checks= 0", sesh)
    inf = Infiles(table='output', schema='cod', session=sesh)
    for directory in directories:
        inf.indir(path=directory, with_replace=True, partial_commit=True,
                  commit=True)


def upload_gbd_summaries(process_version, conn_def, directories):
    if any('multi' in d for d in directories):
        table = 'output_cod_multi_year_v{pv}'.format(pv=process_version)
    else:
        table = 'output_cod_single_year_v{pv}'.format(pv=process_version)
    sesh = get_session(conn_def)
    inf = Infiles(table=table, schema='gbd', session=sesh)
    for directory in directories:
        print "Uploading from {}".format(directory)
        inf.indir(path=directory, with_replace=True, partial_commit=True,
                  commit=True)
    return "Uploaded"


def update_status(output_version_id, status, conn_def):
    # Update status
    sesh = get_session(conn_def)
    update_cmd = """UPDATE
                        cod.output_version
                    SET
                        status = {s}
                    WHERE
                        output_version_id = {v}""".format(s=status,
                                                          v=output_version_id)
    exec_query(update_cmd, session=sesh, close=True)


def unmark_cod_best(output_version_id, cod_conn_def):
    now = datetime.datetime.now()
    cod_sesh = get_session(cod_conn_def)
    update_cod_cmd = """UPDATE cod.output_version SET
                        is_best = 0,
                        best_end = '{now}'
                    WHERE
                        output_version_id != {v}
                        AND best_start IS NOT NULL
                        AND best_end IS NULL""".format(now=now,
                                                       v=output_version_id)
    exec_query(update_cod_cmd, session=cod_sesh, close=True)


def unmark_gbd_best(output_version_id, process_version_id, gbd_conn_def):
    gbd_sesh = get_session(gbd_conn_def)
    update_gbd_cmd = """UPDATE
                        gbd.gbd_process_version
                        SET gbd_process_version_status_id = 2
                        WHERE gbd_process_version_id != {v}
                        AND gbd_round_id = 4
                        AND gbd_process_version_status_id = 1
                        AND gbd_process_id = 3
                        """.format(v=process_version_id)
    exec_query(update_gbd_cmd, session=gbd_sesh, close=True)


def mark_cod_best(output_version_id, cod_conn_def):
    now = datetime.datetime.now()
    cod_sesh = get_session(cod_conn_def)
    update_cod_cmd = """UPDATE
                        cod.output_version
                    SET
                        is_best = 1,
                        best_start = '{now}'
                    WHERE
                        output_version_id = {v}""".format(now=now,
                                                          v=output_version_id)
    exec_query(update_cod_cmd, session=cod_sesh, close=True)


def mark_gbd_best(output_version_id, process_version_id, gbd_conn_def):
    gbd_sesh = get_session(gbd_conn_def)
    update_gbd_cmd = """UPDATE
                        gbd.gbd_process_version
                        SET gbd_process_version_status_id = 1
                        WHERE gbd_process_version_id = {v}
                        """.format(v=process_version_id)
    exec_query(update_gbd_cmd, session=gbd_sesh, close=True)

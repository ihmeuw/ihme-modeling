"""
Helper funcs to create MySQL queries for our estimate tables

Shouldn't be concerned with the connections, just the query statements
"""
import pandas as pd
import warnings
import sys


sys.path.append("FILENAME")
import test_clinical_funcs

def table_name_fixer(table):
    """
    Input is the simplified form of our tables ie, 'intermediate', 'icg', 'bundle'
    Output is the actual table name for the MySQL engine
    """
    if 'intermediate' in table:
        new_table = 'SQL'
    elif 'icg' in table:
        new_table = 'SQL'
    elif 'bundle' in table:
        new_table = 'SQL'
    else:
        assert False, "We couldn't find the actual estimate table name"

    return new_table


def get_col_names(table):
    """
    Given a table name for 1 of our 3 estimates this returns the columns which should be pulled from said table
    """
    cols = "age_group_id, sex_id, location_id"
    final_cols = "mean, upper, lower, sample_size, cases"
    cols2 = "diagnosis_id, representative_id, source_type_id, run_id"

    if table == 'SQL':
        cols += ", year_id, estimate_id, icg_id, val, {}, nid".format(cols2)

    elif table == 'SQL':
        cols += ", year_id, estimate_id, icg_id, {}, {}, nid".format(final_cols, cols2)

    elif table == 'SQL':
        cols += ", year_start, year_end, estimate_id, bundle_id, {}, {}, merged_nid".format(final_cols, cols2)
    else:
        msg = "The {} table isn't recognized. {}".format(table, table_name_fixer(table))
        assert False, msg

    return cols


def get_table_name():


    return


def gen_where_clause(**kwargs):
    """
    where condition generator - Takes a column name and a set of values and
    creates the "where" condition for a sql query

    kwargs needs to be a column name and a set of values. Values must be int or tuple
        eg gen_where_clause(location_id=545, age_group_id=(1, 2, 3))
        returns the str 'WHERE age_group_id IN (1, 2, 3) and location_id = 545'
    """
    q = "WHERE "


    no_cause_q = ""
    counter = 0

    if len(kwargs) > 0:
        for key, value in kwargs.iteritems():
            counter += 1
            if isinstance(value, tuple):
                state = "IN"
            elif isinstance(value, int):
                state = "="
            else:
                assert False, "vals must be either int or tuple, no exceptions"
            q += "{} {} {}".format(key, state, value)
            if counter != len(kwargs):
                q += " and "
    else:
        q = ""
        no_cause_q = ""


    return q, no_cause_q


def agg_est_query(table, run_id, gbd_round_id, **kwargs):
    """
    This will pull together the other sql query generation pieces to create a final
    MySQL query for our intermediate/final databases.

    The human readable part is separate from this. We're just pulling estimates here
    """
    table = table_name_fixer(table)
    cond, no_cause_cond = gen_where_clause(**kwargs)
    cols = get_col_names(table)


    query = "SQL".\
                format(cols=cols, table=table, cond=cond)

    if run_id == 'latest':
        query = """SQL""".\
            format(query, gbd_round_id)
    elif run_id == 'best':
        query = """SQL""".format(query, gbd_round_id)
    else:
        assert isinstance(int(run_id), int)
        query = query + " and run_id = {}".format(run_id)

    test_clinical_funcs.test_agg_query(query)
    return query, cond


def named_query_gen(col, cond):
    """
    Input is a column and a condition from a where clause. This outputs a query to get the table
    we need to merge on human interpretable names but this could really just be done in sql

    Although the bundle table exists on a different database

    Select {cols} from {table} {cond}
    """
    table_d = {}
    counter = 0
    for key in table_d.keys():
        if col in table_d[key]:
            table = key
            counter += 1

    q = """SQL""".format(table_d[table], table, cond)

    assert counter == 1,\
        "Too many or too few loops over the dictionary, we expect exactly 1 trip of the if statement"

    return q


def agg_name_query(**kwargs):
    """
    Outputs a dictionary with an id col as a key as a query as a value
    """

    cond, no_cause_cond = gen_where_clause(**kwargs)

    print("add 'icg_id' to the list below when we get the clinical_mapping db onto the clinical server")
    id_cols = ['location_id', 'bundle_id', 'age_group_id', 'estimate_id']

    q_cols = []
    for col in id_cols:
        q = named_query_gen(col, no_cause_cond)
        q_cols.append(q)

    res = dict(zip(id_cols, q_cols))
    return res

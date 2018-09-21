import gc
import pandas as pd
from db_tools.ezfuncs import query

def get_best_envelope_version(gbd_round=4):
    """ Get best envelope version """
    sql_statement = """ SELECT
                            run_id
                        FROM
                            mortality.process_version mv
                        WHERE
                            process_id = 12
                            AND gbd_round_id = {round}
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

def read_hdf_draws(draws_filepath, year_id, key="draws", filter_sexes=None,
                   filter_ages=None, filter_locations=None):
    """ Read in model draws
    Read in hdf draws from a given filepath and
    filter by year_id.
    """
    # Get data
    where_clause = ["year_id=={year_id}"
                    .format(year_id=year_id)]
    dfs = []
    for chunk in pd.read_hdf(draws_filepath, key=key, where=where_clause,
                             chunksize=20000):
        # Filter if necessary
        if filter_sexes and 'sex_id' in chunk.columns:
            chunk = chunk.ix[chunk['sex_id'].isin(filter_sexes)]
        if filter_ages and 'age_group_id' in chunk.columns:
            chunk = chunk.ix[chunk['age_group_id'].isin(filter_ages)]
        if filter_locations and 'location_id' in chunk.columns:
            chunk = chunk.ix[chunk['location_id'].isin(filter_locations)]
        dfs.append(chunk)
        del chunk
        gc.collect()
    data = pd.concat(dfs)
    # Return data
    return data
    
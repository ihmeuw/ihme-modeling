from db_tools.ezfuncs import query

def parse_desc(df):
    ##########################################
    #Parses an oldCorrect model description
    #to get version number
    ##########################################
    version = int(filter(str.isdigit, str(df['description'])))
    return version

def get_new_vers(gbd_round_id=4):
    ##########################################
    #Returns an integer one higher than the
    #most recent version of oldCorrect
    ##########################################
    q = '''
        SELECT *
        FROM cod.model_version 
        WHERE (sex_id, model_version_id) IN 
            (SELECT sex_id, MAX(model_version_id)
            FROM cod.model_version
            WHERE gbd_round_id = {gbd_round}
            AND model_version_type_id = 9
            AND is_best = 1
            GROUP BY sex_id)
        '''.format(gbd_round=gbd_round_id)
    old_vers_df = query(q, conn_def='cod')
    if len(old_vers_df) > 0:
        old_vers_df['description_parsed'] = old_vers_df.apply(parse_desc,
                                                                axis=1)
        old_vers = max(old_vers_df['description_parsed'].unique().tolist())
        return int(old_vers + 1)
    else:
        return 1
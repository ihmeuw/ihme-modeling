from db_tools.ezfuncs import query
from db_queries import get_cause_metadata

def pull_mvid(cause_id, model_type, gbd_round=4, sex=None):
    ##########################################
    #Pull best model version id, given a cause
    #and a model type. Optionally takes round
    #and sex as well.
    ##########################################
    if sex is not None:
        assert sex in [1, 2], 'Sex must be in [1, 2]'
        sex_append = 'AND sex_id = {sex}'.format(sex=sex)
    else:
        sex_append = ''
    q = '''
        SELECT model_version_id
        FROM cod.model_version
        WHERE cause_id = {cause_id}
        AND model_version_type_id = {model_type}
        AND is_best = 1
        AND gbd_round_id = {gbd_round}
        {sex_append}
    '''.format(cause_id=cause_id, model_type=model_type,
              gbd_round=gbd_round, sex_append=sex_append)
    mvid = query(q, conn_def='cod')
    return mvid['model_version_id'].unique().tolist()

def get_cause_ids(cause_set):
    ##########################################
    #Fetches list of source and target causes
    #given the oldCorrect cause set
    ##########################################
    cause_df = get_cause_metadata(cause_set)

    detail_bool = cause_df['most_detailed'] == 1
    sources_bool = (cause_df['parent_id'] == 952) & detail_bool
    targets_bool = (cause_df['parent_id'] == 953) & detail_bool
    
    sources = cause_df.loc[sources_bool, 'cause_id'].unique().tolist()
    targets = cause_df.loc[targets_bool, 'cause_id'].unique().tolist()    
    return sources, targets
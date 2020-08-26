from db_tools import ezfuncs


def get_rei_type_id_df():
    q = "SELECT rei_id, rei_type_id FROM shared.rei "
    rei_type_id_df = ezfuncs.query(q, conn_def='CONN_DEF').squeeze()

    return rei_type_id_df

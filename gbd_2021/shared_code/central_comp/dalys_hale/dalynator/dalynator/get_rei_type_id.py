from db_tools import ezfuncs


def get_rei_type_id_df():
    q = "SELECT rei_id, rei_type_id FROM shared.rei "
    rei_type_id_df = ezfuncs.query(q, conn_def='COD_DATABASE').squeeze()

    return rei_type_id_df

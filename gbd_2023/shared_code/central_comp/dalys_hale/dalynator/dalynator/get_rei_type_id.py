from gbd import conn_defs

from dalynator.db import select_to_df


def get_rei_type_id_df():
    q = "SELECT rei_id, rei_type_id FROM shared.rei "
    rei_type_id_df = select_to_df(query=q, conn_def=CONN_DEF)

    return rei_type_id_df

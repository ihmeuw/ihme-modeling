from db_tools import ezfuncs


##############################################################################
# location stuff
##############################################################################


def get_current_location_set(loc_set_id=35, gbd_round_id=5):
    """return the most detailed location ids from the computation set"""
    v = ezfuncs.query(
        "SELECT shared.active_location_set_version({ls},{gbd}) AS v".format(
            ls=loc_set_id, gbd=gbd_round_id),
        conn_def="shared"
    )["v"].item()
    loc_df = ezfuncs.query(
        "call shared.view_location_hierarchy_history({v})".format(v=v),
        conn_def="shared")
    return loc_df


def get_most_detailed_location_ids(loc_set_id=35, gbd_round_id=5):
    loc_df = get_current_location_set(loc_set_id, gbd_round_id)
    loc_ids = loc_df.loc[loc_df["most_detailed"] == 1, "location_id"].tolist()
    return loc_ids


##############################################################################
# age stuff
##############################################################################

def get_age_group_set(age_group_set_id):
    q = """
    SELECT
        age_group_id, age_group_years_start, age_group_years_end
    FROM
        DATABASE
    JOIN
        DATABASE USING (age_group_id)
    WHERE
        age_group_set_id = {age_group_set_id}
    """.format(age_group_set_id=age_group_set_id)
    age_df = ezfuncs.query(q, conn_def="shared")
    return age_df

from db_tools import ezfuncs


##############################################################################
# location stuff
##############################################################################


def get_current_location_set(loc_set_id={LOCATION SET ID}, gbd_round_id={GBD ROUND ID}):
    """return the most detailed location ids from the computation set"""
    v = ezfuncs.query(
        "SELECT {DATABASE}({ls},{gbd}) AS v".format(
            ls=loc_set_id, gbd=gbd_round_id),
        conn_def="shared"
    )["v"].item()
    loc_df = ezfuncs.query(
        "call {DATABASE}({v})".format(v=v),
        conn_def={CONN DEF})
    return loc_df


def get_most_detailed_location_ids(loc_set_id={LOCATION SET ID}, gbd_round_id={GBD ROUND ID}):
    loc_df = get_current_location_set(loc_set_id, gbd_round_id)
    loc_ids = loc_df.ix[loc_df["most_detailed"] == {MOST DETAILED}, "location_id"].tolist()
    return loc_ids


##############################################################################
# location stuff
##############################################################################

def get_age_group_set(age_group_set_id):
    q = """
    SELECT
        age_group_id, age_group_years_start, age_group_years_end
    FROM
        {DATABASE}
    JOIN
        {DATABASE} USING (age_group_id)
    WHERE
        age_group_set_id = {age_group_set_id}
    """.format(age_group_set_id=age_group_set_id)
    age_df = ezfuncs.query(q, conn_def={CONN DEF})
    return age_df

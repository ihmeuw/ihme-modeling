import pandas as pd
import sys
import getpass
from db_queries import get_location_metadata
from db_queries import get_cause_metadata

sys.path.append(''\
                .format(getpass.getuser()))
from data_format_tools import long_to_wide

global filepath
filepath = ""\
                    .format(getpass.getuser())

def convert_bearbones_to_who(bearbones_df):
    if "ihme_loc_id" not in list(bearbones_df):
        mort_comp_loc = get_location_metadata(
                        location_set_id=21, gbd_round_id=5)
        bearbones_df = bearbones_df.merge(
            mort_comp_loc[['location_id', "ihme_loc_id"]],
            on='location_id', how='left')

    bearbones_df = bearbones_df.rename(columns =
                                            {'ihme_loc_id':'iso3',
                                             'location_id':'location_id',
                                             'sex_id':'sex',
                                             'year_id':'year',
                                             'source':'source',
                                             'nid':"NID"
                                             })
    if "deaths_num" not in list(bearbones_df):
        bnd_map = pd.read_csv("".format(filepath))
        bnd_map['deaths_num'] = [x[6:] for x in bnd_map['deaths_label']]
        bearbones_df = bearbones_df.merge(
            bnd_map[['age_group_id', 'deaths_num']],
            on='age_group_id', how='left')
        del bearbones_df['age_group_id']
    
    cols = [x for x in list(bearbones_df) if not x.startswith('deaths')]

    who_vr = long_to_wide(bearbones_df,
                            "deaths",
                            cols,
                            'deaths_num')
    who_vr = who_vr.fillna(0).groupby(
    [x for x in list(who_vr) if not x.startswith("deaths")]).sum().reset_index()

    computation_hierarchy = get_cause_metadata(cause_set_id=4, gbd_round_id=5)
    computation_hierarchy_2016 = get_cause_metadata(cause_set_id=4, gbd_round_id=4)
    only_2016_causes = list(set(computation_hierarchy_2016.cause_id.unique())\
                            -set(computation_hierarchy.cause_id.unique())
                           )
    computation_hierarchy_2016 = computation_hierarchy_2016\
                    .loc[computation_hierarchy_2016.cause_id.isin(only_2016_causes)]
    computation_hierarchy_2015 = get_cause_metadata(
                          cause_set_id=4, gbd_round_id=3)
    computation_hierarchy_2015 = computation_hierarchy_2015.loc[
      computation_hierarchy_2015.acause.isin(['digest_gastrititis', 'inj_war'])]

    computation_hierarchy = computation_hierarchy.merge(
                          computation_hierarchy_2015, how='outer')
    computation_hierarchy = computation_hierarchy.merge(
                          computation_hierarchy_2016, how='outer')                        
    who_vr = who_vr.merge(
        computation_hierarchy[['acause','cause_id']], on ="cause_id", how="left")
    del who_vr['cause_id']
            
    return who_vr

def convert_who_to_bearbones(who_vr):
    cols = [x for x in list(who_vr) if not x.startswith("deaths")]
    who_vr = who_vr.set_index(cols).stack().reset_index(name="deaths")
    who_vr.rename(columns={'level_16':'deaths_num'}, inplace=True)
    bnd_map = pd.read_csv("".format(filepath))
    bnd_map['deaths_num'] = [x[6:] for x in bnd_map['deaths_label']]
    who_vr['deaths_num'] = [x[6:] for x in who_vr[['deaths_num']]]
    wide_deaths_w_age = who_vr.merge(
    bnd_map[['age_group_id', 'deaths_num']], on='deaths_num', how='left')

    computation_hierarchy = get_cause_metadata(cause_set_id=4, gbd_round_id=5)
    computation_hierarchy_2016 = get_cause_metadata(cause_set_id=4, gbd_round_id=4)
    only_2016_causes = list(set(computation_hierarchy_2016.cause_id.unique())\
                            -set(computation_hierarchy.cause_id.unique())
                           )
    computation_hierarchy_2016 = computation_hierarchy_2016\
                    .loc[computation_hierarchy_2016.cause_id.isin(only_2016_causes)]
    computation_hierarchy_2015 = get_cause_metadata(
                          cause_set_id=4, gbd_round_id=3)
    computation_hierarchy_2015 = computation_hierarchy_2015.loc[
      computation_hierarchy_2015.acause.isin(['digest_gastrititis', 'inj_war'])]

    computation_hierarchy = computation_hierarchy.merge(
                          computation_hierarchy_2015, how='outer')
    computation_hierarchy = computation_hierarchy.merge(
                          computation_hierarchy_2016, how='outer')
    wide_deaths_w_age = wide_deaths_w_age.merge(
        computation_hierarchy[['acause','cause_id']], on ="acause", how="left")
    wide_deaths_w_age = wide_deaths_w_age.loc[
                            wide_deaths_w_age.acause != "digest_gastrititis"]
    mort_comp_loc = get_location_metadata(location_set_id=21, gbd_round_id=5)
    del wide_deaths_w_age['location_id']
    wide_deaths_w_age.rename(columns = {'iso3':'ihme_loc_id',
                                        'sex':'sex_id',
                                        'year':'year_id',
                                        'NID':'nid'},
                             inplace=True)
    bearbones_vr = wide_deaths_w_age.merge(
        mort_comp_loc[['location_id', "ihme_loc_id"]],
        on='ihme_loc_id', how='left')

    return bearbones_vr

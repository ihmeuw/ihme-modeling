'''
 Script for compiling raw shock datasets into shock database.
'''
from .shock_db_formatting import *
import numpy as np
import pandas as pd
import os

def create_shock_db():
    '''
    Returns compiled, formatted shock database as a pandas dataframe.
    '''
    # conflict
    # GED
    ged_path = r'FILEPATH'
    ged = format_ged(ged_path)

    # PRIO BDD
    bdd_path = r"FILEPATH"
    bdd = format_prio_bdd(bdd_path)

    # ACLED & SCAD
    base_dir = r"FILEPATH"
    msdata = format_multi_war(base_dir)
    multi_source_data = pd.concat(msdata)

    # IISS
    iiss_path = 'FILEPATH'
    iiss = format_iiss(iiss_path)

    # Global Terrorism Database (GTD)
    gtd_path = "FILEPATH"
    gtd = format_gtd(gtd_path)

    # CPOST Suicide Attack Database
    csad_path = "FILEPATH"
    csad = format_csad(csad_path)

    # RAND World Terrorism Incidents
    rand_wti_path = "FILEPATH"
    rant_wti = format_rand_wti(rand_wti_path)

    # Yemen cholera
    yemen_cholera_supp_path = "FILEPATH"
    yemen_cholera_supp = format_yemen_cholera_supp(yemen_cholera_supp_path)

    # war supplements
    war_supp_2015_path = 'FILEPATH'
    war_supp_2015 = format_war_supp_2015(war_supp_2015_path)
    war_exov_path = 'FILEPATH'
    war_exov = format_war_exov(war_exov_path)
    path_dod_icd9 = "FILEPATH"
    path_dod_icd10 = "FILEPATH"
    dod = format_dod(path_dod_icd9, path_dod_icd10)
    terror_scrape_16_path = 'FILEPATH'
    terror_scrape = format_terror_scrape_16(terror_scrape_16_path)
    war_supplement_2014a_paths = [
        'FILEPATH',
        'FILEPATH']
    war_supp_2014a = format_war_supp_2014a(war_supplement_2014a_paths)
    syr_supp_path = 'FILEPATH'
    syr_supp = format_syr_supp(syr_supp_path)
    mzr_path = 'FILEPATH'
    mzr_research = format_mzr(mzr_path)
    irq_ims_ibc_path = 'FILEPATH'
    irq_ims_ibc = format_irq_ims_ibc(irq_ims_ibc_path)
    pse_supp_2015_path = 'FILEPATH'
    pse_supp_2015 = format_pse_supp_2015(pse_supp_2015_path)

    emdat_path = "FILEPATH"
    emdat = format_emdat(emdat_path)

    # disaster supplements
    disaster_exov_path = 'FILEPATH'
    disaster_exov = format_disaster_exov(disaster_exov_path)
    disaster_2015_supp_paths = [
        'FILEPATH',
        'FILEPATH']
    dis_2015_supp = format_dis_supp_2015(disaster_2015_supp_paths)
    dd_121916_path = 'FILEPATH'
    dd_121916 = format_121916_disaster_data(dd_121916_path)
    mina_crowd_collapse_path = 'FILEPATH'
    mina_crowd_collapse = format_mina_crowd_collapse(mina_crowd_collapse_path)
    multi_supp_2015_path = 'FILEPATH'
    multi_supp_2015 = format_multi_supp_2015(multi_supp_2015_path)

    # epidemics
    epidemic_path = "FILEPATH"
    epidemics = format_epidemics(epidemic_path)


    ebola_local_path = 'FILEPATH'
    ebola_local = format_ebola_local(ebola_local_path)
    ebola_ic_path = 'FILEPATH'
    ebola_ic = format_ebola_ic(ebola_ic_path)


    tweet_2017_supp_path = "FILEPATH"
    tweet_2017_supp = format_tweet_2017_supp(tweet_2017_supp_path)


    shocks_2017_path = 'FILEPATH'
    shocks_2017 = format_shocks_2017(shocks_2017_path)

    iraq_iran_war_path = 'FILEPATH'
    iraq_iran_war = format_iraq_iran_war(iraq_iran_war_path)

    Vietnam_war_path = "FILEPATH"
    Vietnam_war = format_Vietnam_War(Vietnam_war_path)

    great_leap_forward_path = "FILEPATH"
    great_leap_forward = format_the_great_leap_forward(great_leap_forward_path)

    collaborator_2018_path = "FILEPATH"
    collaborator_overrides = format_2018_collaborator_shocks(collaborator_2018_path)

    current_year_shocks_path = "FILEPATH"
    current_year_shocks = format_current_year_shocks(current_year_shocks_path)

    shock_db = pd.concat([ged,
                          bdd,
                          multi_source_data,
                          iiss,
                          gtd,
                          csad,
                          rant_wti,
                          yemen_cholera_supp,
                          war_supp_2015,
                          war_exov,
                          dod,
                          terror_scrape,
                          war_supp_2014a,
                          syr_supp,
                          mzr_research,
                          irq_ims_ibc,
                          pse_supp_2015,
                          emdat,
                          disaster_exov,
                          dis_2015_supp,
                          mina_crowd_collapse,
                          multi_supp_2015,
                          dd_121916,
                          epidemics,
                          ebola_local,
                          ebola_ic,
                          tweet_2017_supp,
                          shocks_2017,
                          iraq_iran_war,
                          Vietnam_war,
                          great_leap_forward,
                          collaborator_overrides,
                          current_year_shocks])

    col_reorder = ['country',
                   'iso',
                   'location_id',
                   'gw_country_code',
                   'admin1',
                   'admin2',
                   'admin3',
                   'location',
                   'urban_rural',
                   'latitude',
                   'longitude',
                   'location_precision',
                   'priogrid_gid',   
                   'event_type',
                   'year',
                   'date_start',
                   'date_end',
                   'time_precision',
                   'nid',
                   'dataset',
                   'dataset_event_type',
                   'dataset_notes',
                   'event_clarity',
                   'event_name',
                   'event_sub_name',
                   'event_sub_sub_name',
                   'side_a',
                   'gwnoa',
                   'side_b',
                   'gwnob',
                   'number_of_sources',
                   'source_office',
                   'source_article',
                   'source_headline',
                   'source_date',
                   'source_original',
                   'deaths_a',
                   'deaths_b',
                   'deaths_civilians',
                   'deaths_unknown',
                   'age_group_id',
                   'sex_id',
                   'low',
                   'best',
                   'high'
                   ]
    shock_db = shock_db.loc[:,col_reorder]
    # For all string columns, replace unicode left and right single quotes with
    #  their ASCII equivalents
    for col in shock_db.columns:
        if shock_db[col].dtype is np.dtype("O"):
            shock_db[col] = (shock_db[col].apply(lambda x: 
                                x.replace("\u2018","'").replace("\u2019","'")
                                   if type(x) is str else x))
    # Remove missing or incorrect strings from the event_type field
    def is_valid_string(txt):
        if type(txt) is not str:
            return False
        txt = txt.upper()
        if np.any([txt.startswith(i) for i in list("ABCDE")]) and len(txt)<=4:
            return True
        else:
            return False
    shock_db.loc[~shock_db['event_type'].apply(is_valid_string),
                 'event_type'] = np.nan

    print('load: shock_db created and formatted')
    return shock_db

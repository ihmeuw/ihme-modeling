
import pandas as pd
import re


def assign_events_emdat(db):

    # Floods
    flood = db.dataset_event_type.str.match('^Flood_').fillna(False)
    db.loc[flood, 'event_type'] = 'A6'

    # storms
    storm = db.dataset_event_type.str.match('^Storm_').fillna(False)
    db.loc[storm, 'event_type'] = 'A5'

    # industrial accident & miscellaneous accident
    ind_acc = db.dataset_event_type.str.match(
        '^Industrial accident_').fillna(False)
    misc_acc = db.dataset_event_type.str.match(
        '^Miscellaneous accident_').fillna(False)
    db.loc[(ind_acc | misc_acc), 'event_type'] = 'D3'

    # earthquake
    quake = db.dataset_event_type.str.match('^Earthquake_').fillna(False)
    db.loc[quake, 'event_type'] = 'A2'

    # Famine (drought and complex disasters that include "food shortage" or
    # "famine" in their other disaster types.)
    cd = db.dataset_event_type.str.match('^Complex Disasters_').fillna(False)
    drought = db.dataset_event_type.str.match('^Drought_').fillna(False)
    food = db.dataset_event_type.str.match('.*Food shortage').fillna(False)
    famine = db.dataset_event_type.str.match('.*Famine_').fillna(False)
    db.loc[((cd | drought) & (food | famine)), 'event_type'] = 'C'

    db.loc[drought, 'event_type'] = 'C'

    # Earth movements (landslide, avalanche, etc.)
    slide = db.dataset_event_type.str.match('^Landslide_').fillna(False)
    mm = db.dataset_event_type.str.match('^Mass movement').fillna(False)
    db.loc[(slide | mm), 'event_type'] = 'A4'

    # Extreme Temperature, insect infestation (other)
    hot_cold = db.dataset_event_type.str.match(
        '^Extreme temperature').fillna(False)
    bugs = db.dataset_event_type.str.match('^Insect infestation').fillna(False)
    db.loc[(hot_cold | bugs), 'event_type'] = 'A7'

    # Wildfire
    fire = db.dataset_event_type.str.match('^Wildfire_').fillna(False)
    db.loc[fire, 'event_type'] = 'D2'

    # Volcanic activity
    volcano = db.dataset_event_type.str.match(
        '^Volcanic activity_').fillna(False)
    db.loc[volcano, 'event_type'] = 'A3'

    # transportation - air, rail, water, road
    rail = db.dataset_event_type.str.match(
        '^Transport accident_Rail').fillna(False)
    db.loc[rail, 'event_type'] = 'D5'
    air = db.dataset_event_type.str.match(
        '^Transport accident_Air').fillna(False)
    db.loc[air, 'event_type'] = 'D5'
    boat = db.dataset_event_type.str.match(
        '^Transport accident_Water').fillna(False)
    db.loc[boat, 'event_type'] = 'D5.1'
    road = db.dataset_event_type.str.match(
        '^Transport accident_Road').fillna(False)
    db.loc[road, 'event_type'] = 'D4'


    db.loc[(db.dataset == 'EMDAT') & (db.event_type == ''),
           "event_type"] = 'unassigned'

    assert len(db.loc[(db.dataset == "EMDAT") &
                      (db.event_type == '')].index) == 0

    return db


def assign_events_ged(db):

    is_ged = db.dataset.str.match('')
    state = db.dataset_event_type.astype(str).str.match('^1$').fillna(False)
    non_state = db.dataset_event_type.astype(
        str).str.match('^2$').fillna(False)
    one_sided = db.dataset_event_type.astype(
        str).str.match('^3$').fillna(False)
    side_a_gov = db.side_a.str.match('Government').fillna(False)

    db.loc[(is_ged & (state | non_state | (one_sided & side_a_gov))),
           'event_type'] = 'B1'

    db.loc[(is_ged & (one_sided & ~side_a_gov)), 'event_type'] = 'B2'


    missing = len(db.loc[is_ged & (db.event_type == '')].index)
    assert missing == 0

    return db


def assign_events_prio_bdd(db):

    is_bdd = db.dataset.str.match('').fillna(False)

    # all war
    db.loc[is_bdd, 'event_type'] = 'B1'

    missing = len(db.loc[is_bdd & (db.event_type == '')].index)
    assert missing == 0

    return db


def assign_events_acled(db):

    is_acled = db.dataset.str.match('')
    violence = db.dataset_event_type.str.match('^Violence').fillna(False)
    gov_brut = db.dataset_event_type.str.match('_1\d$').fillna(
        False)      # Legal Intervention, coded as war
    terror = (violence & ~gov_brut)

    # terrorism
    db.loc[(is_acled & terror), 'event_type'] = 'B2'

    # war
    db.loc[(is_acled & ~terror), 'event_type'] = 'B1'

    assert len(db.loc[is_acled & (db.event_type == '')].index) == 0

    return db


def assign_events_scad(db):

    is_scad = db.dataset.str.match('')
    terror = db.dataset_event_type.str.match('^9_').fillna(False)

    # terrorism
    db.loc[(is_scad & terror), 'event_type'] = 'B2'

    # war
    db.loc[(is_scad & ~terror), 'event_type'] = 'B1'

    assert len(db.loc[is_scad & (db.event_type == '')].index) == 0

    return db


def assign_events_iiss(db):

    is_iiss = db.dataset.str.match('')
    terror = db.dataset_event_type.str.match('terrorism').fillna(False)

    # terrorism
    db.loc[(is_iiss & terror), 'event_type'] = 'B2'

    # war
    db.loc[(is_iiss & ~terror), 'event_type'] = 'B1'

    assert len(db.loc[is_iiss & (db.event_type == '')].index) == 0

    return db


def assign_events_war_supp_2015(db):

    # conditions / coding logic
    is_war_supp_2015 = db.dataset.str.match('')
    terror = db.dataset_event_type.str.match('Terroris').fillna(False)

    # terrorism
    db.loc[(is_war_supp_2015 & terror), 'event_type'] = 'B2'

    # war
    db.loc[(is_war_supp_2015 & ~terror), 'event_type'] = 'B1'

    assert len(db.loc[is_war_supp_2015 & (db.event_type == '')].index) == 0

    return db


def assign_events_war_supp_2014a(db):



    # conditions / coding logic
    is_war_supp_2014a = db.dataset.str.match('')

    # war
    db.loc[(is_war_supp_2014a), 'event_type'] = 'B1'

    assert len(db.loc[is_war_supp_2014a & (db.event_type == '')].index) == 0

    return db


def assign_events_war_exov(db):

    is_war_exov = db.dataset.str.match('')
    war = db.dataset_event_type.str.match('inj_war').fillna(False)
    poison = db.dataset_event_type.str.match('inj_poisoning').fillna(
        False)  
    db.loc[(is_war_exov & war), 'event_type'] = 'B1'

    # poisoning
    db.loc[(is_war_exov & poison), 'event_type'] = 'D9'

    assert len(db.loc[is_war_exov & (db.event_type == '')].index) == 0

    return db


def assign_events_dod(db):
    # conditions
    is_dod = db.dataset.str.match('')
    war = db.dataset_event_type.str.match('inj_war_war')
    terror = db.dataset_event_type.str.match('inj_war_terrorism')

    # war
    db.loc[is_dod & war, 'event_type'] = 'B1.2'

    # terror
    db.loc[is_dod & terror, 'event_type'] = 'B2.2'

    assert len(db.loc[is_dod & (db.event_type == '')].index) == 0

    return db


def assign_events_terror_scrape(db):
    # conditions
    is_ts = db.dataset.str.match('')

   
    db.loc[is_ts, 'event_type'] = 'B2'

    assert len(db.loc[is_ts & (db.event_type == '')].index) == 0

    return db


def assign_events_disaster_exov(db):

    is_dis_exov = db.dataset.str.match('')
    disaster = db.dataset_event_type.str.match('inj_disaster').fillna(False)
    poison = db.dataset_event_type.str.match('inj_poisoning').fillna(False)
    trans = db.dataset_event_type.str.match('inj_trans_other').fillna(False)
    fire = db.dataset_event_type.str.match('inj_fire').fillna(False)

    # disaster
    db.loc[(is_dis_exov & disaster), 'event_type'] = 'A'

    # poisoning
    db.loc[(is_dis_exov & poison), 'event_type'] = 'D9'

    # trans
    db.loc[(is_dis_exov & trans), 'event_type'] = 'D5'

    # fires
    db.loc[(is_dis_exov & fire), 'event_type'] = 'D2'

    assert len(db.loc[is_dis_exov & (db.event_type == '')].index) == 0

    return db


def assign_events_dis_supp_15(db):

    is_dis_supp_15 = db.dataset.str.match('')
    non_emdat_type = db.dataset_event_type.str.match('__').fillna(False)
    flood = db.dataset_event_type.str.match(
        '(?i).*flood', re.IGNORECASE).fillna(False)
    quake = db.dataset_event_type.str.match(
        '(?i).*earthquake', re.IGNORECASE).fillna(False)
    storm = (
        db.dataset_event_type.str.match(
              '(?i).*storm',
            re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
              '(?i).*typhoon',
            re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
              '(?i).*severe weather',
            re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
              '(?i).*extreme weather',
            re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
              '(?i).*hurricane',
            re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
              '(?i).*cyclone',
            re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
              '(?i).*tornado',
            re.IGNORECASE).fillna(False))
    measles = db.dataset_event_type.str.match(
        '(?i).*measles', re.IGNORECASE).fillna(False)
    heat_cold = (db.dataset_event_type.str.match(
              '(?i).*heat',
            re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
              '(?i).*snowfall',
            re.IGNORECASE).fillna(False))
    slide = db.dataset_event_type.str.match(
        '(?i).*slide', re.IGNORECASE).fillna(False)
    fire = db.dataset_event_type.str.match(
        '(?i).*fire', re.IGNORECASE).fillna(False)
    drought = db.dataset_event_type.str.match(
        '(?i).*drought', re.IGNORECASE).fillna(False)
    volcanos = db.dataset_event_type.str.match(
        '(?i).*eruption', re.IGNORECASE).fillna(False)

    # floods
    db.loc[(is_dis_supp_15 & non_emdat_type & flood), 'event_type'] = 'A6'

    # quake
    db.loc[(is_dis_supp_15 & non_emdat_type & quake), 'event_type'] = 'A2'

    # storm
    db.loc[(is_dis_supp_15 & non_emdat_type & storm), 'event_type'] = 'A5'

    # slide
    db.loc[(is_dis_supp_15 & non_emdat_type & slide), 'event_type'] = 'A4'

    # fires
    db.loc[(is_dis_supp_15 & non_emdat_type & fire), 'event_type'] = 'D2'

    # drought
    db.loc[(is_dis_supp_15 & non_emdat_type & drought), 'event_type'] = 'C'

    # volcanos
    db.loc[(is_dis_supp_15 & non_emdat_type & volcanos), 'event_type'] = 'A3'

    # heat/cold waves
    db.loc[(is_dis_supp_15 & non_emdat_type & heat_cold), 'event_type'] = 'D8'

    # Measles
    db.loc[(is_dis_supp_15 & non_emdat_type & measles), 'event_type'] = 'E3'

    db.loc[is_dis_supp_15 & (db.event_type == ''), 'event_type'] = 'E4'

    assert len(db.loc[is_dis_supp_15 & (db.event_type == '')].index) == 0

    return db


def assign_events_dis_supp_16(db):

    # conditions / coding logic
    is_dis_supp_16 = db.dataset.str.match('')

    flood = db.dataset_event_type.str.match(
        '(?i).*flood', re.IGNORECASE).fillna(False)
    quake = db.dataset_event_type.str.match(
        '(?i).*earthquake', re.IGNORECASE).fillna(False)
    storm = (
        db.dataset_event_type.str.match(
            '(?i).*storm',
        re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
            '(?i).*severe weather',
        re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
            '(?i).*typhoon',
        re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
            '(?i).*hurricane',
        re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
           '(?i).*cyclone',
        re.IGNORECASE).fillna(False) | db.dataset_event_type.str.match(
            '(?i).*tornado',
        re.IGNORECASE).fillna(False))
    heat_cold = db.dataset_event_type.str.match(
        '(?i).*heatwave',re.IGNORECASE).fillna(False)
    slide = db.dataset_event_type.str.match(
        '(?i).*slide', re.IGNORECASE).fillna(False)
    fire = db.dataset_event_type.str.match(
        '(?i).*fire', re.IGNORECASE).fillna(False)
    drought = db.dataset_event_type.str.match(
        '(?i).*drought', re.IGNORECASE).fillna(False)
    volcanos = db.dataset_event_type.str.match(
        '(?i).*eruption', re.IGNORECASE).fillna(False)
    air = db.dataset_event_type.str.match(
        '(?i).*air', re.IGNORECASE).fillna(False)
    train = db.dataset_event_type.str.match(
        '(?i).*train', re.IGNORECASE).fillna(False)
    collapse = db.dataset_event_type.str.match(
        '(?i).*collapse', re.IGNORECASE).fillna(False)
    explosion = db.dataset_event_type.str.match(
        '(?i).*explosion', re.IGNORECASE).fillna(False)
    inj_mech_other = db.dataset_event_type.str.match(
        '(?i).*industrial', re.IGNORECASE).fillna(False)
    war = db.dataset_event_type.str.match(
        '(?i).*war', re.IGNORECASE).fillna(False)
    terror = db.dataset_event_type.str.match(
        '(?i).*terror', re.IGNORECASE).fillna(False)
    LI = db.dataset_event_type.str.match(
        '(?i).*legal intervention',
        re.IGNORECASE).fillna(False)

    # floods
    db.loc[(is_dis_supp_16 & flood), 'event_type'] = 'A6'

    # quake
    db.loc[(is_dis_supp_16 & quake), 'event_type'] = 'A2'

    # storm
    db.loc[(is_dis_supp_16 & storm), 'event_type'] = 'A5'

    # slide
    db.loc[(is_dis_supp_16 & slide), 'event_type'] = 'A4'

    # fires
    db.loc[(is_dis_supp_16 & fire), 'event_type'] = 'D2'

    # drought
    db.loc[(is_dis_supp_16 & drought), 'event_type'] = 'C'

    # volcanos
    db.loc[(is_dis_supp_16 & volcanos), 'event_type'] = 'A3'

    # air
    db.loc[(is_dis_supp_16 & air), 'event_type'] = 'D5'

    # train
    db.loc[(is_dis_supp_16 & train), 'event_type'] = 'D5'

    # collapse, explosion --> inj_mech_other
    db.loc[(is_dis_supp_16 & (collapse | explosion | inj_mech_other)),
           'event_type'] = 'D3'

    # war
    db.loc[(is_dis_supp_16 & (war | LI)), 'event_type'] = 'B1'

    # terror
    db.loc[(is_dis_supp_16 & terror), 'event_type'] = 'B2'

    # heat/cold waves
    db.loc[(is_dis_supp_16 & terror), 'event_type'] = 'D8'

    db.loc[is_dis_supp_16 & (db.event_type == ''), 'event_type'] = 'unassigned'

    assert len(db.loc[is_dis_supp_16 & (db.event_type == '')].index) == 0

    return db


def assign_events_epidemics(db):


    # cholera
    cholera = db.dataset_event_type.str.match('^cholera').fillna(False)
    db.loc[cholera, 'event_type'] = 'E1.1'

    # meningococcal meningitis
    mm = db.dataset_event_type.str.match('^meningitis').fillna(False)
    db.loc[mm, 'event_type'] = 'E2'

    assert len(db.loc[(db.dataset == "Epidemics")
                      & (db.event_type == '')].index) == 0

    return db


def assign_events_ebola_who_pre2014(db):

    is_ebola_who_pre2014 = db.dataset.str.match('')
    db.loc[is_ebola_who_pre2014, 'event_type'] = 'E8'

    return db


def assign_events_ebola_2017update(db):
    is_ebola_2017update = db.dataset.str.match(
        'ebola_wAfrica_2014_update_2017')
    db.loc[is_ebola_2017update, 'event_type'] = 'E8'

    return db


def assign_events_ebola_mort_apport_2016(db):
    is_ebola_mort_aport = db.dataset.str.match('')
    db.loc[is_ebola_mort_aport, 'event_type'] = 'E8'

    return db


def assign_events_ebola_ic(db):
    is_ebola_ic = db.dataset.str.match('')
    db.loc[is_ebola_ic, 'event_type'] = 'E8'

    return db


def assign_events_mina_crowd_collapse(db):
    ''' Crowd collapse is inj_mech_other '''
    is_mina_cc = db.dataset.str.match('')
    db.loc[is_mina_cc, 'event_type'] = 'D3'

    return db


def assign_events_syr_supp(db):
    is_syr_supp = db.dataset.str.match('')
    db.loc[is_syr_supp, 'event_type'] = 'B1'

    return db


def assign_events_mzr(db):
    is_mzr = db.dataset.str.match('')
    db.loc[is_mzr, 'event_type'] = 'B1'
    return db


def assign_events_irq_ims_ibc(db):
    is_irq_avg = db.dataset.str.match('')
    db.loc[is_irq_avg, 'event_type'] = 'B1'
    return db


def assign_events_phl_supp_2015(db):

    # conditions / logic
    is_phl_supp_2015 = db.dataset.str.contains('').fillna(False)
    war = (
        db.dataset_event_type.str.contains(
            'war',
            case=False).fillna(False) | (
            db.dataset_event_type.str.contains(
                'conflict',
                case=False).fillna(False) & db.dataset_notes.str.contains(
                    'war',
                case=False).fillna(False)))
    storm = db.dataset_event_type.str.contains(
        'cyclone|typhoon|tyhpoon|storm|tornado',
        case=False).fillna(False)

    flood = db.dataset_event_type.str.contains(
        'flood', case=False).fillna(False)
    slide = db.dataset_event_type.str.contains(
        'landslide', case=False).fillna(False)
    quake = db.dataset_event_type.str.contains(
        'earthquake', case=False).fillna(False)
    tsunami = db.dataset_event_type.str.contains(
        'tsunami', case=False).fillna(False)
    drought = db.dataset_event_type.str.contains(
        'drought', case=False).fillna(False)
    volcano = db.dataset_event_type.str.contains(
        'volcano', case=False).fillna(False)
    other_disaster = db.dataset_event_type.str.contains(
        'wave|winter', case=False).fillna(False)
    broad_disaster = (
        (db.dataset_event_type.str.match(
            '^disaster;;$',
            case=False).fillna(False) | db.dataset_event_type.str.match(
            '^;;$',
            case=False).fillna(False)) & db.dataset_notes.str.contains(
                'source maggie found',
            case=False).fillna(False))
    measles = db.dataset_event_type.str.contains(
        'measles', case=False).fillna(False)
    malaria = db.dataset_event_type.str.contains(
        'malaria', case=False).fillna(False)
    smallpox = db.dataset_event_type.str.contains(
        'small pox', case=False).fillna(False)
    other_diarrhea = db.dataset_event_type.str.contains(
        'diarrhoeal', case=False).fillna(False)

    # assignments
    db.loc[is_phl_supp_2015 & war, 'event_type'] = 'B1'
    db.loc[is_phl_supp_2015 & storm, 'event_type'] = 'A5'
    db.loc[is_phl_supp_2015 & flood, 'event_type'] = 'A6'
    db.loc[is_phl_supp_2015 & slide, 'event_type'] = 'A4'
    db.loc[is_phl_supp_2015 & quake, 'event_type'] = 'A2'
    db.loc[is_phl_supp_2015 & tsunami, 'event_type'] = 'A7'
    db.loc[is_phl_supp_2015 & volcano, 'event_type'] = 'A3'
    db.loc[is_phl_supp_2015 & drought, 'event_type'] = 'C'
    db.loc[is_phl_supp_2015 & other_disaster, 'event_type'] = 'A7'
    db.loc[is_phl_supp_2015 & broad_disaster, 'event_type'] = 'A'
    db.loc[is_phl_supp_2015 & measles, 'event_type'] = 'E3'
    db.loc[is_phl_supp_2015 & malaria, 'event_type'] = 'E5'
    db.loc[is_phl_supp_2015 & smallpox, 'event_type'] = 'E7'
    db.loc[is_phl_supp_2015 & other_diarrhea, 'event_type'] = 'E1'

    return db


def assign_events_pse_supp_2015(db):


    # conditions / logic
    is_pse_supp_2015 = db.dataset.str.contains('').fillna(False)
    war = db.dataset_event_type.str.contains('war', case=False).fillna(False)

    # assignments
    db.loc[is_pse_supp_2015 & war, 'event_type'] = 'B1'

    return db


def assign_events_multi_supp_2015(db):

    # conditions / logic
    is_multi_supp_2015 = db.dataset.str.contains(
        'multi_supp_2015').fillna(False)
    war = db.dataset_event_type.str.contains(
        'war|conflict', case=False).fillna(False)
    slide = db.dataset_event_type.str.contains(
        'avalanche', case=False).fillna(False)
    storm = db.dataset_event_type.str.contains(
        'storm|cyclone', case=False).fillna(False)
    flood = db.dataset_event_type.str.contains(
        'flood', case=False).fillna(False)
    poison = db.dataset_event_type.str.contains(
        'poisioning', case=False).fillna(False) 

    # assignments
    db.loc[is_multi_supp_2015 & war, 'event_type'] = 'B1'
    db.loc[is_multi_supp_2015 & slide, 'event_type'] = 'A4'
    db.loc[is_multi_supp_2015 & storm, 'event_type'] = 'A5'
    db.loc[is_multi_supp_2015 & poison, 'event_type'] = 'D9'
    db.loc[is_multi_supp_2015 & flood, 'event_type'] = 'A6'

    return db

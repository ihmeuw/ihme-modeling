'''
Define formatting functions for transforming raw shock data to the shock database format.
'''
# Imports (general)
import datetime as dt
import numpy as np
import pandas as pd
import unidecode
from pandas.tseries.offsets import *
import re
import os
from .tools import format_mixed_yY_dates, split_years_by_date_start_end
from db_queries import get_location_metadata as loc_meta


def format_ged(path):

    ged = pd.read_csv(path, encoding='utf8')
    ged = (ged.drop(['id',
                     'active_year',
                     'side_a_new_id',
                     'side_b_new_id',
                     'dyad_name',
                     'dyad_new_id',
                     'conflict_name',
                     'geom_wkt'], axis=1)
           .rename(columns={'where_coordinates': 'location',
                            'type_of_violence': 'dataset_event_type',
                            'where_prec': 'location_precision',
                            'date_prec': 'time_precision',
                            'adm_1': 'admin1',
                            'adm_2': 'admin2',
                            'notes': 'dataset_notes',
                            'country_id': 'gw_country_code',
                            'conflict_new_id': 'dataset_conflict_group'})
           )

    ged['dataset_event_type'] = ged['dataset_event_type'].astype(str)

    for side in ['side_a', 'side_b']:
        ged[side] = ged[side].str.replace(',', ';;')


    ged.date_start = pd.to_datetime(
        ged.date_start,
        format='%m/%d/%Y').dt.strftime('%Y/%m/%d')
    ged.date_end = pd.to_datetime(ged.date_end,
                                  format='%m/%d/%Y').dt.strftime('%Y/%m/%d')


    ged['dataset'] = "GED"

    ged['nid'] = 326896
    ged['location_id'] = np.nan
    return ged


def format_prio_bdd(path):
    ''' Format UCDP/PRIO Battle Deaths Dataset for inclusion in the
        shocks database.
    '''
    df = pd.read_csv(path, encoding='cp1252')

    # concatenate parties to the conflict.
    df['side_a'] = df['sidea'] + ';;' + df['sidea2nd'].fillna('')
    df['side_b'] = df['sideb'] + ';;' + df['sideb2nd'].fillna('')

    # align time precision
    df.loc[df.startprec >= 4, 'startprec'] = 5
    df.loc[df.startprec.isin([2, 3]), 'startprec'] += 1
    assert df.startprec.isin([1, 3, 4, 5]).all()


    # sources
    df.loc[df.source == 1, 'source'] = 'UCDP_ACD'
    df.loc[df.source == 0, 'source'] = 'unknown'

    # event clarity
    # temp value to allow adjustment on next line.
    df.loc[df.annualdata == 2, 'event_clarity'] = 13
    df.loc[df.annualdata.isin([0, 1]), 'event_clarity'] = 2
    df.loc[df.event_clarity == 13, 'event_clarity'] = 1

    # start and end dates -- this is annual data, much of it documenting multi-year conflicts
    ######
    # in years in which the conflict started, use the acutal start date
    df.loc[df['year'] == df.startdate.str[-4:].astype(int), 'date_start'] = pd.to_datetime(
        df.startdate, format='%m/%d/%Y').dt.strftime('%Y/%m/%d')

    df.loc[df['year'] != df.startdate.str[-4:].astype(
        int), 'date_start'] = df['year'].astype(str) + '/01/01'

    df.loc[(df.epend == 1), 'date_end'] = pd.to_datetime(
        df.ependdate, format='%m/%d/%Y').dt.strftime('%Y/%m/%d')

 
    df.loc[(df.epend == 0), 'date_end'] = df['year'].astype(str) + '/12/31'

    # dataset_event_type
    df['dataset_event_type'] = (df['incomp'].astype(str) + "_" +
                                df['int'].astype(str) + "_" +
                                df['cumint'].astype(str) + "_" +
                                df['type'].astype(str))

    # drop and rename cols
    df = (df.drop(['annualdata',
                   'bdversion',
                   'region',
                   'gwnoa',
                   'gwnob',
                   'gwnoa2nd',
                   'gwnob2nd',
                   'ependprec',
                   'epend',
                   'startdate',
                   'startdate2',
                   'startprec2',
                   'ependdate',
                   'incomp',
                   'int',
                   'cumint',
                   'type',
                   'sidea',
                   'sidea2nd',
                   'sideb',
                   'sideb2nd',
                   'version'], axis=1)
          .rename(columns={'bdeadlow': 'low',
                           'bdeadbes': 'best',
                           'bdeadhig': 'high',
                           'terr': 'location',
                           'startprec': 'location_precision',
                           'gwnoloc': 'gw_country_code',
                           'location': 'country',
                           'id': 'dataset_conflict_group'
                           })
          )

    df.loc[
        df['best'] == -999,
        'best'
    ] = (df['low'] + df['high']) / 2


    df['dataset'] = 'PRIO_BDD'
    df['nid'] = 327302

    return df


def format_acled(path, continent):
    ''' Format ACLED Africa or Asia datasets for inclusion in the
        shocks database.
    '''

    afr = pd.read_csv(path, encoding='latin1')

    afr.columns = afr.columns.str.lower()
    afr = afr.rename(columns={'assoc_actor_1':'ally_actor_1',
                              'assoc_actor_2':'ally_actor_2',
                              'geo_precis':'geo_precision',
                              'source scale':'source_scale'})
    extra_empty_cols = afr.columns[afr.columns.str.match('unnamed')].tolist()
    afr = afr.drop(extra_empty_cols, axis=1)

    afr['date_start'] = afr.event_date
    afr['date_end'] = afr.event_date

    # format start date & end date
    if continent == 'Africa':
        afr.date_start = format_mixed_yY_dates(afr.date_start)
        afr.date_end = format_mixed_yY_dates(afr.date_end)
    elif continent == 'MiddleEast':
        try:
            afr.date_start = pd.to_datetime(
                afr.date_start, format='%d-%B-%Y').dt.strftime('%Y/%m/%d')
            afr.date_end = pd.to_datetime(
                afr.date_end, format='%d-%B-%Y').dt.strftime('%Y/%m/%d')
        except:
            afr.date_start = pd.to_datetime(
                afr.date_start, format='%m/%d/%Y').dt.strftime('%Y/%m/%d')
            afr.date_end = pd.to_datetime(
                afr.date_end, format='%m/%d/%Y').dt.strftime('%Y/%m/%d')
    elif continent == 'Asia':
        afr.date_start = pd.to_datetime(
            afr.date_start, format='%d-%B-%Y').dt.strftime('%Y/%m/%d')
        afr.date_end = pd.to_datetime(
            afr.date_end, format='%d-%B-%Y').dt.strftime('%Y/%m/%d')


    afr['actor1'] = afr['actor1'] + ";;" + afr['ally_actor_1']
    afr['actor2'] = afr['actor2'] + ";;" + afr['ally_actor_2']


    if continent == "MiddleEast":
        pass
    else:
        afr['event_type'] = afr['event_type'] + \
            "_" + afr['interaction'].astype(str)

    # remove "\r" from dataset notes & actor1
    afr['notes'] = afr['notes'].str.replace('\r', '')
    afr['actor1'] = afr['actor1'].str.replace('\r', '')

    # make sure spatial precision code aligns with GED
    afr.loc[(afr.geo_precision > 1), 'geo_precision'] += 1
    assert set(afr.geo_precision.unique()) == set([1, 3, 4])

    drop_cols = afr.columns[((afr.columns.str.contains('event_id')) | 
                             (afr.columns.str.contains('event_date')) |
                             (afr.columns.str.contains('ally'))
                            )].tolist()

    drop_cols += ['inter1', 'inter2', 'interaction']
    afr = (afr.drop(drop_cols, axis=1)
              .rename(columns={'event_type': 'dataset_event_type',
                               'actor1': 'side_a',
                               'actor2': 'side_b',
                               'fatalities': 'best',
                               'source': 'source_office',
                               'gwno': 'gw_country_code',
                               'notes': 'dataset_notes',
                               'geo_precision': 'location_precision'})
           )
    afr['dataset'] = "ACLED_" + continent
    afr['nid'] = 135380
    return afr


def format_scad(path, region):
    ''' Format SCAD Africa or Latin America datasets for inclusion in
        the shocks database.
    '''
    df = pd.read_csv(path, encoding='latin1')

    assert (df.loc[df['eventid'] < 0, 'ndeath'] == -77).all()
    df = df.loc[df['eventid'] >= 0,:]

    # drop duplicated rows
    df = df.loc[df.sublocal == 1,:]

    # fix the sides of the conflict
    side_a_cols = df.columns[df.columns.str.contains('actor').tolist()]
    side_b_cols = df.columns[df.columns.str.match('target\d$').tolist()]
    df['side_a'] = df[side_a_cols].apply(
        lambda x: ";;".join(
            x.dropna().astype(str).values), axis=1)
    df['side_b'] = df[side_b_cols].apply(
        lambda x: ";;".join(
            x.dropna().astype(str).values), axis=1)

    # create dataset_event_type
    df['dataset_event_type'] = (df['etype'].astype(str) + "_" +
                                df['escalation'].astype(str) + "_" +
                                df['repress'].astype(str)
                                )

    acd_q_rows = len(df[df.acd_questionable == 1].index)
    df['dataset_notes'] = (df['geo_comments'].fillna('') + ";;" +
                           df['notes'].fillna('') + ";;" +
                           df['issuenote'].fillna('')
                           )
    df.loc[df.acd_questionable == 1, 'dataset_notes'] += ';;acd_questionable'
    assert len(df[df.dataset_notes.str.contains(
        'acd_questionable')].index) == acd_q_rows

    df.loc[df.locnum.isin([1, 2, 4]), 'urban_rural'] = 'u'
    df.loc[df.locnum.isin([3, 5]), 'urban_rural'] = 'r'


    df.loc[df.locnum.isin([1, 2, 3]), 'location_precision'] = 1
    df.loc[df.locnum.isin([4, 5]), 'location_precision'] = 5
    df.loc[df.locnum == 6, 'location_precision'] = 4
    df.loc[df.locnum.isin([7, -99]), 'location_precision'] = 6

    # drop and rename columns
    issue_cols = df.columns[df.columns.str.contains('issue')].tolist()
    drop_cols = (side_a_cols.tolist() +
                 side_b_cols.tolist() +
                 issue_cols +
                 ['eventid', 'id', 'duration', 'notes', 'geo_comments', 'ilocal'] +
                 ['stday', 'stmo', 'styr'] +
                 ['eday', 'emo', 'eyr', 'coder', 'nsource'] +
                 ['npart', 'sublocal', 'ccode', 'cgovtarget', 'rgovtarget'] +
                 ['etype', 'escalation', 'repress', 'locnum', 'gislocnum', 'acd_questionable']
                 )

    df = (df.drop(drop_cols, axis=1)
            .rename(columns={'startdate': 'date_start',
                             'enddate': 'date_end',
                             'countryname': 'country',
                             'ndeath': 'best',
                             'elocal': 'location'})
          )

    # standardize date_start and date_end
    df.date_start = pd.to_datetime(
        df.date_start,
        format='%d-%b-%y').dt.strftime('%Y/%m/%d')
    df.date_end = pd.to_datetime(df.date_end,
                                 format='%d-%b-%y').dt.strftime('%Y/%m/%d')

    df['dataset'] = "SCAD_" + region
    df['nid'] = 328289
    return df


def format_multi_war(path_to_base):
    ''' Combines data from sources each comprised of mulitple datasets.
        Returns a dict with source_names as keys and source_data as values.
        This way, we can inspect the dataframes returned for each dataset (ACLED, SCAD, etc.)
    '''
    base_dir = path_to_base
    source_names = ['acled','scad']
    data_formatting_funcs = {k: eval('format_' + k) for k in source_names}
    multi_source_dict = {}

    for data_name in data_formatting_funcs.keys():
        datasets = []  # to concatenate data from the same source
        directory = base_dir + data_name + '/'
        files_in_dir = [f for f in os.listdir(directory)
                          if os.path.isfile(os.path.join(directory,f))]
        for f in files_in_dir:
            full_path = directory + f
            if 'Africa' in f:
                continent = 'Africa'
            elif 'Asia' in f:
                continent = 'Asia'
            elif 'LA' in f:
                continent = "Latin America"
            elif 'MIDDLE_EAST' in f:
                continent = 'MiddleEast'
            data = data_formatting_funcs[data_name](full_path, continent)
            datasets.append(data)
        combined_single_source_data = pd.concat(datasets)
        multi_source_dict[data_name] = combined_single_source_data
    return multi_source_dict

# ad-hoc war data


def format_iiss(path):
    ''' Reformat IISS for inclusion in shocks db.'''
    df = pd.read_csv(path, encoding='latin1')

    # dataset-specific formatting needed
    df = df.drop(['Total', 'Unnamed: 0'], axis=1)
    df = pd.melt(df, id_vars=['Conflict'], var_name='year', value_name='best')

    # split conflict column into country and location columns
    sp = (df.Conflict.str.split('(', expand=True)
          .rename(columns={0: 'country',
                           1: 'location'})
          .drop(2, axis=1)
          )
    sp['location'] = sp.location.str.strip(')')
    df = pd.merge(df, sp, how='left', left_index=True, right_index=True)

    actor_words = ['Hizbullah-', 'terrorism/al-Qaeda', 'Islamist terrorism']
    for s in actor_words:
        df.loc[df.country.str.contains(s).fillna(False), 'dataset_notes'] = s
        df.loc[df.country.str.contains(s).fillna(
            False), 'country'] = df.country.str.replace(s, '')

    s = 'Asian'
    df.loc[df.country.str.contains(s).fillna(
        False), 'location'] = 'Southeast Asia'
    df.loc[df.country.str.contains(s).fillna(False), 'country'] = None

    # move acronyms in location column to dataset notes
    acronyms = df.loc[df.location.str.match(
        '[A-Z]{3,}').fillna(False), 'location'].unique().tolist()
    other_notes = ['Shining Path',
                   'Houthis / AQAP / SMM',
                   'Sectarian violence',
                   'Boko Haram',
                   'Polisario Front',
                   'cartels',
                   'Naxalites',
                   '17N',
                   'Lord\'s Resistance Army',
                   'Palipehutu-FNL']
    loc_cleaning = acronyms + other_notes
    for s in loc_cleaning:
        df.loc[df.location.str.contains(s).fillna(False), 'dataset_notes'] = s
        df.loc[df.location.str.contains(s).fillna(False), 'location'] = None

    # event_type
    df.loc[df.dataset_notes.str.contains('terror').fillna(
        False), 'dataset_event_type'] = 'terrorism'
    df.loc[df.dataset_event_type.isnull(), 'dataset_event_type'] = 'war'

    df = (df.rename(columns={})
          .drop(['Conflict'],
                axis=1)
          )
    df['dataset'] = 'IISS'
    df['nid'] = 93128
    return df


# Global Terrorism Database
def format_gtd(path):
    df = pd.read_csv(path, encoding='latin1')
    df = df.drop(labels=['location','country'],
                 axis=1, errors='ignore')
    # Rename columns
    df = df.rename(columns={'nkill':'best',
                            'country_txt':'country',
                            'provstate':'admin1',
                            'city':'location',
                            'summary':'dataset_notes',
                            'iyear':'year',
                            'attacktype1_txt':'dataset_event_type',
                            'gname':'side_a',
                            'targtype1_txt':'side_b',
                            'dbsource':'source_office'})
    # Subset to events with at least one death
    df = df.loc[df['best']>0,:]
    # FORMAT AND CREATE NEW COLUMNS
    df['dataset'] = "GlobalTerrorDB"
    df['nid'] = 328214
    df['location'] = df['location'].replace({'Unknown':''})
    # FORMAT START DATE
    df['imonth'] = df['imonth'].replace({0:6})
    df['iday'] = df['iday'].replace({0:15})
    # Create a single date object from multiple fields
    df['date_start'] = (df.apply(lambda x: dt.date(year=x['year'],month=x['imonth'],
                                               day=x['iday']).strftime('%Y/%m/%d'), axis=1))
    # PROCESS END DATE
    def process_form1(txt):
        if re.match("[0-9][0-9]?/[0-9][0-9]?/[0-9][0-9][0-9][0-9]",txt):
            # The text is in form 1
            threepart = [int(i) for i in txt.split("/")]
            # Make sure that dates fall within valid ranges
            if threepart[0] not in range(1,13):
                return None
            if threepart[1] not in range(1,32):
                return None
            if threepart[2] not in range(1900,2018):
                return None
            # Process the date 
            return dt.date(year=threepart[2],month=threepart[0],
                            day=threepart[1]).strftime('%Y/%m/%d')
        else:
            # The text is not in form 1
            return None
    def process_form2(txt):
        if re.match("(January|February|March|April|May|June|July|August"
                    "|September|October|November|December) [0-9][0-9]?-"
                    "[0-9][0-9]?, [0-9][0-9][0-9][0-9]",txt):
            # The text is in date form 2
            # Get year
            year = int(txt[-4:])
            # Get month
            month_txt = txt[:txt.index(" ")]
            month_dict = {"January":1,"February":2,"March":3,
                          "April":4,"May":5,"June":6,"July":7,
                          "August":8,"September":9,"October":10,
                          "November":11,"December":12}
            month = month_dict[month_txt]
            # Get day
            day = int(txt[txt.index("-")+1:txt.index(",")])
            return dt.date(year,month,day).strftime('%Y/%m/%d')
        else:
            # The text is not in date form 2
            return None
    def get_end_date(date_text):
        if type(date_text) is str:        
            end_date = process_form1(date_text) or process_form1(date_text) or np.nan
        else:
            end_date = np.nan
        return end_date

    df['date_end'] = (df['approxdate']
                       .apply(get_end_date))
    df.loc[df['date_end'].isnull(),'date_end'] = df.loc[df['date_end'].isnull(),'date_start']
    # Format sides
    df['side_a'] = df['side_a'].replace({'Unknown':''})
    df['side_b'] = df['side_b'].replace({'Unknown':''})
    # Format citations
    def join_drop_blank(list_of_text):
        list_of_text = [t for t in list_of_text if type(t) is str and t!='']
        return ";;".join(list_of_text)
    df['source_article'] = df.apply(lambda x: join_drop_blank([x['scite1'],
                                                x['scite2'],x['scite3']]),axis=1)
    df = df.loc[:,['country','admin1','location','latitude','longitude','year',
                   'date_start','date_end','nid','dataset','dataset_event_type',
                   'dataset_notes','side_a','side_b','source_office',
                   'source_article','best']]
    return df


#CPOST Suicide Attack Database
def format_csad(path):
    df = pd.read_csv(path, encoding='utf8')
    # RENAME COLUMNS
    df = df.rename(columns={'number_killed':'best',
                            'verified_attack_type':'dataset_event_type',
                            'campaign_name':'event_name',
                            'location_names':'country'})
    # Subset to only rows where at least one person died
    df = df.loc[df['best'] > 0,:]
    # FORMAT COLUMNS
    df['dataset'] = "CPOST_SAD"
    df['nid'] = 328256
    # Add a dummy NID for now
    df['date_start'] = (pd.to_datetime(df['attack_date'],format="%m/%d/%Y")
                          .dt.strftime('%Y/%m/%d'))
    df['date_end'] = df['date_start']
    df['year'] = df['date_start'].apply(lambda date: date[:4])
    # Replace one country to match the GBD naming conventions
    df['country'] = df['country'].replace(
                                {'Palestinian Territory, Occupied':'Palestine'})
    # Subset to only necessary columns
    df = df.loc[:,['country','year','date_start','date_end','nid','dataset',
                   'dataset_event_type','event_name','best']]
    # Return processed dataframe
    return df


# RAND World Terrorism Incidents database
def format_rand_wti(path):
    df = pd.read_csv(path, encoding='latin1')
    # Process the dataframe
    # Rename columns
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={'city':'location',
                            'perpetrator':'side_a',
                            'description':'dataset_notes',
                            'fatalities':'best'})
    # Subset to only columns where at least one death has occurred
    df = df.loc[df['best'] > 0,:]
    # FORMAT COLUMNS
    df['dataset'] = "RAND_WTI"
    df['nid'] = 328276
    # Date formatting
    date_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
                 'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
    def format_date(date_text,date_dict):
        date_split = date_text.split("-")
        day = int(date_split[0])
        month = date_dict[date_split[1]]
        yr_stub = int(date_split[2])
        year = 1900 + yr_stub if yr_stub > 18 else 2000 + yr_stub
        return dt.date(year=year, month=month, day=day).strftime('%Y/%m/%d')
    df['date_start'] = (df['date'].str.lower()
                                  .apply(lambda x: format_date(x,date_dict)))
    df['date_end'] = df['date_start']
    df['year'] = df['date_start'].apply(lambda x: x[:4])
    df['side_a'] = df['side_a'].replace({'Unknown':''})
    df['dataset_event_type'] = df['weapon'].apply(lambda x: "Terrorism-{}".format(x))
    # Return processed dataframe
    return df


# Yemen Cholera Supplement
def format_yemen_cholera_supp(path):
    df = pd.read_csv(path, encoding='utf8')
    # This dataset is already more or less formatted correctly.
    df['dataset'] = "yemen_cholera"
    df['nid'] = 332916
    # Return processed dataframe
    return df


def format_war_supp_2015(path):
    df = pd.read_csv(path, encoding='latin1')

    # single date events
    single_day_events = df['date'].str.extract(
        '(\d{1,2}-[A-z]{3}$)', expand=False)
    df.loc[single_day_events.notnull(), 'date_start'] = pd.to_datetime(
        single_day_events + '-2015', format='%d-%b-%Y')
    df.loc[single_day_events.notnull(), 'date_end'] = pd.to_datetime(
        single_day_events + '-2015', format='%d-%b-%Y')

    # multi-day events
    df['date'] = df['date'].str.replace('ongoing', '12/31')
    multiday_events = (
        df['date'].str.extract(
            '(\d{1,2}/\d{1,2}-.*$)',
            expand=False) .str.split(
            '-',
            expand=True) .rename(
                columns={
                    0: 'start',
                    1: 'end'}))
    df.loc[multiday_events['start'].notnull(), 'date_start'] = pd.to_datetime(
        multiday_events['start'] + '/2015', format='%m/%d/%Y')
    df.loc[multiday_events['end'].notnull(), 'date_end'] = pd.to_datetime(
        multiday_events['end'] + '/2015', format='%m/%d/%Y')

    # null dates
    df.loc[df.date.isnull(), 'date_start'] = pd.to_datetime('01/01/2015')
    df.loc[df.date.isnull(), 'date_end'] = pd.to_datetime('12/31/2015')

    # date-times back to strings (other datasets are strings)
    df['date_start'] = df.date_start.dt.strftime('%Y/%m/%d')
    df['date_end'] = df.date_end.dt.strftime('%Y/%m/%d')

    # year
    df['year'] = 2015

    # source
    df['source_article'] = df['source_1'] + ";;" + df['source_2'].fillna('')

    # notes
    df['dataset_notes'] = df['comments'].fillna(
        '') + ';; ' + df['indexer comments'].fillna('')

    # fix one typo in the 'high column
    df['high'] = df['high'].str.replace(',', '').astype(float)

    df = (df.rename(columns={'side_1': 'side_a',
                             'side_2': 'side_b',
                             'iso3': 'iso',
                             'event': 'dataset_event_type'
                             })
          .drop(['date',
                 'comments', 'indexer comments',
                 'source_1', 'source_2'],
                axis=1)
          )
    df['dataset'] = 'war_supp_2015'
    return df


def format_war_exov(path):
    ''' Reformat "war_exceptions overrides" for inclusion in shocks db.'''
    df = pd.read_csv(path, encoding='latin1')

    # event_type
    df['dataset_event_type'] = df['cause'].str.replace('-', '_')

    df['location'] = df['admin1'] = df['urban_rural'] = ''
    mpu = (df.location_name == 'Madhya Pradesh, Urban')
    df.loc[mpu, 'location'] = 'Madhya Pradesh'
    df.loc[mpu, 'admin1'] = 'Madhya Pradesh'
    df.loc[mpu, 'urban_rural'] = 'u'

    df = (df.rename(columns={'location_name': 'country',
                             'deathnumberbest': 'best',
                             'source': 'source_name',
                             'ihme_loc_id': 'iso'})
          .drop(['level',
                 'cause'],
                axis=1)
          )
    df['dataset'] = 'war_exceptions_overrides_GBD2016'
    return df


def format_dod(path_icd9, path_icd10):

    dod_icd9 = pd.read_csv(path_icd9)
    dod_icd10 = pd.read_csv(path_icd10)
    assert (dod_icd9.columns == dod_icd10.columns).all()
    df = dod_icd9.append(dod_icd10)

    # remove 'acause' from cause
    df['cause'] = df.cause.str.replace('acause_', '')

    # only keep shocks:
    df = df[df.cause.str.contains('inj_war_')]

    # turn 'deaths#' into age_group_id
    death_vars = df.columns[df.columns.str.contains('deaths')].tolist()[:-1]
    index_cols = ['year',
                  'source_type',
                  'source',
                  'sex',
                  'location_id',
                  'iso3',
                  'cause',
                  'NID']
    df = pd.melt(df,
                 id_vars=index_cols,
                 value_vars=death_vars,
                 var_name='var',
                 value_name='best')
    df = df.loc[(df.best > 0) & (df.best.notnull())]
    df['age_group_id_f0_if2'] = df['var'].str.replace('deaths', '').astype(int)


    df.loc[df.age_group_id_f0_if2.isin(list(range(6, 21))).fillna(
        False), 'age_group_id'] = df.age_group_id_f0_if2 + 1
    age_translation_special = {(3, 4, 5, 6): 5,
                               (22,): 30,
                               (23,): 31,
                               (24,): 32,
                               (25,): 235,
                               (91, 92): 2,
                               (93,): 3,
                               (94,): 4}
    for k, v in age_translation_special.items():
        df.loc[df.age_group_id_f0_if2.isin(
            k).fillna(False), 'age_group_id'] = v

    # rename / drop cols
    df = (df.rename(columns={'iso3': 'iso',
                             'NID': 'nid',
                             'cause': 'dataset_event_type',
                             'sex': 'sex_id',
                             'source': 'source_office'})
          .drop(['var',
                 'age_group_id_f0_if2'],
                axis=1)
          )

    df['dataset'] = 'USDoD'

    return df


def format_terror_scrape_16(path):
    ''' Format terrorism data, as scraped from Wikipedia for GBD 2016,
        for inclusion in the shocks database.
    '''

    df = pd.read_csv(path)


    df['year'] = 2016
    # split start/end days
    df[['start_day', 'end_day']] = (df.Date.str.split('-', expand=True)
                                    .rename(columns={0: 'start_day',
                                                     1: 'end_day'})
                                    )
    df.loc[df.end_day.str.match('^[a-zA-Z]+$').fillna(False), 'end_day'] = None
    # create start/end cols
    df['start_date'] = df.start_day.astype(
        str) + '-' + df.month + '-' + df.year.astype(str)
    df['end_date'] = df.end_day.astype(
        str) + '-' + df.month + '-' + df.year.astype(str)
    df.loc[df.end_date.str.contains("None"), 'end_date'] = None
    df['date_start'] = pd.to_datetime(
        df.start_date, format='%d-%b-%Y', errors='coerce')
    df['date_start'] = df.date_start.dt.strftime('%Y/%m/%d')
    df['date_end'] = pd.to_datetime(
        df.end_date, format='%d-%b-%Y', errors='coerce')
    df['date_end'] = df.date_end.dt.strftime('%Y/%m/%d')
    df.loc[df.date_end.str.contains('NaT').fillna(
        False), 'date_end'] = df.date_start


  
    df_1 = (df.Dead.str.split('\(\+', expand=True)
            .rename(columns={0: 'deaths_civilians',
                             1: 'deaths_a'})
            )
    df_1['deaths_a'] = (df_1.deaths_a.str.replace('[7]', '')
                        .str.replace('25-', '')
                        .str.replace('6-', '')
                        .str.replace('[^0-9]+', '')
                        )
    df_1['deaths_civilians'] = (
        df_1.deaths_civilians.str.replace(
            '\[7\]','')
        .str.replace('\[31\]','')
        .str.replace('135\-','')
        .str.replace('1\-','')
        .str.replace('4\-','')
        .str.replace('56\+ to ','')
        .str.replace('10\+ to ','')
        .str.replace('23\+ to','')
        .str.replace('7\+ to ','')
        .str.replace('64\+ to ','')
        .str.replace('11\-','')
        .str.replace('5 \-','')
        .str.replace('48\-','')
        .str.replace('\/ 4','')
        .str.replace('\(5\) \[159\]','')
        .str.replace('[^0-9]+',''))
    df[['deaths_a', 'deaths_civilians']] = df_1

    # drop /rename cols
    df = (df.drop(['Date',
                   'Dead',
                   'month',
                   'start_day',
                   'end_day',
                   'start_date',
                   'end_date',
                   'Unnamed: 0'], axis=1)
          .rename(columns={'Perpetrator': 'side_a',
                           'Part of': 'event_name',
                           'Location': 'location',
                           'Injured': 'injured',
                           'Type': 'dataset_event_type',
                           'Details': 'dataset_notes'})
          )

    df['dataset'] = 'terrorism_scrape_2016'
    df['nid'] = 283521

    return df


def format_war_supp_2014a(paths):

    me1 = pd.read_excel(paths[0])
    me1 = me1.rename(columns={'ISO_location_of_death': 'iso',
                              'NID': 'nid',
                              'number of deaths': 'best',
                              'low estimate': 'low',
                              'high estimate': 'high',
                              'Weblink': 'source_article',
                              'Source': 'source_office'})
    me2 = pd.read_excel(paths[1])
    me2 = me2.rename(columns={'ISO': 'iso',
                              'number of deaths': 'best',
                              'low estimate': 'low',
                              'high estimate': 'high',
                              'Weblink': 'source_article',
                              'Source': 'source_office'})
    lby = pd.read_excel(paths[2])
    lby = lby.rename(columns={'ISO_location': 'iso',
                              'NID': 'nid',
                              'Year ': 'year',
                              'Best': 'best',
                              'Low': 'low',
                              'High': 'high',
                              'Source ': 'source_article'})
    syr = pd.ExcelFile(paths[3])
    syr_0 = syr.parse(0)
    syr_1 = syr.parse(1)
    syr = syr_0.append(syr_1)
    syr = syr.rename(columns={'ISO': 'iso',
                              'NID': 'nid',
                              'number of deaths': 'best',
                              'low estimate': 'low',
                              'high estimate': 'high',
                              'Weblink': 'source_article',
                              'Source': 'source_office'})
    dfs = [me1, me2, lby, syr]
    df = pd.concat(dfs)
    df = df.reset_index().drop('index', axis=1)


    df.loc[df.year_end.str.contains('now').fillna(False), 'year_end'] = 2014
    df.loc[(df.year_end == df.year_start), 'year'] = df.year_start
    df['start_date'] = None
    # date_start
    df.loc[df['Event Date'].notnull(), 'start_date'] = df['Event Date'].str.replace(
        '[thnds]{2,3}(?= [A-Z])', '')
    df['start_date'] = df.start_date.str.replace('March', "Mar")
    df['start_date'] = df.start_date.str.replace('Sept', "Sep")
    df['start_date'] = df.start_date.str.replace('June', "Jun")
    df.loc[df.start_date.isin(['Dec', 'Aug']),
           'start_date'] = '15 ' + df.start_date
    df['date_start'] = pd.NaT
    df.loc[df.start_date.notnull().fillna(False),
           'date_start'] = pd.to_datetime((df.start_date +
                                           ' ' +
                                           df.year_start.astype(str).str.replace('.0$', '')),
                                          format='%d %b %Y')
    df['date_start'] = df.date_start.dt.strftime('%Y/%m/%d')
    df.loc[df.date_start != pd.NaT, 'date_end'] = df.date_start
    # multi year
    df.loc[((df.year_end != df.year_start) & (df.year_end.notnull())).fillna(
        False), 'date_start'] = df.year_start.astype(str).str.replace('.0$', '') + '/01/01'
    df.loc[((df.year_end != df.year_start) & (df.year_end.notnull())).fillna(
        False), 'date_end'] = df.year_end.astype(str) + '/12/31'


    # dataset_event_type
    df['dataset_event_type'] = df['Conflict type']

    # dataset_notes
    df['dataset_notes'] = df['Conflict type'] + \
        ';;' + df.Content + ';;' + df['Our purpose']

    # drop /rename cols
    df = (df.drop(['multi_country#',
                   'duplicates',
                   'Duplicate',
                   'Conflict type',
                   'Content',
                   'Our purpose',
                   'start_date',
                   'Event Date',
                   'year_start',
                   'year_end',
                   'year_est'], axis=1)
          .rename(columns={'Battle Name': 'event_name',
                           })
          )
    # dataset
    df['dataset'] = 'war_supplement_2014a'

    return df



def format_emdat(path):

    #returns the year range if the event lasted over 1 year
    def find_year(row):
        if row['start year'] == row['end year']:
            return str(row['year'])
        else:
            return str(row['start year']) + '-' + str(row['end year'])

    #puts the start date in the format yyyy/mm/dd
    def date_start(row):
        row = row.fillna(1)
        return str(int(row['start year'])) + '/' + str(int(row['start month'])) + '/' + str(int(row['start day']))

    #puts the end date in the format yyyy/mm/dd
    def date_end(row):
        row = row.fillna(1)
        return str(int(row['end year'])) + '/' + str(int(row['end month'])) + '/' + str(int(row['end day']))

    #removes accents from strings
    def normalize_string(string):
        unaccented_string = unidecode.unidecode(string)
        return unaccented_string

    #formats the longitude and latitutde
    def latlong(row):
        try:
            string = str(row)
        except:
            string = 'nan'
        if string == np.NaN:
            return np.NaN
        elif len(string.split()) == 2:
            string = string.split()
            if (string[1] == 'W' or string[1] == 'S'):
                return float(string[0]) *-1
            else:
                try:
                    return float(string[0])
                except:
                    return np.NaN
        else:
            try:
                return float(string)
            except:
                return np.NaN

    #read in the emdat excel file
    path = 'FILEPATH'
    df = pd.read_excel(path)

    #drop before year 1950
    df = df[df['year'] >= 1950]

    df = df[df['disaster type'] != 'Epidemic']

    #drop events that do not have a death count
    df = df[~df['total deaths'].isnull()]

    #rename the columns we can use
    df = df.rename(columns = {'total deaths': 'best',
                              'country name': 'country',
                              'event name': 'event_name'})

    df['high'] = np.where((df['best'] == 610000) & (df['iso'] == 'PRK'), 3000000, np.NaN)
    df['low'] = np.where((df['best'] == 610000) & (df['iso'] == 'PRK'), 250000, np.NaN)


    df['year'] = df.apply(find_year, axis =1)
    df_to_split = df[df['year'].str.contains('-')]
    df = df[~df['year'].str.contains('-')]

    #splits the events that span multiple years into multiple columns
    new_df = pd.DataFrame()
    for row in df_to_split.index:
        #pulls out the start and end year
        year = df_to_split.loc[row]['year'].split('-')
        #years between the start and end year
        years = int(year[1]) - int(year[0]) + 1
        #dpy = deaths per year
        dpy = df_to_split.loc[row]['best'] / years
        dpyh = df_to_split.loc[row]['high'] / years
        dpyl = df_to_split.loc[row]['low'] / years
        for i in range(years):
            to_add = pd.DataFrame(df_to_split.loc[row]).transpose()
            to_add['year'] = int(year[0]) + i
            to_add['best'] = dpy
            to_add['high'] = dpyh
            to_add['low'] = dpyl
            new_df = new_df.append(to_add)

    #combines the original DataFrame with the year split DataFrame
    df = df.append(new_df)

    #reset the index
    df = df.reset_index(drop = True)

    df['dataset'] = 'EMDAT'
    df['nid'] = 13769
    df['age_group_id'] = 22
    df['dataset_event_type'] = (df['disaster type'] + "_" + 
                                df['disaster subtype'] + "_" +
                                df['associated disaster'] + "_" +
                                df['associated disaster2'])
    df['date_start'] = df.apply(date_start, axis =1)
    df['date_end'] = df.apply(date_end, axis =1)
    df['sex_id'] = 3
    df['latitude'] = df['latitude'].apply(latlong)
    df['longitude'] = df['longitude'].apply(latlong)
    #converts all the years to integers from strings
    df['year'] = df['year'].apply(lambda x: int(float(x)))

    #check the dates and create the time_precision column
    year_regex = r'^\d{4}//$'
    month_regex = r'^\d{4}/\d{1,2}/$'
    day_regex = r'^\d{4}/\d{1,2}/\d{1,2}$'

    df.loc[~df['start year'].isnull().fillna(False), 'time_precision'] = 5
    df.loc[(~(df['start month'].isnull()) & df['start day'].isnull()).fillna(False), 'time_precision'] = 4
    df.loc[(df['start month'].isnull() & df['start day'].isnull()).fillna(False), 'time_precision'] = 1
    df.loc[(df['start month'].isnull() & ~(df['start day'].isnull())).fillna(False), 'time_precision'] = 1

    df.loc[df.date_start == '1992/9/31', 'date_start'] = '1992/9/30'
    df.loc[df.date_end == '1992/9/31', 'date_end'] = '1992/9/30'

    df['country'] = df['country'].apply(normalize_string)
    df['country'] = df['country'].apply(lambda x: str(x)[:-6]
                                              if str(x).lower().endswith(' (the)')
                                              else x)

    #columns going into the final DataFrame
    cols = ['country',
           'iso',
           'location',
           'latitude',
           'longitude',
           'year',
           'date_start',
           'date_end',
           'time_precision',
           'event_name',
           'nid',
           'dataset',
           'dataset_event_type',
           'age_group_id',
           'sex_id',
           'high',
           'low',
           'best']

    df = df[cols]

    assert df.date_start.str.match('^\d{4}/\d{1,2}/\d{1,2}$').all()
    assert df.date_end.str.match('^\d{4}/\d{1,2}/\d{1,2}$').all()

    return df

    
def format_disaster_exov(path):
    df = pd.read_csv(path, encoding='latin1')

    # event_type
    df['dataset_event_type'] = df['cause']

    df['location'] = df['admin1'] = df['urban_rural'] = df['country'] = ''
    mpu = (df.location_name == 'Madhya Pradesh, Urban')
    df.loc[mpu, 'location'] = 'Madhya Pradesh'
    df.loc[mpu, 'admin1'] = 'Madhya Pradesh'
    df.loc[mpu, 'urban_rural'] = 'u'

    has_adm1 = (df.ihme_loc_id.str.contains('_'))
    df.loc[has_adm1 & ~mpu, 'location'] = df.location_name
    df.loc[has_adm1 & ~mpu, 'admin1'] = df.location_name

    df.loc[~has_adm1, 'country'] = df.location_name
    ##########################################################################

    df = (df.rename(columns={'numkilled_adj': 'best',
                             'source': 'source_name',
                             'ihme_loc_id': 'iso',
                             'notes': 'dataset_notes'})
          .drop(['cause',
                 'location_name'],
                axis=1)
          )
    df['dataset'] = 'disaster_exceptions_overrides_GBD2016'
    
    df['high'] = np.where((df['best'] == 30000) & (df['location_id'] == 133) & (df['year'] == 1999), 50000, np.NaN)
    df['low'] = np.where((df['best'] == 30000) & (df['location_id'] == 133) & (df['year'] == 1999), 5000, np.NaN)
    df =df[df['location_id'] != 16]
    
    return df

def format_dis_supp_2015(path):

    tech = pd.read_csv(path[0])
    nat = pd.read_csv(path[1])
    nat = nat.rename(columns={'disaster_date': 'start_date',
                              'death_best': 'best',
                              'deaths_low': 'low',
                              'deaths_high': 'high'}
                     )
    tech = tech.rename(columns={'source': 'source_1',
                                'death_best': 'best',
                                'death_low': 'low',
                                'death_high': 'high'}
                       )
    df = pd.concat([tech, nat])
    df = df.reset_index() 


    df['dataset_notes'] = df['comments'] + ";;" + df['indexer notes']

    df['dataset_event_type'] = df['dis_type'].fillna(
        '') + '_' + df['dis_subtype'].fillna('') + '_' + df['event']

    df['source_article'] = df['source_1'] + ";;" + df['source_2']

    # dates ########################################################
    ed = (df.loc[(df.end_date.isnull()) & (df.start_date.notnull()),
                 'start_date'].str.split('-', expand=True))
    df.loc[(df.end_date.isnull()) & (
        df.start_date.notnull()), 'start_date'] = ed[0]
    df.loc[(df.end_date.isnull()) & (
        df.start_date.notnull()), 'end_date'] = ed[1]

    df.loc[df.start_date == 'july ', 'time_precision'] = 4
    df.loc[df.start_date == 'july ', 'end_date'] = '9/30/2015'
    df.loc[df.start_date == 'july ', 'start_date'] = '7/1/2015'

    df.loc[df.start_date.isnull(), 'end_date'] = '12/7/1988'
    df.loc[df.start_date.isnull(), 'start_date'] = '12/7/1988'

    df.loc[df.end_date == '12/27/20145', 'end_date'] = '12/27/2015'

    df.loc[df.end_date.str.match(
        r'^\d{1,2}/\d{1,2}$'), 'end_date'] = df.end_date + '/2015'
    df.loc[df.start_date.str.match(
        r'^\d{1,2}/\d{1,2}$'), 'start_date'] = df.start_date + '/2015'

    # standardize
    df.start_date = pd.to_datetime(
        df.start_date,
        format='%m/%d/%Y').dt.strftime('%Y/%m/%d')
    df.end_date = pd.to_datetime(df.end_date,
                                 format='%m/%d/%Y').dt.strftime('%Y/%m/%d')

    df = (df.drop(['dis_type',
                   'dis_subtype',
                   'event',
                   'disaster_no',
                   'comments',
                   'indexer notes',
                   'index',
                   'source_1',
                   'source_2'
                   ], axis=1)
          .rename(columns={'start_date': 'date_start',
                           'end_date': 'date_end'})
          )
    df['dataset'] = 'disaster_supplement_2015'

    return df


def format_121916_disaster_data(path):

    df = pd.read_csv(path)

    # sources
    df['Name/url'] = df['Name/url'].str.replace(',', ';; ')

    df.loc[df.specific == "Terorrism", 'specific'] = "Terrorism"

    # rename/drop
    df = (df.rename(columns={'Name/url': 'source_article',
                             'Finest': 'location',
                             'Subnational': 'admin1',
                             'National': 'country',
                             'Year': 'year',
                             'specific': 'dataset_event_type',
                             'Event Name': 'event_name',
                             'Notes': 'dataset_notes'})
          .drop(['Month',
                 'Day',
                 'finest GBD cause',
                 'time',
                 'location',
                 'Unnamed: 14',
                 'NID'],        
                axis=1)
          )
    df['dataset'] = 'disaster_supplement_2016'
    return df



def format_epidemics(path):

    df = pd.read_csv(path)

    # drop and rename cols
    keep_cols = ['country_name',
                 'total_deaths',
                 'iso',
                 'location',
                 'start_date_y',
                 'end_date_y',
                 'cause',
                 'note',
                 'year',
                 'populations_tosplit'
                 ]
    df = df[keep_cols]
    df = df.rename(columns={'country_name': 'country',
                            'total_deaths': 'best',
                            'start_date_y': 'date_start',
                            'end_date_y': 'date_end',
                            'cause': 'dataset_event_type',
                            'note': 'source_office',
                            'populations_tosplit': 'dataset_notes'})

    # format dates
    df.date_start = pd.to_datetime(
        df.date_start,
        format='%d/%m/%Y').dt.strftime('%Y/%m/%d')
    df.date_end = pd.to_datetime(df.date_end,
                                 format='%d/%m/%Y').dt.strftime('%Y/%m/%d')

    df['dataset'] = 'Epidemics'
    df.loc[(df.source_office == 'Gideon') & (df.dataset_event_type == 'cholera'), 'nid'] = 310153
    df.loc[(df.source_office == 'Gideon') & (df.dataset_event_type == 'meningitis'), 'nid'] = 310194
    df.loc[df.source_office.str.contains('WHO cholera'), 'nid'] = 303730
    df.loc[df.source_office.str.contains('WHO meningitis'), 'nid'] = 303733
    df.loc[df.source_office.str.contains('Siddique'), 'nid'] = 303736

    return df


def format_ebola_local(ebola_local_path):

    ebola_local = pd.read_csv(ebola_local_path, encoding='latin1')

    ebola_local = (ebola_local.loc[ebola_local['best']>0,:]
                              .dropna(subset=['best']))
    # Add location IDs by mapping the "iso" column to GBD locations
    match_iso_df = (loc_meta(location_set_id=35)
                      .loc[:,['ihme_loc_id','location_id']]
                      .dropna(axis=0, how='any'))
    iso_to_location_dict = dict(zip(match_iso_df['ihme_loc_id'],
                                    match_iso_df['location_id']))
    ebola_local['location_id'] = ebola_local['iso'].map(iso_to_location_dict)
    # Assign dataset name
    ebola_local['dataset'] = "ebola_local"
    return ebola_local


def format_ebola_ic(path):

    df = pd.read_excel(path, sheetname='Confirmed Deaths')
    df = df.transpose().reset_index().rename(columns={'index': 'idx'})
    df = df.iloc[0:2].transpose()
    fd = df.reset_index()
    fd.columns = fd.iloc[0]
    fd = fd.iloc[2:]
    fd = fd.reset_index()
    fd['age_group_id'] = pd.Series(data=[14, 22, 22], index=[0, 1, 2])
    fd['sex_id'] = pd.Series(data=[1, 3, 3], index=[0, 1, 2])
    fd['location'] = pd.Series(data=['Texas', None, None], index=[0, 1, 2])
    fd.loc[fd.idx == 'USA', 'Both'] = 1
    df = fd.rename(columns={'idx': 'country',
                            'Year': 'year',
                            'Both': 'best'})
    df = df.drop(['index'], axis=1)
    df['dataset'] = 'ebola_ic'
    df['nid'] = 245181
    df['event_type'] = "E8"
    return df


def format_mina_crowd_collapse(path):
    df = pd.read_excel(path)

    # rename/ cols
    df = (df.rename(columns={'iso3': 'country',
                             'numkilled': 'best',
                             'cause': 'dataset_event_type'}))
    df['year'] = 2015
    df['date_start'] = '2015/09/24'
    df['date_end'] = '2015/09/24'
    df['nid'] = 251829
    df['dataset'] = 'Mina_crowd_collapse_2015'

    return df


def format_syr_supp(path):

    df = pd.read_excel(path, sheet='fatalities_year_split', skiprows=1)

    df = df.iloc[:31, :]

    year_cols = df.columns[df.columns.str.match('^year.*$')].tolist()
    df = (df.drop(year_cols + ['duplicate'], axis=1)
            .rename(columns={'total': 'best',
                             'iso3': 'iso',
                             'notes': 'dataset_notes',
                             'source': 'source_office',
                             'article': 'source_headline',
                             'URL': 'source_article',
                             'NID': 'nid'}))


    df[['start_date', 'end_date']] = pd.Series([str(x) + '-' for x in df.timeframe.tolist()]
                                               ).str.split('-', expand=True).drop(2, axis=1)
    df.end_date = (df.end_date.str.replace('april', 'apr')
                              .str.replace('sept', 'sep')
                              .str.replace('july', 'jul'))
    df['date_start'] = pd.to_datetime(
        df.start_date, infer_datetime_format=True)
    df['date_end'] = (pd.to_datetime(df.end_date, infer_datetime_format=True))
    df.loc[df.date_end.isnull(), 'date_end'] = df.date_start
    df.loc[df.end_date.str.match(
        r'^[a-z]{3}.*$'), 'date_end'] = df.date_end + MonthEnd(n=1)
    df.loc[(df.end_date == '') | (df.end_date.str.match(
        r'^\d{4}$')), 'date_end'] = df.date_end + YearEnd(n=1)
    # date to string
    df.date_end = df.date_end.dt.strftime('%Y/%m/%d')
    df.date_start = df.date_start.dt.strftime('%Y/%m/%d')

    # drop auxilliary columsn
    df = df.drop(['start_date', 'end_date', 'timeframe'], axis=1)

    df['dataset'] = 'syria_supplement'
    df['iso'] = 'SYR'

    return df


def format_mzr(path):
    df = pd.read_excel(path)

    df = (
        df.rename(
            columns={
                'source': 'source_article',
                'iso3': 'iso',
                'deaths_total': 'best',
                'Lower bound (if given)': 'lower',
                'Upper bound (if given)': 'upper',
                'Notes': 'dataset_notes'}) .drop(
            [
                'Adult',
                'Children (<18)',
                'Unknown age',
                'Male',
                'Female',
                'children (unspecified sex)',
                'Unknown sex'],
            axis=1)) 

    # format dates
    df.loc[df.year_start.isnull(), 'year_start'] = df.year_end
    df['date_start'] = pd.to_datetime(
        df.year_start.astype(int).astype(str), format='%Y')
    df['date_end'] = pd.to_datetime(df.year_end.astype(
        int).astype(str), format='%Y') + YearEnd(n=1)
    df.date_start = df.date_start.dt.strftime('%Y/%m/%d')
    df.date_end = df.date_end.dt.strftime('%Y/%m/%d')

    df['dataset'] = 'mzr_research'

    return df


def format_irq_ims_ibc(path):

    df = pd.read_csv(path)

    # rename/drop cols
    df = (df.rename(columns={'location_name': 'location',
                             'deathnumberbest': 'best',
                             'source': 'source_office',
                             'ihme_loc_id': 'iso'})
          .drop('level', axis=1))

    df['dataset'] = 'IRQ_IMS_IBC'

    return df


def format_phl_supp_2015(path):
    df = pd.read_excel(path)


    df['dataset_notes'] = df['Specific name'].fillna(
        '') + ';;' + df['table_number_or_text_section'].fillna('')

    # dataset event type
    df['dataset_event_type'] = df['type'].fillna(
        '') + ';;' + df['Cause'].fillna('')

    # rename / drop columns
    df = df = (df.rename(columns={'source': 'source_office',
                                  'website': 'source_article'})
               .drop(['comments',
                      'Duplicate',
                      'type',
                      'Cause',
                      'Specific name',
                      'table_number_or_text_section'], axis=1))

    # reshape long
    year_cols = df.columns[df.columns.str.match("^year_\d{4}$")].tolist()
    id_cols = df.columns[~df.columns.isin(year_cols)].tolist()
    df = pd.melt(
        df,
        id_vars=id_cols,
        value_vars=year_cols,
        value_name='best',
        var_name='year')
    df['year'] = df['year'].str.replace('year_', '').astype(int)
    df = df[df.best.notnull()]

    df.loc[df.dataset_notes.str.contains(
        'source maggie found'), 'source_article'] = 'FILEPATH'
    df.loc[df.dataset_notes.str.contains(
        'source maggie found'), 'source_office'] = 'Asian Disaster Reduction Center'

    df = df[~df.dataset_event_type.str.contains('Rabies')]

    df['country'] = 'Philippines'
    df['iso'] = 'PHL'

    df['dataset'] = 'phl_supp_2015'

    return df


def format_pse_supp_2015(path):

    df = pd.read_csv(path, encoding='latin1')

    df['dataset_notes'] = df['table_number_or_text_section'].fillna(
        '') + ';;' + df['comments'].fillna('')

    # dataset event type
    df['dataset_event_type'] = 'war'

    df['country'] = 'Palestine'
    df['iso'] = "PSE"

    # rename / drop columns
    df = df = (df.rename(columns={'source': 'source_office',
                                  'website': 'source_article'})
               .drop(['comments',
                      'table_number_or_text_section'], axis=1))

    # reshape long
    year_cols = df.columns[df.columns.str.match("^year_\d{4}$")].tolist()
    id_cols = df.columns[~df.columns.isin(year_cols)].tolist()
    df = pd.melt(
        df,
        id_vars=id_cols,
        value_vars=year_cols,
        value_name='best',
        var_name='year')
    df['year'] = df['year'].str.replace('year_', '').astype(int)

    df = df[df.best.notnull()]

    df['dataset'] = 'pse_supp_2015'
    df['nid'] = 253026

    return df


def format_multi_supp_2015(path):

    dfs = []
    for loc in ['ARE', 'AND', "MHL", 'QAT']:
        df = pd.read_excel(path, sheetname=loc)

        # reshape long
        year_cols = df.columns[df.columns.str.match("^year_\d{4}$")].tolist()
        id_cols = df.columns[~df.columns.isin(year_cols)].tolist()
        df = pd.melt(
            df,
            id_vars=id_cols,
            value_vars=year_cols,
            value_name='best',
            var_name='year')
        df['year'] = df['year'].str.replace('year_', '').astype(int)

        df = df[df.best.notnull()]

        # iso
        df['iso'] = loc

        # store
        dfs.append(df)

    df = pd.concat(dfs)

    # rename / drop columns
    df = (df.rename(columns={'source': 'source_office',
                             'website': 'source_article',
                             'comments': 'dataset_event_type',
                             'table_number_or_text_section': 'dataset_notes'})
          .drop(['type'], axis=1))

    df['dataset'] = 'multi_supp_2015'

    return df

def format_tweet_2017_supp(path):
    df = pd.read_csv(path,
                    dtype={'date_start':str, 'date_end':str},
                    encoding = 'utf8')

    df['dataset'] = "Twitter_2017"
    #return the dataframe
    return df

def format_shocks_2017(path):
    df = pd.read_csv(path,
                    dtype={'date_start':str, 'date_end':str},
                    encoding = 'utf8')

    df['dataset'] = "Shocks_2017"
    #return the dataframe
    return df

def format_iraq_iran_war(path):

    df = pd.read_csv(path, encoding = 'utf8')
    df['dataset'] = 'iraq_iran_war'
    df.loc[df.location_id != 143,'iso'] = 'IRN'
    df.loc[df.location_id == 143, 'iso'] = 'IRQ'
    df['event_type_name'] = 'War'
    df['event_type'] = 'B1'
    df.loc[df.location_id != 143, 'country'] = 'Iran'
    df.loc[df.location_id == 143, 'country'] = 'Iraq'
    df['dataset_notes'] = 'supplement for iran_iraq war 1980-2005'
    df['nid'] = 335827

    return df


def format_Vietnam_War(path):

    df = pd.read_csv(path)
    df['date_start'] = df['year'].astype(str) + '/01/01'
    df['date_end'] = df['year'].astype(str) + '/12/31'

    df['dataset'] = 'US_Archives_gov'
    df['event_type_name'] = 'War'
    df['event_type'] = 'B1'
    df.loc[df.location_id == 20, 'iso'] = 'VNM'
    df.loc[df.location_id != 20, 'iso'] = 'USA'
    df.loc[df.location_id == 298, 'iso'] = 'ASM'
    df.loc[df.location_id == 351, 'iso'] = 'GUM'
    df.loc[df.location_id == 68, 'iso'] = 'KOR'
    df.loc[df.location_id == 44850, 'iso'] = 'NZL'
    df.loc[df.location_id == 44851, 'iso'] = 'NZL'
    df.loc[df.location_id == 71, 'iso'] = 'AUS'
    df.loc[df.location_id == 18, 'iso'] = 'THA'

    df.loc[df.location_id == 20, 'country'] = 'Vietnam'
    df.loc[df.location_id != 20, 'country'] = 'United States'
    df.loc[df.location_id == 298, 'country'] = 'American Samoa'
    df.loc[df.location_id == 351, 'country'] = 'Guam'
    df.loc[df.location_id == 68, 'country'] = 'South Korea'
    df.loc[df.location_id == 44850, 'country'] = 'New Zealand'
    df.loc[df.location_id == 44851, 'country'] = 'New Zealand'
    df.loc[df.location_id == 71, 'country'] = 'Australia'
    df.loc[df.location_id == 18, 'country'] = 'Thailand'

    df['dataset_notes'] = 'supplement for Vietnam War death splitting'

    return df

def format_the_great_leap_forward(path):
    df = pd.read_excel(path)
    df['iso'] = 'CHN'
    df['country'] = "China"
    df['event_type'] = "Famine"
    df['cause_id'] = 387
    df['nid'] = 350340 
    df['source'] = "TombStone"
    df.drop(['deaths in millions','death rate'], axis =1, inplace=True)
    df['event_name'] = "the great leap forward"
    df['year'] = df['year_id']
    df['dataset'] = "TombStone"
    df['dataset_event_type'] = "Famine"

    return df


def format_2018_collaborator_shocks(path):
    df = pd.read_excel(path)
    df['dataset'] = "collaborator_overrides"
    return df


def format_current_year_shocks(path):
    df = pd.read_excel(path)
    df['dataset'] = "current_year_shocks"
    return df
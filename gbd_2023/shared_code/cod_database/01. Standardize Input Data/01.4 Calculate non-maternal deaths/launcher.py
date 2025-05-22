
from pathlib import Path
import importlib
import sys
import pandas as pd
import argparse
import logging
import time

from processing_01 import *
from processing_02 import *
from processing_03 import *

from cod_prep.claude.formatting import finalize_formatting, update_nid_metadata_status
from cod_prep.downloaders import get_cause_map, get_cod_ages, add_envelope, add_births
from cod_prep.claude.formatting import finalize_formatting
from cod_prep.utils.formatting import ages
from cod_prep.utils import print_log_message, report_if_merge_fail
from cod_prep.claude.configurator import Configurator

LOG_FILENAME = 'FILEPATH' + time.strftime("%Y%m%d_%H%M%S")+'.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
CONF = Configurator()
ID_COLS = [
    'nid', 'location_id', 'year_id', 'age_group_id', 'sex_id', 'data_type_id',
    'representative_id', 'code_system_id', 'cause', 'site'
]
INT_COLS = ['nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
            'code_system_id', 'data_type_id', 'representative_id']

SOURCE = 'DHS_maternal'
WRITE = False
IS_ACTIVE = True
IS_MORT_ACTIVE = False
PROJECT_ID = CONF.get_id('project')


def launch_dhs(args):
    countries = args.countries

    dhs_sources = pd.read_csv(
        'FILEPATH', 
        encoding='cp437'
    )

    dhs_sources['full_path'] = 'FILEPATH'

    dhs_sources_countries = dhs_sources.copy()
    if args.new:
        dhs_sources_countries = dhs_sources_countries[dhs_sources_countries.new == 1]

    if countries == None:
        countries_to_run = dhs_sources_countries['country'].unique().tolist()
    else:
        countries_to_run = countries

    dfs_countries = []
    success_df = pd.DataFrame()
    for country in countries_to_run:
        try: 
            print_log_message('Starting DHS formatting for ' + country + '...')
            studies = dhs_sources.loc[dhs_sources['country'] == country, ]
            study_years = studies['year'].unique().tolist()
            dfs_step1 = []
            if country == 'AFG':
                study_years.remove('2015_2016')
            if country == 'PHL':
                study_years.remove('1998')
                study_years.remove('2013')
            for study_year in study_years:
                if '_' in study_year:
                    study_year1 = int(study_year.split('_')[0])
                    study_year2 = int(study_year.split('_')[1])
                else:
                    study_year1 = int(study_year)
                    study_year2 = None
                index_end_dict = {
                    'GHA': {2007: 18},
                    'AFG': {2010: 31},
                    'GIN': {2005: 15},
                    'KEN': {1998: 18},
                    'KHM': {2000: 17},
                    'NAM': {2000: 20},
                    'SDN': {1989: 17},
                    'MRT': {2000: 16}
                }
                if country in index_end_dict.keys():
                    country_index_dict = index_end_dict[country]
                    if study_year1 in country_index_dict.keys():
                        index_end = country_index_dict[study_year1]
                    else:
                        index_end = 21
                else:
                    index_end = 21
                filepath = 'FILEPATH'
                data_dir = 'FILEPATH'
                print_log_message('Cleaning ' + study_year + ' survey')
                df_step1 = process_dhs_01(data_dir, country, index_end, study_year1, study_year2)
                dfs_step1.append(df_step1)
            df_step1 = pd.concat(dfs_step1)
            df_step1 = df_step1.reset_index()
            df_country = process_dhs_02(df_step1)
            dfs_countries.append(df_country)
        except:
            logging.exception("Exception occured for file" + str(filepath))
            continue
        else:
            success_df = pd.concat([success_df, studies])
    success_df.to_csv('FILEPATH'+time.strftime("%Y%m%d_%H%M%S")+'.csv', 
                      index=False)
    df_all = pd.concat(dfs_countries)
    print_log_message('Process all countries of interest together for nids, location_ids, and CoD specific id values')
    df = process_dhs_03(df_all)
    for col in INT_COLS:
        df[col] = df[col].astype(int)
    df = df.groupby(ID_COLS, as_index=False).deaths.sum()
    assert df.notnull().values.all()

    locals_present = finalize_formatting(df, SOURCE, PROJECT_ID, write=WRITE)
    nid_meta_df = locals_present['nid_meta_df']

    if WRITE:
        nid_extracts = nid_meta_df[
            ['nid', 'extract_type_id']
        ].drop_duplicates().to_records(index=False)
        for nid, extract_type_id in nid_extracts:
            nid = int(nid)
            extract_type_id = int(extract_type_id)
            update_nid_metadata_status(PROJECT_ID, nid, extract_type_id, is_active=IS_ACTIVE,
                                       is_mort_active=IS_MORT_ACTIVE)

if __name__ == "__main__":

    p = argparse.ArgumentParser(prog = 'launcher',
                                description="process cod DHS maternal survey data")
    p.add_argument('--country', '-c', dest='countries', action='append', help='enter Country iso3')
    p.add_argument('-v', help='verobse', action='store_true')
    p.add_argument('--new', action='store_true')
    args = p.parse_args()
    launch_dhs(args)


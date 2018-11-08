
# Imports (general)
import numpy as np
import os
import sys
import pandas as pd
from os.path import join
from shutil import copyfile
# Imports (this directory)
from .map_location_names import location_dictionary_map, match_to_gbd_locations
from .multi_loc_splitting import multi_loc_split, split_by_side
from .split_location_archetypes import define_and_save_all as archetype_split
from .geocode_prep import geocode_shocks
# Imports (from top-level)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import qsub, save_tools


################################################################################
# HELPER FUNCTIONS
################################################################################
def submit_first_overlay(infile, outfile, long_col, lat_col,
                code_dir="FILEPATH",
                hold_jids=None):
    query_args = ("--infile {} --outfile {} --latitude_col {} "
                  "--longitude_col {}".format(infile, outfile, lat_col, long_col))
    out_jid = qsub.qsub(program_name=join(code_dir,
                            "FILEPATH"),
                        program_args=query_args,
                        slots=15,
                        hold_jids=hold_jids,
                        qsub_name="first_overlay")
    return out_jid


def submit_all_geocoding(geocode_dir, outdir, code_dir,
                         location_col='location_name',iso2_col='iso2'):
    # Add all Google Maps keys directly
    gmaps_keys = ["PASSWORD"]
    # Get the list of all files to submit
    files_to_submit = [i for i in os.listdir(geocode_dir) 
                           if i.lower().endswith('.xlsx')
                           or i.lower().endswith('.xls')]
    assert len(gmaps_keys) >= len(files_to_submit)
    job_jids = list()

    submit_code_path = 'FILEPATH'
    for i in range(len(files_to_submit)):
        infile = join(geocode_dir,files_to_submit[i])
        outfile = join(outdir,files_to_submit[i])
        query_args = ("--infile {} --outfile {} --address {} --iso {} "
                      "--keygm {} --geonames --nomaps".format(infile,
                            outfile, location_col, iso2_col, gmaps_keys[i]))
        this_jid = qsub.qsub(program_name = submit_code_path,
                        program_args=query_args,
                        slots=10,
                        project="",
                        qsub_name = "geocode{}".format(i),
                        email_after=False)
        job_jids.append(this_jid)
    return job_jids


def submit_geocoding_compiler(in_dir, outfile, code_dir,
                              encoding='latin1', hold_jids=None):
    query_args = ("--indir {} --outfile {} --encoding {}".format(in_dir, 
                                                             outfile, encoding))
    submit_code_path = join(code_dir,'FILEPATH',
                            'FILEPATH')
    out_jid = qsub.qsub(program_name=submit_code_path,
                        program_args=query_args,
                        slots=15,
                        project="",
                        qsub_name="geocode_compile",
                        email_after=False,
                        hold_jids=hold_jids)
    return out_jid


def submit_location_compiler_all(storage_dir, outfile, outfile_split, code_dir,
                                 encoding='latin1',hold_jids=None):
    query_args = ("--storage_dir {} --outfile {} --outfile_split {}"
                  " --encoding {}".format(storage_dir, outfile,
                                          outfile_split, encoding))
    submit_code_path = join(code_dir,'FILEPATH',
                            'FILEPATH')
    out_jid = qsub.qsub(program_name=submit_code_path,
                        program_args=query_args,
                        slots=15,
                        project="",
                        qsub_name="loc_comp_all",
                        email_after=True,
                        hold_jids=hold_jids)
    return out_jid


################################################################################
# MAIN FUNCTION
################################################################################
def loc_standardize(in_df, storage_dir, code_dir,
                    location_map_file,
                    location_set_id=21,
                    data_encoding='latin1',
                    save_snapshot=True):

    mapped = location_dictionary_map(
                                    in_df=in_df,
                                    map_df_path=location_map_file)
    mapped = multi_loc_split(in_df=mapped)
    gbd_standard = match_to_gbd_locations(
                                    in_df=mapped.copy(),
                                    location_set_id=location_set_id,
                                    fuzzy_match=False)

    #Changing Guam Plane Crash 1997 to South Korea
    gbd_standard.loc[(gbd_standard['country']=='Guam') &
               (gbd_standard['dataset']=='EMDAT') &
               (gbd_standard['year']==1997), 'location_id'] = 68

    #Changing Iran Kurdistan to Iraq (Kurdistan) where PRIO_BDD reports "Iraq":
    gbd_standard.loc[(gbd_standard['country']=='Iraq') &
               (gbd_standard['location']=='Kurdistan') &
               (gbd_standard['location_id']==44884), 'location_id'] = 143

    #Changing East Pakistan police conflict deaths from Pakistan to Bangladesh:
    gbd_standard.loc[(gbd_standard['country']=='Pakistan') &
             (gbd_standard['location']=='East Pakistan') &
             (gbd_standard['location_id']==165), 'location_id'] = 161

    #Changing all Tonga, Sudan issues mapped to Tonga (country) back to Sudan:
    gbd_standard.loc[(gbd_standard['country'].str.contains('Sudan')) &
               (gbd_standard['admin3']=='Tonga') &
               (gbd_standard['location_id']==29), 'location_id'] = 522

    #Changing Finnmark Submarine crash to Russia. The Sub was a Russian attack sub
    gbd_standard.loc[(gbd_standard['country'].str.contains('Norway')) &
               (gbd_standard['year']==1989) &
               (gbd_standard['dataset']=='EMDAT') &
               (gbd_standard['location_id']==4928), 'location_id'] = 62

    #fuzzy string matching is messing up for The republic of the Congo. fix
    gbd_standard.loc[(gbd_standard['iso'] == "COG"), 'location_id'] = 170
    gbd_standard.loc[(gbd_standard['iso'] == "COG"), 'country'] = "the Republic of Congo"


    # Map multi-side conflicts to multiple locations where the locations associated
    #  with all sides is known
    PATH_TO_SIDE_SPLIT_MAP = ('FILEPATH')
    gbd_standard = split_by_side(in_df=gbd_standard,
                             sides_to_countries_filepath=PATH_TO_SIDE_SPLIT_MAP)
    # Save a snapshot of the mapped data
    mapped_file_snapshot = join(storage_dir,'FILEPATH')
    save_tools.save_pandas(gbd_standard, filepath=mapped_file_snapshot,
                           encoding=data_encoding)

    archetype_split(in_filepath=mapped_file_snapshot,
              save_dir=join(storage_dir,'FILEPATH'),
              csv_encoding=data_encoding)
    # Submit direct overlay job
    first_overlay_jid = submit_first_overlay(
                         infile=join(storage_dir,'FILEPATH'),
                         outfile=join(storage_dir,'FILEPATH'),
                         long_col='longitude',
                         lat_col='latitude',
                         code_dir=code_dir,
                         hold_jids=None)
    # Prep geocoding data
    geocode_shocks(
      shocks_for_geocoding_file=join(storage_dir,
                                     'FILEPATH'),
      iso3_to_iso2_match_file=join(code_dir,
                                   'FILEPATH'),
      cols_to_geocode=["admin1","admin2","admin3","location"],
      save_folder=join(storage_dir,'FILEPATH'),
      gbd_location_set_id=location_set_id,
      csv_encoding=data_encoding)
    # Submit all geocoding jobs
    geocode_jids = submit_all_geocoding(
                    geocode_dir=join(storage_dir,'FILEPATH'),
                    outdir=join(storage_dir,'FILEPATH'),
                    code_dir=code_dir)
    # Submit geocoding compiler
    geo_compiled_jid = submit_geocoding_compiler(
                        in_dir=join(storage_dir,'FILEPATH'),
                        outfile=join(storage_dir,'FILEPATH',
                                     'FILEPATH'),
                        code_dir=code_dir,
                        encoding=data_encoding,
                        hold_jids=geocode_jids)

    for loc_file in ['already_positioned','not_detailed','needs_manual_add']:
        loc_file = "FILEPATH".format(loc_file)
        if os.path.isfile(join(storage_dir,'FILEPATH',loc_file)):
            copyfile(src=join(storage_dir,'FILEPATH',loc_file),
                     dst=join(storage_dir,'FILEPATH',loc_file))
    # Submit the job that will stitch together ALL matched results
    locations_done_jid = submit_location_compiler_all(
                            storage_dir=storage_dir,
                            outfile=join(storage_dir,'FILEPATH',
                                         'FILEPATH'),
                            outfile_split=join(storage_dir,'FILEPATH',
                                         'FILEPATH'),
                            code_dir=code_dir,
                            encoding=data_encoding,
                            hold_jids=[geo_compiled_jid,first_overlay_jid])
    return locations_done_jid


if __name__ == "__main__":
    pass
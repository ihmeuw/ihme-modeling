'''
Generate shock database.
'''
# Imports (general)
import numpy as np
import os
import pandas as pd
import sys
from datetime import date
from os.path import join
from .format_shocks import create_shock_db
from .name_event_types import name_event_types
from .assign_events import assign_events
from .tools import review_db_contents
from .data_standardization import standardize_all_but_locs
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities.save_tools import save_snapshot
from utilities import qsub


def db_greater_than_one(compiled_data):

    max_deaths_by_row = (compiled_data.loc[:,['low','best','high',
                                      'deaths_a','deaths_b','deaths_civilians']]
                                                .max(axis=1))
    compiled_data = compiled_data.loc[max_deaths_by_row >= 1,:]
    return compiled_data

def etl():
    # Extract (and format)
    db = create_shock_db()
    for col in ['best','low','high','deaths_a','deaths_b','deaths_civilians']:
        db[col] = pd.to_numeric(db[col], errors='coerce')
    db = db_greater_than_one(db)
    # Transform
    db = assign_events(db)
    event_type_name_path = ('')
    db = name_event_types(db, event_type_name_path)
    if 'uid' not in db.columns:
        db['uid'] = db.index
    # Standardize all groups (age, sex, year) except for locations, which will
    #  come later
    db = standardize_all_but_locs(db)
    review_db_contents(db)

    # Database compiled!
    return db


def pull_vr_data(code_dir,
                 vr_temp_folder,
                 compiled_vr_filepath,
                 events_map_filepath,
                 start_year,
                 end_year,
                 encoding='utf8'):

    # Get the list of all unique cause IDs that should be pulled
    events_map = pd.read_excel(events_map_filepath)
    cause_ids = events_map.loc[events_map['pull_vr']==1,
                               'cause_id'].unique().tolist()
    cause_ids_str = ','.join([str(c) for c in cause_ids])
    # Create the VR temp directory if it does not already exist
    if not os.path.exists(vr_temp_folder):
        os.mkdir(vr_temp_folder)

    assert type(start_year) is int
    assert type(end_year) is int
    period_start_year = start_year
    period_end_year = min([period_start_year + 5, end_year])
    hold_jids = []
    while (period_start_year <= end_year):
        print("Pulling VR for years {} to {}".format(period_start_year,
                                                     period_end_year))
        pulled_vr_file = join(vr_temp_folder,"vr_{}_to_{}.csv"
                                     .format(period_start_year,period_end_year))
        pull_run_args = ("--causes {0} --startyear {1} --endyear {2} "
                         "--outfile {3} --encoding {4}"
                            .format(cause_ids_str, period_start_year,
                                    period_end_year, pulled_vr_file,encoding))
        pull_jid = qsub.qsub(
                    program_name=join(code_dir,"FILEPATH"),
                    program_args=pull_run_args,
                    python_filepath="",
                    slots=35,
                    hold_jids=None,
                    qsub_name="vr_{}to{}".format(period_start_year,
                                                 period_end_year))
        hold_jids.append(pull_jid)
        # Update to the new period
        period_start_year = period_end_year + 1
        period_end_year = min([period_start_year + 5, end_year])
    # All VR pulling jobs have now been submitted
    # Submit a final job to compile all individual VR data and delete temporary
    #  VR files
    compile_run_args = ("--indir {0} --outfile {1} --encoding {2}"
                          .format(vr_temp_folder,compiled_vr_filepath,encoding))
    compiled_vr_jid = qsub.qsub(
                        program_name=join(code_dir,"FILEPATH"),
                        program_args=compile_run_args,
                        slots=35,
                        hold_jids=hold_jids,
                        qsub_name="vr_compile")
    return compiled_vr_jid


if __name__ == "__main__":
    compiled = etl()
    compiled.to_csv('',
                    encoding='utf-8',index=False)
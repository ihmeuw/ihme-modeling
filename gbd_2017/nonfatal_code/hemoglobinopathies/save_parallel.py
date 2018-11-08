import sys
import pandas as pd
from save_results import save_results_epi
import argparse
import datetime

today_string = datetime.date.today().strftime("%d/%m/%y")

def save_hw(meid):
    save_results_epi(
        input_dir = 'FILEPATH'+ str(meid), 
        input_file_pattern = '{year_id}.h5', 
        modelable_entity_id = meid, 
        description='result of Hardy - Weinberg calculation; ' + today_string, 
        gbd_round_id = 5, 
        mark_best=True,
        birth_prevalence=True
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("meid", help="Hardy Weinberg output MEID", type=int)
    args = parser.parse_args()
    save_hw(args.meid)



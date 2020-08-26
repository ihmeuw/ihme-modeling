import sys
import pandas as pd
from save_results import save_results_epi
import argparse
import datetime

today_string = datetime.date.today().strftime("%d/%m/%y")

def save_hw(meid):
    save_results_epi(
        input_dir = 'FILEPATH', 
        input_file_pattern = 'FILEPATH', 
        modelable_entity_id = meid, 
        description='result of Hardy - Weinberg calculation; ' + today_string, 
        gbd_round_id = 6,
	decomp_step = 'step1', 
        mark_best=False,
        birth_prevalence=True
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("meid", help="Hardy Weinberg output MEID", type=int)
    args = parser.parse_args()
    save_hw(args.meid)



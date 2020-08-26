"""Correct misdiagnosis of Alzheimers.

Inputs called 'misdiagnosis_probabilies' are used and need to be re-generated
when there are substantial changes to ICD10 mapping, especially to which
packages garbage codes are redistributed in.
"""
import sys
import os
import time
import pandas as pd
import numpy as np

from configurator import Configurator
from cod_prep.utils import print_log_message
from cod_prep.claude.misdiagnosis_correction import MisdiagnosisCorrector
from cod_prep.claude.claude_io import get_claude_data, write_phase_output
from cod_prep.downloaders import get_value_from_nid
from cod_prep.claude.cause_reallocation import (
    manually_reallocate_gbdcauses)

CONF = Configurator('standard')

AVAILABLE_CODE_SYSTEMS = [1, 2, 4, 6, 7, 8, 9, 40, 73, 74]

def clear_intermediate_data(mc_process_dir, adjust_id, nid, extract_type_id):
    """Clear intermediate files as soon as run starts"""
    for subdir in ['ui', 'pct', 'orig']:
        int_file_path = 'FILEPATH'
        if os.path.exists(int_file_path):
            os.remove(int_file_path)


def run_pipeline(df, nid, extract_type_id, code_system_id, code_map_version_id,
                 remove_decimal, data_type_id, iso3):
    """Run full pipeline"""
    if code_system_id in AVAILABLE_CODE_SYSTEMS:
        adjust_ids = [543, 544, 500]
        for adjust_id in adjust_ids:
            print_log_message("Clearing intermediate files.")
            clear_intermediate_data(CONF.get_directory('mc_process_data'),
                                    adjust_id, nid, extract_type_id)
            print_log_message("Running misdiagnosis correction for cause {}".format(adjust_id))
            CorrectMisdiagnosis = MisdiagnosisCorrector(
                nid, extract_type_id, code_system_id, code_map_version_id,
                adjust_id, remove_decimal)
            df = CorrectMisdiagnosis.get_computed_dataframe(df)

    if code_system_id == 1 and data_type_id == 9 and iso3 == "ZAF":
        print_log_message("Special correction step for ICD10 South Africa VR.")
        df = manually_reallocate_gbdcauses(df)
    return df


def main(nid, extract_type_id, code_system_id, code_map_version_id, launch_set_id, remove_decimal):
    """Main method."""

    start_time = time.time()
    df = get_claude_data(
        "disaggregation", nid=nid, extract_type_id=extract_type_id
    )

    data_type_id = get_value_from_nid(nid, 'data_type_id', extract_type_id=extract_type_id)
    iso3 = get_value_from_nid(nid, 'iso3', extract_type_id=extract_type_id)

    df = run_pipeline(df, nid, extract_type_id, code_system_id, code_map_version_id,
                      remove_decimal, data_type_id, iso3)

    run_time = time.time() - start_time
    print_log_message("Finished in {} seconds".format(run_time))

    write_phase_output(
        df, "misdiagnosiscorrection", nid, extract_type_id, launch_set_id
    )
    return df


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    launch_set_id = int(sys.argv[3])
    code_system_id = int(sys.argv[4])
    code_map_version_id = int(sys.argv[5])
    remove_decimal = sys.argv[6]
    assert remove_decimal in ["True", "False"], \
        "invalid remove_decimal: {}".format(remove_decimal)
    remove_decimal = (remove_decimal == "True")
    main(nid, extract_type_id, code_system_id, code_map_version_id, launch_set_id, remove_decimal)

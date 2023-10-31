import sys
import os
import time

from cod_prep.claude.configurator import Configurator
from cod_prep.utils import print_log_message
from cod_prep.claude.misdiagnosis_correction import MisdiagnosisCorrector
from cod_prep.claude.claude_io import get_claude_data, write_phase_output
from cod_prep.downloaders import get_value_from_nid
from cod_prep.downloaders.ages import get_cod_ages
from cod_prep.claude.cause_reallocation import (
    manually_reallocate_gbdcauses, PoliceConflictCorrector)

CONF = Configurator('standard')

AVAILABLE_CODE_SYSTEMS = [1, 2, 4, 6, 7, 8, 9, 40, 73, 74]


def clear_intermediate_data(mc_process_dir, adjust_id, nid, extract_type_id):
    """Clear intermediate files as soon as run starts"""
    for subdir in ['ui', 'pct', 'orig']:
        int_file_path = 'FILEPATH'
        if os.path.exists(int_file_path):
            os.remove(int_file_path)


def get_above_forty():
    """ Returns a list of ages 40 and older. Msdc only affects deaths in those ages """
    cod_ages = get_cod_ages(force_rerun=False, block_rerun=True, cache_results=False)
    above_forty = list(cod_ages.loc[cod_ages['age_group_years_start'] >= 40, 'age_group_id'])
    return above_forty


def run_pipeline(df, nid, extract_type_id, project_id, code_system_id, code_map_version_id,
                 remove_decimal, data_type_id, iso3):
    """Run full pipeline"""
    over_40 = get_above_forty()
    has_over_40 = df['age_group_id'].isin(over_40).values.any()
    if code_system_id in AVAILABLE_CODE_SYSTEMS and has_over_40:
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
        df = manually_reallocate_gbdcauses(df, code_map_version_id, force_rerun=False, block_rerun=True)

    if data_type_id == 9 and iso3 == 'USA':
        print_log_message("Special correction step for Police Violence in United States.")
        pcc = PoliceConflictCorrector(
            tolerance=0.01, 
            code_map_version_id=code_map_version_id,
            force_rerun=False,
            block_rerun=True,
        )
        df = pcc.get_computed_dataframe(df)
    return df


def main(nid, extract_type_id, project_id, code_system_id, code_map_version_id, launch_set_id, remove_decimal):
    """Main method."""

    start_time = time.time()
    df = get_claude_data(
        "disaggregation", nid=nid, extract_type_id=extract_type_id, project_id=project_id
    )

    data_type_id = get_value_from_nid(nid, 'data_type_id', project_id=project_id,
        extract_type_id=extract_type_id)
    iso3 = get_value_from_nid(nid, 'iso3', project_id=project_id, extract_type_id=extract_type_id)

    df = run_pipeline(df, nid, extract_type_id, project_id, code_system_id, code_map_version_id,
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
    project_id = int(sys.argv[3])
    launch_set_id = int(sys.argv[4])
    code_system_id = int(sys.argv[5])
    code_map_version_id = int(sys.argv[6])
    remove_decimal = sys.argv[7]
    assert remove_decimal in ["True", "False"], \
        "invalid remove_decimal: {}".format(remove_decimal)
    remove_decimal = (remove_decimal == "True")
    main(nid, extract_type_id, project_id, code_system_id, code_map_version_id, launch_set_id, remove_decimal)

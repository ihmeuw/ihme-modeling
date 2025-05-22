# -*- coding: utf-8 -*-
"""
Updated 2018/7/31

This is the worker script to map inpatient hospital data from ICD code
to Intermediate Cause Group (ICG). There aren't any functions defined
in this script itself, the mapping code is stored in 1 place, the
clinical_mapping module to ensure uniformity across all our processes
"""
import sys

import pandas as pd
from crosscutting_functions import general_purpose
from crosscutting_functions.mapping import clinical_mapping

# first system arg, the data source
source = sys.argv[1]
# second system arg, whether it wants the deaths master data
deaths = sys.argv[2]
# third arg, whether to write a log
write_log = sys.argv[3]

# new paradigm of run_ids
run_id = sys.argv[4]

map_version = sys.argv[5]

map_version = int(map_version.replace("\r", ""))

print("Params for this job are", source, deaths, write_log, run_id)

if source == "":  # just a source to test on
    source = "IDN_SIRS"


if __name__ == "__main__":

    if deaths == "deaths":
        deaths = "deaths/"
    else:
        deaths = "formatted/"

    if write_log == "True":
        write_log = True
    else:
        write_log = False

    # read in source data
    filepath = (
        "FILEPATH"
        "FILEPATH/.H5".format(r=run_id, d=deaths, s=source)
    )
    df = pd.read_hdf(filepath)

    df = clinical_mapping.map_to_gbd_cause(
        df=df,
        retain_active_bundles=True,
        input_type="cause_code",
        output_type="icg",
        write_unmapped=True,
        truncate_cause_codes=True,
        extract_pri_dx=True,
        prod=True,
        map_version=map_version,
        write_log=write_log,
        groupby_output=True,
    )

    # write output

    outpath = (
        "FILEPATH"
        "FILEPATH.H5".format(run=run_id, source=source)
    )
    general_purpose.write_hosp_file(df, outpath, backup=False)

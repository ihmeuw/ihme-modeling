
"""
This is the worker script to map inpatient hospital data from ICD code
to Intermediate Cause Group (ICG). There aren't any functions defined
in this script itself, the mapping code is stored in 1 place, the
clinical_mapping module to ensure uniformity across all our processes
"""
import platform
import getpass
import sys
import pdb
import pandas as pd

if platform.system() == "Linux":
    root = "FILENAME"
else:
    assert False, "This code accesses "FILEPATH" and must be run on the cluster"


user = getpass.getuser()

repo = r"FILEPATH".format(user)

sys.path.append(repo + "FILENAME")
sys.path.append(repo + "FILENAME")

import hosp_prep
import clinical_mapping


source = sys.argv[1]

deaths = sys.argv[2]

write_log = sys.argv[3]


run_id = sys.argv[4]

map_version = sys.argv[5]

map_version = map_version.replace("\r", "")

print("Params for this job are", source, deaths, write_log, run_id)

if source == "":
    source = "IDN_SIRS"


if __name__ == "__main__":

    if deaths == "deaths":
        deaths = "FILENAME"
    else:
        deaths = "FILENAME"

    if write_log == "True":
        write_log = True
    else:
        write_log = False


    filepath = "FILEPATH"\
                "FILEPATH".\
        format(r=run_id, d=deaths, s=source)
    df = pd.read_hdf(filepath)

    df = clinical_mapping.map_to_gbd_cause(\
                df=df,
                input_type='cause_code',
                output_type='icg',
                write_unmapped=True,
                truncate_cause_codes=True,
                extract_pri_dx=True,
                prod=True,
                map_version=map_version,
                write_log=write_log,
                groupby_output=True
                )



    outpath = "FILEPATH"\
            "FILEPATH".format(run = run_id, source = source)
    hosp_prep.write_hosp_file(df, outpath, backup=False)

"""
Re making numerators and denominators for the outpatient correction factor.
"""

import pandas as pd
import getpass
import sys

user = getpass.getuser()
repo = r"FILEPATH".format(user)
for module_path in ["FILEPATH", "FILEPATH", "FILEPATH"]:
    sys.path.append(repo + module_path)

import hosp_prep


def main(group, run_id):
    filepath = ("FILEPATH"
                "FILENAME"
                "FILEPATH".format(run_id, group))
    filepath = filepath.replace("\r", "")
    ms = pd.read_hdf(filepath)

    numer = ms[ms.estimate_type == 'otp_any_indv_cases']

    denom = ms[ms.estimate_type == 'otp_any_claims_cases']

    good_facility_ids = [11, 22, 95]
    denom = denom[denom.facility_id.isin(good_facility_ids)]

    denom = denom.groupby(denom.drop(['facility_id', 'val'], axis=1).
                          columns.tolist()).agg({'val': 'sum'}).reset_index()

    numer = numer.groupby(numer.drop(['facility_id', 'val'], axis=1).
                          columns.tolist()).agg({'val': 'sum'}).reset_index()

    result = pd.concat([denom, numer], ignore_index=True)

    result = hosp_prep.age_binning(result, terminal_age_in_data=False,
                                   drop_age=True)

    return result


if __name__ == '__main__':
    run_id = sys.argv[1]
    group = sys.argv[2]
    df = main(group, run_id)
    filepath = ("FILEPATH"
                "FILENAME"
                "FILEPATH".format(run_id, group))
    filepath = filepath.replace("\r", "")
    df.to_csv(filepath, index=False)

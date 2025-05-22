"""
Re making numerators and denominators for the outpatient correction factor.
"""

import pandas as pd
import getpass
import sys

from crosscutting_functions.demographic import age_binning


def main(group, run_id):
    filepath = (FILEPATH.format(run_id, group))
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

    result = age_binning(result, terminal_age_in_data=False,
                        drop_age=True, break_if_not_contig=False,
                        clinical_age_group_set_id=1)

    return result


if __name__ == '__main__':
    run_id = sys.argv[1]
    group = sys.argv[2]
    df = main(group, run_id)
    filepath = (FILEPATH.format(run_id, group))
    filepath = filepath.replace("\r", "")  # being careful
    df.to_csv(filepath, index=False)

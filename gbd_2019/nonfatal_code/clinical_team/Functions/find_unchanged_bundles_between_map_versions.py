import sys
import pandas as pd
from getpass import getuser

prep_path = "FILEPATH".format(getuser())
sys.path.append(prep_path)

import clinical_mapping


def compare_bundle_map(new_vers, old_vers):

    if old_vers == 18:

        old_path = "FILEPATH"
        old_df = pd.read_excel(old_path)
        old_df.rename(columns={'bid_duration': 'bundle_duration'}, inplace=True)
        old_df = old_df[['bundle_id', 'cause_code', 'bid_measure', 'bundle_duration', 'code_system_id', 'map_version']]
    else:
        old_df = get_icd_to_bundle_map(old_vers)

    new_df = get_icd_to_bundle_map(new_vers)


    df_list = []
    bundles = list(set(old_df.bundle_id.unique().tolist() + new_df.bundle_id.unique().tolist()))

    for b in bundles:
        cols = ['bundle_id', 'present_in_mapv{}'.format(old_vers),
                'present_in_mapv{}'.format(new_vers), 'use_in_decomp_2_r2']

        res_tmp = pd.DataFrame(columns=cols)
        old_tmp = old_df.query("bundle_id == @b").copy()
        new_tmp = new_df.query("bundle_id == @b").copy()

        if old_tmp.shape[0] == 0:
            old_val = False
        else:
            old_val = True

        if new_tmp.shape[0] == 0:
            new_val = False
        else:
            new_val = True

        diffs = set(old_tmp['cause_code']).symmetric_difference(set(new_tmp['cause_code']))
        if diffs:
            use_val = False
        else:
            use_val = True

        res_tmp.loc[0, cols] = [b, old_val, new_val, use_val]
        df_list.append(res_tmp)

    res = pd.concat(df_list, sort=True, ignore_index=True)

    return res

def get_icd_to_bundle_map(map_version):
    """
    read in a specific version of the map from our mapping table
    """
    cc = clinical_mapping.get_clinical_process_data("cause_code_icg", map_version=map_version)

    cc = cc[cc.code_system_id < 3].copy()

    dur = clinical_mapping.create_bundle_durations(map_version=map_version)

    bun = clinical_mapping.get_clinical_process_data("icg_bundle", map_version=map_version)

    df = bun.merge(cc, how='left', on=['icg_id', 'icg_name', 'map_version'])
    df = df.merge(dur, how='left', on=['bundle_id', 'map_version'])


    keeps = ['bundle_id', 'cause_code', 'icg_measure', 'bundle_duration', 'code_system_id', 'map_version']
    df = df[keeps].drop_duplicates()
    df['cause_code'] = df['cause_code'].str.replace("\W", "")

    return df

# 2
import pandas as pd
import os

raw_vr_directory = "FILEPATH"

vr = pd.DataFrame()
for vr_file in os.listdir(raw_vr_directory):
    if vr_file == "archive":
        pass
    else:
        print(vr_file)
        vr_file_path = os.path.join(raw_vr_directory, vr_file)
        vr_hold = pd.read_csv(vr_file_path, error_bad_lines=False)
        vr = vr.append(vr_hold)

vr['cause_id'] = vr['cause_id'].replace(725, 724)
vr['cause_id'] = vr['cause_id'].replace(726, 724)
vr['cause_id'] = vr['cause_id'].replace(727, 724)

vr = vr.groupby(['location_id', 'cause_id', 'year_id', 'age_group_id', 'sex_id',
                'nid'], as_index=False)['deaths'].sum()

assert vr[vr.duplicated(subset=['location_id', 'cause_id', 'year_id', 'age_group_id',
                        'sex_id'])].shape[0] == 0

vr.to_csv("FILEPATH",
          index=False)

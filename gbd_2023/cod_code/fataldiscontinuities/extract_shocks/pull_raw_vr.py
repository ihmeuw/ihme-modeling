import getpass
import numpy as np
import sys
from db_queries import get_location_metadata
sys.path.append(FILEPATH)
from cod_prep.claude.claude_io import get_claude_data
from cod_prep.claude.claude_io import get_datasets

def subset_causes(df, cause_ids):
    assert type(cause_ids) is list
    return df.loc[df['cause_id'].isin(cause_ids), :]

if __name__ == "__main__":
    cod_start_year = 1980
    cod_end_year = 2024
    cod_year_bins = np.linspace(cod_start_year, cod_end_year, 8, dtype=int)
    shock_causes = [985, 986, 987, 988, 989, 990, 945, 855,
                    851, 387, 699, 707, 693, 695, 724, 854,
                    842, 303, 317, 335, 341, 345, 725, 726, 727]
    vr_save_path = "FILEPATH"
    for i in range(len(cod_year_bins) - 1):
        start = cod_year_bins[i]
        end = cod_year_bins[i + 1]
        years = list(range(start, end))
        print("pulling {} to {}".format(min(years), max(years)))
        try:
            vr = get_claude_data("disaggregation",
                                 year_id=years,
                                 data_type_id=9,
                                 is_active=True,
                                 )
            vr = vr[vr['cause_id'].isin(shock_causes)]
            vr = vr[['location_id', 'cause_id', 'year_id', 'location_id',
                     'age_group_id', 'sex_id', 'deaths', 'nid']]
            print("Saving {} to {}".format(min(years), max(years)))
            vr.to_csv(vr_save_path.format(min(years), max(years)), index=False)
        except:
            print("No data found in {} to {}".format(min(years), max(years)))
    mort_start_year = 1950
    mort_end_year = 1970
    mort_year_bins = np.linspace(mort_start_year, mort_end_year, 8, dtype=int)

    for i in range(len(mort_year_bins) - 1):
        start = mort_year_bins[i]
        end = mort_year_bins[i + 1]
        years = list(range(start, end))
        print("pulling {} to {}".format(min(years), max(years)))
        try:
            vr = get_claude_data("disaggregation",
                                 year_id=years,
                                 data_type_id=9,
                                 is_mort_active=True 
                                 )
            vr = vr[vr['cause_id'].isin(shock_causes)]

            vr = vr[['location_id', 'cause_id', 'year_id', 'location_id',
                     'age_group_id', 'sex_id', 'deaths', 'nid']]
            print("Saving {} to {}".format(min(years), max(years)))
            vr.to_csv(vr_save_path.format(min(years), max(years)), index=False)
        except:
            print("No data found in {} to {}".format(min(years), max(years)))

    old_ds = get_datasets(data_type_id=[9, 10], source=['ICD8A', 'ICD8_detail', 'ICD7A'], year_id=list(range(1970, 1980)))
    nids = list(old_ds.nid.unique())
    years = list(range(1970,1980))
    vr = get_claude_data("disaggregation",
                         year_id=years,
                         data_type_id=9,
                         is_mort_active=True,
                        )
    vr = vr[vr['cause_id'].isin(shock_causes)]
    vr = vr[['location_id', 'cause_id', 'year_id', 'location_id',
             'age_group_id', 'sex_id', 'deaths', 'nid']]
    vr = vr[~vr['nid'].isin(nids)]
    print("Saving {} to {}".format(min(years), max(years)))
    vr.to_csv(vr_save_path.format(min(years), max(years)), index=False)
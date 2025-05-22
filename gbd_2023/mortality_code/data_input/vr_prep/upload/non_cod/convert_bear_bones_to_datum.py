"""
This script takes data that is in the standard format with GBD ids (e.g.
age_group_id) in the long format and transforms it into a format that is wide
on age (the "DATUM") format and converts id variables to conform with
historical mortality format.
"""


import pandas as pd
from db_queries import get_ids
from db_queries import get_location_metadata
import sys
import re
import getpass
import numpy as np
sys.path.append("FILEPATH")
from data_format_tools import long_to_wide


# Globals / passed in arguments
NEW_RUN_ID = sys.argv[1]
RELEASE_ID = sys.argv[2]
if not isinstance(RELEASE_ID, int):
    RELEASE_ID = int(RELEASE_ID)


def set_datum_dict(ac):
    datum_dict = {"5-14years": "5to14",
                  "15+years": "15plus",
                  "Unknown": "UNK",
                  "<1year": "0to0",
                  "AllAges": "TOT",
                  "EarlyNeonatal": "enntoenn",
                  "LateNeonatal": "lnntolnn",
                  "1-5months": "pnatopna",
                  "6-11months": "pnbtopnb",
                  "12 to 23 months": "1to1",
                  "2to4": "2to4",
                  "Under5": "0to4"}
    for age_group in ac.age_group_name.unique():
        if not age_group in datum_dict.keys():
            datum_dict.update({age_group: age_group})
    return datum_dict


def _test_contiguous_age_group(ac):
    '''
    test whether the age groups in the all cause vr data are continuous
    '''
    aa = {2: [0, 0.01917808],
          3: [0.01917808, 0.07671233],
          388: [0.07671233, 0.50136986],
          389: [0.50136986, 1],
          4: [0.07671233, 1],
          28: [0, 1],
          238: [1, 2],
          34: [2, 5],
          5: [1, 5],
          1: [0,5],
          6: [5, 10],
          7: [10, 15],
          8: [15, 20],
          9: [20, 25],
          10: [25, 30],
          11: [30, 35],
          12: [35, 40],
          13: [40, 45],
          14: [45, 50],
          15: [50, 55],
          16: [55, 60],
          17: [60, 65],
          18: [65, 70],
          19: [70, 75],
          20: [75, 80],
          30: [80, 85],
          31: [85, 90],
          32: [90, 95],
          235: [95, 125]}

    lt = list(aa.keys())[1:]

    ac = ac.loc[~ac.sex_id.isin([3,9])]
    ac = ac.loc[ac.age_group_id.isin(aa.keys())]
    ad = ac.groupby(["nid", "location_id", "year_id", "sex_id"])\
            ["age_group_id"].apply(np.unique).reset_index()
    def is_contiguous(age_group):
        """
        Tests that all age groups are contiguous
        Returns boolean
        """
        age_group = age_group.tolist()
        if (set([2,3,388,389, 28]) < set(age_group) | set([2,3,4, 28]) < set(age_group)):
            # Remove <1 since the enn/lnn/pn groups exist
            age_group.remove(28)
        if (set([388,389, 4]) < set(age_group)):
            # Remove pnn since pna and pnb exist
            age_group.remove(4)
        if set([238, 34, 5]) < set(age_group):
            # Remove 1-4 since 1 and 2-4 exist
            age_group.remove(5)
        if 1 in age_group:
            # 1) ENN, LNN, PNA, PNB - all U1 groups exist, as long as 238/34 or 5 are present, remove 1
            if set([2,3,388,389]) < set(age_group):
                if (set([238, 34]) < set(age_group) | 5 in age_group):
                    age_group.remove(1)
            # 2) ENN, LNN, PNN present
            if set([2,3,4]) < set(age_group):
                if (set([238, 34]) < set(age_group) | 5 in age_group):
                    age_group.remove(1)
            # 3) <1 year and either 238/34 or 5 are present
            if (set([28, 238, 34]) < set(age_group) | set([28, 5]) < set(age_group)):
                age_group.remove(1)
        else:
            return False

        age = age_group[0]
        age_end = aa[age][1]
        for i in range(1, len(age_group)):
            age = age_group[i]
            age_start = aa[age][0]
            if age_start != age_end:
                return False
            age_end = aa[age][1]
        return True

    not_contiguous = pd.DataFrame()
    for i in ad.index.unique():
        subset = ad.loc[ad.index == i].copy()
        contiguous = is_contiguous(subset.age_group_id[i])
        if not contiguous:
            subset["age_group_id"] = str(list(set(lt) - set(subset.age_group_id[i])))
            not_contiguous = not_contiguous.append(subset)
    print("Tests done running.")
    return not_contiguous.reset_index(drop=True)


def format_for_reshape(ac):
    """
    Recodes age and sex
    """
    print("format for reshape")
    age = pd.read_csv("FILEPATH")
    age = age[age.age_group_id!=161]
    pre = ac.shape[0]
    ac.loc[(ac['location_id']==146) & (ac['age_group_id']==28),'age_group_id'] = 4
    ac = ac.merge(age, on="age_group_id", how="left")
    assert ac.shape[0] == pre, "Number of rows changed during merge indicating either a loss of data or a duplication of data, check the age dataframe."
    ac = ac.drop(ac[(ac['nid']==325327) & ac['age_group_id']==28].index)
    ac = ac.drop(ac[(ac['nid']==325029) & ac['age_group_id']==42].index)

    del ac["age_group_id"]
    ac.age_group_name = ac.age_group_name.astype(str)
    ac["age_group_name"] = [x.replace(" ", "") for x in ac["age_group_name"]]

    locs = get_location_metadata(location_set_id=35, release_id=RELEASE_ID)\
        [["location_id", "ihme_loc_id"]]

    ac = ac.merge(locs, on="location_id", how="left")
    ac.rename(columns={"deaths":"DATUM"},
             inplace=True)
    ac.loc[(ac.nid == 469163), 'ihme_loc_id'] = "CHN_44533"
    ac.loc[(ac.nid == 469165), 'ihme_loc_id'] = "CHN_44533"
    ac.loc[(ac.nid == 528380), 'ihme_loc_id'] = "CHN_44533"
    ac.loc[(ac.nid == 528381), 'ihme_loc_id'] = "CHN_44533"

    sex_dict = {3: 0,
                1: 1,
                2: 2,
                9: 9}
    ac["sex_id"] = ac["sex_id"].map(sex_dict)
    # Aggregate <1 year age groups to 1 year
    subsets = ['location_id','year_id','sex_id','nid','source','code_system_id','site','parent_nid','representative_id','extract_type','ihme_loc_id']
    ac = ac.loc[ac.sex_id != 9]
    datum_dict = set_datum_dict(ac)
    ac["age_group_name"]=ac["age_group_name"].map(datum_dict)
    ac = ac[ac.age_group_name!="nan"]
    return ac

def format_vars(ac_datum, input_folder):
    """
    Renames / creates and recodes columns and values to fit into new format
    """
    ac_datum["outlier"] = 0
    ac_datum["SUBDIV"] = "VR"
    ac_datum["FOOTNOTE"] = ""
    ac_datum.rename(columns={"ihme_loc_id":"COUNTRY",
                            "nid":"NID",
                            "parent_nid":"ParentNID",
                            "sex_id":"SEX",
                            "year_id":"YEAR",
                            "source":"VR_SOURCE"},
                   inplace=True)
    ac_datum = ac_datum[["COUNTRY", "NID", "ParentNID", "SEX", "YEAR",
                         "VR_SOURCE", "outlier", "FOOTNOTE", "SUBDIV"] +
                        [x for x in list(ac_datum) if x.startswith("DATUM")]]

    return ac_datum
  

def main(ac, input_folder, output_folder, save=True):
    """
    Run all
    """
    duplicates = ac.duplicated(keep=False)
    assert duplicates.sum() == 0, "There are duplicates: \n\n{}\n".format(ac[duplicates].sort_values(['sex_id', 'age_group_id', 'year_id']).to_string())
    ac = format_for_reshape(ac)

    # There are 2 separate terminal 70 age groups - 70+years and 70plus
    ac.loc[ac.age_group_name=="70+years", 'age_group_name'] = "70plus"

    print("Reshaping...")
    cols = [x for x in list(ac) if not x.startswith("DATUM") and "age" not in x]
    ac_datum = long_to_wide(ac,
                            "DATUM",
                            cols,
                            'age_group_name')
    ac_datum = format_vars(ac_datum, input_folder)

    # Manually force DATUM1 to DATUM12to23months, consistent with GBD nomenclature
    ac_datum = ac_datum.rename(columns={"DATUM1":"DATUM12to23months"})

    if save:
        print("Writing all cause vr files...")
        savepath = "FILEPATH"
        ac_datum.to_stata(savepath, write_index=False)
        print("Done. Saved here: {}".format(savepath))

if __name__ == '__main__':

    input_folder = "FILEPATH"
    output_folder = "FILEPATH"

    print("Loading data from")
    ac = pd.read_csv("FILEPATH")

    irn = pd.read_csv("FILEPATH")

    # China CDC 1979 - 2016
    include_chn_cdc = True
    if include_chn_cdc:
        china_cdc = pd.read_csv("FILEPATH")
        # add columns
        china_cdc = china_cdc.drop(['cause', 'data_type_id'], axis=1)
        china_cdc['source'] = "CHN_DSP"
        china_cdc['extract_type'] = "all cause mortality"
        assert 44533 in china_cdc.location_id.unique()
        china_cdc['parent_nid'] = np.nan

        assert set(china_cdc.columns) == set(ac.columns)
        ac = ac.append(china_cdc)

    # China DSP 2016
    include_chn_dsp = False 
    if include_chn_dsp:
        china_dsp = pd.read_csv("FILEPATH")
        china_dsp = china_dsp.drop(['cause', 'data_type_id'], axis=1)
        china_dsp['source'] = "CHN_DSP"
        china_dsp['extract_type'] = "all cause mortality"
        assert 44533 in china_dsp.location_id.unique()
        china_dsp['parent_nid'] = np.nan

        assert set(china_dsp.columns) == set(ac.columns)
        ac = ac.append(china_dsp)

    ac = ac.append(irn)
    nc = _test_contiguous_age_group(ac)
    nc.to_csv("FILEPATH", index=False)
    main(ac, input_folder, output_folder)

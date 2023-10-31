import platform
import pandas as pd
import numpy as np
import datetime
import re
import sys
import getpass
from db_tools.ezfuncs import query

from clinical_info.Functions.hosp_prep import sanitize_diagnoses

if platform.system() == "Linux":
    root = "FILEPATH"

else:
    root = "FILEPATH"


def dupe_icd_checker(df):
    """
    Make sure that every processed ICD code maps to only 1 baby sequelae

        df: Pandas DataFrame of USER map for a single ICD system
    """
    assert (
        df.code_system_id.unique().size == 1
    ), "This function can only process one ICD system at a time"
    test = df[["icd_code", "nonfatal_cause_name"]].copy()
    test["icd_code"] = test["icd_code"].str.upper()
    test["icd_code"] = test["icd_code"].str.replace("\W", "")
    test = test.drop_duplicates()
    assert (
        test.shape[0] == test.icd_code.unique().size
    ), "There are duplicated icd code/baby sequelae \n {}".format(
        test[test.icd_code.duplicated(keep=False)]
    )
    return


def extract_maps(
    update_ms_durations=False,
    remove_period=True,
    manually_fix_bundles=True,
    update_restrictions=False,
):

    maps_folder = r"FILEPATH"

    # read in mapping data
    map_9 = pd.read_excel(
        "{}2- Master cause list for GBD 2020- Just ICD9-MAR-6-2020-0.xlsx".format(
            maps_folder
        ),
        sheet_name="ICD9-map",
        converters={"icd_code": str},
    )
    if map_9.drop_duplicates().shape[0] < map_9.shape[0]:
        print("There are duplicated rows in this icd9 map")

    map_10_path = "{}1- Master cause list for GBD 2020- Just ICD10-JAN-1-2020-3.xlsx".format(
        maps_folder
    )
    map_10 = pd.read_excel(
        map_10_path, sheet_name="ICD10_map", converters={"icd_code": str}
    )
    if map_10.drop_duplicates().shape[0] < map_10.shape[0]:
        print("There are duplicated rows in this icd10 map")

    # rename
    # in ICD 9
    # me name level 3 has measure
    # there is NO level 3 name
    map_9.rename(
        columns={
            "ME name level 3": "level 3 measure",
            "Baby Sequelae  (nonfatal_cause_name)": "nonfatal_cause_name",
        },
        inplace=True,
    )

    # In ICD 10
    # ME name level 3 has measure
    # this looks like it was fixed
    # map_10.drop('level 3 measure', axis=1, inplace=True)  # this had MEIDSs
    # map_10.rename(columns={'ME name level 3': 'level 3 measure'}, inplace=True)

    # select just the columns we need for mapping. ME ID 2 is here
    # for potential use in future
    map_9 = map_9[
        [
            "icd_code",
            "nonfatal_cause_name",
            "Level1-Bundel ID",
            "level 1 measure",
            "Level2-Bundel ID",
            "level 2 measure",
            "Level3-Bundel ID",
            "level 3 measure",
        ]
    ]

    map_10 = map_10[
        [
            "icd_code",
            "nonfatal_cause_name",
            "Level1-Bundel ID",
            "level 1 measure",
            "Level2-Bundel ID",
            "level 2 measure",
            "Level3-Bundel ID",
            "level 3 measure",
        ]
    ]

    assert (
        set(map_10.columns).symmetric_difference(set(map_9.columns)) == set()
    ), "columns are not matched"

    # add code system id
    map_9["code_system_id"] = 1
    map_10["code_system_id"] = 2

    # verify there aren't duped ICDs going to multiple baby seq
    dupe_icd_checker(map_9)
    dupe_icd_checker(map_10)

    # append and make icd col names match
    maps = map_9.append(map_10)
    maps = maps.rename(columns={"icd_code": "cause_code"})

    # convert baby sequela to lower case to fix some mapping errors
    maps["nonfatal_cause_name"] = maps["nonfatal_cause_name"].str.lower()

    # removing rows with null ICD codes
    null_rows = maps.cause_code.isnull().sum()
    if null_rows > 0:
        maps = maps[maps.cause_code.notnull()]
        print("Dropping {} row(s) because of NA / NAN cause code".format(null_rows))

    # map cause codes without a nfc to the "_none" nfc
    maps.loc[maps.nonfatal_cause_name.isnull(), "nonfatal_cause_name"] = "_none"

    # fix the bad bundle ID from USER map per USER
    maps.loc[
        maps["nonfatal_cause_name"].str.contains("inj_homicide_sexual"),
        "Level1-Bundel ID",
    ] = 765

    # Updated Feb 2018: Seems like there was a typo in the latest map from USER
    maps.loc[maps["Level1-Bundel ID"] == 2872, "Level1-Bundel ID"] = 2972

    def check_nfc_bundle_relat(maps):
        """
        loop over every nfc and bundle level to verify that each nfc has only
        1 unique bundle ID per level. If an nfc has more than 1 bundle for a
        single level then print that nfc name and break on an assert after the
        entire loop has run
        """
        col_names = [
            "cause_code",
            "nonfatal_cause_name",
            "bundle1",
            "measure1",
            "bundle2",
            "measure2",
            "bundle3",
            "measure3",
            "code_system_id",
        ]
        # a small bit of data cleaning
        test = maps.copy()
        test.columns = col_names
        test.nonfatal_cause_name = test.nonfatal_cause_name.astype(str).str.lower()

        bad_nfc = []  # ticker to count the number of bad NFC
        for level in ["1", "2", "3"]:
            for nfc in test.nonfatal_cause_name.unique():
                if (
                    test.loc[test.nonfatal_cause_name == nfc, "bundle" + level]
                    .unique()
                    .size
                    > 1
                ):
                    print("This nfc has multiple level " + level + " bundles ", nfc)
                    bad_nfc.append(nfc)  # add to the list cause there's a bad one
        print("NFC - bundle ID check finished")
        return bad_nfc

    # manually fix the bundle mapping for an error
    # if the mapping has changed for this baby we don't want to hardcode BID
    # 131 onto this ICD code.
    if maps.loc[
        maps.nonfatal_cause_name.str.contains("Liver, chronic, etoh, cirrhosis"),
        "Level1-Bundel ID",
    ].unique().tolist() == ["_none", 131]:
        maps.loc[
            maps.cause_code == "571.2", ["Level1-Bundel ID", "level 1 measure"]
        ] = [131, "prev"]

    # Update March 2018: Hardcoding a NFC since old_nfc mapped to two different Level1-Bundel ID(s)
    old_nfc = "cong, gu, kidney malformation"
    new_nfc = "cong, gu, kidney malformation (polycystic)"
    maps.loc[
        (maps["Level1-Bundel ID"] == 3227) & (maps.nonfatal_cause_name == old_nfc),
        "nonfatal_cause_name",
    ] = new_nfc

    # Updatae March 29 2018: One cause code was not being mapped to the hemo thal bundle
    maps.loc[
        maps.nonfatal_cause_name == "hemog, thal (total)",
        ["Level2-Bundel ID", "level 2 measure"],
    ] = [3008, "prev"]

    # Update March 2020: These ICGs were hitting the 'bad_ones' assert

    # this is too broad but we will fix outside of this script
    maps.loc[
        maps.nonfatal_cause_name == "e-code, (inj_electrocution)", "Level1-Bundel ID"
    ] = 7610

    maps.loc[
        maps.nonfatal_cause_name == "gastrointestinal bleeding", "Level2-Bundel ID"
    ] = 1

    maps.loc[
        maps.nonfatal_cause_name == "_none",
        ["Level1-Bundel ID", "Level2-Bundel ID", "Level3-Bundel ID"],
    ] = np.nan

    assert (
        3227
        not in maps.loc[(maps.nonfatal_cause_name == old_nfc), "Level1-Bundel ID"]
        .unique()
        .tolist()
    ), "There is a mismatch of bundle ids for nfc Cong, gu, kidney malformation"

    bad_ones = check_nfc_bundle_relat(maps)

    assert len(bad_ones) == 0, (
        "There are baby seqeulae with more than 1 "
        r"bundle id. This will lead to mapping differences. Review the var "
        r"'bad_ones' for these baby sequelae"
    )

    if update_restrictions:
        update_restrictions_file()

    # select one level at a time
    map_level_1 = maps.drop(
        ["level 2 measure", "Level2-Bundel ID", "level 3 measure", "Level3-Bundel ID"],
        axis=1,
    )
    map_level_2 = maps.drop(
        ["level 1 measure", "Level1-Bundel ID", "level 3 measure", "Level3-Bundel ID"],
        axis=1,
    )
    map_level_3 = maps.drop(
        ["level 1 measure", "Level1-Bundel ID", "level 2 measure", "Level2-Bundel ID"],
        axis=1,
    )
    # add an indicator var for the level of ME
    map_level_1["level"] = 1
    map_level_2["level"] = 2
    map_level_3["level"] = 3

    # rename to match
    # rename level 1
    map_level_1.rename(
        columns={"level 1 measure": "bid_measure", "Level1-Bundel ID": "bundle_id"},
        inplace=True,
    )

    # rename level 2
    map_level_2.rename(
        columns={"level 2 measure": "bid_measure", "Level2-Bundel ID": "bundle_id"},
        inplace=True,
    )

    # rename level 3
    map_level_3.rename(
        columns={"level 3 measure": "bid_measure", "Level3-Bundel ID": "bundle_id"},
        inplace=True,
    )

    # making a forth level for the maternal team
    map_level_4 = map_level_3.copy()

    cols = ["bundle_id", "bid_measure"]
    map_level_4["bundle_id"] = np.nan
    map_level_4["bid_measure"] = "_none"
    map_level_4["level"] = 4

    # fix measure for bundle_id 1010
    map_level_3.loc[map_level_3.bundle_id == 1010, "bid_measure"] = "prev"

    maps = pd.concat([map_level_1, map_level_2, map_level_3, map_level_4])

    osteo_arth_icd = (
        maps.loc[maps.cause_code.str.contains("^715.\d"), "cause_code"]
        .unique()
        .tolist()
    )

    assert (
        pd.Series(osteo_arth_icd).apply(len).min() > 3
    ), "There seems to be a 3 digit icd code present in the data to map to all OA"

    # it looks like .4, .5 .6 and .7 don't exist as icd 9 codes
    to_overwrite = maps.loc[
        (maps.cause_code.isin(osteo_arth_icd))
        & (maps.level == 2)
        & (maps.code_system_id == 1),
        "bundle_id",
    ]
    # zeros will be made into null bundle IDs
    to_overwrite.loc[to_overwrite == 0] = np.nan
    assert (
        to_overwrite.notnull().sum() == 0
    ), "There is something that will be overwritten and it's this {}".format(
        to_overwrite.unique()
    )

    # update the map to include dummy bundle 215 for ALL OA
    maps.loc[
        (maps.cause_code.isin(osteo_arth_icd))
        & (maps.level == 2)
        & (maps.code_system_id == 1),
        ["bundle_id", "bid_measure"],
    ] = [215, "prev"]

    # for every column, force the datatypes of the values inside to be what they
    # are expected to be

    # make icd codes strings
    maps["cause_code"] = maps["cause_code"].astype(str)

    # make nonfatal_cause_name strings
    maps["nonfatal_cause_name"] = maps["nonfatal_cause_name"].astype(str)

    # convert baby sequela to lower case to fix some mapping errors
    maps["nonfatal_cause_name"] = maps["nonfatal_cause_name"].str.lower()

    # convert TBD bundle_ids (or anything else that isn't a number) & 0 to np.nan
    maps["bundle_id"] = pd.to_numeric(
        maps["bundle_id"], errors="coerce", downcast="integer"
    )
    maps.loc[maps["bundle_id"] == 0, "bundle_id"] = np.nan

    # assert we aren't mixing me_ids and bundle ids
    if not (maps["bundle_id"] > 1010).sum() == 0:  # bundle ids so far tend to be
        # less than 1000 in magnitude
        print("You're mixing bundle IDs and ME IDs")

    # make bid_measure strings
    maps["bid_measure"] = maps["bid_measure"].astype(str)
    # make lower case
    maps["bid_measure"] = maps["bid_measure"].str.lower()

    # make ints smaller
    maps["code_system_id"] = pd.to_numeric(
        maps["code_system_id"], errors="raise", downcast="integer"
    )
    maps["level"] = pd.to_numeric(maps["level"], errors="raise", downcast="integer")

    # clean ICD codes
    # remove non-alphanumeric characters
    if remove_period:
        maps["cause_code"] = maps["cause_code"].str.replace("\W", "")
    # make sure all letters are capitalized
    maps["cause_code"] = maps["cause_code"].str.upper()

    # Adding the duration col for above reasons
    maps["bid_duration"] = np.nan

    # reorder columns
    cols_before = maps.columns
    maps = maps[
        [
            "cause_code",
            "code_system_id",
            "level",
            "nonfatal_cause_name",
            "bundle_id",
            "bid_measure",
            "bid_duration",
        ]
    ]
    cols_after = maps.columns
    assert set(cols_before) == set(
        cols_after
    ), "you dropped columns while changing the column order"

    # CHECK MEASURE IN BIDS
    if (
        maps.loc[
            (maps.bundle_id.notnull()) & (maps.cause_code == "Q91") & (maps.level == 1),
            "bundle_id",
        ]
        == 646
    ).all():
        print("Yup, USER still has Q91 going to 646. it should be 638. fixing...")
        maps.loc[
            (maps.cause_code == "Q91") & (maps.level == 1), ["bundle_id", "bid_measure"]
        ] = [638, "prev"]
    # bundle 502 has a numeric measure, but it should be prev
    if "500" in maps.loc[maps.bundle_id == 502, "bid_measure"].unique():
        maps.loc[(maps.bundle_id == 502) & (maps.level == 3), "bid_measure"] = "prev"
        assert maps.loc[maps.bundle_id == 502, "bid_measure"].unique().size == 1

    # add an assert to break script if a bundle has more
    # than one measure
    mapG = maps.groupby("bundle_id")

    # Update March 03: bundle_id 3260 and 3263 have bad bid_measures. These measures are present in the map
    gi_prev = ["upper gi chronic", "upper gi other"]
    maps.loc[
        (maps.bundle_id == 3260) & (maps.bid_measure.isin(gi_prev)), "bid_measure"
    ] = "prev"
    maps.loc[
        (maps.bundle_id == 3263) & (maps.bid_measure == "upper gi acute"), "bid_measure"
    ] = "inc"

    # Update March 03: bundle_id 502 has two bid_measures. Measure is from previous map
    maps.loc[maps.bundle_id == 502, "bid_measure"] = "prev"

    # Updated Jan 2018: ensure measures for bundle 3039 is 'prev'.
    measures = set(maps[maps.bundle_id == 3039].bid_measure)
    if "prev" in measures and "inc" not in measures:
        maps.loc[maps.bundle_id == 3039, "bid_measure"] = "prev"

    # Updated March 2018: Remapping parent inj_poisoning to child inj_poisoning
    maps.loc[maps.bundle_id == 269, "bundle_id"] = 3047

    # Update Feb 2019: Multiple measure bundle. Should only be prev
    maps.loc[maps.bundle_id == 1, "bid_measure"] = "prev"

    for measure in mapG.bid_measure.unique():
        assert len(measure) == 1, "A bundle ID has multiple measures"

    assert (
        maps[
            (maps.bid_measure != "prev")
            & (maps.bid_measure != "inc")
            & (maps.bundle_id.notnull())
        ].shape[0]
        == 0
    ), (
        r"There is a bundle ID with an incorrect measure. Run the code above "
        "without .shape to find it."
    )

    bid_durations = pd.read_excel(root + r"FILEPATH")

    # drop measure
    bid_durations.drop("measure", axis=1, inplace=True)

    # no need to drop duplicates

    # merge
    maps = maps.merge(bid_durations, how="left", on="bundle_id")

    # compare durations
    before = maps[["bid_measure", "bid_duration", "duration"]].drop_duplicates()

    maps.loc[maps.bid_duration.isnull(), "bid_duration"] = maps.loc[
        maps.duration.notnull(), "duration"
    ]

    # make any prev measures have null duration
    maps.loc[maps.bid_measure == "prev", "bid_duration"] = np.nan

    after = maps[["bid_measure", "bid_duration", "duration"]].drop_duplicates()

    maps.drop("duration", axis=1, inplace=True)

    # check durations
    print("checking if all inc BIDs have at least one non null duration")

    maps.loc[maps.bundle_id == 2996, "bid_duration"] = 28

    # Update March 03: Map is missing duration for bundle 3263. USER confirmed duration
    maps.loc[maps.bundle_id == 3263, "bid_duration"] = 60

    # Update Feb 07 19: Missing durations. The durations were included in v20 as such we are
    # using that information.
    temp = list(zip([3074, 3020, 4196], [90, 365, 27]))
    for e in temp:
        maps.loc[maps.bundle_id == e[0], "bid_duration"] = e[1]

    # Update electroction March 2020:
    maps.loc[
        maps.nonfatal_cause_name == "e-code, (inj_electrocution)", "bid_duration"
    ] = 365

    cond = "(maps.bid_measure=='inc') & (maps.bundle_id.isnull()) & (maps.bid_duration.isnull())"
    maps.loc[eval(cond), "bid_duration"] = 365

    assert (
        maps.loc[
            (maps.bid_measure == "inc") & (maps.bundle_id.notnull()), "bid_duration"
        ]
        .notnull()
        .all()
    ), "We expect that 'inc' BIDs only have Non-Null durations"

    assert len(
        maps.loc[
            (maps.bid_measure == "inc") & (maps.bundle_id.notnull()), "bid_duration"
        ].unique()
    ), "We expect that 'inc' BIDs only have Non-Null durations"

    print("checking if all prev BIDs only have null durations")
    assert (
        maps.loc[maps.bid_measure == "prev", "bid_duration"].isnull().all()
    ), "We expect that 'prev' BIDs only have null durations"

    #####################################
    # check measure
    #####################################
    # BIDs should only have 2 unique measures
    assert len(maps[maps["bundle_id"].notnull()].bid_measure.unique()) == 2

    # BIDs should not have more than 1 measure
    for bid in maps["bundle_id"].unique():
        assert len(maps[maps["bundle_id"] == bid].bid_measure.unique()) <= 1

    if manually_fix_bundles:
        maps.loc[
            maps.nonfatal_cause_name == "sti, syphilis, tertiary (endocarditis)",
            "bundle_id",
        ] = np.nan
        # drop 2 baby sequela from bundle 618
        maps.loc[
            maps.nonfatal_cause_name == "cong, gu, hypospadias", "bundle_id"
        ] = np.nan
        maps.loc[
            maps.nonfatal_cause_name == "cong, gu, male genitalia", "bundle_id"
        ] = np.nan
        # drop a baby sequela from 131 Cirrhosis
        maps.loc[
            maps.nonfatal_cause_name == "neonatal, digest, other (cirrhosis)",
            "bundle_id",
        ] = np.nan

    # get cause_id so we can apply cause restrictions
    cause_id_query = "SELECT bundle_id, cause_id FROM bundle.bundle"
    cause_id_info = query(cause_id_query, conn_def="epi")

    pre = maps.shape[0]
    maps = maps.merge(cause_id_info, how="left", on="bundle_id")
    assert maps.shape[0] == pre

    maps = fix_deprecated_bundles(maps)

    # next version will be '27'
    map_vers = 26
    maps["map_version"] = map_vers
    gastritis_10 = [
        "K29",
        "K290",
        "K2900",
        "K291",
        "K292",
        "K2920",
        "K293",
        "K2930",
        "K294",
        "K2940",
        "K295",
        "K2950",
        "K296",
        "K2960",
        "K297",
        "K2970",
        "K2971",
        "K298",
        "K2980",
        "K299",
        "K2990",
        "K2991",
        "K2901",
        "K2921",
        "K2931",
        "K2941",
        "K2951",
        "K2961",
        "K2981",
    ]

    gastritis_09 = [
        "535",
        "5350",
        "53500",
        "5351",
        "53510",
        "5352",
        "53520",
        "5353",
        "53530",
        "5354",
        "53540",
        "5355",
        "53550",
        "5356",
        "53560",
        "5357",
        "53570",
        "5359",
        "53501",
        "53511",
        "53521",
        "53531",
        "53541",
        "53551",
        "53561",
        "53571 ",
    ]

    maps.loc[
        (maps.cause_code.isin(gastritis_09))
        & (maps["level"] == 2)
        & (maps["code_system_id"] == 1),
        "bundle_id",
    ] = 545
    maps.loc[
        maps.cause_code.isin(gastritis_10)
        & (maps["level"] == 2)
        & (maps["code_system_id"] == 2),
        "bundle_id",
    ] = 545

    beta_thal = ["28244"]
    maps.loc[
        (maps.cause_code.isin(beta_thal)) & (maps["level"] == 1), "bundle_id"
    ] = 206

    maps.loc[maps.bundle_id.isin(["79"]), "bundle_id"] = 3419

    keep = ["4329", "I629"]

    # fix the baby sequelae and mapping for bundle 116
    # first fix the mapping
    maps.loc[maps.bundle_id == 116, "bundle_id"] = np.nan
    maps.loc[(maps.cause_code.isin(keep)) & (maps.level == 2), "bundle_id"] = 116

    # now create a new baby sequelae for these 2 icd codes
    maps.loc[maps.cause_code.isin(keep), "nonfatal_cause_name"] = (
        maps.loc[maps.cause_code.isin(keep), "nonfatal_cause_name"]
        + "_special_to_split"
    )

    # fix the baby sequelae for bundle 206
    # this code is beta-thal, not s-thal
    maps.loc[
        maps.cause_code == "28244", "nonfatal_cause_name"
    ] = "hemog, beta-thal major_icd9"

    newenceph = pd.read_excel(root + r"FILEPATH")
    new_codes = (
        newenceph.loc[newenceph.new_mapping == 338, "icd_code"]
        .str.replace("\W", "")
        .tolist()
    )

    icgs = (
        maps.loc[maps.cause_code.isin(new_codes), "nonfatal_cause_name"]
        .unique()
        .tolist()
    )

    # remove the existing bundle_id mapping
    maps.loc[maps.bundle_id == 338, "bundle_id"] = np.nan

    # create the bundle ID mapping just where we want it
    maps.loc[
        (maps.nonfatal_cause_name.isin(icgs))
        & (maps.code_system_id == 2)
        & (maps.level == 1),
        "bundle_id",
    ] = 338

    assert not set(new_codes).symmetric_difference(
        set(maps.loc[maps.bundle_id == 338, "cause_code"])
    )

    # Correct error of cause code going to TB
    nfc = "gastrointestinal bleeding"
    cond = "(maps.nonfatal_cause_name == '{}') & (maps.level == 2)".format(nfc)
    maps.loc[eval(cond), ["bundle_id", "bid_duration"]] = None, None

    # drop duplicates across ALL columns right before saving
    maps = maps.drop_duplicates().reset_index(drop=True)
    maps.to_csv(
        f"FILEPATH", index=False,
    )

    # bs_key['bs_id_version'] = map_vers


def update_restrictions_file(map_10_path, maps):
    """
    Update the age and sex restrictions for baby sequelae. This writes over the
    'all_restrictions.csv' file and the 'bundle_restrictions.csv' file
    
    Parameters:
        map_10_path(str): this contains the filepath to the icd 10 map which
        contains the restrictions on one of its sheets
        maps(Pandas DataFrame): Our finalized clean_maps, used to get map
        version and to merge restrictions onto bundle IDs
    """
    # get the map version
    map_vers = int(maps.map_version.unique())

    restrict = pd.read_excel(
        map_10_path,
        skiprows=[0],
        sheet_name="Lookup20",
        converters={"icd_code": str},
        usecols="BE, BW, BX, BY, BZ",
    )

    restrict.rename(
        columns={
            "Baby Sequelae  (nonfatal_cause_name)": "nonfatal_cause_name",
            "male.1": "male",
            "female.1": "female",
        },
        inplace=True,
    )

    print(restrict.columns)
    restrict.nonfatal_cause_name = restrict.nonfatal_cause_name.str.lower()
    restrict.sample(5)
    # drop all the duplicate null values
    restrict = restrict[restrict.nonfatal_cause_name.notnull()]
    restrict.drop_duplicates(inplace=True)

    restrict = restrict[
        (restrict.nonfatal_cause_name != "cancer, liver")
        | (restrict.yld_age_start < 15)
    ]
    restrict = restrict[
        (restrict.nonfatal_cause_name != "other benign and in situ neoplasms")
        | (restrict.yld_age_start < 15)
    ]

    # the ICD codes that go into this baby are clearly for females
    restrict.loc[restrict.nonfatal_cause_name == "sti, pid, total", "male"] = 0

    # looks like these icgs are being mapped to two bundle ids 0 and _none. Setting it to none
    restrict.loc[restrict.nonfatal_cause_name == "hiv", "Level1-Bundel ID"] = "_none"
    restrict.loc[
        restrict.nonfatal_cause_name == "sti, other", "Level1-Bundel ID"
    ] = "_none"

    # missing restrictions. setting them to default. the digest icgs were included
    # in the 2018_01_10 version
    icgs = [
        "rubella ",
        "exposure, pediculos",
        "digest, gastritis and duodenitis acute without complication",
        "digest, gastritis and duodenitis chronic without complication",
        "digest, gastritis and duodenitis unspecified without complication",
    ]

    cols = ["male", "female", "yld_age_start", "yld_age_end"]
    restrict.loc[restrict.nonfatal_cause_name.isin(icgs), cols] = [1, 1, 0, 95]

    restrict.drop_duplicates(keep="first", inplace=True)
    # check for duplicated babies
    if restrict.nonfatal_cause_name.unique().size != restrict.shape[0]:
        print(
            "There are more rows than unique baby sequelae. "
            r"Look at this dataframe!{}{}".format(
                "\n\n\n", restrict[restrict.nonfatal_cause_name.duplicated(keep=False)]
            )
        )
        assert False, "There are duplicated baby sequelae"
    # compare current restrictions to last version
    prior_restrict = pd.read_csv(root + r"FILEPATH")
    prior_restrict = prior_restrict[
        [
            "Baby Sequelae  (nonfatal_cause_name)",
            "male",
            "female",
            "yld_age_start",
            "yld_age_end",
        ]
    ].copy()
    prior_restrict = prior_restrict.reset_index(drop=True)
    prior_restrict.rename(
        columns={"Baby Sequelae  (nonfatal_cause_name)": "nonfatal_cause_name"},
        inplace=True,
    )
    prior_restrict.nonfatal_cause_name = prior_restrict.nonfatal_cause_name.str.lower()
    # compare current restricts to map
    print(
        "These baby sequelae were removed {}".format(
            set(prior_restrict.nonfatal_cause_name.unique())
            - set(restrict.nonfatal_cause_name.unique())
        )
    )
    print(
        "These baby sequelae were added {}".format(
            set(restrict.nonfatal_cause_name.unique())
            - set(prior_restrict.nonfatal_cause_name.unique())
        )
    )
    # not sure what to do with this programatically but let's print the
    # inner diff for now
    merged = restrict.merge(prior_restrict, how="inner", on="nonfatal_cause_name")
    for col in ["male_", "female_", "yld_age_start_", "yld_age_end_"]:
        q = "{} != {}".format(col + "x", col + "y")
        print("The diff for column {} is \n {}".format(col, merged.query(q)))

    assert abs(restrict.shape[0] - prior_restrict.shape[0]) < 25, (
        "25 or more baby sequelae have different restrictions. "
        "Confirm this is correct before updating"
    )

    # run a few more tests
    assert set(restrict.male.unique()).symmetric_difference(set([0, 1])) == set()
    assert set(restrict.female.unique()).symmetric_difference(set([0, 1])) == set()

    for age in restrict.yld_age_start:
        assert age == float(age), "An age start is not type int"
    for age in restrict.yld_age_end:
        assert age == float(age), "An age end is not type int"

    # write the new restrictions following the original data structure

    restrict.to_csv(
        f"FILEPATH", index=False,
    )


def fix_deprecated_bundles(df):
    """
    hardcode remove deprecated bundles that find their way back into the map.

    Updated Jan 2018

    """

    updated_bundles = {808: 2978, 799: 2972, 803: 2975}
    for bundle in list(updated_bundles.keys()):
        if bundle in df.bundle_id.unique():
            df.loc[df.bundle_id == bundle, "bundle_id"] = updated_bundles[bundle]
    return df


def get_acause_bundle_map():
    """
    marketscan age/sex restrictions use acause rather than bundle id
    so create a map to get from acause/rei to bundle id
    """

    bundle_acause_query = "QUERY"

    df = query(bundle_acause_query, conn_def="CONN")

    # replace null acauses with REIs, sort of misleading but ce la vie
    df.loc[df.acause.isnull(), "acause"] = df.loc[df.acause.isnull(), "rei"]
    df.drop("rei", axis=1, inplace=True)
    return df

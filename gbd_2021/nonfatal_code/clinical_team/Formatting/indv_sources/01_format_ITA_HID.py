import pandas as pd
import platform
import numpy as np
import warnings
import time
import glob
from getpass import getuser
from pathlib import Path

from clinical_info.Functions import hosp_prep, stage_hosp_prep
from clinical_info.Functions.live_births import live_births

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

#####################################################
# Create 2 global variables for processing
#####################################################

process_dict = {
    "year_start": 2005,
    "year_end": 2019,
    # years contain admission and discharge dates
    "admit_date": list(range(2005, 2017)),
    # years which do not contain admit and discharge dates
    "no_admit_date": list(range(2017, 2019)),
}

read_from_disk = False

#####################################################
# Define functions to remove day cases
#####################################################


def prep_overnight_weights(years="all"):
    """Create age, sex, cause code weights for Italy non-day cases so we
    can remove the day cases. Currently 2005-2016 have admit/discharge info
    so those are the weight inputs. There was a clear declining trend in
    admissions year over year so I thought it might be preferable to select
    just the 2 or 3 most recent years near the new data to perhaps get a clearer
    signal. Might loop back to this in the future. For now we're using all years

    Params:
        years (str or list):
            Defines the years to use when creating weights
    Returns:
        df2 (pd.DataFrame):
            dataframe of age/sex/icd weights to reduce total admissions
    """

    files = glob.glob("FILEPATH")

    df_list = []
    for f in files:
        df = pd.read_csv(f)
        df["year_start"] = int(Path(f).stem)
        df_list.append(df)
    df = pd.concat(df_list, sort=False, ignore_index=False)

    if years == "all":
        pass
    else:
        assert isinstance(years, list), "years arg isn't a list"
        df = df[df["year_start"].isin(years)]
        print(
            (f"dropping all years except " f"{df['year_start'].sort_values().unique()}")
        )

    df2 = (
        df.groupby(["age_start", "age_end", "sex_id", "dx_1"])
        .agg({"val_day": "sum", "val_total": "sum", "val_overnight": "sum"})
        .reset_index()
    )

    df2["overnight_weight"] = (df2["val_total"] - df2["val_day"]) / df2["val_total"]
    # adjust the overnight weight when total == overnight stays
    df2.loc[
        df2.val_total.round(2) == df2.val_overnight.round(2), "overnight_weight"
    ] = 1
    assert df2.overnight_weight.notnull().any(), "No null weights allowed"
    df2.rename(columns={"dx_1": "cause_code"}, inplace=True)
    return df2


def apply_overnight_weights(df, weight_years):
    """Reduce the primary dx admission counts to remove day cases"""

    second = df[df["diagnosis_id"] == 2].copy()
    df = df[df["diagnosis_id"] == 1].copy()

    weight = prep_overnight_weights(years=weight_years)
    pre = len(df)
    df = df.merge(
        weight,
        how="left",
        on=["age_start", "age_end", "sex_id", "cause_code"],
        validate="m:1",
    )
    assert pre == len(df), "rows changed"

    df["adj_val"] = df["val"]
    init_cases = df["adj_val"].sum()
    mask = "df['overnight_weight'].notnull()"
    df.loc[eval(mask), "adj_val"] = (
        df.loc[eval(mask), "adj_val"] * df.loc[eval(mask), "overnight_weight"]
    )
    loss = init_cases - df["adj_val"].sum()
    print(f"The adjustment has dropped {loss} day case admissions")

    df.loc[(df["adj_val"].notnull()), "val"] = df.loc[
        (df["adj_val"].notnull()), "adj_val"
    ]
    drops = ["val_day", "val_total", "val_overnight", "overnight_weight", "adj_val"]
    df.drop(drops, axis=1, inplace=True)

    df = pd.concat([df, second], sort=False, ignore_index=True)

    return df


#####################################################
# Process all years of Italy or read from disk
#####################################################

if read_from_disk:
    print(
        (
            "Read from disk is true, so we'll read in the existing year files. "
            "Please ensure they're up to date!"
        )
    )
    files = sorted(glob.glob(("FILEPATH")))
    df_agg = pd.concat([pd.read_hdf(f) for f in files], ignore_index=True)
else:
    # too large to process together, loop over each year
    # even reading in the file in /raw was using 120+ gigs, totally absurd
    # format data by individual year
    final_list = []

    years = np.arange(process_dict["year_start"], process_dict["year_end"], 1)
    for year in years:
        print("Starting on year {}".format(year))
        start = time.time()
        # start = time.time()
        if year == 2007:
            print("2007 Stata is corrupted as of 2018-01-12")
            fpath = "FILEPATH"
            df = pd.read_sas(fpath)
        else:
            # read from stata
            fpath = r"FILEPATH"
            df = pd.read_stata(fpath)
            # read in header
            # df = hosp_prep.read_stata_chunks(fpath, chunksize=50000,
            #                            chunks=10)
            read_time = (time.time() - start) / 60
            print("It took {} min to read in year {}".format(read_time, year))
            # df = pd.read_sas(filepath)
            # sdict[year] = df.columns.tolist()

        try:
            # give keeping these cols a shot
            ideal_keep = [
                "sesso",
                "eta",
                "eta_gg",
                "reg_ric",
                "regric",
                "gg_deg",
                "mod_dim",
                "mot_dh",
                "tiporic",
                "dpr",
                "dsec1",
                "dsec2",
                "dsec3",
                "dsec4",
                "dsec5",
                "causa_ext",
                "tip_ist2",
                "data_ric",
                "data_dim",
                "data_ricA",
                "data_dimA",
                "cod_reg",
            ]
            to_keep = [n for n in ideal_keep if n in df.columns]
            print(
                (
                    "Missing {} for this year".format(
                        [n for n in ideal_keep if n not in df.columns]
                    )
                )
            )
            df = df[to_keep]
            df["year_start"] = year

        except:
            print("well that didn't work for {}".format(year))

        yr_start = time.time()
        print("Starting on year {}".format(year))

        if "data_ricA" in df.columns:
            df.rename(columns={"data_ricA": "data_ric"}, inplace=True)
        if "data_dimA" in df.columns:
            df.rename(columns={"data_dimA": "data_dim"}, inplace=True)
        # df.drop(['data_ricA', 'data_dimA'], axis=1, inplace=True)

        df["gg_deg"] = pd.to_numeric(df.gg_deg, errors="raise")

        # check that dates are always present
        if year not in process_dict["no_admit_date"]:
            assert not df.data_dim.isnull().sum()
            assert not df.data_ric.isnull().sum()

        # df = back[back.year_start == year].copy()
        start_cases = df.shape[0]

        # Everything in the for loop below is basically our standard formatting
        # steps

        # If this assert fails uncomment this line:
        # df = df.reset_index(drop=True)
        assert_msg = f"""index is not unique, the index has a length of
        {str(len(df.index.unique()))} while the DataFrame has
        {str(df.shape[0])} rows. Try this: df = df.reset_index(drop=True)"""
        assert_msg = " ".join(assert_msg.split())
        assert df.shape[0] == len(df.index.unique()), assert_msg

        # Replace feature names on the left with those found in data where
        # appropriate
        # ALL OF THESE COLUMNS WILL BE MADE unless you comment out the
        # ones you don't want
        hosp_wide_feat = {
            "nid": "nid",
            # 'cod_reg': 'location_id',
            "representative_id": "representative_id",
            "year_start": "year_start",
            "year_end": "year_end",
            "sesso": "sex_id",
            "eta": "age",
            "eta_gg": "age_days",
            "age_group_unit": "age_group_unit",
            "code_system_id": "code_system_id",
            "data_dim": "dis_date",
            "data_ric": "adm_date",
            "gg_deg": "los",
            # measure_id variables
            "mod_dim": "outcome_id",
            "tip_ist2": "facility_id",
            # diagnosis varibles
            "dpr": "dx_1",
            "dsec1": "dx_2",
            "dsec2": "dx_3",
            "dsec3": "dx_4",
            "dsec4": "dx_5",
            "dsec5": "dx_6",
            "causa_ext": "ecode_1",
            # look into these later
            "tiporic": "hosp_type",
            "reg_ric": "hosp_scheme",
            "mot_dh": "hosp_reason",
        }

        # Rename features using dictionary created above
        df.rename(columns=hosp_wide_feat, inplace=True)

        # set difference of the columns you have and the columns you want,
        # yielding the columns you don't have yet
        new_col_df = pd.DataFrame(
            columns=list(set(hosp_wide_feat.values()) - set(df.columns))
        )
        df = df.join(new_col_df)

        # If the data is 2007, many of the columns are bytes and not strings.
        cols = [
            "sex_id",
            "dx_1",
            "dx_2",
            "dx_3",
            "dx_4",
            "dx_5",
            "dx_6",
            "adm_date",
            "dis_date",
        ]
        if year == 2007:
            for col in cols:
                try:
                    df[col] = df[col].str.decode("utf-8")
                # Catch error when column not found
                # not all years have all columns
                except KeyError:
                    print(f"Year 2007 didn't have column {col}")
                    pass

        # drop them for now
        extra_cols = ["hosp_type", "hosp_scheme", "hosp_reason"]
        df.drop(extra_cols, axis=1, inplace=True)

        assert start_cases == df.shape[0], "Some rows were lost or added"

        # drop missing primary dx
        null_count = df.dx_1.isnull().sum()
        # print("null dx 1 is {}".format(null_count))
        warnings.warn(
            (
                "\n\nDropping rows with missing primary dx. {} "
                "rows will be dropped".format(null_count)
            )
        )

        df = df[df.dx_1.notnull()]
        print("next up is empty string dx")

        blank_count = (df.dx_1 == "").sum()
        warnings.warn(
            (
                "\n\nDropping rows with blank primary dx. {} "
                "rows will be dropped".format(blank_count)
            )
        )
        df = df[df.dx_1 != ""]
        start_cases = df.shape[0]

        # Make sure null vals are typed as np.nan. Thanks STATA
        df.loc[df.age_days.isnull(), "age_days"] = np.nan

        # swap e and n codes
        # fill missing ecodes with null
        df.loc[df["ecode_1"] == "", "ecode_1"] = np.nan

        if not df[df.year_start == year].ecode_1.isnull().all():
            df["dx_7"] = np.nan
            # put n codes into dx_7 col
            df.loc[df["ecode_1"].notnull(), "dx_7"] = df.loc[
                df["ecode_1"].notnull(), "dx_1"
            ]
            # overwrite n codes in dx_1
            df.loc[df["ecode_1"].notnull(), "dx_1"] = df.loc[
                df["ecode_1"].notnull(), "ecode_1"
            ]
        # drop the ecode col
        df.drop("ecode_1", axis=1, inplace=True)

        #####################################################
        # FILL COLUMNS THAT SHOULD BE HARD CODED
        # this is where you fill in the blanks with the easy
        # stuff, like what version of ICD is in the data.
        #####################################################

        # These are completely dependent on data source

        # -1: "Not Set",
        # 0: "Unknown",
        # 1: "Nationally representative only",
        # 2: "Representative for subnational location only",
        # 3: "Not representative",
        # 4: "Nationally and subnationally representative",
        # 5: "Nationally and urban/rural representative",
        # 6: "Nationally, subnationally and urban/rural representative",
        # 7: "Representative for subnational location and below",
        # 8: "Representative for subnational location and urban/rural",
        # 9: "Representative for subnational location, urban/rural and below",
        # 10: "Representative of urban areas only",
        # 11: "Representative of rural areas only"

        df["representative_id"] = 1  # Do not take this as gospel, it's guesswork

        # group_unit 1 signifies age data is in years, 2 is days. Prepping for
        # neonatal age groups
        df["age_group_unit"] = df.apply(
            lambda x: 1 if pd.isnull(x["age_days"]) else 2, axis=1
        )
        df["age"] = df.apply(
            lambda x: x["age"] if x["age"] != 0.0 else x["age_days"], axis=1
        )
        # df.drop(columns=['age_days'], inplace=True)
        assert df.loc[df["age"] == 0, "age_days"].isnull().sum() == 0

        df["source"] = "ITA_HID"

        cod_reg_dict = {
            "010": "Piemonte",
            "020": "Valle d'Aosta",
            "030": "Lombardia",
            "041": "Provincia autonoma di Bolzano",
            "042": "Provincia autonoma di Trento",
            "050": "Veneto",
            "060": "Friuli-Venezia Giulia",
            "070": "Liguria",
            "080": "Emilia-Romagna",
            "090": "Toscana",
            "100": "Umbria",
            "110": "Marche",
            "120": "Lazio",
            "130": "Abruzzo",
            "140": "Molise",
            "150": "Campania",
            "160": "Puglia",
            "170": "Basilicata",
            "180": "Calabria",
            "190": "Sicilia",
            "200": "Sardegna",
        }

        loc_dict = {
            "Piemonte": 35494,
            "Valle d'Aosta": 35495,
            "Liguria": 35496,
            "Lombardia": 35497,
            "Provincia autonoma di Bolzano": 35498,
            "Provincia autonoma di Trento": 35499,
            "Veneto": 35500,
            "Friuli-Venezia Giulia": 35501,
            "Emilia-Romagna": 35502,
            "Toscana": 35503,
            "Umbria": 35504,
            "Marche": 35505,
            "Lazio": 35506,
            "Abruzzo": 35507,
            "Molise": 35508,
            "Campania": 35509,
            "Puglia": 35510,
            "Basilicata": 35511,
            "Calabria": 35512,
            "Sicilia": 35513,
            "Sardegna": 35514,
        }

        loc_df = pd.DataFrame(
            {
                "cod_reg": list(cod_reg_dict.keys()),
                "location_name": list(cod_reg_dict.values()),
            }
        ).merge(
            pd.DataFrame(
                {
                    "location_name": list(loc_dict.keys()),
                    "location_id": list(loc_dict.values()),
                }
            ),
            how="outer",
            on="location_name",
        )
        assert loc_df.shape[0] == 21, "Shape is off"
        assert loc_df.isnull().sum().sum() == 0, "Null should not be present"

        if year == 2007:
            df["cod_reg"] = df.cod_reg.str.decode("utf-8")
        if year in process_dict["no_admit_date"]:
            df["cod_reg"] = df["cod_reg"].astype(str)
            df["cod_reg"] = (f"0" + df["cod_reg"]).str[-3:]
            assert (df["cod_reg"].apply(len) == 3).all()

        pre = df.shape[0]
        df = df.merge(loc_df, how="left", on="cod_reg")
        assert pre == df.shape[0]
        assert df.location_id.isnull().sum() == 0, "missing location IDs"
        df.drop(["cod_reg", "location_name"], axis=1, inplace=True)

        # code 1 for ICD-9, code 2 for ICD-10
        df["code_system_id"] = 1
        print(
            f"Does this look like ICD 9 data? {df.dx_1.str[0:1].value_counts().head()}"
        )

        # case is the sum of live discharges and deaths
        # df['outcome_id'] = "case/discharge/death"
        # df['outcome_id'] = df['outcome_id'].str.decode('ascii')
        df["outcome_id"] = df["outcome_id"].astype(str)
        df["outcome_id"].replace(["1"], ["death"], inplace=True)
        df.loc[df["outcome_id"] != "death", "outcome_id"] = "discharge"

        # metric_id == 1 signifies that the 'val' column consists of counts
        df["metric_id"] = 1

        df["year_end"] = df["year_start"]

        # remove x value from sex_id
        df.loc[df.sex_id == "X", "sex_id"] = 3

        # keep these but commented out if we ever look more into these vars
        # df.loc[df.hosp_scheme == 'X', 'hosp_scheme'] = 0
        # df.loc[df.hosp_type == "", 'hosp_type'] = np.nan

        # this is hospital discharge data
        df["facility_id"] = "hospital"

        # fix data types
        # num_cols = ['hosp_scheme', 'sex_id', 'hosp_type']
        num_cols = ["sex_id"]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="raise")

        diag_cols = df.filter(regex="^dx_|^ecode_").columns

        # below was necessary to process sas files
        # diagnosis columns should be ascii instead of byte
        # for col in diag_cols:
        #    df[col] = df[col].str.decode('ascii')

        ## live birth swapping subroutine
        # remove dummy null values, swap live births requires actual nulls
        for col in list(df.filter(regex="^(dx_)").columns.drop("dx_1")):
            df.loc[df[col] == "", col] = np.nan
        # swap live birth codes
        df = live_births.swap_live_births(
            df, user=getuser(), drop_if_primary_still_live=False
        )

        # now that swap is done, remove the day cases and create overnight weights
        # 2017/18 doesn't have discharge or admit dates
        if df["dis_date"].isnull().all():
            assert (
                year in process_dict["no_admit_date"]
            ), "Why doesn't this year have dates?"
            print(f"There is no discharge date for year {year}")
            df["los2"] = df["los"]
        else:
            # date cols to datetime obj
            for date_type in ["adm_date", "dis_date"]:
                df[date_type] = pd.to_datetime(df[date_type], errors="coerce")
                nulls = df[date_type].isnull().sum()
                if nulls > 0:
                    warnings.warn("Dropping {} null date rows".format(nulls))
                    df = df[df[date_type].notnull()]

            start_cases = df.shape[0]

            df["los2"] = df["dis_date"].subtract(df["adm_date"])
            df["los2"] = df["los2"].dt.days

        los_mismatch = df[df.los != df.los2].shape[0]
        print(f"{los_mismatch} los rows don't match the los2 column")

        # output data for day case weight on primary diagnosis only when admit/discharge
        # dates are present
        if year in process_dict["admit_date"]:
            tmp = df[["age", "age_days", "sex_id", "dx_1", "los2"]].copy()
            tmp.loc[tmp["age"] > 95, "age"] = 95  # this way everything older than GBD
            tmp.loc[tmp.age_days.notnull(), "age"] = (
                tmp[tmp.age_days.notnull()].age_days / 365
            )
            tmp = hosp_prep.age_binning(
                tmp, under1_age_detail=True, clinical_age_group_set_id=2
            )

            tmp["day_case"] = 0
            tmp.loc[tmp["los2"] == 0, "day_case"] = 1
            tmp["val"] = 1
            groups = ["age_start", "age_end", "sex_id", "dx_1"]
            day_cases = (
                tmp.query("day_case == 1").groupby(groups).val.sum().reset_index()
            )
            overnight_cases = (
                tmp.query("day_case == 0").groupby(groups).val.sum().reset_index()
            )
            overnight_cases.rename(columns={"val": "val_overnight"}, inplace=True)
            total_cases = tmp.groupby(groups).val.sum().reset_index()
            tmp = day_cases.merge(
                total_cases, how="outer", on=groups, suffixes=("_day", "_total")
            )
            tmp = tmp.merge(overnight_cases, how="outer", on=groups)
            assert start_cases == tmp.val_total.sum()

            tmp["overnight_weight"] = (tmp["val_total"] - tmp["val_day"]) / tmp[
                "val_total"
            ]
            # adjust the overnight weight when total == overnight stays
            tmp.loc[
                tmp.val_total.round(2) == tmp.val_overnight.round(2), "overnight_weight"
            ] = 1
            assert tmp.overnight_weight.notnull().any(), "No null weights allowed"

            outpath = f"FILEPATH"
            tmp.to_csv(outpath, index=False)

        assert start_cases == df.shape[0]
        # remove day cases
        # print("describe los2 col {}".format(df.los2.describe()))
        print(("There are {} rows with los2 less than 0".format((df.los2 < 0).sum())))

        # remove day cases and negative stay (nonsense) cases
        df = df[df.los2 > 0]
        end_cases = start_cases - df.shape[0]
        print(
            (
                "{} cases were lost when dropping day cases. or {} ".format(
                    end_cases, float(end_cases) / df.shape[0]
                )
            )
        )
        int_cases = df.shape[0]

        df.drop(["los", "los2"], axis=1, inplace=True)

        # Create a dictionary with year-nid as key-value pairs
        nid_dictionary = {
            2005: 331137,
            2006: 331138,
            2007: 331139,
            2008: 331140,
            2009: 331141,
            2010: 331142,
            2011: 331143,
            2012: 331144,
            2013: 331145,
            2014: 331146,
            2015: 331147,
            2016: 331148,
            2017: 421046,
            2018: 421047,
        }
        df = hosp_prep.fill_nid(df, nid_dictionary)

        #####################################################
        # CLEAN VARIABLES
        #####################################################

        # Columns contain only 1 optimized data type
        int_cols = [
            "location_id",
            "year_start",
            "year_end",
            "age_group_unit",
            "age",
            "sex_id",
            "nid",
            "representative_id",
            "metric_id",
        ]

        # BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
        # to the string "nan"
        # fast way to cast to str while preserving Nan:
        # df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
        str_cols = ["source", "facility_id", "outcome_id"]

        # use this to infer data types
        # df.apply(lambda x: pd.lib.infer_dtype(x.values))

        if df[str_cols].isnull().any().any():
            warnings.warn(
                f"""

                There are NaNs in the column(s)
                {format(df[str_cols].columns[df[str_cols].isnull().any()])}.
                These NaNs will be converted to the string 'nan'

                """
            )

        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
        for col in str_cols:
            df[col] = df[col].astype(str)

        # Turn 'age' into 'age_start' and 'age_end'
        #   - bin into year age ranges
        #   - under 1, 1-4, 5-9, 10-14 ...

        df.loc[df["age"] > 95, "age"] = 95  # this way everything older than GBD
        # terminal age start will be lumped into the terminal age group.

        # Convert neonatal cases from days to years
        unit_dict = {"days": 2, "months": 3, "years": 1}

        df.loc[df.age_days.notnull(), "age"] = df[df.age_days.notnull()].age_days / 365

        # now everything is in years
        df["age_group_unit"] = 1

        df = df.drop("age_days", axis=1)

        # df.loc[df.age_group_unit == 2, 'age'] = df[df.age_group_unit == 2].age / 365
        df = hosp_prep.age_binning(
            df, under1_age_detail=True, clinical_age_group_set_id=2
        )
        assert df.age_start.notnull().all()
        assert df.age_end.notnull().all()
        # df = stage_hosp_prep.convert_age_units(df, unit_dict)
        # df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)

        # drop unknown sex_id
        df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

        # drop unneeded columns, review hospital types
        for col in ["age", "regric", "dis_date", "adm_date"]:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)
        # df.drop(['age', 'regric',
        #          'dis_date', 'adm_date'], axis=1, inplace=True)

        # store the data wide for the EN matrix
        df_wide = df.copy()
        df_wide["metric_discharges"] = 1
        # any rows with a null get dropped in the groupby, fill these missing values with ""
        dxs = df_wide.filter(regex="^dx_").columns.tolist()
        for dxcol in dxs:
            df_wide[dxcol].fillna("", inplace=True)
            df_wide[dxcol] = hosp_prep.sanitize_diagnoses(df_wide[dxcol])
        wide_miss = df_wide.isnull().sum()
        if wide_miss.sum() > 0:
            print("Missing values", df_wide.isnull().sum())
        assert (df_wide.isnull().sum() == 0).all()
        df_wide = (
            df_wide.groupby(df_wide.columns.drop("metric_discharges").tolist())
            .agg({"metric_discharges": "sum"})
            .reset_index()
        )
        df_wide.to_stata(
            "FILEPATH"
        )
        write_path = "FILEPATH"
        )
        hosp_prep.write_hosp_file(df_wide, write_path, backup=False)
        del df_wide

        #####################################################
        # IF MULTIPLE DX EXIST:
        # TRANSFORM FROM WIDE TO LONG
        #####################################################
        pre_reshape_rows = df.shape[0]

        # Find all columns with dx_ at the start
        diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
        # Remove non-alphanumeric characters from dx feats
        for feat in diagnosis_feats:
            df[feat] = hosp_prep.sanitize_diagnoses(df[feat])

        cols = df.columns
        print(df.shape, "shape before reshape")
        if len(diagnosis_feats) > 1:
            # Reshape diagnoses from wide to long
            stack_idx = [n for n in df.columns if "dx_" not in n]
            # print(stack_idx)
            len_idx = len(stack_idx)

            df = df.set_index(stack_idx).stack().reset_index()

            # print(df["level_{}".format(len_idx)].value_counts())

            # drop the empty strings
            pre_dx1 = df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
            df = df[df[0] != ""]
            diff = pre_dx1 - df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
            print("{} dx1 cases/rows were lost after dropping blanks".format(diff))

            df = df.rename(
                columns={"level_{}".format(len_idx): "diagnosis_id", 0: "cause_code"}
            )

            df.loc[df["diagnosis_id"] != "dx_1", "diagnosis_id"] = 2
            df.loc[df.diagnosis_id == "dx_1", "diagnosis_id"] = 1

        elif len(diagnosis_feats) == 1:
            df.rename(columns={"dx_1": "cause_code"}, inplace=True)
            df["diagnosis_id"] = 1

        else:
            print("Something went wrong, there are no ICD code features")

        # If individual record: add one case for every diagnosis
        df["val"] = 1

        print(df.shape, "shape after reshape")

        assert abs(int_cases - df[df.diagnosis_id == 1].val.sum()) < 350
        chk = (
            df.query("diagnosis_id == 1")
            .groupby("source")
            .agg({"diagnosis_id": "sum"})
            .reset_index()
        )
        assert (chk["diagnosis_id"] >= pre_reshape_rows * 0.999).all()
        #####################################################
        # GROUPBY AND AGGREGATE
        #####################################################

        # Check for missing values
        print("Are there missing values in any row?\n")
        null_condition = df.isnull().values.any()
        if null_condition:
            warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
            assert False, "can't do it"
        else:
            print(">> No.")

        # Group by all features we want to keep and sums 'val'
        group_vars = [
            "cause_code",
            "diagnosis_id",
            "sex_id",
            "age_start",
            "age_end",
            "year_start",
            "year_end",
            "location_id",
            "nid",
            "age_group_unit",
            "source",
            "facility_id",
            "code_system_id",
            "outcome_id",
            "representative_id",
            "metric_id",
        ]
        df_agg = df.groupby(group_vars).agg({"val": "sum"}).reset_index()

        #####################################################
        # ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
        #####################################################

        # Arrange columns in our standardized feature order
        columns_before = df_agg.columns
        hosp_frmat_feat = [
            "age_group_unit",
            "age_start",
            "age_end",
            "year_start",
            "year_end",
            "location_id",
            "representative_id",
            "sex_id",
            "diagnosis_id",
            "metric_id",
            "outcome_id",
            "val",
            "source",
            "nid",
            "facility_id",
            "code_system_id",
            "cause_code",
        ]
        df_agg = df_agg[hosp_frmat_feat]
        columns_after = df_agg.columns

        if year in process_dict["no_admit_date"]:
            print("applying the age/sex/icd weights")
            df_agg = apply_overnight_weights(df_agg, weight_years="all")

        # check if all columns are there
        assert set(columns_before) == set(
            columns_after
        ), "You lost or added a column when reordering"
        for i in range(len(hosp_frmat_feat)):
            assert (
                hosp_frmat_feat[i] in df_agg.columns
            ), "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

        # check data types
        for i in df_agg.drop(
            ["cause_code", "source", "facility_id", "outcome_id"], axis=1, inplace=False
        ).columns:
            # assert that everything but cause_code, source, measure_id (for now)
            # are NOT object
            assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

        # check number of unique feature levels
        assert len(df_agg["year_start"].unique()) == len(
            df_agg["nid"].unique()
        ), "number of feature levels of years and nid should match number"
        assert len(df_agg["age_start"].unique()) == len(df_agg["age_end"].unique()), (
            "number of feature levels age start should match number of feature"
            " levels age end"
        )
        assert (
            len(df_agg["diagnosis_id"].unique()) <= 2
        ), "diagnosis_id should have 2 or less feature levels"

        s = [1, 2, 3]
        check_sex = [n for n in df.sex_id.unique() if n not in s]
        assert len(check_sex) == 0, "There is an unexpected sex_id value"

        assert (
            len(df_agg["code_system_id"].unique()) <= 2
        ), "code_system_id should have 2 or less feature levels"
        assert (
            len(df_agg["source"].unique()) == 1
        ), "source should only have one feature level"

        assert (df.val >= 0).all(), "for some reason there are negative case counts"

        assert abs(int_cases - df[df.diagnosis_id == 1].val.sum()) < 350
        chk = (
            df.query("diagnosis_id == 1")
            .groupby("source")
            .agg({"diagnosis_id": "sum"})
            .reset_index()
        )
        assert (chk["diagnosis_id"] >= pre_reshape_rows * 0.999).all()

        single_year_filepath = f"FILEPATH"
        hosp_prep.write_hosp_file(df_agg, single_year_filepath, backup=False)

        final_list.append(df_agg)
        del df
        del df_agg
        yr_run = round((time.time() - yr_start) / 60, 2)
        print("Done with {} in {} min".format(year, yr_run))
    df_agg = pd.concat(final_list, ignore_index=True)

print("Finished with the for loop or reading from disk")

# somehow "nan" is making it through, nulls were cast to string
df_agg = df_agg[df_agg.cause_code != "nan"]
# df_agg.info(memory_usage='deep')
print(df_agg.shape)

# NEW- test the newly prepped data against the last formatted version
# This is manually pulled in, and doesn't break if the test results are an issue, so carefully
# run this portion of the formatting script and review the output for warnings
compare_df = pd.read_hdf(
    "FILEPATH"
)

# test_df = df_agg.query("year_start not in (2017, 2018)").copy()
test_df = df_agg.copy()
# lots of rounded cases! test_df['val'] = test_df['val'].astype(int)

test_results = stage_hosp_prep.test_case_counts(test_df, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = "\n --- \n".join(test_results)
    warnings.warn(
        (
            f"Tests failed. old had cases {compare_df.val.sum()} while new "
            f"had {test_df.val.sum()} "
            f"and here are test results {msg}"
        )
    )

#####################################################
# WRITE TO FILE
#####################################################

# # Saving the file
write_path = (
    "FILEPATH"
)

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)

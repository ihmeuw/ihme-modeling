"""
For each processing group compare the raw to the final data to get an idea of how many
race values were changed and also to test for any bugs or errors

Validations:
1) The set of races for a bene after transforming is a subset of those available before
   splitting, ie we're not adding a new race value for a person
2) ALL non-missing bene_ids are present
3) print states to review the code that randomly samples between 2 unique values


"""
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.helpers.intermediate_tables.race import worker
from cms.utils.database import database


def proc_raw(g):
    df_list = []
    for cms_sys in ["max", "mdcr"]:
        fp = (
            "FILEPATH"
        )
        df = pd.read_csv(fp)
        if cms_sys == "max":
            df = worker.convert_el_race_to_rti(df)

            rename_dict = {
                "BENE_ID": "bene_id",
                "MAX_YR_DT": "year_id",
                "RTI_RACE_CD": "raw_rti_race_cd",
            }
            df.rename(columns=rename_dict, inplace=True)

        else:
            rename_dict = {
                "BENE_ID": "bene_id",
                "BENE_ENROLLMT_REF_YR": "year_id",
                "RTI_RACE_CD": "raw_rti_race_cd",
            }
            df.rename(columns=rename_dict, inplace=True)

        # keep only cols in final data
        df = df[rename_dict.values()]
        df["cms_sys"] = cms_sys
        df_list.append(df)

    df = pd.concat(df_list, sort=False, ignore_index=True)
    return df


def get_final(g):
    df_list = []
    for cms_sys in ["max", "mdcr"]:
        tmp = pd.read_csv(
            "FILEPATH"
        )
        tmp["cms_sys"] = cms_sys
        df_list.append(tmp)

    df = pd.concat(df_list, sort=False, ignore_index=True)
    return df


def compare_against_all(all_benes, groups):
    df_list = []
    for g in groups:
        tmp = get_final(g)[["bene_id", "year_id", "cms_sys"]].drop_duplicates()
        df_list.append(tmp)
        del tmp

    df = pd.concat(df_list)

    for cms_sys in ["max", "mdcr"]:
        exp_years = eval(f"constants.{cms_sys}_years")
        ydiff = set(df.loc[df.cms_sys == cms_sys, "year_id"].unique()).symmetric_difference(
            exp_years
        )
        if ydiff:
            raise ValueError(
                "The expected years and observed years are different "
                f"expected {exp_years}, diff is {ydiff} for system '{cms_sys}'"
            )

    diff = set(df.bene_id.unique()).symmetric_difference(all_benes.BENE_ID)
    if diff:
        raise ValueError(f"There's a diff of {len(diff)} bene ids")
    else:
        print(
            f"There is no diff between all ordered bene ids and final compiled "
            f"bene ids. Diff size is {len(diff)}"
        )
    return


def review_sampled_rows(rev):
    """quick print statements to make sure that sampled cases look like a coin flip"""
    rev2 = rev.groupby(["rti_race_cd", "raw_rti_race_cd"])[0].sum().reset_index()
    for rti in rev2.rti_race_cd.unique():
        for raw in rev2.raw_rti_race_cd.unique():
            if raw == rti:
                continue
            print(f"\nCOMPARING CODES {rti} and {raw}")
            a = rev2.query(f"rti_race_cd == {rti} and raw_rti_race_cd == {raw}")[0].iloc[0]
            b = rev2.query(f"rti_race_cd == {raw} and raw_rti_race_cd == {rti}")[0].iloc[0]
            tot = a + b
            print(f"pct of raw {(a / tot) * 100}")
            print(f"pct of rti {(b / tot) * 100}")
    return


def compare_raw_fin(raw, fin, m):

    m2 = m[(m.rti_race_cd != m.raw_rti_race_cd) & (m.raw_rti_race_cd != 0)]
    mraw = (
        m[m.bene_id.isin(m2.bene_id)]
        .groupby("bene_id")["raw_rti_race_cd"]
        .unique()
        .reset_index()
    )
    mraw["raw_rti_race_cd"] = mraw.raw_rti_race_cd.astype(str)
    mraw = mraw.merge(fin, how="left", on="bene_id")
    bad_switch = []
    for b in mraw.bene_id.unique():
        tmp = mraw[mraw.bene_id == b]
        for year in tmp.year_id.unique():
            tmp2 = mraw.loc[(mraw.year_id == year) & (mraw.bene_id == b)]
            rti = str(tmp2.rti_race_cd.iloc[0])
            if rti not in tmp2.raw_rti_race_cd.iloc[0]:
                bad_switch.append(b)
    if bad_switch:
        raise ValueError(f"There are {len(bad_switch)} bene ids that got bad race switches")

    diff = set(raw.bene_id).symmetric_difference(set(fin.bene_id))
    if diff:
        raise ValueError(f"There's a diff of {len(diff)} bene ids")

    return


def validate_bene_race():
    all_benes = pd.read_csv(
        "FILEPATH"
    )

    upper = int(pd.np.ceil(len(all_benes) / 5e6))
    groups = [i for i in range(0, upper)]

    compare_against_all(all_benes, groups)

    rev_list = []
    for g in groups:

        raw = proc_raw(g)
        fin = get_final(g)
        m = fin.merge(raw, how="outer", on=["bene_id", "year_id", "cms_sys"], validate="1:m")

        compare_raw_fin(raw, fin, m)

        m2 = m[m.rti_race_cd != m.raw_rti_race_cd]

        m3 = m2[m2.raw_rti_race_cd != 0]

        rev = m3.groupby(["rti_race_cd", "raw_rti_race_cd"]).size().reset_index()
        rev["group"] = g
        rev_list.append(rev)

    rev = pd.concat(rev_list)

    review_sampled_rows(rev)
    return


def validate_db_tables(cms_sys):
    """The CSVs in the for_upload table have been validated using the functions
    above so now we just need to compare that data against the db data. These files
    are large but not unmanageable on a big qlogin. 200Gb and 1 thread should work."""

    cms = database.CmsDatabase()
    cms.load_odbc()

    print("Querying CMS intermediate race-bene table:")
    db_df = cms.read_table("QUERY")
    print("Query complete")

    print("Reading flat file")
    read_path = "FILEPATH"
    flat_df = pd.read_csv(read_path, names=["bene_id", "year_id", "rti_race_cd"])
    print("Complete")

    if not db_df.equals(flat_df):
        print("Merging Data")
        db_df = db_df.merge(
            flat_df,
            how="outer",
            on=["bene_id", "year_id"],
            suffixes=("_db", "_flat"),
            validate="1:1",
        )
        mis_match = db_df[db_df.rti_race_cd_db != db_df.rti_race_cd_flat]
        if len(mis_match) != 0:
            raise ValueError(f"There are {len(mis_match)} disagreeable rows")
        else:
            if len(db_df) != len(flat_df):
                raise ValueError("The row counts don't match")
            if not db_df.year_id.value_counts().equals(flat_df.year_id.value_counts()):
                raise ValueError("The year row counts don't match")
            if db_df.isnull().sum().sum() != 0:
                raise ValueError("There are unexpected Null values")
            print(
                "The race-bene DFs are in agreement between the flat file and database table"
            )
    else:
        print("The race-bene DFs are in agreement between the flat file and database table")
    return

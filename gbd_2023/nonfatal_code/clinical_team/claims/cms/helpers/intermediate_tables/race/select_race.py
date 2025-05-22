"""
Select the most common race coding for a beneficiary or randomly
choose between the remainder
"""
from typing import Tuple

import pandas as pd


def identify_disagreement(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Given a dataframe with some beneficiary id race disagreement in the same year
    identify the benes which disagree and split them into a separate df
    """

    assert df.year_id.unique().size == 1, "wrong number of years"
    # get unique races per bene, anything greater than 1 is disagreement
    u_races = df.groupby("bene_id").nunique()[["rti_race_cd"]].reset_index()

    # get the list of bene_ids in disagreement
    disagree_benes = (
        df.loc[df.bene_id.isin(u_races.loc[u_races.rti_race_cd > 1, "bene_id"]), "bene_id"]
        .unique()
        .tolist()
    )

    # create the two dfs, one with only disagree
    dupe_df = df[df.bene_id.isin(disagree_benes)]
    # and the other without disagreement and remove duplication
    df = df[
        ~df.bene_id.isin(disagree_benes)
    ].drop_duplicates()  # drop dupes that are in agreement
    assert not (
        dupe_df.bene_id.value_counts() == 1
    ).any(), "there are single bene_ids here somehow"

    return df, dupe_df


def count_codings(df: pd.DataFrame) -> pd.DataFrame:
    # vote on commonly occuring codes and sum them
    df["race_cnt"] = 1  # 1 row 1 vote
    df["race_cnt_sum"] = df.groupby(["bene_id", "rti_race_cd"])["race_cnt"].transform("sum")

    df["max_cnt"] = df.groupby(["bene_id"])["race_cnt_sum"].transform("max")

    return df


def extract_winners(dupe_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # find rows with a max count equal to coding count and greater than 1
    cond = "(dupe_df.race_cnt_sum == dupe_df.max_cnt) & (dupe_df.max_cnt > 1)"
    win_df = (
        dupe_df.loc[eval(cond), ["bene_id", "year_id", "rti_race_cd"]].copy().drop_duplicates()
    )

    # pass all benes that don't have a clear or contested winner to the random choice func
    dupe_df = dupe_df[~dupe_df.bene_id.isin(win_df.bene_id)].copy()

    return dupe_df, win_df


def randomly_select_race(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sample(frac=1, random_state=21)
    return df.drop_duplicates(subset="bene_id", keep="first")


def sample_and_win(dupe_df: pd.DataFrame, win_df: pd.DataFrame) -> pd.DataFrame:
    # sometimes there are benes that have ties, extract these
    contested_df = win_df[win_df.bene_id.duplicated(keep=False)].copy()
    # randomly sample a winner
    contested_df = randomly_select_race(contested_df)

    # and then remove them
    win_df = win_df[~win_df.bene_id.duplicated(keep=False)].copy()
    assert win_df.bene_id.unique().size == len(win_df), "Voting algo didnt work"

    # if tied, choose a random loc
    dupe_df = randomly_select_race(dupe_df)
    new_agreement = pd.concat([dupe_df, win_df, contested_df], sort=False, ignore_index=True)

    print(
        f"All disagreement has been resolved. There were {len(win_df)} benes with a clear winner"
        f" and {len(contested_df) + len(dupe_df)} benes which were randomly selected"
    )
    return new_agreement


def set_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["year_id", "rti_race_cd"]:
        df[col] = df[col].astype(int)

    return df


def resolve_disagreement_by_year(fdf: pd.DataFrame) -> pd.DataFrame:

    if fdf.empty:
        return fdf
    else:
        cols = fdf.columns.tolist()
        res = []
        # loop over years
        for year in fdf.year_id.unique():
            # subset by year
            df = fdf[fdf.year_id == year].copy()

            df, dupe_df = identify_disagreement(df)
            res.append(df[cols])
            print(
                f"There are {len(df)} rows without any in-year race disagreement and "
                f"{len(dupe_df)} rows with some kind of disagreement"
            )

            dupe_df = count_codings(dupe_df)

            dupe_df, win_df = extract_winners(dupe_df)

            new_agreement = sample_and_win(dupe_df, win_df)
            res.append(new_agreement[cols])

        res_df = pd.concat(res, sort=False, ignore_index=True)
        res_df.sort_values(["bene_id", "year_id"], inplace=True)
        res_df.reset_index(drop=True, inplace=True)

        res_df = set_dtypes(res_df)
        # now run a few tests
        bene_diff = set(res_df.bene_id.unique()).symmetric_difference(fdf.bene_id.unique())
        if bene_diff:
            raise ValueError(f"There are missing benes!! {bene_diff}")
        if len(res_df) != len(res_df[["bene_id", "year_id"]].drop_duplicates()):
            raise ValueError(
                "duplicate bene_ids are present in the final data. "
                "Disagreements have not been resolved"
            )
        if res_df.isnull().sum().sum() != 0:
            raise ValueError(
                f"There should not be any null values, but there are:\n"
                f"{res_df.isnull().sum()}"
            )

        return res_df


def unique_coding_across_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function performs a final check on the dataset to ensure that
    there's only one race code for each bene_id across all years.
    """
    grouped_df = (
        df.groupby(["bene_id"])["year_id", "rti_race_cd"]
        .nunique()
        .add_prefix("count_")
        .reset_index()
    )
    multi_race_bene = grouped_df.loc[grouped_df.count_rti_race_cd >= 2].bene_id.tolist()
    for bene in multi_race_bene:
        most_occur = df.loc[df.bene_id == bene].rti_race_cd.mode().tolist()
        most_recent_year = max(
            df.loc[(df.bene_id == bene) & (df.rti_race_cd.isin(most_occur))].year_id
        )
        df.loc[df.bene_id == bene, "rti_race_cd"] = df.loc[
            (df.bene_id == bene) & (df.year_id == most_recent_year)
        ].rti_race_cd.item()

    return df

import numpy as np
import pandas as pd


def age_gap(df):
    """
    Edge case where a bene transitions between three age groups
    in the month procceding their birth month. Occurs
    when the dob is the last few days of the month.
    """
    df_2 = df.copy()

    # age_start : 6, age_end : 28
    df.loc[df.age_end > 27.5, ["age_end", "days_end"]] = [27.5, 21.5]

    # age_start : 182,  age_end : 182
    df_2.loc[df_2.age_start < 6.5, ["age_start", "days_start", "days_end"]] = [29, 0, 0]

    df = pd.concat([df, df_2], sort=False)
    df = df.sort_values(by=["bene_id", "age_start"]).reset_index(drop=True)

    df.loc[df.age_start <= 6.5, "days_start"] = 6 - df.days_start
    df.loc[df.age_end > 27.5, ["days_end", "days_start"]] = 30.5 - (
        df.days_end.shift(1) + df.days_start.shift(1)
    )
    return df


def age_gap_chk(df):
    """
    Check if there is an age gap (eg. for month x
    age_start = 6 and age_end = 182)
    """
    temp = df[(df.age_start <= 6) & (df.age_end > 28)]
    if temp.shape[0] > 0:
        # remove rows that are in the temp df
        df = df.drop(temp.index.tolist())
        temp = age_gap(temp)
        df = pd.concat([df, temp], sort=False)
        df = df.sort_values(by=["bene_id", "age_start"]).reset_index(drop=True)

    return df


def non_ENN_ages(df):

    # offsetting by half a day since the columns age_start and age_end
    # are inclusive to inclusive.
    #
    # aside from dob and dod months the diff
    # between age_start and age_end is 30
    df["age_start"] = df.age_end - 30

    df = age_range(df)

    df["days_start"] = np.where(
        df.age_stop_start == df.age_stop_end,
        df.days_start,  # 30.5,
        df.age_stop_start - df.age_start,
    )

    df["days_end"] = np.where(
        df.age_stop_start == df.age_stop_end, df.days_end, 30.5 - df.days_start  # 30.5
    )
    return df.drop(["age_stop_start", "age_stop_end"], axis=1)


def ENN_ages(df):

    df["age_end"] = np.where(
        df.dob.dt.month == df.month_id, 30.5 - df.dob.dt.day, df.age_end
    )

    df["age_start"] = np.where(
        df.dob.dt.month == df.month_id, 0, 6 - (30.5 - df.dob.dt.day)
    )

    df["days_start"] = np.where(df.age_end > 6, 6 - (df.age_start), df.age_end)

    df["days_end"] = np.where(df.age_end > 6, df.age_end - df.days_start, df.days_start)

    return age_gap_chk(df)


def age_range(df):
    """
    Cast age days counts to gbd age bins
    """
    lookup = pd.concat(age_lookup())

    df = (
        df.merge(lookup, left_on="age_start", right_on="ages")
        .drop("ages", axis=1)
        .rename({"age_stop": "age_stop_start"}, axis=1)
    )

    return (
        df.merge(lookup, left_on="age_end", right_on="ages")
        .drop("ages", axis=1)
        .rename({"age_stop": "age_stop_end"}, axis=1)
    )


def dod_age_days(df):
    df.loc[df.dod.dt.month == df.month_id, ["days_start", "days_end"]] = df.dod.dt.day

    return df[(df.month_id <= df.dod.dt.month) | (df.dod.isnull())].sort_values(
        by=["bene_id", "age_start"]
    )


def gbd_age_days():
    gbd_dict = {6.5: 0, 27.5: 0.01917808, 182.5: 0.07671233, 366: 0.5, 729.5: 1}
    for k, v in gbd_dict.items():
        yield pd.DataFrame({"age": k, "gbd_age": v}, index=[0])


def age_lookup():
    """
    Create a df to bin age days to GBD U1 ages.

    Adding an extra two days for 6-11 months to fit within our
    defintion of 30.5 days per month.
    """
    u1_range = {
        6.5: np.arange(0, 7, 0.5),
        27.5: np.arange(7, 28, 0.5),
        182.5: np.arange(28, 183, 0.5),
        366: np.arange(183, 366.5, 0.5),
        729.5: np.arange(366.5, 730, 0.5),
    }
    for k, v in u1_range.items():
        yield pd.DataFrame({"age_stop": k, "ages": v})


def process(df):
    """
    Subroutine performed after the creation of the ages.
    """
    # correct for edge case where month_id < dob.month.
    df = df[~((df.dob.dt.year == df.year_id) & (df.month_id < df.dob.dt.month))]

    # correct for edge case where dob is the 31st
    for col in ["age_start", "age_end", "days_start", "days_end"]:
        df[col] = np.where(df[col] < 0, df[col] * -1, df[col])

    df = df.sort_values(by=["bene_id", "month_id"]).reset_index(drop=True)

    # remove age days
    df_range = age_range(df)
    df_range.drop(["age_start", "age_end"], axis=1, inplace=True)
    df_range.rename(
        {"age_stop_start": "age_start", "age_stop_end": "age_end"}, axis=1, inplace=True
    )

    gbd_ages = pd.concat(gbd_age_days())

    for e in ["age_start", "age_end"]:
        df_range = (
            df_range.merge(gbd_ages, left_on=e, right_on="age")
            .drop([e, "age"], axis=1)
            .rename({"gbd_age": e}, axis=1)
        )

    df_range = df_range.sort_values(
        by=["bene_id", "month_id", "age_start"]
    ).reset_index(drop=True)

    return df_range


def create_ages(df):
    df = df.join(
        pd.DataFrame(
            {
                "age_start": None,
                "age_end": (30.5 * (df.month_id - df.dob.dt.month))
                + (30.5 - df.dob.dt.day),
                "days_start": 30.5,
                "days_end": 30.5,
            },
            index=df.index,
        )
    )

    enn_df = df[
        (df.dob.dt.month == df.month_id)
        | (((30.5 - df.dob.dt.day) < 6) & ((df.dob.dt.month + 1) == df.month_id))
    ]

    # merge to drop ENN bene rows
    df = df.merge(enn_df, on=df.columns.tolist(), indicator=True, how="outer")
    df = df[df._merge == "left_only"].drop("_merge", axis=1)
    df = pd.concat([ENN_ages(enn_df), non_ENN_ages(df)], sort=False)

    return process(df)


def prev_yr_ages(df):
    """
    Create U1 age groups where the dob year is not the same
    as the processing year
    """
    df = df.join(
        pd.DataFrame(
            {
                "prev_yr_days": (30.5 - df.dob.dt.day)
                + (30.5 * (12 - df.dob.dt.month)),
                "age_start": None,
                "age_end": None,
                "days_start": 30.5,
                "days_end": 30.5,
            },
            index=df.index,
        )
    )

    df["age_end"] = df.prev_yr_days + (30.5 * df.month_id)
    df["age_start"] = df.age_end - 30
    df.drop("prev_yr_days", axis=1, inplace=True)

    df = non_ENN_ages(df)
    df = age_gap_chk(df)

    return process(df)

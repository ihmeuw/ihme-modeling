import pandas as pd


def wide_to_long(df, wide_cols, id_cols, review_group_counts=True):
    """
    Reshape df to long format. Retains
    order of wide_cols and number of unique values of wide_cols.

    Order of the wide_cols:
        0 is oldest occurrence
        n is the newest occurrence

    Requires the col 'unique_codes'.

    Parameteres:
        df (pandas Dataframe): df that contains both wide_cols and id_cols
        wide_cols (str list): columns to transform to long
        id_cols (str list): unique columns
    """

    cols = id_cols + wide_cols
    temp = df[cols]

    temp = temp.melt(id_vars=id_cols, value_vars=wide_cols, value_name="col", var_name="order")

    temp.dropna(inplace=True)

    if review_group_counts:

        temp["unique_codes"] = 1
        temp.drop_duplicates(inplace=True)

        gb = temp.groupby(id_cols).agg({"unique_codes": "sum"}).reset_index()
        temp = temp.drop("unique_codes", axis=1)

        temp = temp.merge(gb, on=id_cols)

        df = df[id_cols]
        df = df.merge(temp, on=id_cols)
        return df
    else:
        return temp.reset_index(drop=True)

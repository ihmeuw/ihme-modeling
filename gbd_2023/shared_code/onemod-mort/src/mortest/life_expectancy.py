import pandas as pd

BY = ["sex_id", "location_id", "year_id"]
IDS = BY + ["age_group_years_start", "age_group_years_end", "age_group_id"]


def _merge_age_meta(data: pd.DataFrame, age_meta: pd.DataFrame) -> pd.DataFrame:
    columns_to_add = []
    for col in ["age_group_years_start", "age_group_years_end"]:
        if col not in data.columns:
            columns_to_add.append(col)
    if columns_to_add:
        data = data.merge(
            age_meta[["age_group_id"] + columns_to_add],
            on="age_group_id",
            how="left",
        )
    return data


def _sort(data: pd.DataFrame) -> pd.DataFrame:
    return data.sort_values(IDS, ignore_index=True)


def _compute_n(data: pd.DataFrame) -> pd.DataFrame:
    data["n"] = data.eval("age_group_years_end - age_group_years_start")
    return data


def _compute_ax(data: pd.DataFrame) -> pd.DataFrame:
    data["ax"] = data.eval("n + 1 / mx - n / (1 - exp(-n * mx))")
    return data


def _compute_qx(data: pd.DataFrame) -> pd.DataFrame:
    data["qx"] = data.eval("n * mx / (1 + (n - ax) * mx)")
    return data


def _compute_lx(data: pd.DataFrame) -> pd.DataFrame:
    data["tmp"] = data.eval("1 - qx")
    data["tmp"] = data.groupby(BY)["tmp"].shift(1, fill_value=1.0)
    data["lx"] = data.groupby(BY)["tmp"].cumprod()
    data.drop(columns=["tmp"], inplace=True)
    return data


def _compute_dx(data: pd.DataFrame) -> pd.DataFrame:
    data["dx"] = data["lx"] - data.groupby(BY)["lx"].shift(-1)
    return data


def _compute_nLx(data: pd.DataFrame) -> pd.DataFrame:
    data["nLx"] = data["n"] * data.groupby(BY)["lx"].shift(-1) + data.eval(
        "ax * dx"
    )
    index = data["nLx"].isna()
    data.loc[index, "nLx"] = data[index].eval("lx / mx")
    return data


def _compute_Tx(data: pd.DataFrame) -> pd.DataFrame:
    data = data.sort_values(IDS, ascending=False, ignore_index=True)
    data["Tx"] = data.groupby(BY)["nLx"].cumsum()
    data = data.sort_values(IDS, ascending=True, ignore_index=True)
    return data


def _compute_ex(data: pd.DataFrame) -> pd.DataFrame:
    data["ex"] = data.eval("Tx / lx")
    return data


def _compute_life_expectancy(data: pd.DataFrame, column: str) -> pd.DataFrame:
    data = (
        data.rename(columns={column: "mx"})
        .pipe(_compute_ax)
        .pipe(_compute_qx)
        .pipe(_compute_lx)
        .pipe(_compute_dx)
        .pipe(_compute_nLx)
        .pipe(_compute_Tx)
        .pipe(_compute_ex)
        .drop(columns=["ax", "qx", "lx", "dx", "nLx", "Tx"])
        .rename(columns={"mx": column, "ex": f"ex_{column}"})
    )
    return data


def compute_life_expectancy(
    data: pd.DataFrame, age_meta: pd.DataFrame, columns: list[str]
) -> pd.DataFrame:
    data = data.pipe(_merge_age_meta, age_meta).pipe(_sort).pipe(_compute_n)
    for column in columns:
        data = _compute_life_expectancy(data, column)
    data.drop(
        columns=["age_group_years_start", "age_group_years_end", "n"],
        inplace=True,
    )
    return data

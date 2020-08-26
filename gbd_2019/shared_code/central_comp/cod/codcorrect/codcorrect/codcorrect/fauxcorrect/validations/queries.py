import pandas as pd


def one_row_returned(sql_results: pd.DataFrame) -> bool:
    return sql_results.shape[0] == 1

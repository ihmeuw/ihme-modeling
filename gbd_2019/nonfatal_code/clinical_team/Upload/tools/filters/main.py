import numpy as np


""" This file holds all filter functions """

""" Functions with _ are helper functions """
def _empty_column(df, col):
    """Filters out rows that are empty for a given column"""
    if col not in df.columns:
        raise FilterError(f"Can't filter out rows with empty {col} from "
                          "a DataFrame without a {col} column.")
    mask = df[col].isnull()
    return mask

def empty_mean(df):
    """Filters out rows with null values in the mean column"""
    return _empty_column(df, 'mean')

def inf_mean(df):
    """ Filters out all rows with inf values in the mean column"""
    if "mean" not in df.columns:
        raise FilterError(f"Can't filter out inf from "
                          "a DataFrame without a {mean} column.")
    return np.isinf(df['mean'])

def duplicates(df):
    """Filters out rows that are duplicates, excluding the first occurence."""
    mask = df.duplicated()
    return mask

def impossible_prevalence(df):
    """Filters out rows with prevalence > 1"""
    if "measure_id" not in df.columns or "mean" not in df.columns:
        raise FilterError("Cannot remove prevalence from a df without a "
                          "`measure_id` and `mean` column")
    mask = (df.measure_id == 5) & (df['mean'] > 1)
    return mask

def impossible_incidence(df):
    """Filters out rows with incidence > (365/duration)"""
    required_columns = ["measure_id", "upper", "duration"]
    if any([col not in df.columns for col in required_columns]):
        raise FilterError("Cannot remove incidence from a df without "
                          "`bundle_id`, `upper` and `duration` columns")
    
    mask = ((df.measure_id == 6) &
            (df.upper > (365.0 / df.duration)))
    return mask

def mean_greater_than_upper(df):
    """Filters out rows with mean > upper"""
    if "mean" not in df.columns or "upper" not in df.columns:
        raise FilterError("Cannot remove mean > upper from a df without a "
                          "`mean` and `upper` column")
    mask = (df['mean'] > df.upper)
    return mask
    

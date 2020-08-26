import pandas as pd
import numpy as np
from typing import Union, List, Dict

from le_decomp.constants import AgeGroup, Demographics, LifeTable
from le_decomp import validations


def generate_paired_years(start_years: Union[List[int], int, None] = None,
                          end_years: Union[List[int], int, None] = None,
                          year_ids: Union[List[int], int, None] = None):
    """
    Takes either two equal length lists of start_years and end_years,
    or a single list of year_ids. If two lists are passed, they
    are validated using le_decomp.validations.validate_year_lists
    and then returned. If year_ids are passed, every possible time
    span that can be created using those years is generated and returned
    in the form of two lists. Example: if year_ids=[1990, 2000, 2010, 2019],
    this function returns [1990, 1990, 1990, 2000, 2000, 2010] and
    [2000, 2010, 2019, 2010, 2019, 2019].
    """
    if start_years and end_years and not year_ids:
        start_years = list(np.atleast_1d(start_years))
        end_years = list(np.atleast_1d(end_years))
        validations.validate_year_lists(start_years, end_years)
        return start_years, end_years
    elif not start_years and not end_years and year_ids:
        validations.validate_year_ids(year_ids)
        year_ids = sorted(year_ids)
        start_years = []
        end_years = []
        for start_year_ind in range(len(year_ids) - 1):
            for end_year_ind in range(start_year_ind + 1, len(year_ids)):
                start_years.append(year_ids[start_year_ind])
                end_years.append(year_ids[end_year_ind])
        return start_years, end_years
    else:
        raise ValueError(f"Either both of 'start_years' and 'end_years', "
                         f"OR 'year_ids' can be specified.  Given: "
                         f"start_years={start_years}, end_years={end_years}, ",
                         f"year_ids={year_ids}")


def life_exp_at_birth(life_table: pd.DataFrame):
    # Make sure change in life exp at birth matches delta_x summed
    return life_table.loc[
        life_table['age_group_id'] == AgeGroup.YOUNGEST_AGE_ID,
        [LifeTable.LIFE_EXPECTANCY_ABBR, 'year_id']].set_index(
            'year_id')


def reshape_lt_wide(data: pd.DataFrame):
    """
    Takes a life table dataframe in the form returned
    by get_life_table_with_shocks and returns it in 'wide'
    format, ie with columns for each life table parameter,
    'ex', 'lx', etc.
    """
    value_col = 'mean'
    new_cols = data[LifeTable.PARAMETER_NAME].drop_duplicates()
    reshaped = []
    for i, c in enumerate(new_cols):
        temp = data.loc[(data[LifeTable.PARAMETER_NAME] == c)].copy(deep=True)
        temp = temp[Demographics.INDEX_COLUMNS + [value_col]]
        temp = temp.rename(columns={value_col: c})
        if i == 0:
            reshaped = temp.copy(deep=True)
        else:
            reshaped = pd.merge(
                reshaped, temp, on=Demographics.INDEX_COLUMNS,
                how='left'
            )
    return reshaped


def dataframe_to_xarray(df: pd.DataFrame,
                        index: str,
                        filters: Dict=None,
                        keep_columns: List[str]=None):
    temp = df.copy(deep=True)
    if filters:
        for key in filters:
            values = list(np.atleast_1d(filters[key]))
            temp = temp.loc[df[key].isin(values)]
    if keep_columns:
        keep = list(set(keep_columns + [index]))
        temp = temp[keep]
    return temp.set_index(index).to_xarray()

from typing import List

import numpy as np
import pandas as pd


def comorbidity_value_combine(array_list: List[np.ndarray]) -> np.ndarray:
    """Standard function for combining disability weights, performed element-wise across
    a list of arrays. The arrays provided can be a mix of shapes. The following shape.

    combinations are allowed:
    - all arrays share the same shape (nrow, ncol) = (N, M). Every element will be
        combined by the comorbidity equation.
    - all arrays share the same shape (nrow, ncol) = (1, M). Every element will be
        combined by the comorbidity equation.
    - arrays are of mixed shapes (nrow, ncol) = (N, M) and (1, M). Arrays of shape (1, M)
        are broadcast to shape (N, M) and then every element is combined by the comorbidity
        equation.
    Shape mixtures like these examples are not supported:
    - {(N, M), (2, M)} - 1 of the 2 shapes must have nrows = 1
    - {(N, M), (K, M), (1, M)} - Only 2 unique shape types are possible

    Args:
        array_list: A list of np.ndarrays that can be a mix of shapes (N, M) and (1, M)

    Returns:
        combined: An np.ndarray with the same number of rows of the largest input (N or 1)
        and M columns.

    Raises:
        ValueError:
            - if any values of any array are outside the bounds of [0, 1) - disability weights
            should lie in this range.
            - if there are more than 2 shapes of arrays in the list
            - if any of the arrays are not rank 2
            - if there are < 2 arrays in the list to combine
            - if the shapes are (N, M) and (K, M) and N != K and neither N nor K = 1
    """
    in_range = [np.all((i >= 0) & (i < 1)) for i in array_list]
    if not all(in_range):
        raise ValueError(
            "array elements must be in range [0, 1). The test results "
            f"for this check were {in_range}."
        )

    ndims = np.array([i.ndim for i in array_list])
    shapes = [i.shape for i in array_list]
    if (len(set(shapes)) > 2) or (np.any(ndims != 2)) or (len(array_list) <= 1):
        raise ValueError(
            "comorbidity_value_combine() requires a list of more than one array "
            "with shapes that can be a mix of (nrow, ncol) and (1, ncol). "
            f"It was provided with {len(array_list)} arrays with shapes {shapes}"
        )

    nrows = [i.shape[0] for i in array_list]
    if len(set(shapes)) == 2:
        if 1 not in set(nrows):
            raise ValueError(
                "comorbidity_value_combine() with multiple shaped arrays "
                "requires 1 of the shapes to have only 1 row. The provided "
                f"shapes were {shapes}"
            )
        one_row_product = np.prod([1.0 - i for i in array_list if i.shape[0] == 1], axis=0)
        multi_row_product = np.prod([1.0 - i for i in array_list if i.shape[0] != 1], axis=0)
        combined = 1.0 - one_row_product * multi_row_product
    else:
        combined = 1.0 - np.prod([1.0 - i for i in array_list], axis=0)
    return combined


def comorbidity_row_combine(df: pd.DataFrame) -> pd.Series:
    """Combines disability weights in each column named 'draw_*'.

    Args:
        df: the input DataFrame

    Returns:
        combined_draws: the output Series each draw column list of values
        combined into a single value. non-draw columns are dropped relative to inputs.

    Notes:
        this is a preserved function from combine_epilepsy_any. It can be improved
        but that usage needs to follow with any change.
    """
    draws_to_combine = df.filter(like="draw_")
    combined_draws = 1 - (1 - draws_to_combine).prod()
    return combined_draws

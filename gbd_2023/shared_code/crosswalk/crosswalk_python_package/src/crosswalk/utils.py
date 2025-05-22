# -*- coding: utf-8 -*-
"""
    utils
    ~~~~~
    `utils` module for the `crosswalk` package, provides utility functions.
"""
from typing import List, Iterable, Union
import numpy as np
from scipy.stats import norm


def is_numerical_array(x, shape=None, not_nan=True, not_inf=True):
    """Check if the given variable a numerical array.
    Args:
        x (numpy.ndarray):
            The array need to be checked.
        shape (tuple{int, int} | None, optional)
            The shape of the array
        not_nan (bool, optional):
            Optional variable check if the array contains nan.
        not_inf (bool, optional):
            Optional variable check if the array contains inf.
    Returns:
        bool: if x is a numerical numpy array.
    """
    ok = isinstance(x, np.ndarray)
    if not ok:
        return ok
    ok = ok and np.issubdtype(x.dtype, np.number)
    if not_nan:
        ok = ok and (not np.isnan(x).any())
    if not_inf:
        ok = ok and (not np.isinf(x).any())
    if shape is not None:
        ok = ok and (x.shape == shape)

    return ok


def sizes_to_indices(sizes):
    """Converting sizes to corresponding indices.

    Args:
        sizes (numpy.dnarray):
            An array consist of non-negative number.

    Returns:
        list{range}:
            List the indices.
    """
    indices = []
    a = 0
    b = 0
    for i, size in enumerate(sizes):
        b += size
        indices.append(range(a, b))
        a += size

    return indices


def sizes_to_slices(sizes):
    """Converting sizes to corresponding slices.
    Args:
        sizes (numpy.dnarray):
            An array consist of non-negative number.
    Returns:
        list{slice}:
            List the slices.
    """
    slices = []
    a = 0
    b = 0
    for i, size in enumerate(sizes):
        b += size
        slices.append(slice(a, b))
        a += size

    return slices


def array_structure(x):
    """Return the structure of the array.

    Args:
        x (Iterable):
            The numpy array need to be studied.

    Returns:
        tuple{int, numpy.ndarray, numpy.ndarray}:
            Return the number of unique elements in the array, counts for each
            unique element and unique element.
    """
    x = flatten_list(list(x))
    unique_x, x_sizes = np.unique(x, return_counts=True)
    num_x = x_sizes.size

    return num_x, x_sizes, unique_x


def default_input(input, default=None):
    """Process the keyword input in the function.

    Args:
        input:
            Keyword input for the function.

    Return:
        `default` when input is `None`, otherwise `input`.
    """
    if input is None:
        return default
    else:
        return input


def log_to_linear(mean, sd):
    """Transform mean and standard deviation from log space to linear space.
    Using Delta method.

    Args:
        mean (numpy.ndarray):
            Mean in log space.
        sd (numpy.ndarray):
            Standard deviation in log space.

    Returns:
        tuple{numpy.ndarray, numpy.ndarray}:
            Mean and standard deviation in linear space.
    """
    assert mean.size == sd.size
    assert (sd >= 0.0).all()
    linear_mean = np.exp(mean)
    linear_sd = np.exp(mean)*sd

    return linear_mean, linear_sd


def linear_to_log(mean, sd):
    """Transform mean and standard deviation from linear space to log space.
    Using delta method.

    Args:
        mean (numpy.ndarray):
            Mean in linear space.
        sd (numpy.ndarray):
            Standard deviation in linear space.

    Returns:
        tuple{numpy.ndarray, numpy.ndarray}:
            Mean and standard deviation in log space.
    """
    assert mean.size == sd.size
    assert (mean > 0.0).all()
    assert (sd >= 0.0).all()
    log_mean = np.log(mean)
    log_sd = sd/mean

    return log_mean, log_sd


def logit_to_linear(mean, sd):
    """Transform mean and standard deviation from logit space to linear space.
    Using Delta method.

    Args:
        mean (numpy.ndarray):
            Mean in logit space.
        sd (numpy.ndarray):
            Standard deviation in logit space.

    Returns:
        tuple{numpy.ndarray, numpy.ndarray}:
            Mean and standard deviation in linear space.
    """
    assert mean.size == sd.size
    assert (sd >= 0.0).all()
    linear_mean = 1.0/(1.0 + np.exp(-mean))
    linear_sd = (np.exp(mean)/(1.0 + np.exp(mean))**2)*sd

    return linear_mean, linear_sd


def linear_to_logit(mean, sd):
    """Transform mean and standard deviation from linear space to logit space.
    Using delta method.

    Args:
        mean (numpy.ndarray):
            Mean in linear space.
        sd (numpy.ndarray):
            Standard deviation in linear space.

    Returns:
        tuple{numpy.ndarray, numpy.ndarray}:
            Mean and standard deviation in logit space.
    """
    assert mean.size == sd.size
    assert ((mean > 0.0) & (mean < 1.0)).all()
    assert (sd >= 0.0).all()
    logit_mean = np.log(mean/(1.0 - mean))
    logit_sd = sd/(mean*(1.0 - mean))

    return logit_mean, logit_sd


def flatten_list(my_list: List) -> List:
    """Flatten list so that it will be a list of non-list object.

    Args:
        my_list (List): List need to be flattened.

    Returns:
        List: Flattened list.
    """
    if not isinstance(my_list, list):
        raise ValueError("Input must be a list.")

    result = []
    for element in my_list:
        if isinstance(element, list):
            result.extend(flatten_list(element))
        else:
            result.append(element)

    return result


def process_dorms(dorms: Union[str, None] = None,
                  size: Union[int, None] = None,
                  default_dorm: str = 'Unknown',
                  dorm_separator: Union[str, None] = None) -> List[List[str]]:
    """Process the dorms.

    Args:
        dorms (Union[List[str], None], optional): Input definition or methods. Default to None.
        size (Union[int, None], optional):
            Size of the dorm array, only used and required when dorms is None. Default to None.
        default_dorm (str, optional):
            Default dorm used when dorms is None.
        dorm_separator (Union[str, None], optional):
            Dorm separator for when multiple definition or methods present.

    Returns:
        List[List[str]]:
            List of list of definition or methods. The second layer of list is for convenience
            when there are multiple definition or methods.
    """
    if dorms is None:
        if size is None:
            raise ValueError("Size cannot be None, when dorms is None.")
        return [[default_dorm]]*size
    else:
        return [dorm.split(dorm_separator) for dorm in dorms]


def p_value(mean: np.ndarray, std: np.ndarray, one_tailed: bool = False) -> np.ndarray:
    """Compute the p value from mean and std.

    Args:
        mean (Union[np.ndarray, float]): Mean of the samples.
        std (Union[np.ndarray, float]): Standard deviation of the samples.
        one_tailed (bool): If `True` use the one tailed test.

    Returns:
        np.ndarray: An array of p-values.
    """
    assert all(std > 0.0), "Standard deviation has to be greater than zero."
    if hasattr(mean, '__iter__') and hasattr(std, '__iter__'):
        assert len(mean) == len(std), "Mean and standard deviation must have same size."

    prob = norm.cdf(np.array(mean)/np.array(std))
    pval = np.minimum(prob, 1 - prob)
    if not one_tailed:
        pval *= 2
    return pval
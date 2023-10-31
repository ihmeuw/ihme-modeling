import numpy as np


def transform_log_data(log_data):
    """Transform from log space to normal space

    Args:
        log_data (numpy.ndarray):
        A two dimensional array that store draws in the log space, each row
        store 1000 draws for a single characterization (location/year/
        age_group).

    Returns:
        numpy.ndarray:
        A two dimensional array that has the same shape with `log_data`, and
        store draws after transforming to the normal space.
    """
    # check input
    assert isinstance(log_data, np.ndarray)
    is_vector = True if log_data.ndim == 1 else False
    if is_vector:
        log_data = log_data.copy()
        log_data = log_data[np.newaxis, :]
    assert log_data.ndim == 2
    assert not np.isnan(log_data).any()
    assert not np.isinf(log_data).any()

    # calculate standard deviation of the log data
    sigma = np.std(log_data, axis=1)[:, np.newaxis]

    # transform data
    offsite = sigma**2/2.0
    transformed_data = log_data - offsite
    transformed_data = np.exp(transformed_data)

    if is_vector:
        transformed_data = np.squeeze(transformed_data)

    return transformed_data


def transform_logit_data(logit_data):
    """Transform from logit space to normal space

    Args:
        logit_data (numpy.ndarray):
        A two dimensional array that store draws in the logit space, each row
        store 1000 draws for a single characterization (location/year/
        age_group).

    Returns:
        numpy.ndarray:
        A two dimensional array that has the same shape with `logit_data`, and
        store draws after transforming to the normal space.
    """
    # check input
    assert isinstance(logit_data, np.ndarray)
    is_vector = True if logit_data.ndim == 1 else False
    if is_vector:
        logit_data = logit_data.copy()
        logit_data = logit_data[np.newaxis, :]
    assert logit_data.ndim == 2
    assert not np.isnan(logit_data).any()
    assert not np.isinf(logit_data).any()

    # calculate standard mean and deviation of the log data
    mu = np.mean(logit_data, axis=1)[:, np.newaxis]
    sigma = np.std(logit_data, axis=1)[:, np.newaxis]

    # transform data
    offsite = -(np.sqrt(1.0 + 2.0*np.pi*sigma**2/16.0) - 1.0)*mu
    transformed_data = logit_data - offsite
    transformed_data = 1.0/(1.0 + np.exp(-transformed_data))

    if is_vector:
        transformed_data = np.squeeze(transformed_data)

    return transformed_data

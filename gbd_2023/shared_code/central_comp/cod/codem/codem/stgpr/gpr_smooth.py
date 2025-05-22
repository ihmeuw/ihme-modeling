import concurrent.futures
import logging
import re
import sys

import numpy as np
import pandas as pd
from tqdm import tqdm

from codem.stgpr.gpr_utils import gpr

pd.set_option("chained", None)

logger = logging.getLogger(__name__)


def mad(a, axis=None):
    """
    (array, int) -> array (or int if axis is not given)

    Compute *Median Absolute Deviation* of an array along given axis.
    """
    med = np.median(a, axis=axis)
    if axis is None:
        umed = med
    else:
        umed = np.expand_dims(med, axis)
    mad = np.median(np.absolute(a - umed), axis=axis)
    return mad


def find_variance_type(df, ko):
    """
    (data frame, data frame) -> Series

    Given a input data frame and knockout pattern will return a pandas Series
    with the data types of each valid data point.
    """
    logger.info("Finding variance type.")
    df = pd.concat([df, ko], axis=1)
    locations = df.location_id.unique()
    ages = df.age_group_id.unique()
    index = df.index.values
    df["old_index"] = index
    df = df.set_index(["location_id", "age_group_id"])
    df = df.sort_index()
    df["variance_type"] = "intermediate"
    for location in locations:
        for age in ages:
            df2 = df.loc[location, age]
            representative_count = sum(
                (df2.source_type.str.contains("VR"))
                & (df2.is_representative == 1)
                & df2.filter(regex="%*_train").iloc[:, 0].values
            )
            all_data = df2[df2.filter(regex="%*_train").values.flatten()].source_type.count()
            non_representative_count = sum(
                df2[df2.filter(regex="%*_train").values.flatten()].is_representative == 0
            )
            if all_data == representative_count >= 10:
                df2.loc[:, "variance_type"] = "rich"
            elif non_representative_count > 0 and (non_representative_count != all_data):
                df2.loc[df2.is_representative == 0, "variance_type"] = "sparse"
            df.loc[location, age]["variance_type"] = df2.variance_type.values
    df = df.sort_values("old_index")
    df = df.set_index("old_index")
    variance_types = df.variance_type.unique()
    logger.info(f"Found {variance_types} for variance types")
    return df["variance_type"]


def variance_map(inputs):
    """
    (list) -> list of arrays

    Helper function that allows for a parallelized version of find variance
    function across all knockout patterns.
    """
    df, ko = inputs
    return find_variance_type(df, ko)


def calculate_nsv(df, ko, simple_ln, simple_lt, residuals, variance, amplitude_scalar):
    """
    (data frame, data frame, array, array, array, array, series, list of str,
    float) -> array

    Run GPR for a single knockout pattern, then find the data variance and
    do a bit of data organization.
    """
    logger.info("Calculating non-sampling variance.")
    df2 = pd.concat([df, ko, variance], axis=1)
    train_var = list(filter(lambda x: re.search(r"ko[0-9]+_train", x), ko.columns))[0]
    df2["ln_rate_nsv"] = 0.0
    df2["lt_cf_nsv"] = 0.0
    res_df = pd.DataFrame(residuals)
    res_df = res_df.set_index(df2[(df2[train_var])].index)
    simple_df = pd.DataFrame({"ln_rate": simple_ln, "lt_cf": simple_lt})
    simple_df = simple_df.set_index(df.index)
    ages = df2.age_group_id.unique()
    inputs = [
        (df2[(df2.age_group_id == age)], res_df, simple_df, amplitude_scalar) for age in ages
    ]
    try:
        with concurrent.futures.ProcessPoolExecutor(30) as executor:
            df_list, amplitudes = list(
                map(list, zip(*list(executor.map(age_group_nsv_map, inputs))))
            )
    except concurrent.futures.process.BrokenProcessPool:
        logger.error(
            "Process pool died abruptly. Returning exit code 137 for jobmon resource retry"
        )
        sys.exit(137)
    amplitudes = {ages[i]: amplitudes[i] for i in range(len(ages))}
    for d in df_list:
        df2.loc[d.index, ["ln_rate_nsv", "lt_cf_nsv"]] = d.values
    return df2[["ln_rate_nsv", "lt_cf_nsv"]], amplitudes


def age_group_nsv(age_df, res_df, simple_df, amplitude_scalar):
    train_var = list(filter(lambda x: re.search(r"ko[0-9]+_train", x), age_df.columns))[0]
    rich = age_df[(age_df.variance_type == "rich") & (age_df[train_var])].index
    not_rich = age_df[(age_df.variance_type != "rich") & (age_df[train_var])].index
    intermediate = age_df[
        (age_df.variance_type == "intermediate") & (age_df[train_var])
    ].index
    sparse = age_df[(age_df.variance_type == "sparse") & (age_df[train_var])].index
    amplitude = {
        "rich": mad(res_df.loc[rich].to_numpy(), axis=0) * 1.4826 * amplitude_scalar,
        "intermediate": mad(res_df.loc[not_rich].to_numpy(), axis=0)
        * 1.4826
        * amplitude_scalar,
    }
    amplitude["sparse"] = amplitude["intermediate"]
    nsv = {
        "rich": (mad(simple_df.loc[rich].to_numpy(), axis=0) * 1.4826) ** 2,
        "intermediate": (mad(simple_df.loc[intermediate].to_numpy(), axis=0) * 1.4826) ** 2,
        "sparse": (mad(simple_df.loc[sparse].to_numpy(), axis=0) * 1.4826) ** 2,
    }
    nsv = {k: {simple_df.columns[i]: nsv[k][i] for i in range(2)} for k in list(nsv.keys())}
    for k in list(nsv.keys()):
        age_df["lt_cf_nsv"][age_df.variance_type == k] = nsv[k]["lt_cf"]
        age_df["ln_rate_nsv"][age_df.variance_type == k] = nsv[k]["ln_rate"]
    return age_df[["ln_rate_nsv", "lt_cf_nsv"]], amplitude


def age_group_nsv_map(inputs):
    age_df, res_df, simple_df, amplitude_scalar = inputs
    return age_group_nsv(age_df, res_df, simple_df, amplitude_scalar)


def nsv_map(inputs):
    df, ko, simple_ln, simple_lt, residuals, variance, amplitude_scalar = inputs
    return calculate_nsv(df, ko, simple_ln, simple_lt, residuals, variance, amplitude_scalar)


def new_gpr(
    df,
    ko,
    amplitude,
    preds,
    variance,
    response_list,
    scale,
    parallel,
    cores,
    model_version_id=None,
):
    logger.info("Running GPR.")
    df2 = pd.concat([df, ko, variance], axis=1)
    gp_smooth = np.zeros(preds.shape).astype("float32")
    ca_df = df2.loc[
        (ko.iloc[:, 1]) | (ko.iloc[:, 2]), ["location_id", "age_group_id"]
    ].drop_duplicates()
    inputs = [
        {
            "df": df2.loc[
                (df2.age_group_id == ca_df.age_group_id[i])
                & (df2.location_id == ca_df.location_id[i])
            ],
            "age_group_id": ca_df.age_group_id[i],
            "location_id": ca_df.location_id[i],
        }
        for i in ca_df.index.values
    ]
    inputs = [
        (
            inputs[i]["df"],
            amplitude[inputs[i]["age_group_id"]],
            preds[inputs[i]["df"].index.values, :],
            response_list,
            scale,
            model_version_id,
        )
        for i in range(len(inputs))
    ]
    if parallel:
        logger.info("Parallel processing for running GPR.")
        try:
            with concurrent.futures.ProcessPoolExecutor(cores) as executor:
                new_preds = list(
                    tqdm(executor.map(age_group_gpr_map, inputs), total=len(inputs))
                )
        except concurrent.futures.process.BrokenProcessPool:
            logger.error(
                "Process pool died abruptly. Returning exit code 137 for jobmon resource retry"
            )
            sys.exit(137)
    else:
        new_preds = list(map(age_group_gpr_map, inputs))
    for i in range(len(new_preds)):
        gp_smooth[inputs[i][0].index.values, :] = new_preds[i]
    return gp_smooth


def age_group_gpr_map(inputs):
    ca_df, age_amplitude, preds, response_list, scale, model_version_id = inputs
    return age_group_gpr(
        ca_df, age_amplitude, preds, response_list, scale, random_seed=model_version_id
    )


def age_group_gpr(ca_df, age_amplitude, preds, response_list, scale, random_seed):
    new_pred = np.zeros(preds.shape).astype("float32")
    var_type = ca_df.variance_type.values[0]
    for i in range(len(response_list)):
        log = f"Running age group GPR for model {i} of {len(response_list)}"
        logger.debug(log)
        new_pred[:, i] = gpr(
            df=ca_df,
            response=response_list[i],
            amplitude=age_amplitude[var_type][i],
            prior=preds[:, i],
            scale=scale,
            random_seed=random_seed,
        )
    return new_pred


def check_amplitude_array_validity(arr):
    """
    Checks an array of amplitude values for validity of values.

    :param arr: array
        array of values equal to model number
    :return: bool
        True if valid amplitudes, otherwise False
    """
    return all(np.isfinite(arr)) & all(arr != 0)


def median_amplitude(amp):
    """
    (dict) -> dict

    Takes a dictionary of dictionaries which contain the amplitudes for a given
    data type and age group and replaces any NaN age groups with the median of
    all other age groups.
    """
    amplitudes = amp.copy()
    for data_type in ["intermediate", "rich", "sparse"]:
        needs_replace = []
        valid_data = np.array([])
        for age in list(amplitudes.keys()):
            if np.any(np.isnan(amplitudes[age][data_type])) or (
                np.any(amplitudes[age][data_type] == 0)
            ):
                needs_replace.append(age)
            else:
                valid_data = np.append(valid_data, amplitudes[age][data_type], 0)
        if len(valid_data) == 0:
            continue
        valid_data = np.reshape(valid_data, (len(amplitudes[age][data_type]), -1))
        valid_data = np.percentile(valid_data, 50, axis=1)
        for age in needs_replace:
            amplitudes[age][data_type] = valid_data
    if not all(
        [
            check_amplitude_array_validity(amplitudes[a]["intermediate"])
            for a in list(amplitudes.keys())
        ]
    ):
        for a in list(amplitudes.keys()):
            amplitudes[a]["intermediate"] = amplitudes[a]["rich"]
    return amplitudes

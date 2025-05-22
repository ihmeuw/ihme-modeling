###########################################################
### Date: 4/5/2018
### Project: ST-GPR
### Purpose: Database utility functions *temporary*
###########################################################
import os

import numpy as np
import pandas as pd

import db_tools_core
import stgpr_helpers
import stgpr_schema

from stgpr.legacy.st_gpr import spacetime_beta as st_beta
from stgpr.legacy.st_gpr import spacetime_prod as st_prod
from stgpr.lib import constants

####################################################################################################################################################
#                                                              Table of Contents
####################################################################################################################################################

# Functions

# Generally useful

# antijoin
# intersection
# logit
# invlogit
# logit_var
# invlogit_var

# Cluster tools

# job_hold

# Spacetime functions

# run_spacetime
# st_launch

# Amplitude and NSV

# mad
# nsv_formula


####################################################################################################################################################
#                                                                    Functions
####################################################################################################################################################

################### Generally useful stuff #########################################################################################################


def antijoin(a, b):
    return list(set(a).symmetric_difference(set(b)))


def intersection(a, b):
    return list(set(a) & set(b))


def equal_sets(a, b):
    return set(a) == set(b)


def logit(x):
    return np.log(x / (1 - x))


def invlogit(x):
    return np.exp(x) / (np.exp(x) + 1)


def logit_var(mu, var):
    return var * (1 / (mu * (1 - mu))) ** 2


def invlogit_var(mu, var):
    return var / (1 / (mu * (1 - mu))) ** 2


def drop_spaces(item):
    return item.replace(" ", "")


def drop_ko_values(dataframe, holdout_num, datavar):
    """If not running for the first holdout (holdout 0),
    drop values that are held out according to the 'ko_{}'
    column that should be present in prepped data."""

    df = dataframe.copy()

    df.loc[df["ko_{}".format(holdout_num)] == 0, datavar] = np.nan

    return df


def separate_string_to_list(item, typ, typ2="one_type_only", delimiter=","):
    """Take input list in comma-delimited string format and split into list of items of type 'type'

    Example input:

        item = '1, 2, 3'
        type = int

    Output would be:

        [1,2,3]
    """

    # drop any spaces from input item
    if item in ["None", ""]:

        assert (typ is None) or (typ2 is None), 'Value cannot be "None"!'
        out = []
    else:

        item = drop_spaces(item)

        # split and output
        if isinstance(item, str):
            out = item.split(delimiter)
            if typ == int:
                out = [typ(float(i)) for i in out]
            else:
                out = [typ(i) for i in out]
        else:
            msg = (
                "This function only takes input items in format <string> to "
                "separate into list of items of type t!"
            )
            raise ValueError(msg)

    return out


def square_skeleton(components, names):
    """Cartesian product of four variables to make a square.

    Inputs:
    -components (list): list of LISTS to turn into dataframes and merge
    -names (list): names of output columns, in same order as componets

    Outputs: pandas dataframe with all combinations of each list entered in components"""

    key = "tmpkey"
    sqr = pd.DataFrame({key: 1}, index=[0])

    for i in range(0, len(components)):
        components[i] = pd.DataFrame({names[i]: components[i], key: 1})
        sqr = sqr.merge(components[i], on=key, how="outer")

    sqr.drop(columns=key, inplace=True)

    return sqr


def get_parallelization_location_group(
    prediction_location_set_version,
    standard_location_set_version,
    nparallel,
    location_group,
    national_level=constants.location.NATIONAL_LEVEL,
):
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        locs = stgpr_helpers.get_stgpr_locations(
            prediction_location_set_version, standard_location_set_version, scoped_session
        )
    locs = locs.sort_values(by=["level_{}".format(national_level)])
    parallel_groups = np.array_split(
        locs.loc[locs.level >= national_level]["location_id"].values, nparallel
    )
    prediction_location_ids = parallel_groups[location_group].tolist()

    return prediction_location_ids


def find_parallelization_group_for_loc(
    loc,
    prediction_location_set_version,
    standard_location_set_version,
    nparallel=50,
    national_level=constants.location.NATIONAL_LEVEL,
):
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        locs = stgpr_helpers.get_stgpr_locations(
            prediction_location_set_version, standard_location_set_version, scoped_session
        )
    locs = locs.sort_values(by=["level_{}".format(national_level)])
    parallel_groups = np.array_split(
        locs.loc[locs.level >= national_level]["location_id"].values, nparallel
    )

    for i in range(0, len(parallel_groups)):
        if loc in list(parallel_groups[i]):
            location_group = print(i)
        else:
            "No location IDs found."

    return location_group


################### Cluster tools ######################################################################################################################


def get_sys_output(command, typ=int):
    return typ(os.popen(command).read().replace("\n", ""))


def model_path(run_id, obj, holdout=0, param_set=None):
    """Assign output paths for major ourputs in st-gpr

    Inputs:
        - run_id (int): ST-GPR run_id
        - obj (str) : item to save, like 'data' or 'gpr'
        - holdout (int): which ko, default 0"""

    data = ["data", "prepped"]
    square = [
        "location_hierarchy",
        "aggregation_location_hierarchy",
        "me_record",
        "populations",
        "square",
    ]
    parameters = ["parameters", "density_cutoffs"]
    temp = ["stage1"]
    output = ["st", "amp_nsv", "adj_data", "gpr", "raked"]

    settings = stgpr_schema.get_settings()
    output_root = settings.output_root_format.format(stgpr_version_id=run_id)
    for f in ["data", "square", "parameters"]:
        if obj in eval(f):
            path = f"{output_root}/{f}.h5"
    for f in ["temp"]:
        if obj in eval(f):
            path = f"{output_root}/{f}_{holdout}.h5"
    for f in ["output"]:
        if obj in eval(f):
            msg = "Parameter set missing when finding model_path for {} for run_id {}".format(
                obj, run_id
            )
            assert param_set is not None, msg
            path = f"{output_root}/{f}_{holdout}_{param_set}.h5"

    return path


def model_load(run_id, obj, holdout=0, param_set=0):
    path = model_path(run_id, obj, holdout, param_set)

    if (obj == "parameters") & (param_set is not None):
        obj = "{}_{}".format(obj, param_set)

    return pd.read_hdf(path, obj)


def model_save(df, run_id, obj, holdout=0, mode="a", attribute=None, param_set=None):
    path = model_path(run_id, obj, holdout, param_set)

    if (obj == "parameters") & (param_set is not None):
        obj = "{}_{}".format(obj, param_set)

    store = pd.HDFStore(path, mode)
    store.put(obj, df, format="fixed", data_columns=True)
    if attribute is not None:
        store.get_storer(obj).attrs.metadata = attribute
    store.close()

    print("Saved {} to {}".format(obj, path))


################### Spacetime #########################################################################################################################


def assign_density_cats(df, country_year_count_var, cutoffs):

    # create temp df so no unnecessary cols saved to df
    tmp = df.copy()

    # make dataframe of min and max country-years that fit into a density cat
    cutoffs.sort()
    min_cy = cutoffs
    max_cy = [i - 1 for i in cutoffs[1 : len(cutoffs)]]
    max_cy.append(999)
    cy = pd.DataFrame(
        {"density_cat": list(range(0, len(cutoffs))), "min_cy": min_cy, "max_cy": max_cy}
    )

    tmp["density_cat"] = np.nan
    for i in list(range(0, cy.shape[0])):
        cat = cy.iloc[i]["density_cat"]
        tmp.loc[
            tmp.location_id_count.isin(list(range(cy.min_cy[i], cy.max_cy[i] + 1))),
            "density_cat",
        ] = cat

    return tmp


def run_spacetime_beta(location_id, df, run_id, age_dict):

    ################################
    ## Setup
    ################################

    # get lambda and zeta specific to each country by data density
    st_lambda = float(df["lambda"][df.location_id == location_id].drop_duplicates())
    zeta = float(df["zeta"][df.location_id == location_id].drop_duplicates())
    omega = float(df["omega"][df.location_id == location_id].drop_duplicates())
    df = df.drop(["lambda", "zeta", "omega"], axis=1)

    ## Detect level and parent
    national_id = float(df.level_3[df.location_id == location_id].values[0])
    level = int(df.level[df.location_id == location_id].values[0])

    # get locations
    locations = model_load(run_id, "location_hierarchy")

    # Detect year_start and year_end
    years = df["year_id"].unique()

    # Detect age_start and age_end
    age_start = int(np.min(df["age_group_id"]))
    age_end = int(np.max(df["age_group_id"]))

    # Making sure that only borrowing strength from higher levels
    columns_to_keep = list(df.columns.values)
    df = df[
        (
            (df.level <= float(constants.location.NATIONAL_LEVEL))
            | (df.level_3 == float(national_id)) & (df.level <= float(level))
        )
    ]
    df = df[columns_to_keep]

    # Count the number of data (maximum number of data in an age group for that sex)
    # data_count = df.loc[df.location_id == location_id].groupby('age_group_id').agg('count')
    # data_count = np.max(data_count.data)

    # If data count is less than threshold, pass a flag to ST
    # if data_count >= data_threshold:
    #    zeta_threshold = 1
    # else:
    #    zeta_threshold = 0

    ################################
    ## Set weights
    ################################

    # Initialize the smoother
    s = st_beta.Smoother(
        df,
        locations,
        timevar="year_id",
        agevar="age_group_id",
        spacevar="location_id",
        datavar="data",
        modelvar="stage1",
        pred_age_group_ids=range(age_start, age_end + 1),
        pred_year_ids=years,
    )

    # Set parameters (can additionally specify omega (age weight, positive real number) and zeta (space weight, between 0 and 1))
    s.lambdaa = st_lambda
    s.zeta = zeta

    # s.zeta_no_data = zeta_no_data
    if len(pd.unique(df["age_group_id"])) > 1:
        s.omega = omega

    # print('getting age weights')
    # Tell the smoother to calculate both time weights and age weights
    s.exp_time_weights()
    if len(pd.unique(df["age_group_id"])) > 1:
        s.age_map = age_dict
        s.age_weights()

    ################################
    ## Run Smoother
    ################################

    s.smooth(locs=location_id, level=level)
    results = pd.merge(
        df, s.long_result(), on=["age_group_id", "year_id", "location_id"], how="right"
    )

    ################################
    ## Clean
    ################################
    cols = [
        "location_id",
        "year_id",
        "age_group_id",
        "age_group_id_orig",
        "sex_id",
        "data",
        "variance",
        "stage1",
        "st",
        "scale",
    ]
    results = results[cols].drop_duplicates()

    # print('ST stage complete for location_id {loc}, sex_id {sx}'.format(loc = location_id,
    # 	sx = results.sex_id.unique().iat[0]))

    return results


def run_spacetime_prod(location_id, df, run_id):

    ################################
    ## Setup
    ################################

    # get lambda and zeta specific to each country by data density
    st_lambda = float(df["lambda"][df.location_id == location_id].drop_duplicates())
    zeta = float(df["zeta"][df.location_id == location_id].drop_duplicates())
    omega = float(df["omega"][df.location_id == location_id].drop_duplicates())
    df = df.drop(["lambda", "zeta", "omega"], axis=1)

    ## Detect level and parent
    national_id = float(df.level_3[df.location_id == location_id].values[0])
    level = int(df.level[df.location_id == location_id].values[0])

    # get locations
    locations = model_load(run_id, "location_hierarchy")

    # Detect year_start and year_end
    years = df["year_id"].unique()

    # Detect age_start and age_end
    age_start = int(np.min(df["age_group_id"]))
    age_end = int(np.max(df["age_group_id"]))

    # Making sure that only borrowing strength from higher levels
    columns_to_keep = list(df.columns.values)
    df = df[
        (
            (df.level <= float(constants.location.NATIONAL_LEVEL))
            | (df.level_3 == float(national_id)) & (df.level <= float(level))
        )
    ]
    df = df[columns_to_keep]

    # Count the number of data (maximum number of data in an age group for that sex)
    # data_count = df.loc[df.location_id == location_id].groupby('age_group_id').agg('count')
    # data_count = np.max(data_count.data)

    # If data count is less than threshold, pass a flag to ST
    # if data_count >= data_threshold:
    #    zeta_threshold = 1
    # else:
    #    zeta_threshold = 0

    ################################
    ## Set weights
    ################################

    # Initialize the smoother
    s = st_prod.Smoother(
        df,
        locations,
        timevar="year_id",
        agevar="age_group_id",
        spacevar="location_id",
        datavar="data",
        modelvar="stage1",
        pred_age_group_ids=range(age_start, age_end + 1),
        pred_year_ids=years,
    )

    # Set parameters (can additionally specify omega (age weight, positive real number) and zeta (space weight, between 0 and 1))
    s.lambdaa = st_lambda
    s.zeta = zeta

    # s.zeta_no_data = zeta_no_data
    if len(pd.unique(df["age_group_id"])) > 1:
        s.omega = omega

    # Tell the smoother to calculate both time weights and age weights
    s.time_weights()
    if len(pd.unique(df["age_group_id"])) > 1:
        s.age_weights()

    ################################
    ## Run Smoother
    ################################

    s.smooth(locs=location_id, level=level)
    results = pd.merge(
        df, s.long_result(), on=["age_group_id", "year_id", "location_id"], how="right"
    )

    ################################
    ## Clean
    ################################
    cols = [
        "location_id",
        "year_id",
        "age_group_id",
        "{}_orig".format("age_group_id"),
        "sex_id",
        "data",
        "variance",
        "stage1",
        "st",
        "scale",
    ]
    results = results[cols].drop_duplicates()

    # print('ST stage complete for location_id {loc}, sex_id {sx}'.format(loc = location_id,
    # 	sx = results["sex_id"].unique().iat[0]))

    return results


class st_launch:
    def __init__(self, df, run_id, age_dict, version):
        self.df = df
        self.run_id = run_id
        self.age_dict = age_dict
        self.version = version

    def __call__(self, ls):
        df = self.df
        location_id = ls[0]
        df = df.loc[df.sex_id == ls[1]]
        if self.version == "prod":
            return run_spacetime_prod(location_id, df, self.run_id)
        elif self.version == "beta":
            return run_spacetime_beta(location_id, df, self.run_id, self.age_dict)


################### Amplitude and NSV stuff #############################################################################################################


def mad(x):
    """Calculate median absolute deviation"""
    return np.median(np.abs(x - np.median(x)))


def nsv_formula(df, residual, variance):
    """Calculate non-sampling variance - intuitively, it's looking in some way at points whose CIS are
    implausibly far away from each other, and adding that back in to account for whatever unobserved
    bias or data differences are making for unlikely data combination. In math, I have no idea what is
    happening. Will break if there is only one observation, so nsv dataframes with only one observation
    is just set to Nan.

    Inputs:
        - Vector of residuals between spacetime and data observations to be included for that location/sex
        - Vector of variances corresponding to the residuals
    Outputs: Non-sampling variance to be added back into data variance for that location/sex"""

    if len(df[residual]) == 1:
        nsv = np.nan
    else:
        N = len(df[residual].dropna(how="all"))
        inv_var = 1 / df[variance]
        sum_wi = np.sum(inv_var.dropna(how="all"))
        sum_wi_xi = np.sum((df[residual] * inv_var).dropna(how="all"))
        weighted_mean = sum_wi_xi / sum_wi
        norm_weights = N * (inv_var / sum_wi)
        nsv = (
            1
            / (float(N) - 1)
            * np.sum(((norm_weights * (df[residual] - weighted_mean) ** 2)).dropna(how="all"))
        )

    return nsv

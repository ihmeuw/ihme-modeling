from codem.data.shared import get_ages_in_range
from codem.stgpr.space_time_smoothing import import_json


def inspect_parameters(parameters):
    """
    Inspect the parameters of the model version ID.
    :param parameters:
    :return:
    """
    num_ages = len(
        get_ages_in_range(
            age_start=parameters["age_start"],
            age_end=parameters["age_end"],
            release_id=parameters["release_id"],
        )
    )
    return {
        "holdout_number": int(parameters["holdout_number"]),
        "n_psi_values": int(len(parameters["psi_values"])),
        "n_age_groups": int(num_ages),
        "global_model": bool(parameters["model_version_type_id"] == 1),
    }


def inspect_all_inputs(inputs):
    information = {}
    information.update(inspect_priors(inputs.priors))
    information.update(inspect_data(inputs.data_frame))
    information.update(inspect_demographics(inputs.data_frame))
    return information


def inspect_priors(priors):
    return {
        "n_lvl1": int(len(priors.loc[priors.level == 1])),
        "n_lvl2": int(len(priors.loc[priors.level == 2])),
        "n_lvl3": int(len(priors.loc[priors.level == 3])),
    }


def inspect_demographics(covariate_df):
    return {"n_demographic_rows": int(len(covariate_df))}


def inspect_data(df):
    return {
        "n_data_points": int(len(df.loc[~df.cf.isnull()])),
        "n_time_series_with_data": int(
            len(df.loc[~df.cf.isnull()][["age_group_id", "location_id"]].drop_duplicates())
        ),
    }


def inspect_submodels(paths):
    rate_vars = import_json(paths.COVARIATE_FILES["ln_rate"])
    cf_vars = import_json(paths.COVARIATE_FILES["lt_cf"])
    return {"num_submodels": int(len(rate_vars["ln_rate_vars"] + cf_vars["lt_cf_vars"]))}

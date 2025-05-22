from copy import deepcopy

import fire
import pandas as pd
from pplkit.data.interface import DataInterface
from scipy.special import logit
from spxmod.model import XModel


def main(directory: str, sex_id: int) -> None:
    dataif = DataInterface(directory=directory)
    for dirname in ["config", "data"]:
        dataif.add_dir(dirname, dataif.directory / dirname)
    dataif.add_dir("stage", dataif.directory / "global_model")

    settings = dataif.load_config("settings.yaml")
    config = dataif.load_config("pattern.yaml")
    data = dataif.load_data("data.parquet").query(f"sex_id == {sex_id}")

    age_model, data = build_age_pattern(data, config["age_pattern"])
    time_model = None
    if "time_pattern" in config:
        time_model, data = build_time_pattern(data, config["time_pattern"])

    covid_cov, obs_scale = settings["covid_cov"], settings.get("obs_scale", 4.0)
    data = build_pred(
        data, covid_cov, age_model, time_model, obs_scale=obs_scale
    )

    dataif.dump_stage(age_model, f"models/age/{sex_id}.pkl")
    if time_model is not None:
        dataif.dump_stage(time_model, f"models/time/{sex_id}.pkl")
    dataif.dump_stage(data, f"predictions/{sex_id}.parquet")


def _get_training_data(data: pd.DataFrame) -> pd.DataFrame:
    # data_train = data.query("~test & year_id <= 2019")
    data_train = data.query("~test")
    return data_train


def build_age_pattern(
    data: pd.DataFrame, config: dict
) -> tuple[XModel, pd.DataFrame]:
    data_train = _get_training_data(data)

    var_builders = config["xmodel"]["var_builders"]
    for var_builder in var_builders:
        if "uprior" in var_builder:
            var_builder["uprior"] = {
                k: float(v) for k, v in var_builder["uprior"].items()
            }

    model_config = dict(
        model_type="binomial",
        obs="obs",
        spaces=config["xmodel"]["spaces"],
        var_builders=var_builders,
        weights="weights",
    )
    model = XModel.from_config(model_config)
    model.fit(data_train, data_span=data, **config["xmodel_fit"])
    data["pred_pattern"] = model.predict(data)
    return model, data


def build_time_pattern(
    data: pd.DataFrame, config: dict
) -> tuple[XModel, pd.DataFrame]:
    data["offset"] = logit(data["pred_pattern"])
    data_train = _get_training_data(data)

    model_config = dict(
        model_type="binomial",
        obs="obs",
        spaces=config["xmodel"]["spaces"],
        var_builders=config["xmodel"]["var_builders"],
        weights="weights",
        param_specs=dict(offset="offset"),
    )
    model = XModel.from_config(model_config)
    model.fit(data_train, data_span=data, **config["xmodel_fit"])
    data["pred_pattern"] = model.predict(data)
    data = data.drop(columns=["offset"])
    return model, data


def set_var_builder_name(
    model: XModel, name_map: dict[tuple[str, str], str]
) -> XModel:
    model = deepcopy(model)
    for var_builder in model.var_builders:
        for (name, space_name), new_name in name_map.items():
            condition = var_builder.name == name
            if space_name is not None:
                condition &= var_builder.space.name == space_name
            if condition:
                var_builder.name = new_name
    return model


def build_pred(
    data: pd.DataFrame,
    covid_cov: str,
    age_model: XModel,
    time_model: XModel | None = None,
    obs_scale: float = 4.0,
) -> pd.DataFrame:
    name_map = {
        ("covid_asdr", None): "covid_asdr_0",
        ("covid_age_sex", None): "covid_age_sex_0",
        ("intercept", "region_id*age_mid"): "intercept_0",
    }
    age_model = set_var_builder_name(age_model, name_map)
    if time_model is not None:
        time_model = set_var_builder_name(time_model, name_map)

    data["covid_asdr_0"] = 0.0
    data["covid_age_sex_0"] = 0.0
    data["intercept_0"] = 1.0

    data["pred_pattern"] = age_model.predict(data)
    if time_model is not None:
        data["offset"] = logit(data["pred_pattern"])
        data["pred_pattern"] = time_model.predict(data)

    data["pred_region"] = data["pred_pattern"]
    data["intercept_0"] = 0.0
    data["pred_super_region"] = age_model.predict(data)
    if time_model is not None:
        data["offset"] = logit(data["pred_super_region"])
        data["pred_super_region"] = time_model.predict(data)

    data = data.drop(
        columns=["covid_asdr_0", "covid_age_sex_0", "intercept_0", "offset"]
    )

    for col in ["pred_pattern", "pred_region", "pred_super_region"]:
        data[col] += data[covid_cov] / obs_scale

    return data


if __name__ == "__main__":
    fire.Fire(main)

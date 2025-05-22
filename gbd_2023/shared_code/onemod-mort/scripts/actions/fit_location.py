import fire
import numpy as np
import pandas as pd
from pplkit.data.interface import DataInterface
from scipy.special import logit
from spxmod.model import XModel

from mortest.models.kreg import build_pred_kreg


def main(directory: str, sex_id: int, location_id: int) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("config", dataif.directory / "config")
    dataif.add_dir("prev_stage", dataif.directory / "national_model")
    dataif.add_dir("stage", dataif.directory / "location_model")

    config = (
        dataif.load_config("detailed.csv")
        .set_index("location_id")
        .loc[location_id]
    )

    national_id = (
        dataif.load(dataif.load_config("settings.yaml")["loc_meta"])
        .set_index("location_id")
        .loc[location_id, "national_id"]
    )

    data = dataif.load_prev_stage(
        f"predictions/{sex_id}_{national_id}.parquet"
    ).query(f"location_id == {location_id}")

    model_spxmod, data = build_pred_spxmod(data, config)
    model_kreg, data = build_pred_kreg(data, config)

    obs_scale = dataif.load_config("settings.yaml").get("obs_scale", 4.0)
    data = prepare_pred(data, obs_scale)

    dataif.dump_stage(data, f"predictions/{sex_id}_{location_id}.parquet")
    if model_spxmod is not None:
        dataif.dump_stage(
            model_spxmod, f"models/spxmod/{sex_id}_{location_id}.pkl"
        )
    dataif.dump_stage(model_kreg, f"models/kreg/{sex_id}_{location_id}.pkl")


def build_pred_spxmod(
    data: pd.DataFrame, config: dict
) -> tuple[XModel, pd.DataFrame]:
    if data.eval("national_id == location_id").all():
        data["pred_spxmod"] = data["pred_national"]
        return None, data

    data["offset"] = logit(data["pred_national"])
    data_train = data.query("~test")

    var_builders = []
    for col in ["intercept", "trend"]:
        if np.isnan(config[f"lam_{col}"]):
            continue
        var_builder = dict(name=col)
        if not np.isinf(config[f"lam_{col}"]):
            var_builder.update(dict(space="age_mid", lam=config[f"lam_{col}"]))
        if not np.isinf(config[f"gprior_sd_{col}"]):
            var_builder.update(
                dict(gprior=dict(mean=0, sd=config[f"gprior_sd_{col}"]))
            )
        var_builders.append(var_builder)

    model_config = dict(
        model_type="binomial",
        obs="obs",
        spaces=[
            dict(
                name="age_mid",
                dims=[dict(name="age_mid", dim_type="numerical")],
            )
        ],
        var_builders=var_builders,
        weights="weights",
        param_specs=dict(offset="offset"),
    )
    model = XModel.from_config(model_config)

    xmodel_fit = dict(
        options=dict(
            verbose=True,
            gtol=0.000001,
            maxiter=100,
            precon_builder="lbfgs",
            cg_maxiter_init=1000,
            cg_maxiter_incr=200,
            cg_maxiter=5000,
        )
    )
    model.fit(data_train, data_span=data, **xmodel_fit)

    data["pred_spxmod"] = model.predict(data)

    data = data.drop(columns=["offset"])

    return model, data


def prepare_pred(data: pd.DataFrame, obs_scale: float) -> pd.DataFrame:
    for col in [
        "pred_super_region",
        "pred_region",
        "pred_national",
        "pred_spxmod",
        "pred_kreg",
    ]:
        data[col] *= obs_scale
    return data


if __name__ == "__main__":
    fire.Fire(main)

import fire
import numpy as np
import pandas as pd
from pplkit.data.interface import DataInterface
from scipy.special import logit
from spxmod.model import XModel


def main(directory: str, sex_id: int, national_id: int) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("config", dataif.directory / "config")
    dataif.add_dir("prev_stage", dataif.directory / "global_model")
    dataif.add_dir("stage", dataif.directory / "national_model")

    config = (
        dataif.load_config("detailed.csv")
        .set_index("location_id")
        .loc[national_id]
    )

    data = dataif.load_prev_stage(f"predictions/{sex_id}.parquet").query(
        f"national_id == {national_id}"
    )

    model, data = build_pred_national(data, config)

    dataif.dump_stage(data, f"predictions/{sex_id}_{national_id}.parquet")
    dataif.dump_stage(model, f"models/{sex_id}_{national_id}.pkl")


def build_pred_national(
    data: pd.DataFrame, config: dict
) -> tuple[XModel, pd.DataFrame]:
    data["offset"] = logit(data["pred_pattern"])
    data_train = data.query("~test and national_id == location_id")

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

    data["pred_national"] = model.predict(data)

    data = data.drop(columns=["offset"])

    return model, data


if __name__ == "__main__":
    fire.Fire(main)

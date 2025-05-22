import fire
import numpy as np
import pandas as pd
from msca.c2fun import c2fun_dict
from onemod.modeling.residual import ResidualCalculator
from pplkit.data.interface import DataInterface
from scipy.special import expit

_inv_link_funs = {
    "binomial": c2fun_dict["expit"],
    "poisson": c2fun_dict["exp"],
    "gaussian": c2fun_dict["identity"],
}


def get_calibration_alpha(
    data: pd.DataFrame, by: list[str], alpha_col: str, inflate: bool
) -> pd.DataFrame:
    data[alpha_col] = 1.0
    data_group = data.groupby(by)

    for _, data_sub in data_group:
        data_sub_train = data_sub.query("~test")
        cali_pred_kreg_raw_sd = calibrate_pred_sd(
            data_sub_train,
            "binomial",
            "pred_kreg_logit",
            "pred_kreg_raw_sd",
            "obs",
            "weights",
            inflate=inflate,
        )
        data.loc[data_sub.index, alpha_col] = (
            cali_pred_kreg_raw_sd / data_sub_train["pred_kreg_raw_sd"]
        ).mean()

    return data


def calibrate_pred_sd(
    data: pd.DataFrame,
    model_type: str,
    pred: str,
    pred_sd: str,
    obs: str,
    weights: str,
    inflate: bool = True,
) -> float:
    data = data[[pred, pred_sd, obs, weights]].copy()

    get_residual = ResidualCalculator(model_type)
    data[f"pred_{obs}"] = _inv_link_funs[model_type](data[pred])

    residual = get_residual(data, f"pred_{obs}", obs, weights)

    alpha = (
        residual["residual"]
        / np.sqrt(residual["residual_se"] ** 2 + data[pred_sd] ** 2)
    ).std()

    if inflate:
        alpha = max(alpha, 1.0)

    return alpha * data[pred_sd]


def main(directory: str) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("stage", dataif.directory / "location_model")
    data = dataif.load_stage("predictions.parquet")

    data["pred_kreg_logit"] = data.eval("pred_kreg_raw + offset")

    # calibrate by region
    # TODO: hard coded by region
    data = get_calibration_alpha(
        data, ["sex_id", "region_id"], "alpha_region", True
    )
    data = get_calibration_alpha(
        data, ["sex_id", "location_id"], "alpha_location", False
    )
    data["alpha"] = data["alpha_location"].where(
        data["high_coverage"], data["alpha_region"]
    )

    obs_scale = dataif.load_directory("config/settings.yaml").get(
        "obs_scale", 4.0
    )
    data["cali_pred_kreg_raw_sd"] = data.eval("alpha * pred_kreg_raw_sd")
    data["pred_kreg_lwr"] = obs_scale * expit(
        data.eval("pred_kreg_logit - 1.96 * cali_pred_kreg_raw_sd")
    )
    data["pred_kreg_upr"] = obs_scale * expit(
        data.eval("pred_kreg_logit + 1.96 * cali_pred_kreg_raw_sd")
    )

    dataif.dump_stage(data, "predictions.parquet")


if __name__ == "__main__":
    fire.Fire(main)

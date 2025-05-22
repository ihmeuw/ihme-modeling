"""Create a batch of draws. Parallel draw creation so that it is faster and
less memory intensive.
"""

import warnings

import fire
import numpy as np
import pandas as pd
from kreg.model import KernelRegModel
from pplkit.data.interface import DataInterface
from scipy.special import expit, logit
from scipy.stats import multivariate_normal

from mortest.models.kreg import get_vcov

IDS = ["location_id", "age_group_id", "sex_id", "year_id"]
OBS = "obs"
WEIGHTS = "weights"
OFFSET = "offset"
OTHERS = [
    OBS,
    WEIGHTS,
    OFFSET,
    "region_id",
    "super_region_id",
    "age_mid",
    "transformed_age",
    "transformed_year",
    "alpha",
    "pred_kreg_raw",
    "pred_kreg",
    "cali_pred_kreg_raw_sd",
]

warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)


def create_draws(
    data: pd.DataFrame,
    model: KernelRegModel,
    sample_size: int,
    obs_scale: float,
    anchor: float,
    lam_uncertainty: float,
) -> pd.DataFrame:
    data = data.sort_values(model.kernel.names, ignore_index=True)
    y_opt = np.asarray(data["pred_kreg_raw"])
    alpha = data["alpha"].to_numpy()

    mean = logit(data["pred_kreg"] / obs_scale)
    vcov = get_vcov(model, data, y_opt, anchor, lam_uncertainty, alpha=alpha)

    sampler = multivariate_normal(mean=mean, cov=vcov)
    draws = pd.DataFrame(
        data=obs_scale * expit(sampler.rvs(size=sample_size).T),
        columns=[f"draw_{i}" for i in range(sample_size)],
    )
    draws = pd.concat([data[IDS], draws], axis=1)
    draws["mean"] = data["pred_kreg"]
    draws["lower"] = obs_scale * expit(
        mean - 1.96 * data["cali_pred_kreg_raw_sd"]
    )
    draws["upper"] = obs_scale * expit(
        mean + 1.96 * data["cali_pred_kreg_raw_sd"]
    )
    return draws


def main(
    directory: str, sex_id: int, location_id: int, sample_size: int = 1000
) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("config", dataif.directory / "config")
    dataif.add_dir("location_model", dataif.directory / "location_model")

    name = f"{sex_id}_{location_id}"
    data = (
        dataif.load_location_model("predictions.parquet", columns=IDS + OTHERS)
        .query(f"sex_id == {sex_id} and location_id == {location_id}")
        .reset_index(drop=True)
    )
    config = (
        dataif.load_config("detailed.csv")
        .set_index("location_id")
        .loc[location_id]
    )

    obs_scale = dataif.load_config("settings.yaml").get("obs_scale", 4.0)
    model = dataif.load_location_model(f"models/kreg/{name}.pkl")
    draws = create_draws(
        data,
        model,
        sample_size,
        obs_scale,
        config["anchor"],
        config["lam_uncertainty"],
    )
    dataif.dump_location_model(draws, f"draws/{name}.parquet")


if __name__ == "__main__":
    fire.Fire(main)

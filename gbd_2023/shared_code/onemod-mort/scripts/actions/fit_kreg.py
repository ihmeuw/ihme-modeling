import fire
import pandas as pd
from pplkit.data.interface import DataInterface

from mortest.models.kreg import build_pred_kreg

IDS = [
    "sex_id",
    "super_region_id",
    "region_id",
    "national_id",
    "location_id",
    "age_group_id",
    "year_id",
]


def main(directory: str, sex_id: int, location_id: int) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("data", dataif.directory / "data")
    dataif.add_dir("config", dataif.directory / "config")
    dataif.add_dir("stage", dataif.directory / "location_model")

    config = (
        dataif.load_config("detailed.csv")
        .set_index("location_id")
        .loc[location_id]
    )

    data = (
        dataif.load_data("data.parquet")
        .query(f"sex_id == {sex_id} & location_id == {location_id}")
        .reset_index(drop=True)
    )
    if config["use_spxmod_1950"]:
        settings = dataif.load_directory("1950_component/config/settings.yaml")
        pred = dataif.load(settings["handoff_predictions"])
    else:
        pred = dataif.load_directory("predictions.parquet")

    pred = (
        pred.query(f"sex_id == {sex_id} & location_id == {location_id}")
        .reset_index(drop=True)
        .pipe(convert_names)
    )
    data = data.merge(pred, on=IDS, how="left")

    obs_scale = dataif.load_config("settings.yaml").get("obs_scale", 4.0)

    data["pred_spxmod"] /= obs_scale
    model_kreg, data = build_pred_kreg(data, config)
    for col in ["pred_spxmod", "pred_kreg"]:
        data[col] *= obs_scale
    dataif.dump_stage(data, f"predictions/{sex_id}_{location_id}.parquet")
    dataif.dump_stage(model_kreg, f"models/kreg/{sex_id}_{location_id}.pkl")


def convert_names(data: pd.DataFrame) -> pd.DataFrame:
    data.rename(
        columns={
            "spxmod": "pred_spxmod",
            "spxmod_region": "pred_region",
            "spxmod_super_region": "pred_super_region",
        },
        inplace=True,
    )
    return data


if __name__ == "__main__":
    fire.Fire(main)

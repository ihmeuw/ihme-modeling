import fire
from pplkit.data.interface import DataInterface

from mortest.life_expectancy import compute_life_expectancy

COLUMNS = {"point_estimate": "kreg", "lwr": "kreg_upr", "upr": "kreg_lwr"}


def main(directory: str) -> None:
    dataif = DataInterface(directory=directory)
    settings = dataif.load_directory("config/settings.yaml")
    dataif.add_dir("pred_dir", settings["handoff_predictions"])

    predictions = dataif.load_pred_dir("predictions.parquet")
    age_meta = dataif.load(settings["age_meta"])
    for col in COLUMNS.keys():
        col = f"ex_{col}"
        if col in predictions:
            predictions.drop(columns=[col], inplace=True)

    predictions = compute_life_expectancy(
        predictions, age_meta, list(COLUMNS.values())
    )
    predictions.rename(
        columns={f"ex_{value}": f"ex_{key}" for key, value in COLUMNS.items()},
        inplace=True,
    )
    dataif.dump_pred_dir(predictions, "predictions.parquet")


if __name__ == "__main__":
    fire.Fire(main)

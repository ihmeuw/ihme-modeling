import fire
from pplkit.data.interface import DataInterface

COLUMNS = [
    "super_region_id",
    "region_id",
    "national_id",
    "location_id",
    "sex_id",
    "age_group_id",
    "year_id",
    "spxmod_super_region",
    "spxmod_region",
    "spxmod",
    "kreg",
    "kreg_lwr",
    "kreg_upr",
]


def main(directory: str) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir(
        "result", dataif.directory / "location_model/predictions.parquet"
    )
    settings = dataif.load_directory("config/settings.yaml")
    dataif.add_dir("target", settings["handoff_predictions"])

    result = dataif.load_result()

    result.rename(
        columns=dict(
            pred_super_region="spxmod_super_region",
            pred_region="spxmod_region",
            pred_spxmod="spxmod",
            pred_kreg="kreg",
            pred_kreg_lwr="kreg_lwr",
            pred_kreg_upr="kreg_upr",
        ),
        inplace=True,
    )
    result = result[COLUMNS].copy()
    dataif.dump_target(result, "predictions.parquet")


if __name__ == "__main__":
    fire.Fire(main)

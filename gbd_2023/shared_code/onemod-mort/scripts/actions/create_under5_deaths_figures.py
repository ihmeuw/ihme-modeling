import fire
import matplotlib.pyplot as plt
import seaborn as sns
from pplkit.data.interface import DataInterface

from mortest.figures import add_title, get_title

IDS = ["sex_id", "location_id", "age_group_id", "year_id"]


def main(directory: str, sex_id: int, location_id: int):
    dataif = DataInterface(directory=directory)
    settings = dataif.load_directory("config/settings.yaml")

    cond = " & ".join(
        [f"sex_id == {sex_id}", f"location_id == {location_id}", "age_mid <= 5"]
    )
    data = (
        dataif.load_directory(
            "location_model/predictions.parquet",
            columns=IDS + ["pred_kreg", "age_mid"],
        )
        .query(cond)
        .merge(
            dataif.load(
                "/path/to/workdir/data/population.parquet",
                columns=IDS + ["population"],
            ),
            on=IDS,
            how="left",
        )
    )
    data["deaths"] = data.eval("pred_kreg * population")
    data = (
        data.drop(columns=["pred_kreg", "population"])
        .groupby(["sex_id", "location_id", "age_group_id"])
        .mean()
        .reset_index()
        .merge(
            dataif.load(
                settings["age_meta"], columns=["age_group_id", "age_group_name"]
            ),
            on="age_group_id",
            how="left",
        )
        .sort_values(["age_mid"], ignore_index=True)
    )

    fig, axes = plt.subplots(1, 2, figsize=(7 * 2, 5))
    sns.barplot(data=data, x="deaths", y="age_group_name", ax=axes[0])
    axes[1].pie(
        data["deaths"], labels=data["age_group_name"], autopct="%1.1f%%"
    )

    loc_meta = dataif.load(
        settings["loc_meta"],
        columns=["location_id", "location_name", "ihme_loc_id"],
    )
    title = get_title(sex_id, location_id, loc_meta)
    fig = add_title(fig, title)
    fig.tight_layout()

    dirname = "under5_deaths"
    dirpath = dataif.directory / "figures" / dirname
    dirpath.mkdir(exist_ok=True, parents=True)
    fig.savefig(dirpath / f"{sex_id}_{location_id}.pdf", bbox_inches="tight")


if __name__ == "__main__":
    fire.Fire(main)

import fire
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import colormaps
from pplkit.data.interface import DataInterface
from scipy.special import expit

from mortest.models.spxmod import get_coef


def get_intercepts(
    global_model_age, global_model_time, loc_meta: pd.Series, obs_scale: float
) -> pd.DataFrame:
    # prepare dataframe
    df_coef_global_age = get_coef(global_model_age)
    df_coef_global_time = get_coef(global_model_time)

    global_intercept = df_coef_global_age.query(
        "cov == 'intercept' & super_region_id == -1 & region_id == -1"
    ).set_index("age_mid")[["coef"]].rename(
        columns={"coef": "1"}
    ) + df_coef_global_time.query(
        "cov == 'intercept' & super_region_id == -1 & region_id == -1"
    ).set_index("age_mid")[["coef"]].rename(columns={"coef": "1"})

    super_region_intercept_map = {}
    for super_region_id in loc_meta["super_region_id"].unique():
        name = f"{int(super_region_id)}"
        super_region_intercept = (
            df_coef_global_age.query(
                f"cov == 'intercept' & super_region_id == {super_region_id} & region_id == -1"
            )
            .set_index("age_mid")[["coef"]]
            .rename(columns={"coef": name})
            + df_coef_global_time.query(
                f"cov == 'intercept' & super_region_id == {super_region_id} & region_id == -1"
            )
            .set_index("age_mid")[["coef"]]
            .rename(columns={"coef": name})
            + global_intercept.rename(columns={"1": name})
        )
        super_region_intercept_map[super_region_id] = super_region_intercept

    region_intercept_map = {}
    for region_id in loc_meta["region_id"].unique():
        name = f"{int(region_id)}"
        super_region_id = loc_meta.loc[
            loc_meta["region_id"] == region_id, "super_region_id"
        ].values[0]
        region_intercept = (
            df_coef_global_age.query(
                f"cov == 'intercept' & region_id == {region_id}"
            )
            .set_index("age_mid")[["coef"]]
            .rename(columns={"coef": name})
            + df_coef_global_time.query(
                f"cov == 'intercept' & region_id == {region_id}"
            )
            .set_index("age_mid")[["coef"]]
            .rename(columns={"coef": name})
            + super_region_intercept_map[super_region_id].rename(
                columns={f"{int(super_region_id)}": name}
            )
        )
        region_intercept_map[region_id] = region_intercept

    intercepts = pd.concat(
        [global_intercept]
        + list(super_region_intercept_map.values())
        + list(region_intercept_map.values()),
        axis=1,
    )
    intercepts = obs_scale * expit(intercepts)

    return intercepts


def main(directory: str) -> None:
    dataif = DataInterface(directory=directory)
    settings = dataif.load_directory("config/settings.yaml")
    loc_meta = dataif.load(settings["loc_meta"])
    obs_scale = settings.get("obs_scale", 4.0)

    dataif.add_dir("models", dataif.directory / "global_model/models")
    dataif.add_dir("figure_dir", settings["handoff_figures"])
    dataif.figure_dir.mkdir(exist_ok=True, parents=True)

    sex_ids = [
        sex_id
        for sex_id in [1, 2]
        if (
            (dataif.models / f"age/{sex_id}.pkl").exists()
            and (dataif.models / f"time/{sex_id}.pkl").exists()
        )
    ]

    for sex_id in sex_ids:
        sex_name = "Male" if sex_id == 1 else "Female"
        global_model_age = dataif.load_models(f"age/{sex_id}.pkl")
        global_model_time = dataif.load_models(f"time/{sex_id}.pkl")

        intercepts = get_intercepts(
            global_model_age, global_model_time, loc_meta, obs_scale
        )
        tab10 = colormaps["tab10"]

        n = loc_meta["super_region_id"].nunique() + 1

        fig, axes = plt.subplots(n, 1, figsize=(8, 4 * n))

        ax = axes[0]

        ax.plot(intercepts.index, intercepts["1"], c="k", label="Global")
        for i, row in (
            loc_meta[["super_region_id", "super_region_name"]]
            .drop_duplicates(ignore_index=True)
            .iterrows()
        ):
            ax.plot(
                intercepts.index,
                intercepts[str(int(row["super_region_id"]))],
                c=tab10(i),
                label=row["super_region_name"],
                linewidth=1,
            )
        ax.set_yscale("log")
        ax.legend(loc=(1, 0))
        ax.set_title(sex_name + ", " + "Super Regions", loc="left")

        for i, row in (
            loc_meta[["super_region_id", "super_region_name"]]
            .drop_duplicates(ignore_index=True)
            .iterrows()
        ):
            ax = axes[i + 1]
            ax.plot(
                intercepts.index,
                intercepts[str(int(row["super_region_id"]))],
                c="k",
                label=row["super_region_name"],
            )
            for j, row_sub in (
                loc_meta.query(f"super_region_id == {row['super_region_id']}")[
                    ["region_id", "region_name"]
                ]
                .drop_duplicates(ignore_index=True)
                .iterrows()
            ):
                ax.plot(
                    intercepts.index,
                    intercepts[str(int(row_sub["region_id"]))],
                    c=tab10(j),
                    label=row_sub["region_name"],
                    linewidth=1,
                )
            ax.set_yscale("log")
            ax.set_title(sex_name + ", " + row["super_region_name"], loc="left")
            ax.legend(loc=(1, 0))

        fig.savefig(
            dataif.figure_dir / f"super_region_effects_{sex_id}.pdf",
            bbox_inches="tight",
        )


if __name__ == "__main__":
    fire.Fire(main)

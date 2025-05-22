from pathlib import Path

import fire
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import colormaps
from pplkit.data.interface import DataInterface

from mortest.figures import add_title, get_title, plot_component
from mortest.models.spxmod import get_coef


def get_covs(pattern_config: dict) -> list[str]:
    names = [
        b["name"]
        for b in pattern_config["age_pattern"]["xmodel"]["var_builders"]
    ]
    if "time_pattern" in pattern_config:
        names += [
            b["name"]
            for b in pattern_config["time_pattern"]["xmodel"]["var_builders"]
        ]
    names = [
        name
        for name in set(names)
        if not ("intercept" in name or "trend" in name)
    ]
    return names


def get_all_coefs(
    global_model_age,
    global_model_time,
    local_model,
    covs: list[str],
    loc_meta: pd.Series,
) -> pd.DataFrame:
    # prepare dataframe
    df_coef_global_age = get_coef(global_model_age)
    df_coef_global_time = get_coef(global_model_time)
    df_coef_local = get_coef(local_model)

    global_intercept = df_coef_global_age.query(
        "cov == 'intercept' & super_region_id == -1 & region_id == -1"
    ).set_index("age_mid")[["coef"]].rename(
        columns={"coef": "global_intercept"}
    ) + df_coef_global_time.query(
        "cov == 'intercept' & super_region_id == -1 & region_id == -1"
    ).set_index("age_mid")[["coef"]].rename(
        columns={"coef": "global_intercept"}
    )

    super_region_intercept = df_coef_global_age.query(
        f"cov == 'intercept' & super_region_id == {loc_meta['super_region_id']} & region_id == -1"
    ).set_index("age_mid")[["coef"]].rename(
        columns={"coef": "super_region_intercept"}
    ) + df_coef_global_time.query(
        f"cov == 'intercept' & super_region_id == {loc_meta['super_region_id']} & region_id == -1"
    ).set_index("age_mid")[["coef"]].rename(
        columns={"coef": "super_region_intercept"}
    )

    region_intercept = df_coef_global_age.query(
        f"cov == 'intercept' & region_id == {loc_meta['region_id']}"
    ).set_index("age_mid")[["coef"]].rename(
        columns={"coef": "region_intercept"}
    ) + df_coef_global_time.query(
        f"cov == 'intercept' & region_id == {loc_meta['region_id']}"
    ).set_index("age_mid")[["coef"]].rename(
        columns={"coef": "region_intercept"}
    )

    all_covs = []
    age_mids = df_coef_global_age.query("age_mid.notna()")["age_mid"].unique()
    for cov in covs:
        df_cov = df_coef_global_age.query(f"cov == '{cov}'")[
            ["age_mid", "coef"]
        ]
        if df_cov["age_mid"].isna().any():
            df_cov = pd.DataFrame(
                dict(age_mid=age_mids, coef=df_cov["coef"].item())
            )
        df_cov = df_cov.rename(columns={"coef": cov}).set_index("age_mid")
        all_covs.append(df_cov)
    all_covs = pd.concat(all_covs, axis=1)

    global_trend = (
        df_coef_global_time.query("cov == 'trend'")
        .set_index("age_mid")[["coef"]]
        .rename(columns={"coef": "global_trend"})
    )

    super_region_id = int(loc_meta["super_region_id"])
    covs = df_coef_global_time["cov"].unique()
    super_region_trend = f"super_region_id_{super_region_id}_trend"

    if super_region_trend in covs:
        super_region_trend = (
            df_coef_global_time.query(f"cov == '{super_region_trend}'")
            .set_index("age_mid")[["coef"]]
            .rename(columns={"coef": "super_region_trend"})
        )
    else:
        super_region_trend = global_trend.copy()
        super_region_trend["super_region_trend"] = 0.0
        super_region_trend.drop(columns="global_trend", inplace=True)

    region_id = int(loc_meta["region_id"])
    region_trend = f"region_id_{region_id}_trend"
    if region_trend in covs:
        region_trend = (
            df_coef_global_time.query(f"cov == '{region_trend}'")
            .set_index("age_mid")[["coef"]]
            .rename(columns={"coef": "region_trend"})
        )
    else:
        region_trend = global_trend.copy()
        region_trend["region_trend"] = 0.0
        region_trend.drop(columns="global_trend", inplace=True)

    location_intercept = df_coef_local.set_index("age_mid")[["coef"]].rename(
        columns={"coef": "location_intercept"}
    )

    all_coefs = pd.concat(
        [
            global_intercept,
            super_region_intercept,
            region_intercept,
            location_intercept,
            global_trend,
            super_region_trend,
            region_trend,
            all_covs,
        ],
        axis=1,
    )
    all_coefs = pd.melt(
        all_coefs.reset_index(),
        id_vars="age_mid",
        var_name="cov",
        value_name="coef",
    )
    return all_coefs


def plot_all_coefs(
    all_coefs: pd.DataFrame,
    sex_id: int,
    location_id: int,
    loc_meta: pd.Series,
    plot_options: dict,
) -> plt.Figure:
    facet_options = {"col": "cov", "wrap": 3}
    fig_options = {"figsize": (14, 8)}
    share_options = {"y": False}

    versions = list(plot_options.keys())[::-1]

    fig = plot_component(
        data=all_coefs,
        x="age_mid",
        y_line=versions,
        line_options=plot_options,
        facet_options=facet_options,
        fig_options=fig_options,
        share_options=share_options,
    )

    for ax in fig.get_axes():
        ax.axhline(0.0, color="grey", linestyle="--")

    title = get_title(sex_id, location_id, loc_meta)
    fig = add_title(fig, title)
    handles, labels = fig.get_axes()[-1].get_legend_handles_labels()
    legend_options = {
        "loc": "lower center",
        "bbox_to_anchor": (0.5, -0.05),
        "ncol": len(handles),
    }
    fig.legend(handles, labels, **legend_options)

    return fig


def load_all_coefs(
    directory: str, sex_id: int, location_id: int, loc_meta: pd.Series
) -> pd.DataFrame:
    dataif = DataInterface(directory=directory)
    pattern_config = dataif.load_directory("config/pattern.yaml")
    covs = get_covs(pattern_config)

    global_model_age = dataif.load_directory(
        f"global_model/models/age/{sex_id}.pkl"
    )
    global_model_time = dataif.load_directory(
        f"global_model/models/time/{sex_id}.pkl"
    )

    if loc_meta["national_id"] == location_id:
        local_model = dataif.load_directory(
            f"national_model/models/{sex_id}_{location_id}.pkl"
        )
    else:
        local_model = dataif.load_directory(
            f"location_model/models/spxmod/{sex_id}_{location_id}.pkl"
        )

    all_coefs = get_all_coefs(
        global_model_age, global_model_time, local_model, covs, loc_meta
    )
    return all_coefs


def main(directory: str, sex_id: int, location_id: int) -> None:
    sex_id, location_id = int(sex_id), int(location_id)
    dataif = DataInterface(directory=directory)
    settings = dataif.load_directory("config/settings.yaml")
    loc_meta = (
        dataif.load(settings["loc_meta"])
        .set_index("location_id")
        .loc[location_id]
    )
    all_coefs = load_all_coefs(directory, sex_id, location_id, loc_meta)
    plot_options = {"coef": {"color": colormaps["tab10"](0), "marker": "o"}}
    for i, (name, cmp_directory) in enumerate(settings["coef_compare"].items()):
        cmp_all_coefs = load_all_coefs(
            cmp_directory, sex_id, location_id, loc_meta
        ).rename(columns={"coef": name})
        all_coefs = all_coefs.merge(
            cmp_all_coefs, on=["age_mid", "cov"], how="outer"
        )
        plot_options[name] = {"color": colormaps["tab10"](i + 1), "marker": "o"}
    for key in plot_options:
        plot_options[key]["marker"] = "o"
        plot_options[key]["alpha"] = 0.5

    fig = plot_all_coefs(
        all_coefs,
        sex_id,
        location_id,
        dataif.load(settings["loc_meta"]),
        plot_options,
    )
    dirname = "coef"
    dirpath = Path(directory) / "figures" / dirname
    dirpath.mkdir(exist_ok=True, parents=True)
    fig.savefig(dirpath / f"{sex_id}_{location_id}.pdf", bbox_inches="tight")


if __name__ == "__main__":
    fire.Fire(main)

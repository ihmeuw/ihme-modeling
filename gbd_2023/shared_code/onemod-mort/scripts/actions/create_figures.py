from itertools import product
from pathlib import Path

import fire
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from pplkit.data.interface import DataInterface

from mortest.figures import (
    COLORS,
    MARKERS,
    add_legend,
    add_title,
    add_ylabel,
    adjust_ylim,
    get_facet_options,
    get_sort_by,
    get_title,
    plot_component,
)

IDS = ["sex_id", "location_id", "age_group_id", "year_id"]


LINES = [
    "spxmod_super_region",
    "spxmod_region",
    "spxmod",
    "kreg",
    "GBD21",
    "WPP",
]


def _load_data(
    path: str,
    sex_id: int,
    location_id: int,
    handoff: int,
    age_meta: pd.DataFrame,
    obs_scale: float,
) -> pd.DataFrame:
    dataif = DataInterface()
    data = (
        dataif.load(path)
        .query(
            f"sex_id == {sex_id} & source_location_id == {location_id} and mx.notna()"
        )
        .reset_index(drop=True)
    )
    if handoff == 1:
        data["onemod_process_type"] = "standard gbd age group data"

    data["excluded"] = data.eval(
        f"obs > {obs_scale} | weights == 0 | outlier == 1"
    )

    data = data.merge(age_meta, on="age_group_id", how="left")
    return data


def _load_pred(
    pred_path: str,
    gbd_path: str,
    wpp_path: str,
    sex_id: int,
    location_id: int,
    age_meta: pd.DataFrame,
) -> pd.DataFrame:
    cond = " & ".join([f"sex_id == {sex_id}", f"location_id == {location_id}"])
    dataif = DataInterface()
    pred = (
        dataif.load(
            pred_path,
            columns=IDS
            + [
                "spxmod_super_region",
                "spxmod_region",
                "spxmod",
                "kreg",
                "kreg_lwr",
                "kreg_upr",
            ],
        )
        .query(cond)
        .reset_index(drop=True)
    )
    gbd = (
        dataif.load(gbd_path, columns=IDS + ["gbd_mx_21", "gbd_mx_23"])
        .query(cond)
        .rename(columns={"gbd_mx_21": "GBD21", "gbd_mx_23": "GBD23"})
        .reset_index(drop=True)
    )
    wpp = (
        dataif.load(wpp_path, columns=IDS + ["mx"])
        .query(cond)
        .rename(columns={"mx": "WPP"})
        .reset_index(drop=True)
    )
    pred = pred.merge(gbd, on=IDS, how="left")
    pred = pred.merge(age_meta, on="age_group_id", how="left")
    pred = pred.merge(wpp, on=IDS, how="left")

    return pred


def _get_pred_handles(lines: list) -> dict[str, list]:
    """Get prediction legend handles."""
    handles = []
    options = {"xdata": [], "ydata": [], "lw": 2}
    for line in lines:
        if line in ["spxmod", "spxmod_super_region", "spxmod_region"]:
            handles.append(
                Line2D(color=COLORS[line], linestyle=":", label=line, **options)
            )
        elif line in ["kreg", "GBD21", "WPP"]:
            handles.append(Line2D(color=COLORS[line], label=line, **options))
        elif line == "uncertainty":
            handles.append(
                Patch(facecolor=COLORS["kreg"], alpha=0.25, label=line)
            )
        elif line == "obs":
            handles.append(
                Line2D(
                    color=COLORS["obs"],
                    marker="o",
                    linestyle="",
                    alpha=0.5,
                    label=line,
                    **options,
                )
            )
        else:
            raise ValueError(f"Invalid line: {line}")
    return {"$\\bf{Predictions}$": handles}


def _get_obs_handles(
    data: pd.DataFrame, handoff: int, excluded: bool
) -> dict[str, list]:
    """Get observation legend handles."""
    options = {"xdata": [], "ydata": [], "linestyle": "", "alpha": 0.5}
    sources = data["source_type_name"].unique()
    if handoff == 1:
        handles = [
            Line2D(color=COLORS[source], marker="o", label=source, **options)
            for source in sources
        ]
        if excluded:
            handles.append(
                Line2D(color="k", marker="x", label="excluded", **options)
            )
        return {"$\\bf{Observations}$": handles}
    if handoff == 2:
        handles = {
            "$\\bf{Sources}$": [
                Patch(color=COLORS[source], alpha=0.5, label=source)
                for source in sources
            ]
        }
        included_processes = [
            Line2D(
                color="k",
                marker=MARKERS["included"][process],
                label=process,
                **options,
            )
            for process in data.query("outlier == 0")[
                "onemod_process_type"
            ].unique()
        ]
        if excluded:
            handles["$\\bf{Processes  (Included)}$"] = included_processes
            handles["$\\bf{Processes  (Excluded)}$"] = [
                Line2D(
                    color="k",
                    marker=MARKERS["excluded"][process],
                    label=process,
                    **options,
                )
                for process in data.query("outlier == 1")[
                    "onemod_process_type"
                ].unique()
            ]
        else:
            handles["$\\bf{Processes}$"] = included_processes
        return handles
    raise ValueError(f"Invalid handoff: {handoff}")


def plot_predictions(
    pred: pd.DataFrame,
    x: str = "year_id",
    uncertainty: bool = True,
    fig: plt.Figure | None = None,
) -> plt.Figure | None:
    """Plot predictions for given sex and location."""
    fig = plot_component(
        data=pred,
        x=x,
        y_line=LINES,
        y_fill=["kreg"] if uncertainty else [],
        line_options={
            "spxmod_super_region": {
                "color": COLORS["spxmod_super_region"],
                "lw": 2,
                "linestyle": ":",
            },
            "spxmod_region": {
                "color": COLORS["spxmod_region"],
                "lw": 2,
                "linestyle": ":",
            },
            "spxmod": {"color": COLORS["spxmod"], "lw": 2, "linestyle": ":"},
            "kreg": {"color": COLORS["kreg"], "lw": 2},
            "GBD21": {"color": COLORS["GBD21"], "lw": 2},
            "WPP": {"color": COLORS["WPP"], "lw": 2},
        },
        fill_options=(
            {"kreg": {"facecolor": COLORS["kreg"], "alpha": 0.25}}
            if uncertainty
            else {}
        ),
        facet_options=get_facet_options(x),
        share_options={"y": False},
        yscale="linear" if x == "year_id" else "log",
        fig=fig,
    )
    return fig


def plot_observations(
    data: pd.DataFrame,
    x: str = "year_id",
    scaled: bool = True,
    excluded: bool = True,
    fig: plt.Figure | None = None,
) -> plt.Figure | None:
    """Plot observations for given sex and location."""

    fig = _plot_included(data.query("~excluded"), x, scaled, fig)
    if excluded and data["excluded"].sum() > 0:
        fig = _plot_excluded(data.query("excluded"), x, fig)
    return fig


def _plot_included(
    data: pd.DataFrame, x: str, scaled: bool, fig: plt.Figure | None
) -> plt.Figure:
    """Plot included observations."""
    for (source, process), df in data.groupby(
        ["source_type_name", "onemod_process_type"]
    ):
        if len(df) > 0:
            marker = MARKERS["included"][process]
            dots_options = {
                "obs": {
                    "color": COLORS[source],
                    "marker": marker,
                    "alpha": 0.5,
                    "edgecolor": "none",
                }
            }
            if scaled:
                dots_options["obs"]["s"] = "point_size"
            fig = plot_component(
                data=df,
                x=x,
                y_dots=["obs"],
                dots_options=dots_options,
                facet_options=get_facet_options(x),
                share_options={"y": False},
                yscale="linear" if x == "year_id" else "log",
                fig=fig,
            )
    return fig


def _plot_excluded(data: pd.DataFrame, x: str, fig: plt.Figure) -> plt.Figure:
    """Plot excluded observations."""
    for (source, process), df in data.groupby(
        ["source_type_name", "onemod_process_type"]
    ):
        if len(df) > 0:
            marker = MARKERS["excluded"][process]
            fig = plot_component(
                data=df,
                x=x,
                y_dots=["obs"],
                dots_options={
                    "obs": {
                        "color": COLORS[source],
                        "marker": marker,
                        "alpha": 0.7,
                    }
                },
                facet_options=get_facet_options(x),
                share_options={"y": False},
                fig=fig,
            )
    return fig


def plot_results(
    data: pd.DataFrame,
    pred: pd.DataFrame,
    x: str = "year_id",
    scaled: bool = True,
    excluded: bool = True,
    uncertainty: bool = False,
    y_ub_scale: float = 2.0,
) -> plt.Figure:
    """Plot predictions for given sex and location."""

    # Plot predictions
    fig = plot_predictions(pred, x=x, uncertainty=uncertainty)

    # Plot observations
    fig = plot_observations(
        data, x=x, scaled=scaled, excluded=excluded, fig=fig
    )

    # adjust ylim upper bound
    fig = adjust_ylim(
        pred, x, LINES, ["kreg"] if uncertainty else [], fig, y_ub_scale
    )

    return fig


def main(
    directory: str,
    sex_id: int,
    location_id: int,
    x: str | list[str] = "year_id",
    scaled: bool | list[bool] = True,
    excluded: bool | list[bool] = True,
    uncertainty: bool | list[bool] = True,
    y_ub_scale: float = 2.0,
) -> None:
    dataif = DataInterface(directory=directory)
    settings = dataif.load_directory("config/settings.yaml")

    handoff = settings["handoff"]
    loc_meta = dataif.load(
        settings["loc_meta"],
        columns=["location_id", "location_name", "ihme_loc_id"],
    )
    age_meta = dataif.load(
        settings["age_meta"],
        columns=["age_group_id", "age_group_name", "age_mid"],
    )
    obs_scale = settings.get("obs_scale", 4.0)
    data = _load_data(
        dataif.directory / "data/raw_data.parquet",
        sex_id,
        location_id,
        handoff,
        age_meta,
        obs_scale,
    )
    pred = _load_pred(
        Path(settings["handoff_predictions"]) / "predictions.parquet",
        settings["gbd_results"],
        settings["wpp_results"],
        sex_id,
        location_id,
        age_meta,
    )

    x = [x] if isinstance(x, str) else list(x)
    scaled = [scaled] if isinstance(scaled, bool) else list(scaled)
    excluded = [excluded] if isinstance(excluded, bool) else list(excluded)
    uncertainty = (
        [uncertainty] if isinstance(uncertainty, bool) else list(uncertainty)
    )

    for x_sub, scaled_sub, excluded_sub, uncertainty_sub in product(
        x, scaled, excluded, uncertainty
    ):
        data.sort_values(get_sort_by(x_sub), ignore_index=True, inplace=True)
        pred.sort_values(get_sort_by(x_sub), ignore_index=True, inplace=True)

        data["point_size"] = (
            50.0
            * data["weights"]
            / data.groupby("age_mid" if x_sub == "year_id" else "year_id")[
                "weights"
            ].transform("median")
        )

        fig = plot_results(
            data,
            pred,
            x_sub,
            scaled_sub,
            excluded_sub,
            uncertainty_sub,
            y_ub_scale,
        )

        # Add title
        title = get_title(sex_id, location_id, loc_meta)
        fig = add_title(fig, title)

        # Add legend
        components = LINES
        if uncertainty:
            components = components + ["uncertainty"]
        handles = _get_pred_handles(components)
        handles.update(**_get_obs_handles(data, handoff, excluded))
        fig = add_legend(fig, handles)

        # add ylabel
        fig = add_ylabel(fig, "mx")

        # Save or return plot
        dirname = "_".join(
            [
                "scaled" if scaled_sub else "unscaled",
                "with_excluded" if excluded_sub else "without_excluded",
                "with_uncertainty"
                if uncertainty_sub
                else "without_uncertainty",
                "yearx" if x_sub == "year_id" else "agex",
            ]
        )
        dirpath = dataif.directory / "figures" / dirname
        dirpath.mkdir(exist_ok=True, parents=True)
        fig.savefig(
            dirpath / f"{sex_id}_{location_id}.pdf", bbox_inches="tight"
        )


if __name__ == "__main__":
    fire.Fire(main)

from itertools import product

import fire
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import colormaps
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


DEFAULT_LINE_OPTIONS = {"lw": 2}
DEFAULT_FILL_OPTIONS = {"alpha": 0.2}


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
    config: list, sex_id: int, location_id: int, age_meta: pd.DataFrame
) -> tuple[pd.DataFrame, dict, dict]:
    cond = " & ".join([f"sex_id == {sex_id}", f"location_id == {location_id}"])
    dataif = DataInterface()

    line_options = {}
    fill_options = {}
    pred = []
    for file_config in config:
        path, line_config = file_config
        cname_map = {}
        for name, item_map in line_config.items():
            col = item_map.pop("col", name)
            include_uncertainty = item_map.pop("include_uncertainty", False)
            cname_map[col] = name
            if include_uncertainty:
                cname_map[f"{col}_lwr"] = f"{name}_lwr"
                cname_map[f"{col}_upr"] = f"{name}_upr"
            colormap_name, color_index = item_map["color"]
            item_map["color"] = colormaps[colormap_name](color_index)
            line_options[name] = DEFAULT_LINE_OPTIONS | item_map
            if include_uncertainty:
                fill_options[name] = DEFAULT_FILL_OPTIONS | {
                    "facecolor": item_map["color"]
                }

        pred.append(
            dataif.load(path, columns=IDS + list(cname_map.keys()))
            .query(cond)
            .rename(columns=cname_map)
            .set_index(IDS)
        )

    pred = pd.concat(pred, axis=1).reset_index()
    pred = pred.merge(age_meta, on="age_group_id", how="left")
    return pred, line_options, fill_options


def _get_pred_handles(line_options: dict) -> dict[str, list]:
    """Get prediction legend handles."""
    handles = [
        Line2D(label=name, xdata=[], ydata=[], **options)
        for name, options in line_options.items()
    ]
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
    line_options: dict,
    fill_options: dict,
    x: str = "year_id",
    fig: plt.Figure | None = None,
) -> plt.Figure | None:
    """Plot predictions for given sex and location."""

    fig = plot_component(
        data=pred,
        x=x,
        y_line=list(line_options.keys()),
        y_fill=list(fill_options.keys()),
        line_options=line_options,
        fill_options=fill_options,
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
    line_options: dict,
    fill_options: dict,
    x: str = "year_id",
    scaled: bool = True,
    excluded: bool = True,
    y_ub_scale: float = 2.0,
) -> plt.Figure:
    """Plot predictions for given sex and location."""

    # Plot predictions
    fig = plot_predictions(pred, line_options, fill_options, x=x)

    # Plot observations
    fig = plot_observations(
        data, x=x, scaled=scaled, excluded=excluded, fig=fig
    )

    fig = adjust_ylim(
        pred, x, list(line_options.keys()), [], fig, ub_scale=y_ub_scale
    )

    return fig


def main(
    directory: str,
    sex_id: int,
    location_id: int,
    x: str | list[str] = "year_id",
    scaled: bool | list[bool] = True,
    excluded: bool | list[bool] = True,
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
        settings["data"], sex_id, location_id, handoff, age_meta, obs_scale
    )
    pred, line_options, fill_options = _load_pred(
        settings["predictions"], sex_id, location_id, age_meta
    )

    x = [x] if isinstance(x, str) else list(x)
    scaled = [scaled] if isinstance(scaled, bool) else list(scaled)
    excluded = [excluded] if isinstance(excluded, bool) else list(excluded)

    for x_sub, scaled_sub, excluded_sub in product(x, scaled, excluded):
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
            line_options,
            fill_options,
            x_sub,
            scaled_sub,
            excluded_sub,
            y_ub_scale,
        )

        # Add title
        title = get_title(sex_id, location_id, loc_meta)
        fig = add_title(fig, title)

        # Add legend
        handles = _get_pred_handles(line_options)
        handles.update(**_get_obs_handles(data, handoff, excluded))
        fig = add_legend(fig, handles)

        # add ylabel
        fig = add_ylabel(fig, "mx")

        # Save or return plot
        dirname = "_".join(
            [
                "scaled" if scaled_sub else "unscaled",
                "with_excluded" if excluded_sub else "without_excluded",
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

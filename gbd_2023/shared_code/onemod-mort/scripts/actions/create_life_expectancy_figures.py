from pathlib import Path

import fire
import matplotlib.pyplot as plt
import pandas as pd
from pplkit.data.interface import DataInterface

from mortest.figures import (
    add_title,
    add_ylabel,
    get_facet_options,
    get_sort_by,
    get_title,
    plot_component,
)

IDS = ["sex_id", "location_id", "age_group_id", "year_id"]


def _load_pred(
    pred_path: str, sex_id: int, location_id: int, age_meta: pd.DataFrame
) -> pd.DataFrame:
    cond = " & ".join([f"sex_id == {sex_id}", f"location_id == {location_id}"])
    dataif = DataInterface(
        gbd="/path/to/workdir/data/gbd_results/without_shock/gbd_data.parquet",
        wpp="/path/to/workdir/data/wpp.parquet",
    )
    pred = (
        dataif.load(
            pred_path, columns=IDS + ["ex_point_estimate", "ex_lwr", "ex_upr"]
        )
        .query(cond)
        .merge(
            dataif.load_gbd(
                columns=IDS + ["ex_gbd_21", "ex_gbd_21_lwr", "ex_gbd_21_upr"]
            ),
            on=IDS,
            how="left",
        )
        .merge(dataif.load_wpp(columns=IDS + ["ex_wpp"]), on=IDS, how="left")
        .reset_index(drop=True)
    )
    pred = pred.merge(age_meta, on="age_group_id", how="left")

    return pred


def plot_predictions(
    pred: pd.DataFrame, x: str = "year_id", fig: plt.Figure | None = None
) -> plt.Figure | None:
    """Plot predictions for given sex and location."""
    fig = plot_component(
        data=pred,
        x=x,
        y_line=["ex_point_estimate", "ex_gbd_21", "ex_wpp"],
        y_fill=["ex", "ex_gbd_21"],
        fill_options={"ex": {"alpha": 0.3}, "ex_gbd_21": {"alpha": 0.3}},
        facet_options=get_facet_options(x),
        share_options={"y": False},
        yscale="linear" if x == "year_id" else "log",
        fig=fig,
    )
    return fig


def plot_results(pred: pd.DataFrame, x: str = "year_id") -> plt.Figure:
    """Plot predictions for given sex and location."""

    # Plot predictions
    fig = plot_predictions(pred, x=x)

    return fig


def main(
    directory: str,
    sex_id: int,
    location_id: int,
    x: str | list[str] = "year_id",
) -> None:
    dataif = DataInterface(directory=directory)
    settings = dataif.load_directory("config/settings.yaml")

    loc_meta = dataif.load(
        settings["loc_meta"],
        columns=["location_id", "location_name", "ihme_loc_id"],
    )
    age_meta = dataif.load(
        settings["age_meta"],
        columns=["age_group_id", "age_group_name", "age_mid"],
    )
    pred = _load_pred(
        Path(settings["handoff_predictions"]) / "predictions.parquet",
        sex_id,
        location_id,
        age_meta,
    )

    x = [x] if isinstance(x, str) else list(x)

    for x_sub in x:
        pred.sort_values(get_sort_by(x_sub), ignore_index=True, inplace=True)

        fig = plot_results(pred, x_sub)

        # Add title
        title = get_title(sex_id, location_id, loc_meta)
        fig = add_title(fig, title)

        # add ylabel
        fig = add_ylabel(fig, "life expectancy")
        handles, labels = fig.get_axes()[-1].get_legend_handles_labels()
        legend_options = {
            "loc": "lower center",
            "bbox_to_anchor": (0.5, -0.05),
            "ncol": len(handles),
        }
        fig.legend(handles, labels, **legend_options)

        # Save or return plot
        dirname = "_".join(
            ["life_expectancy", "yearx" if x_sub == "year_id" else "agex"]
        )
        dirpath = dataif.directory / "figures" / dirname
        dirpath.mkdir(exist_ok=True, parents=True)
        fig.savefig(
            dirpath / f"{sex_id}_{location_id}.pdf", bbox_inches="tight"
        )


if __name__ == "__main__":
    fire.Fire(main)

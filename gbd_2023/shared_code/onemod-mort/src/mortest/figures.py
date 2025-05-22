import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import seaborn.objects as so
from matplotlib import colormaps

COLORS = {
    "spxmod_super_region": colormaps["tab10"](5),  # brown
    "spxmod_region": colormaps["tab10"](2),  # green
    "spxmod": colormaps["tab10"](0),  # blue
    "kreg": colormaps["tab10"](1),  # orange
    "GBD21": colormaps["tab10"](7),  # black
    # "GBD23": colormaps["tab10"](7),  # gray
    "WPP": colormaps["tab10"](4),
    "CBH": colormaps["tab10"](9),  # cyan
    "Census": colormaps["tab10"](5),  # brown (China)
    "DSP": colormaps["tab10"](7),  # gray (China)
    "HHD": colormaps["tab10"](2),
    "SIBS": colormaps["tab10"](8),  # yellow
    "SRS": colormaps["tab10"](1),  # green (Bangladesh, India, Pakistan)
    # "SSPC": colormaps["tab10"](1),  # pink (China)
    "Survey": colormaps["tab10"](0),  # blue (China)
    "SBH": colormaps["tab10"](6),
    "VR": colormaps["tab10"](4),  # purple
    "obs": colormaps["tab10"](7),  # gray
    "excluded": colormaps["tab10"](3),  # red
    "MCCD": "#008080",
    "HDSS": colormaps["tab20b"](14),
}

MARKERS = {
    "included": {
        "age-sex split data": "^",
        "standard gbd age group data": "o",
        "standard+split_data": "s",
    },
    "excluded": {
        "age-sex split data": "+",
        "standard gbd age group data": "x",
        "standard+split_data": "1",
    },
}


def plot_component(
    data: pd.DataFrame,
    x: str,
    y_dots: list[str] = [],
    y_line: list[str] = [],
    y_fill: list[str] = [],
    dots_options: dict = {},
    line_options: dict = {},
    fill_options: dict = {},
    fig_options: dict = {},
    facet_options: dict = {},
    share_options: dict = {},
    yscale: str = "linear",
    fig: plt.Figure | None = None,
) -> plt.Figure:
    """Plot for a single sex and location."""
    n = np.ceil(data[facet_options["col"]].nunique() / 25)
    fig_options = {"figsize": (20, 12 * n), **fig_options}

    # Initialize figure and subplots
    if fig is None:
        fig = plt.Figure(**fig_options)
        so.Plot.config.theme.update(
            {
                **sns.axes_style("whitegrid"),
                **{"axes.spines.right": False, "axes.spines.top": False},
            }
        )
        so.Plot(data, x=x).facet(**facet_options).share(**share_options).on(
            fig
        ).plot()
    axes = fig.get_axes()
    by = [
        facet_options.get(key)
        for key in ["col", "row"]
        if facet_options.get(key) is not None
    ]

    # Query data by subplot
    if by:
        values = pd.DataFrame(
            data=[ax.get_title().split(" | ") for ax in axes], columns=by
        ).astype(dict(zip(by, data[by].dtypes.to_list())))
        data_list = []
        for value in values.itertuples(index=False, name=None):
            selection = " & ".join(
                [f"{k} == {repr(v)}" for k, v in zip(by, value)]
            )
            data_list.append(data.query(selection))
    else:
        data_list = [data]

    # Plot data
    for ax, df in zip(axes, data_list):
        for y in y_fill:
            ax.fill_between(
                df[x],
                df[f"{y}_lwr"],
                df[f"{y}_upr"],
                **{"label": y, **fill_options.get(y, {})},
            )
        for y in y_line:
            ax.plot(df[x], df[y], **{"label": y, **line_options.get(y, {})})
        for y in y_dots:
            y_options = {"label": y, **dots_options.get(y, {})}
            if "s" in y_options and isinstance(y_options["s"], str):
                s = y_options["s"]
                y_options["s"] = df[s]
            ax.scatter(df[x], df[y], **y_options)

    # Rescale y axes
    if yscale != "linear":
        for ax, df in zip(axes, data_list):
            ax.set_yscale(yscale)

    # Format labels, legend, title
    fig.tight_layout()

    return fig


def get_sort_by(x: str) -> list[str]:
    """Get data sorting columns."""
    if x == "year_id":
        return ["age_mid", "year_id"]
    if x == "age_mid":
        return ["year_id", "age_mid"]
    raise ValueError(f"Invalid x: {x}")


def get_facet_options(x: str) -> dict[str, int | str]:
    """Get facet options."""
    if x == "year_id":
        facet = "age_group_name"
    elif x == "age_mid":
        facet = "year_id"
    else:
        raise ValueError(f"Invalid x: {x}")
    return {"col": facet, "wrap": 5}


def adjust_ylim(
    data: pd.DataFrame,
    x: str,
    y_line: list[str],
    y_fill: list[str],
    fig: plt.Figure,
    ub_scale: float = 2.0,
) -> plt.Figure:
    axes = fig.get_axes()
    facet_options = get_facet_options(x)
    by = [
        facet_options.get(key)
        for key in ["col", "row"]
        if facet_options.get(key) is not None
    ]
    if by:
        values = pd.DataFrame(
            data=[ax.get_title().split(" | ") for ax in axes], columns=by
        ).astype(dict(zip(by, data[by].dtypes.to_list())))
        data_list = []
        for value in values.itertuples(index=False, name=None):
            selection = " & ".join(
                [f"{k} == {repr(v)}" for k, v in zip(by, value)]
            )
            data_list.append(data.query(selection))
    else:
        data_list = [data]

    for ax, df in zip(axes, data_list):
        y_lb, y_ub = ax.get_ylim()
        y_max = df[y_line + y_fill].max().max()
        y_lb = max(y_lb, 0 - y_max * 0.1)
        y_ub = min(y_ub, max(y_max * ub_scale, abs(y_lb)))
        ax.set_ylim(y_lb, y_ub)

    return fig


def get_title(sex_id: int, location_id: int, loc_meta: pd.Series) -> str:
    """Get plot title."""
    sex = "Males" if sex_id == 1 else "Females"
    loc_meta = loc_meta.set_index("location_id").loc[location_id]
    return ", ".join(
        [
            loc_meta["ihme_loc_id"],
            loc_meta["location_name"],
            str(location_id),
            sex,
        ]
    )


def add_title(fig: plt.Figure, title: str) -> plt.Figure:
    """Add plot title."""
    height = fig.get_size_inches()[0]
    fig.suptitle(title, y=1 + 0.12 / height, va="bottom", fontsize="xx-large")
    return fig


def add_ylabel(fig: plt.Figure, ylabel: str) -> plt.Figure:
    """Add ylabels to plot."""
    for idx, ax in enumerate(fig.get_axes()):
        if idx % 5 == 0:
            ax.set_ylabel(ylabel)
    return fig


def add_legend(
    fig: plt.Figure,
    handle_dict: dict[str, list] = {},
    legend_options: dict = {},
) -> plt.Figure:
    """Add plot legend.

    FIXME: making assumptions about legend items existing...
    """
    axes = fig.get_axes()
    last_ax = axes if not isinstance(axes, list) else axes[-1]
    n = np.ceil(len(axes) / 25)
    legend_options = {"loc": "upper center", **legend_options}
    if handle_dict:
        base_legend = None
        first_title = True
        for title, handles in handle_dict.items():
            if handles:
                legend = last_ax.legend(
                    fig,
                    handles=handles,
                    labels=[handle.get_label() for handle in handles],
                    title=title if first_title else "\n" + title,
                    ncol=len(handles),
                    bbox_transform=fig.transFigure,
                    bbox_to_anchor=(0.5, -0.01 / n),
                    **legend_options,
                )
                first_title = False
                if base_legend:
                    base_legend_box = base_legend.get_children()[0]
                    this_legend_box = legend.get_children()[0]
                    base_legend_box.get_children().extend(
                        this_legend_box.get_children()
                    )
                else:
                    base_legend = legend
                    fig.legends.append(legend)
        last_ax.get_legend().remove()
    else:
        handles, labels = last_ax.get_legend_handles_labels()
        fig.legend(
            handles,
            labels,
            ncol=min(8, len(handles)),
            bbox_to_anchor=(0.5, -0.01 / n),
            **legend_options,
        )
    return fig

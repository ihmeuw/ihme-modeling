import fire
import pandas as pd
import pymupdf
from pplkit.data.interface import DataInterface

LOC_IDS = ["super_region_id", "region_id", "location_id"]


def build_toc(
    data: pd.DataFrame, loc_meta: pd.DataFrame, levels=LOC_IDS
) -> list:
    toc = []
    for i, level in enumerate(levels):
        level_name = level.replace("_id", "_name")
        new_toc = (
            data.groupby(level)["page_id"]
            .first()
            .reset_index()
            .merge(
                loc_meta[[level, level_name]].drop_duplicates(),
                on=level,
                how="left",
            )
            .assign(level=i + 1)[["level", level_name, "page_id"]]
            .values.tolist()
        )
        toc.extend(new_toc)
    toc.sort(key=lambda x: (x[2], x[0]))
    return toc


def main(directory: str) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("figures", dataif.directory / "figures")
    settings = dataif.load_directory("config/settings.yaml")

    dataif.add_dir("handoff_predictions", settings["handoff_predictions"])
    dataif.add_dir("handoff_figures", settings["handoff_figures"])
    dataif.handoff_figures.mkdir(exist_ok=True, parents=True)

    columns = LOC_IDS + ["sex_id"]
    loc_meta = dataif.load(settings["loc_meta"])
    data = (
        dataif.load_handoff_predictions("predictions.parquet", columns=columns)
        .drop_duplicates()
        .merge(loc_meta[["location_id", "sort_order"]], on="location_id")
        .sort_values(["sort_order", "sex_id"], ignore_index=True)
        .drop(columns="sort_order")
    )
    data["page_id"] = data.index + 1

    filenames = (
        data["sex_id"].astype(str)
        + "_"
        + data["location_id"].astype(str)
        + ".pdf"
    ).to_list()
    toc = build_toc(data, loc_meta)

    for figure_dir in dataif.figures.iterdir():
        if not figure_dir.is_dir():
            continue
        combined_pdf = pymupdf.open()
        for filename in filenames:
            with pymupdf.open(figure_dir / filename) as mfile:
                combined_pdf.insert_pdf(mfile)
        combined_pdf.set_toc(toc)
        combined_pdf.save(dataif.handoff_figures / f"{figure_dir.name}.pdf")


if __name__ == "__main__":
    fire.Fire(main)

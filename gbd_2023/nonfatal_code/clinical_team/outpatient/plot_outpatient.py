"""
plot final outpatient file
"""

import sys

import db_queries
import db_tools
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from crosscutting_functions import demographic
from crosscutting_functions.mapping import clinical_mapping_db
from matplotlib.backends.backend_pdf import PdfPages

from crosscutting_functions.pipeline import get_release_id


def read_final_outpatient(run_id):

    return pd.read_csv(
        "FILEPATH"
    )


def prep_for_plotting(df, run_id):
    used_bundles = clinical_mapping_db.get_active_bundles(
        cols=["bundle_id"], estimate_id=[23, 24, 25], map_version="current"
    )
    used_bundles = used_bundles.bundle_id.unique().tolist()

    df = df.loc[df.bundle_id.isin(used_bundles), :]

    bundle_names = db_tools.ezfuncs.query(
        "QUERY", conn_def="epi"
    )

    df = df.merge(bundle_names, how="left", on="bundle_id")

    assert "bundle_name" in df.columns

    locations = db_queries.get_location_metadata(
        location_set_id=35,
        release_id=get_release_id(run_id=run_id),
    )

    locations = locations.loc[:, ["location_id", "location_name", "parent_id"]]

    df = df.merge(locations, how="left", on="location_id")

    df = df.sort_values("parent_id")

    df = demographic.all_group_id_start_end_switcher(
        df, clinical_age_group_set_id=1, remove_cols=False
    )

    df = df.groupby(
        [
            "bundle_id",
            "bundle_name",
            "location_id",
            "location_name",
            "sex_id",
            "age_start",
            "year_start",
        ],
        as_index=False,
    ).agg({"cases": "sum", "sample_size": "sum"})

    df["rate"] = df.cases / df.sample_size

    return df


def plot_one_bundle(df, bundle_id):

    df = df.loc[df.bundle_id == bundle_id, :]

    bundle_name = df.bundle_name.unique().tolist()[0]

    g = sns.FacetGrid(
        df,
        col="sex_id",
        row="location_name",
        hue="year_start",
        legend_out=True,
        sharey=False,
        sharex=True,
        margin_titles=True,
    )
    g = g.map(plt.plot, "age_start", "rate", alpha=0.3).add_legend()
    g.fig.suptitle(str(bundle_name))
    g.fig.subplots_adjust(top=0.975)


def plot_to_pdf(df, filepath):

    bundle_list = sorted(df.bundle_id.unique())

    with PdfPages(filepath) as pdf:
        for bundle in bundle_list:
            if df.loc[df.bundle_id == bundle, :].shape[0] == 0:
                print(f"Bundle {bundle} is empty")
                continue
            print(f"Plotting bundle {bundle}...")
            plot_one_bundle(df=df, bundle_id=bundle)
            pdf.savefig()
            plt.close()

    print(f"Plot saved at {filepath}")


def main(df, filepath, run_id):
    df = prep_for_plotting(df, run_id)
    plot_to_pdf(df=df, filepath=filepath)


if __name__ == "__main__":
    run_id = sys.argv[1]
    filepath = sys.argv[2]

    df = read_final_outpatient(run_id=run_id)
    main(df=df, filepath=filepath, run_id=run_id)

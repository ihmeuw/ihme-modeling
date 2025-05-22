import os
import shutil
import warnings
from argparse import ArgumentParser
from pathlib import Path

import bopforge.continuous_pipeline.functions as functions
import numpy as np
from bopforge.utils import fill_dict, ParseKwargs
from pplkit.data.interface import DataInterface

warnings.filterwarnings("ignore")


def pre_processing(dataif: DataInterface) -> None:
    name = dataif.result.name
    df = dataif.load_result(f"raw-{name}.csv")
    all_settings = dataif.load_result("settings.yaml")
    settings = all_settings["select_bias_covs"]["cov_finder"]

    # get bias covariates that need to be removed
    all_covs = [col for col in df.columns if col.startswith("cov_")]
    covs_to_remove = [col for col in all_covs if len(df[col].unique()) == 1]

    # remove from dataframe
    df.drop(columns=covs_to_remove, inplace=True)

    # remove from settings
    all_covs = set("em" + col[3:] for col in all_covs)
    covs_to_remove = set("em" + col[3:] for col in covs_to_remove)
    pre_selected_covs = set(settings["pre_selected_covs"])
    pre_selected_covs = pre_selected_covs & all_covs
    pre_selected_covs = pre_selected_covs - covs_to_remove

    settings["pre_selected_covs"] = list(pre_selected_covs)
    all_settings["select_bias_covs"]["cov_finder"] = settings

    # save results
    dataif.dump_result(df, f"{name}.csv")
    dataif.dump_result(all_settings, "settings.yaml")


def fit_signal_model(dataif: DataInterface) -> None:
    """Fit signal model. This step involves, non-linear curve fitting and
    trimming, but does not use a mixed effect model. A single panel plot will be
    created for vetting the fit of the signal and a summary file will be
    generated to store the results of signal model.

    Parameters
    ----------
    dataif
        Data interface in charge of file reading and writing.

    """
    pre_processing(dataif)
    name = dataif.result.name

    df = dataif.load_result(f"{name}.csv")

    all_settings = dataif.load_result("settings.yaml")
    settings = all_settings["fit_signal_model"]

    signal_model = functions.get_signal_model(settings, df)
    signal_model.fit_model(outer_step_size=200, outer_max_iter=100)

    df = functions.convert_bc_to_em(df, signal_model)

    summary = functions.get_signal_model_summary(name, all_settings, df)

    fig = functions.plot_signal_model(
        name,
        summary,
        df,
        signal_model,
        show_ref=all_settings["figure"]["show_ref"],
    )

    dataif.dump_result(df, f"{name}.csv")
    dataif.dump_result(signal_model, "signal_model.pkl")
    dataif.dump_result(summary, "summary.yaml")
    fig.savefig(dataif.result / "signal_model.pdf", bbox_inches="tight")


def select_bias_covs(dataif: DataInterface) -> None:
    """Select the bias covariates. In this step, we first fit a linear model to
    get the prior strength of the bias-covariates. And then we use `CovFinder`
    to select important bias-covariates. A summary of the result will be
    generated and store in file `cov_finder_result.yaml`.

    Parameters
    ----------
    dataif
        Data interface in charge of file reading and writing.

    """
    name = dataif.result.name

    df = dataif.load_result(f"{name}.csv")
    df = df[df.is_outlier == 0].copy()

    all_settings = dataif.load_result("settings.yaml")
    settings = all_settings["select_bias_covs"]

    cov_finder_linear_model = functions.get_cov_finder_linear_model(df)
    cov_finder_linear_model.fit_model()

    cov_finder = functions.get_cov_finder(settings, cov_finder_linear_model)
    cov_finder.select_covs(verbose=True)

    cov_finder_result = functions.get_cov_finder_result(
        cov_finder_linear_model, cov_finder
    )

    # save results
    dataif.dump_result(cov_finder_result, "cov_finder_result.yaml")
    dataif.dump_result(cov_finder_linear_model, "cov_finder_linear_model.pkl")
    dataif.dump_result(cov_finder, "cov_finder.pkl")


def fit_linear_model(dataif: DataInterface) -> None:
    """Fit the final linear mixed effect model for the process. We will fit the
    linear model using the signal and selected bias covariates in this step.
    And we will create draws and quantiles for the risk curve. A two panels
    figure will be plotted to show the fit and all the important result
    information is documented in the `summary.yaml` file.

    Parameters
    ----------
    dataif
        Data interface in charge of file reading and writing.

    """
    name = dataif.result.name

    df = dataif.load_result(f"{name}.csv")
    df_train = df[df.is_outlier == 0].copy()

    cov_finder_result = dataif.load_result("cov_finder_result.yaml")
    all_settings = dataif.load_result("settings.yaml")
    settings = all_settings["complete_summary"]
    summary = dataif.load_result("summary.yaml")
    signal_model = dataif.load_result("signal_model.pkl")

    linear_model = functions.get_linear_model(df_train, cov_finder_result)
    linear_model.fit_model()

    summary = functions.get_linear_model_summary(
        settings,
        summary,
        df,
        signal_model,
        linear_model,
    )

    df_inner_draws, df_outer_draws = functions.get_draws(
        settings, summary, signal_model
    )

    df_inner_quantiles, df_outer_quantiles = functions.get_quantiles(
        settings, summary, signal_model
    )

    fig = functions.plot_linear_model(
        name,
        summary,
        df,
        signal_model,
        linear_model,
        show_ref=all_settings["figure"]["show_ref"],
    )

    dataif.dump_result(linear_model, "linear_model.pkl")
    dataif.dump_result(summary, "summary.yaml")
    dataif.dump_result(df_inner_draws, "inner_draws.csv")
    dataif.dump_result(df_outer_draws, "outer_draws.csv")
    dataif.dump_result(df_inner_quantiles, "inner_quantiles.csv")
    dataif.dump_result(df_outer_quantiles, "outer_quantiles.csv")
    fig.savefig(dataif.result / "linear_model.pdf", bbox_inches="tight")


def run(
    i_dir: str,
    o_dir: str,
    pairs: list[str],
    actions: list[str],
    metadata: dict,
) -> None:
    i_dir, o_dir = Path(i_dir), Path(o_dir)

    # check the input and output folders
    if not i_dir.exists():
        raise FileNotFoundError("input data folder not found")

    o_dir.mkdir(parents=True, exist_ok=True)

    dataif = DataInterface(i_dir=i_dir, o_dir=o_dir)
    settings = dataif.load_i_dir("settings.yaml")

    # check pairs
    all_pairs = [pair for pair in settings.keys() if pair != "default"]
    pairs = pairs or all_pairs
    for pair in pairs:
        data_path = dataif.get_fpath(f"{pair}.csv", key="i_dir")
        if not data_path.exists():
            raise FileNotFoundError(f"Missing data file {data_path}")

    # check actions
    # TODO: might be good to use enum here
    all_actions = ["fit_signal_model", "select_bias_covs", "fit_linear_model"]
    actions = actions or all_actions
    invalid_actions = set(actions) - set(all_actions)
    if len(invalid_actions) != 0:
        raise ValueError(f"{list(invalid_actions)} are invalid actions")

    # fit each pair
    for pair in pairs:
        pair_o_dir = o_dir / pair
        pair_o_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy(i_dir / f"{pair}.csv", pair_o_dir / f"raw-{pair}.csv")

        if pair not in settings:
            pair_settings = settings["default"]
        else:
            pair_settings = fill_dict(settings[pair], settings["default"])
        pair_settings["metadata"] = metadata
        dataif.dump_o_dir(pair_settings, pair, "settings.yaml")

        np.random.seed(pair_settings["seed"])
        pair_dataif = DataInterface(result=pair_o_dir)
        for action in actions:
            globals()[action](pair_dataif)


def main(args=None) -> None:
    parser = ArgumentParser(description="Continuous burden of proof pipeline.")
    parser.add_argument(
        "-i", "--input", type=os.path.abspath, required=True, help="Input data folder"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=os.path.abspath,
        required=True,
        help="Output result folder",
    )
    parser.add_argument(
        "-p",
        "--pairs",
        required=False,
        default=None,
        nargs="+",
        help="Included pairs, default all pairs",
    )
    parser.add_argument(
        "-a",
        "--actions",
        choices=["fit_signal_model", "select_bias_covs", "fit_linear_model"],
        default=None,
        nargs="+",
        help="Included actions, default all actions",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        nargs="*",
        required=False,
        default={},
        action=ParseKwargs,
        help="User defined metadata",
    )
    args = parser.parse_args(args)
    run(args.input, args.output, args.pairs, args.actions, args.metadata)


if __name__ == "__main__":
    main()

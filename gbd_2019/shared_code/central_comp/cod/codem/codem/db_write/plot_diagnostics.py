import matplotlib
matplotlib.use('Agg')
import pylab
import matplotlib.pyplot as plt
import seaborn as sns
import logging

logger = logging.getLogger(__name__)


def create_global_table(global_df, filepath):
    logger.info(f"Creating the global plot and saving to {filepath}")
    df = global_df.copy()
    columns = ["draw_%d" % i for i in range(100)]
    df["mean_deaths"] = df[columns].mean(axis=1)
    df["lower_deaths"] = df[columns].quantile(.025, axis=1)
    df["upper_deaths"] = df[columns].quantile(.975, axis=1)
    plt.figure(figsize=(16, 12))
    plt.plot(df.year_id.values, df.mean_deaths.values, label="Mean Death Prediction")
    plt.fill_between(df.year_id.values, df.lower_deaths.values, df.upper_deaths.values,
                     alpha=.3, color='blue', label='95% confidence interval')
    plt.legend(loc=0)
    plt.xlabel("$Year$", fontsize=20)
    plt.ylabel("$Deaths$", fontsize=20)
    plt.title("CODEm Estimates", fontsize=24)
    pylab.savefig(filepath)


def plot_comparisons(df, name, varname, filepath, sharex=False, sharey=False):
    """
    Plot the kernel densities of covariate distributions in the same seaborn FacetGrid. Outputs the plot
    to the model directory.

    :param df: input data frame with draws
    :param name: name of the plot
    :param varname: name of the variable to put in the kernel density (one of "beta", "standard_beta")
    :param filepath: the file path to save the plot
    :param sharex: fix x-scales
    :param sharey: fix y-scales
    """
    df["model"] = df["model_type"] + " " + df["dependent_variable"]
    g = sns.FacetGrid(df, col="covariate", hue="dependent_variable", margin_titles=True, col_wrap=2, sharex=sharex,
                      sharey=sharey, aspect=3)
    g.map(sns.distplot, varname, hist=False, kde_kws={"shade": True}).add_legend(title="Model Type")
    g.map(plt.axvline, x=0, ls=":", c=".5")
    plt.subplots_adjust(top=0.9)
    g.fig.suptitle(name)
    g.set_titles(col_template='{col_name}')
    pylab.savefig(filepath)


def create_draw_plots(beta_draws, cause_name, date, model_version_id, sex_id, filepath):
    """
    Create both beta draw plots -- one for standard betas and one for betas.

    :return df: data frame of beta draws
    """
    logger.info("Creating beta draws plots.")
    df = beta_draws.copy()
    title = f"{cause_name} \n Date: {date} \n Model Version ID: {model_version_id}, Sex ID: {sex_id}"
    sns.set(style="white", palette="muted", color_codes=True, font_scale=1.5)
    plot_comparisons(
        df=df,
        name=f"Standard Betas: {title}",
        varname="standard_beta",
        filepath=filepath.format("standard_beta"),
        sharex=True,
        sharey=False)
    plot_comparisons(
        df,
        name=f"Betas: {title}",
        varname="beta",
        filepath=filepath.format("beta"),
        sharex=False,
        sharey=False)


def create_covar_ndraws_plot(covariate_summary_df, cause, date, model_version_id, sex_id, filepath):
    """
    Create plot of number of draws per covariate for modeler email.
    """
    logger.info("Creating covariate n draws plot.")
    df = covariate_summary_df.copy()
    plt.figure(figsize=(16, 12))
    plt.bar(df.index, height=df.n_draws.values, align="center")
    plt.xticks(df.index, df.name, fontsize=15, rotation=45, ha='right')
    plt.ylabel("Number of Draws", fontsize=20)
    plt.title(f"{cause} \n Date: {date} \n Model Version ID: {model_version_id}, Sex ID: {sex_id}", fontsize=22)
    sns.set_style("darkgrid")
    plt.tight_layout()
    pylab.savefig(filepath)


def create_covar_submodel_plot(covariate_summary_df, cause, date, model_version_id, sex_id, filepath):
    """
    Create plot fo number of submodels per covariate for modeler email.
    """
    logger.info("Creating covariate n submodels plot.")
    df = covariate_summary_df.copy()
    plt.figure(figsize=(16, 12))
    plt.bar(df.index, height=df.n_submodels.values, align="center")
    plt.xticks(df.index, df.name, fontsize=15, rotation=45, ha='right')
    plt.ylabel("Number of Submodels")
    plt.title(f"{cause} \n Date: {date} \n Model Version ID: {model_version_id}, Sex ID: {sex_id}", fontsize=22)
    sns.set_style("darkgrid")
    plt.tight_layout()
    pylab.savefig(filepath)

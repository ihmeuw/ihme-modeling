"""
Post analysis module
"""
from typing import Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import norm
from crosswalk import CWData, CWModel
from crosswalk.scorelator import Scorelator


class PostAnalysis:
    def __init__(self,
                 model: CWModel,
                 draw_bounds: Tuple[float, float] = (0.05, 0.95),
                 type: str = 'harmful',
                 name: str = 'unknown'):
        self.model = model
        self.draw_bounds = draw_bounds
        self.type = type
        self.name = name

        self.df = self.model.cwdata.df.copy()
        self.se_model = {}
        self.se_model_all = {}
        self.num_fill = 0
        self.model_fill = None
        self.scorelator = None
        self.scorelator_fill = None

    def analyze(self):
        self.process_data()

        # detect publication bias
        self.detect_pub_bias()

        # adjust for publication bias
        if self.has_pub_bias:
            self.adjust_pub_bias()

        # create scorelator
        self.scorelator = Scorelator(
            self.model,
            draw_bounds=self.draw_bounds,
            type=self.type,
            name=self.name
        )

        if self.model_fill is not None:
            self.scorelator_fill = Scorelator(
                self.model_fill,
                draw_bounds=self.draw_bounds,
                type=self.type,
                name=self.name
            )

    def process_data(self):
        # sort id
        data = self.model.cwdata
        sort_id = np.argsort(self.model.cwdata.data_id)
        self.df[data.col_study_id] = self.df[data.col_study_id].astype(str)

        # add outlier column
        self.df["outlier"] = self.model.lt.w[sort_id] <= 0.1

        # add residual column
        obs = data.obs[sort_id]
        pred = self.model.design_mat.dot(self.model.lt.beta)[sort_id]
        self.df["pred"] = pred
        self.df["residual"] = obs - pred

        # add residual_se column
        obs_se = data.obs_se[sort_id]
        gamma = self.model.lt.gamma[0]
        self.df["residual_se"] = np.sqrt(obs_se**2 + gamma)

    def detect_pub_bias(self):
        weighted_residual_all = self.df.residual/self.df.residual_se
        weighted_residual = weighted_residual_all[~self.df.outlier]

        self.se_model["mean"] = np.mean(weighted_residual)
        self.se_model["sd"] = 1/np.sqrt(weighted_residual.size)
        self.se_model["pval"] = get_p_val(self.se_model["mean"], self.se_model["sd"])

        self.se_model_all["mean"] = np.mean(weighted_residual_all)
        self.se_model_all["sd"] = 1/np.sqrt(weighted_residual_all.size)
        self.se_model_all["pval"] = get_p_val(self.se_model_all["mean"], self.se_model_all["sd"])

    @property
    def has_pub_bias(self) -> bool:
        return self.se_model["pval"] < 0.05

    def adjust_pub_bias(self):
        df = self.df[~self.df.outlier].reset_index(drop=True)
        residual = df.residual[~self.df.outlier].to_numpy()
        # compute rank of the absolute residual
        rank = np.zeros(residual.size, dtype=int)
        rank[np.argsort(np.abs(residual))] = np.arange(1, residual.size + 1)
        # get the furthest residual according to the sign of egger regression
        sort_index = np.argsort(residual)
        if self.se_model["mean"] > 0.0:
            sort_index = sort_index[::-1]
        # compute the number of data points need to be filled
        self.num_fill = residual.size - rank[sort_index[-1]]
        # get data to fill
        data = self.model.cwdata
        df_fill = df.iloc[sort_index[:self.num_fill], :]
        df_fill[data.col_study_id] = "fill_" + df_fill[data.col_study_id]
        df_fill["data_id"] = self.df.shape[0] + np.arange(self.num_fill)
        df_fill["residual"] = -df_fill["residual"]
        df_fill[data.col_obs] = df_fill["pred"] + df_fill["residual"]
        # combine with filled dataframe
        self.df = pd.concat([self.df, df_fill])
        data_fill = CWData(
            self.df[~self.df.outlier].copy(),
            obs=data.col_obs,
            obs_se=data.col_obs_se,
            alt_dorms=data.col_alt_dorms,
            ref_dorms=data.col_ref_dorms,
            dorm_separator=data.dorm_separator,
            covs=data.col_covs,
            study_id=data.col_study_id,
            data_id=data.col_data_id,
            add_intercept=True
        )
        self.model_fill = CWModel(
            data_fill,
            obs_type=self.model.obs_type,
            cov_models=self.model.cov_models,
            gold_dorm=self.model.gold_dorm,
            order_prior=self.model.order_prior,
            use_random_intercept=self.model.use_random_intercept,
            prior_gamma_uniform=self.model.prior_gamma_uniform,
            prior_gamma_gaussian=self.model.prior_gamma_guassian
        )
        self.model_fill.fit(inlier_pct=1.0, max_iter=500)

    def summarize_model(self) -> pd.DataFrame:
        summary = {
            "ro_pair": self.name,
            "num_fill": self.num_fill,
            "gamma": self.scorelator.gamma,
            "gamma_sd": self.scorelator.gamma_sd,
            "score": self.scorelator.get_score()
        }
        if self.model_fill is not None:
            summary.update({
                "gamma_adjusted": self.scorelator_fill.gamma,
                "gamma_sd_adjusted": self.scorelator_fill.gamma_sd,
                "score_adjusted": self.scorelator_fill.get_score()
            })
        summary.update({"egger_" + name: value
                        for name, value in self.se_model.items()})
        summary.update({"egger_all_" + name: value
                        for name, value in self.se_model_all.items()})
        return pd.DataFrame(summary, index=[0])

    def plot_residual(self, ax: plt.Axes) -> plt.Axes:
        # compute the residual and observation standard deviation
        residual = self.df["residual"]
        obs_se = self.df["residual_se"]
        max_obs_se = np.quantile(obs_se, 0.99)
        fill_index = self.df[self.model.cwdata.col_study_id].str.contains("fill")

        # create funnel plot
        ax = plt.subplots()[1] if ax is None else ax
        ax.set_ylim(max_obs_se, 0.0)
        ax.scatter(residual, obs_se, color="gray", alpha=0.4)
        if fill_index.sum() > 0:
            ax.scatter(residual[fill_index], obs_se[fill_index], color="#008080", alpha=0.7)
        ax.scatter(residual[self.df.outlier == 1], obs_se[self.df.outlier == 1],
                   color='red', marker='x', alpha=0.4)
        ax.fill_betweenx([0.0, max_obs_se],
                         [0.0, -1.96*max_obs_se],
                         [0.0, 1.96*max_obs_se],
                         color='#B0E0E6', alpha=0.4)
        ax.plot([0, -1.96*max_obs_se], [0.0, max_obs_se], linewidth=1, color='#87CEFA')
        ax.plot([0.0, 1.96*max_obs_se], [0.0, max_obs_se], linewidth=1, color='#87CEFA')
        ax.axvline(0.0, color='k', linewidth=1, linestyle='--')
        ax.set_xlabel("residual")
        ax.set_ylabel("ln_rr_se")
        ax.set_title(
            f"{self.name}: egger_mean={self.se_model['mean']: .3f}, "
            f"egger_sd={self.se_model['sd']: .3f}, "
            f"egger_pval={self.se_model['pval']: .3f}",
            loc="left")
        return ax


def get_p_val(mean: float, sd: float) -> float:
    p = norm.cdf(0.0, loc=mean, scale=sd)
    return min(p, 1 - p)

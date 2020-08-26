import sys
import os

import string
import dill as pickle

import numpy as np
import pandas as pd

from limetr import LimeTr
from xspline import xspline

sys.path.append(os.path.join(os.path.dirname(__file__)))
from globals import DATA_DIR, OUT_DIR

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from mrbrt.__init__ import MR_BRT, MR_BeRT
from mrbrt.utils import ratioInit, sampleKnots


class AirPollutionModel:
    _orig_covs = ['cv_subpopulation', 'cv_exposure_population', 'cv_exposure_selfreport', 'cv_exposure_study',
                 'cv_outcome_selfreport', 'cv_outcome_unblinded', 'cv_reverse_causation', 'cv_confounding_nonrandom',
                 'cv_counfounding.uncontroled', 'cv_selection_bias', 'incidence']

    def __init__(self, outcome, model_type, measure,
                 include_smoking=True, include_shs=True,
                 x_covs=None, z_covs=None):
        assert model_type in ['spline', 'linear'], \
            '`model_type` must either be "spline" or "linear"'

        # data frame
        if outcome.startswith('cvd'):
            df = pd.read_csv(os.path.join(DATA_DIR, 'age_adjust', f'{outcome}.csv'))
        else:
            df = pd.read_csv(os.path.join(DATA_DIR, f'{outcome}.csv'))
        df = df.sort_values('nid').reset_index(drop=True)

        # drop smoking if not including it this run
        if not include_smoking:
            df = df[df.ier_source != 'AS']
        elif not include_shs:
            df = df[df.ier_source != 'SHS']

        # get viable covariates
        bias_covs = []
        for cov in self._orig_covs:
            df.loc[(df.ier_source == 'AS') & (df[cov].isna()), cov] = 0  # fill smoking w/ 0
            if not df[cov].isin([0, 1, np.nan]).all():
                for n, x in enumerate(string.ascii_lowercase[:df[cov].max()]):
                    df[f'{cov}_{x}'] = 1
                    df.loc[df[cov] == n, f'{cov}_{x}'] = 0
                    if df[f'{cov}_{x}'].min() == 0 and df[f'{cov}_{x}'].max() == 1:
                        bias_covs += [f'{cov}_{x}']
            elif df[cov].min() == 0 and df[cov].max() == 1:
                bias_covs += [cov]

        # drop variables we can't use
        model_cols = []
        for bc in bias_covs:
            if not bc in list(df):
                # print(f'{bc} not in dataset')
                continue
            # lose if NAs
            if any(np.isnan(df[bc])):
                # print(f'{bc} contains nans')
                continue
            # lose if singular
            if len(df[bc].unique()) == 1:
                # print(f'{bc} is singular')
                continue
            # lose if same as other column
            for obc in model_cols:
                if abs(np.corrcoef(df[bc], df[obc])[0, 1]) > 0.99:
                    # print(f'{bc} is redundant {obc}')
                    continue
            # if you made it this far, congratulations!
            model_cols += [bc]

        # define variables
        if measure == 'diff':
            mean_var = 'shift'
            se_var = 'shift_se'
        elif measure == 'log_ratio':
            mean_var = 'log_rr'
            se_var = 'log_se'
        if model_type == 'linear':
            df[mean_var] = df[mean_var] / (df['conc'] - df['conc_den'])
            df[se_var] = df[se_var] / (df['conc'] - df['conc_den'])

        # check for NAs in essential vars
        for data_col in ['nid', mean_var, se_var, 'conc_den', 'conc']:
            if len(df.loc[df[data_col].isnull()]) > 0:
                problem_idx_list = df.loc[df[data_col].isnull()].index.tolist()
                problem_idx = ', '.join(problem_idx_list)
                raise ValueError(f'Missing value for {data_col} in index {problem_idx}')

        # # fill in vars we won't be using
        # if 'median_age_fup' not in list(df):
        #     df['median_age_fup'] = np.nan
        # if 'child' not in list(df):
        #     df['child'] = np.nan
        #
        # # add other air pollution indicator
        # df['other_ap'] = 1
        # df.loc[df.ier_source == 'OAP', 'other_ap'] = 0

        # format data
        df = df[['nid', 'ier_source', mean_var, se_var, 'conc_den', 'conc'] + model_cols]
        df = df.sort_values('nid').reset_index(drop=True)
        self.df = df

        # select covs
        if x_covs is None:
            self.x_covs = bias_covs
        else:
            self.x_covs = [i for i in bias_covs if i in x_covs]
        if z_covs is None:
            self.z_covs = bias_covs
        else:
            self.z_covs = [i for i in bias_covs if i in z_covs]

        self.obs_mean = df[mean_var]
        self.obs_std = df[se_var]

        self.model_type = model_type

        self.study_sizes = df.groupby('nid', sort=False).count()[mean_var].tolist()

    def create_cov_lists(self):
        x_cov_list = []
        z_cov_list = []

        if self.model_type == 'spline':
            x_cov_list += [{
                'cov_type': 'log_ratio_spline',
                'mat': self.df[['exposure_level_packyears', 'reference_exposure']].values.T,
                'spline_id': 0,
                'x_cov_id': 0,
                'name': 'packyears'
            }]
        elif self.model_type == 'log_linear':
            x_cov_list += [{
                'cov_type': 'linear',
                'mat': np.ones(np.sum(self.study_sizes)),
                'x_cov_id': 0,
                'name':'intercept'
            }]

        x_i = 1
        for x_cov in self.x_covs:
            x_cov_list += [{
                'cov_type': 'linear',
                'mat': self.df[x_cov].values,
                'name': x_cov,
                'x_cov_id': x_i
            }]
            x_i += 1

        z_cov_list += [{
            'cov_type': 'linear',
            'mat': np.ones(np.sum(self.study_sizes)),
            'z_cov_id': 0,
            'name':'intercept'
        }]

        z_i = 1
        for z_cov in self.z_covs:
            z_cov_list += [{
                'cov_type': 'linear',
                'mat': self.df[z_cov].values,
                'name': z_cov,
                'z_cov_id': z_i
            }]
            z_i += 1

        self.x_cov_list = x_cov_list
        self.z_cov_list = z_cov_list

    def create_spline_list(self, n_splines=50, n_knots=5, width_pct=0.2, degree=3):
        if self.model_type == 'spline':
            spline_mat = self.x_cov_list[0]['mat'][0]
            dose_max = spline_mat.max()
            dose_min = 0
            start = (np.percentile(spline_mat, 10) - dose_min) / \
                    (dose_max - dose_min)
            end = (np.percentile(spline_mat, 90) - dose_min) / \
                  (dose_max - dose_min)
            print(f'Knot range: {np.percentile(spline_mat, 10)} to {np.percentile(spline_mat, 90)}')
            b = np.array([[start, end]] * (n_knots - 2))
            min_dist = (end - start) * width_pct
            min_dist_val = min_dist * (dose_max - dose_min)
            print(f'Minimum interval width: {min_dist_val}')
            d = np.array([[min_dist, 1.]] * (n_knots - 1))
            knots_samples = sampleKnots(dose_min, dose_max,
                                        n_knots-1,
                                        b=b, d=d,
                                        N=n_splines)
            self.spline_list = [xspline(knots, degree, r_linear=True) for knots in knots_samples]
        else:
            print(f'Spline list not needed for model_type {self.model_type}')


    def create_prior_list(self, lprior_var=None, custom_priors=[]):
        prior_list = []

        if self.model_type == 'spline':
            # set exp(beta0) equal to 1
            prior_list += [{
                'prior_type': 'x_cov_uprior',
                'x_cov_id': 0,
                'prior': np.array([
                    [1.] + [-np.inf] * (self.spline_list[0].num_spline_bases - 1),
                    [1.] + [np.inf] * (self.spline_list[0].num_spline_bases - 1)
                ])
            }]

            # force to be positive
            prior_list += [{
                'prior_type': 'spline_shape_function_uprior',
                'x_cov_id': 0,
                'interval': [self.spline_list[0].knots[0],
                             self.spline_list[0].knots[-1]],
                'indicator': [0.01, 20.],
                'num_points': 30
            }]

            # monotonically increasing
            prior_list += [{
                'prior_type': 'spline_shape_monotonicity',
                'x_cov_id': 0,
                'interval': [self.spline_list[0].knots[0],
                             self.spline_list[0].knots[-1]],
                'indicator': 'increasing',
                'num_points': 30
            }]

        # z-cov positive
        prior_list += [{
            'prior_type': 'z_cov_uprior',
            'z_cov_id': 0,
            'prior': np.array([[1e-7], [2]])
        }]

        if lprior_var is not None:
            # x-cov laplace
            for cov_id in range(1, len(self.x_cov_list)):
                prior_list += [{
                    'prior_type': 'x_cov_lprior',
                    'x_cov_id': cov_id,
                    'prior': np.array([[0], np.sqrt([lprior_var])])
                }]

            # z-cov laplace
            for cov_id in range(1, len(self.z_cov_list)):
                prior_list += [{
                    'prior_type': 'z_cov_lprior',
                    'z_cov_id': cov_id,
                    'prior': np.array([[1e-7], np.sqrt([lprior_var])])
                }]

        # add priors from outside method
        prior_list += custom_priors

        self.prior_list = prior_list

    def run_mr(self):
        if self.model_type == 'spline':
            mr = MR_BeRT(obs_mean=self.obs_mean,
                         obs_std=self.obs_std,
                         study_sizes=self.study_sizes,
                         x_cov_list=self.x_cov_list, z_cov_list=self.z_cov_list,
                         spline_list=self.spline_list,
                         rr_random_slope=True,
                         inlier_percentage=0.9)
            mr.addPriors(self.prior_list)
            x0 = ratioInit(mr, 0)
            mr.fitModel(x0=x0)
            mr.scoreModel()
        elif self.model_type == 'log_linear':
            mr = MR_BRT(obs_mean=self.obs_mean,
                        obs_std=self.obs_std,
                        study_sizes=self.study_sizes,
                        x_cov_list=self.x_cov_list, z_cov_list=self.z_cov_list,
                        inlier_percentage=0.9)
            mr.addPriors(self.prior_list)
            mr.fitModel()
        self.mr = mr

    def sample_params(self, n_samples=500):
        if 'mr_list' in dir(self.mr):
            # sample for each submodel
            sample_size_list = self.mr.compute_sample_sizes(n_samples)
            param_samples = [
                LimeTr.sampleSoln(sub_mr.lt, sample_size=ss) for sub_mr, ss in zip(self.mr.mr_list, sample_size_list)
            ]
            given_samples = {
                'given_beta_samples_list': [i[0] for i in param_samples],
                'given_gamma_samples_list': [i[1] for i in param_samples]
            }
        else:
            beta_samples, gamma_samples = LimeTr.sampleSoln(self.mr.lt, sample_size=n_samples)
            given_samples = {
                'given_beta_samples': beta_samples,
                'given_gamma_samples': gamma_samples
            }
        self.given_samples = given_samples

    def prepare_prediction_covariates(self, domain, n_bins):
        # get exposure range
        x = np.linspace(domain[0], domain[1], n_bins)

        # make x-cov list
        pred_x_cov_list = [{
            'x_cov_id': 0,
            'cov_type': 'spline',
            'spline_id': 0,
            'mat': x,
            'name': 'dose'
        }]
        for i in range(1, len(self.x_cov_list)):
            if self.x_cov_list[i]['name'] == 'follow_up':
                mat = np.ones(n_bins)
            else:
                mat = np.zeros(n_bins)
            pred_x_cov_list += [{
                'x_cov_id': i,
                'cov_type': 'linear',
                'mat': mat,
                'name': self.x_cov_list[i]['name']
            }]

        # make z-cov list
        pred_z_cov_list = [{
            'z_cov_id': 0,
            'cov_type': 'linear',
            'mat': np.ones(n_bins),
            'name': 'intercept'
        }]
        for i in range(1, len(self.z_cov_list)):
            pred_z_cov_list += [{
                'z_cov_id': i,
                'cov_type': 'linear',
                'mat': np.zeros(n_bins),
                'name': self.z_cov_list[i]['name']
            }]

        return pred_x_cov_list, pred_z_cov_list

    def mr_predict(self, N=101, n_samples=500):
        if self.model_type == 'spline':
            pred_x_cov_list, pred_z_cov_list = self.prepare_prediction_covariates(
                domain=(self.mr.spline_list[0].knots[0],
                        self.mr.spline_list[0].knots[-1]),
                n_bins=N
            )

            y_samples_fe = self.mr.predictData(
                pred_x_cov_list, pred_z_cov_list,
                sample_size=n_samples,
                pred_study_sizes=[N],
                ref_point=self.mr.spline_list[0].knots[0],
                include_random_effect=False,
                **self.given_samples
            )[0]
            y_samples = self.mr.predictData(
                pred_x_cov_list, pred_z_cov_list,
                sample_size=n_samples,
                pred_study_sizes=[N],
                ref_point=self.mr.spline_list[0].knots[0],
                include_random_effect=True,
                **self.given_samples
            )[0]

            y_samples_fe = np.vstack(y_samples_fe).T
            y_samples = np.vstack(y_samples).T

            # return pd.DataFrame(
            #     columns=['exposure'] + [f'draw_{i}' for i in range(n_samples)] + [f'draw_fe_{i}' for i in range(n_samples)],
            #     data=np.hstack([np.expand_dims(pred_x_cov_list[0]['mat'], 1), y_samples, y_samples_fe])
            # )
            return pred_x_cov_list, pred_z_cov_list, y_samples, y_samples_fe
        else:
            print(f'Prediction not expected for model_type {self.model_type}.')


class CovFinder:
    def __init__(self, x=10):
        # settings
        self.exp_counter = x

        # our eject buttons
        self.stop_iter = False
        self.x_stop = False
        self.z_stop = False

        # starting point for X-covs
        self.ratio_x_covs = []
        self.beta_priors = []

        # starting point for Z-covs
        self.ratio_z_covs = []
        self.gamma_priors = []

    def iterate(self, laplace_threshold=1e-5, n_samples=250):
        lprior_var = 1 / 10**self.exp_counter
        print(f'Running with Laplace prior variance of 1e-{self.exp_counter}')
        laplace_mod = SmokingModel('log_linear')
        laplace_mod.create_cov_lists()
        laplace_mod.create_prior_list(lprior_var)
        laplace_mod.run_mr()

        laplace_x_covs = np.array([i['name'] for i in laplace_mod.x_cov_list])[1:]
        beta_preserved = np.abs(laplace_mod.mr.beta_soln)[1:] > laplace_threshold

        laplace_z_covs = np.array([i['name'] for i in laplace_mod.z_cov_list])[1:]
        gamma_preserved = np.abs(laplace_mod.mr.gamma_soln)[1:] > laplace_threshold
        gamma_preserved = beta_preserved & gamma_preserved  # only use in Z if also in X

        if beta_preserved.sum() + gamma_preserved.sum() > 0:
            # identify cov list for X
            if beta_preserved.sum() > 0 and not self.x_stop:
                keep_x_covs = self.ratio_x_covs + [i for i in laplace_x_covs[beta_preserved] if i not in self.ratio_x_covs]
                print(f'    Testing X-covs: {", ".join(keep_x_covs)}')
            elif self.x_stop:
                print('    Done adding X-covs.')
                keep_x_covs = self.ratio_x_covs
            else:
                keep_x_covs = []

            # identify cov list for Z (must also be X-cov)
            if gamma_preserved.sum() > 0 and not self.z_stop:
                keep_z_covs = self.ratio_z_covs + [i for i in laplace_z_covs[gamma_preserved] if i not in self.ratio_z_covs]
                if len(keep_z_covs) > 0:
                    print(f'    Testing Z-covs: {", ".join(keep_z_covs)}')
            elif self.z_stop:
                print('    Done adding Z-covs.')
                keep_z_covs = self.ratio_z_covs
            else:
                keep_z_covs = []

            # run model and sample
            mod = SmokingModel('log_linear',
                               x_covs=list(keep_x_covs),
                               z_covs=list(keep_z_covs))
            mod.create_cov_lists()
            mod.create_prior_list(custom_priors=self.beta_priors + self.gamma_priors)
            mod.run_mr()
            mod.sample_params(n_samples)

            # find good X-covs
            x_covs = np.array([i['name'] for i in mod.x_cov_list])[1:]
            if list(x_covs) and not self.x_stop:
                beta_means = mod.mr.beta_soln[1:]
                beta_stds = mod.given_samples['given_beta_samples'].std(axis=0)[1:]
                beta_uis = np.percentile(mod.given_samples['given_beta_samples'], (5, 95), axis=0)
                beta_sig = beta_uis.prod(axis=0)[1:] > 0

                for xi in range(mod.mr.k_beta - 1):
                    if beta_sig[xi] and x_covs[xi] not in self.ratio_x_covs:
                        print(f'    Adding {x_covs[xi]} as X-cov')
                        self.ratio_x_covs += [x_covs[xi]]
                        self.beta_priors += [{
                            'prior_type': 'x_cov_gprior',
                            'x_cov_id': len(self.ratio_x_covs),
                            'prior': np.array([[beta_means[xi]],
                                               [beta_stds[xi]]])
                        }]
                    elif not beta_sig[xi]:
                        self.x_stop = True

            # find good Z-covs
            z_covs = np.array([i['name'] for i in mod.z_cov_list])[1:]
            if list(z_covs) and not self.z_stop:
                gamma_means = mod.mr.gamma_soln[1:]
                gamma_stds = mod.given_samples['given_gamma_samples'].std(axis=0)[1:]
                gamma_sig = np.abs(gamma_means / gamma_stds) > 1.645

                for zi in range(mod.mr.k_gamma - 1):
                    if gamma_sig[zi] and z_covs[zi] not in self.ratio_z_covs:
                        print(f'    Adding {z_covs[zi]} as Z-cov')
                        self.ratio_z_covs += [z_covs[zi]]
                        self.gamma_priors += [{
                            'prior_type': 'z_cov_gprior',
                            'z_cov_id': len(self.ratio_z_covs),
                            'prior': np.array([[gamma_means[zi]],
                                               [gamma_stds[zi]]])
                        }]
                    elif not gamma_sig[zi]:
                        self.z_stop = True
        else:
            print('    No preserved covariates')

        # check if we're done
        if beta_preserved.sum() == len(laplace_x_covs) and gamma_preserved.sum() == len(laplace_z_covs):
            self.stop_iter = True

    def converge_on_covs(self):
        while not self.stop_iter and (not self.x_stop or not self.z_stop) and self.exp_counter >= 0:
            self.iterate()
            self.exp_counter -= 0.1

        self.exp_counter += 0.1

    def return_covs(self):
        print(f'Final Laplace variance tested: 1e-{self.exp_counter}')

        ## JUST RETURN COVS... NOT USING PRIORS IN STAGE 2
        return self.ratio_x_covs, self.ratio_z_covs
        # return self.ratio_x_covs, self.beta_priors, self.ratio_z_covs, self.gamma_priors


def main(out_dir):
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    # get our bias covariates
    stage1 = CovFinder(5)
    stage1.converge_on_covs()
    x_covs, z_covs = stage1.return_covs()

    # run log-linear model with our selected covs, get slope prior
    stage2_mod = SmokingModel('log_linear', x_covs=x_covs, z_covs=z_covs)
    stage2_mod.create_cov_lists()
    stage2_mod.create_prior_list()
    stage2_mod.run_mr()
    with open(os.path.join(out_dir, 'stage2_mod.pkl'), 'wb') as fwrite:
        pickle.dump(stage2_mod, fwrite, -1)

    # run log-linear model with our selected covs [mono]
    ratio_mod = SmokingModel('spline', x_covs=x_covs, z_covs=z_covs)
    ratio_mod.create_cov_lists()
    ratio_mod.create_spline_list()
    ratio_mod.create_prior_list()
    ratio_mod.run_mr()
    ratio_mod.sample_params()
    with open(os.path.join(out_dir, 'ratio_mod.pkl'), 'wb') as fwrite:
        pickle.dump(ratio_mod, fwrite, -1)


if __name__ == '__main__':
    main(sys.argv[1])

import numpy as np
from copy import deepcopy
from xspline import xspline
import limetr
from limetr import LimeTr
from mrbrt import utils

class MR_BRT:
    def __init__(self, obs_mean, obs_std, study_sizes,
                 x_cov_list, z_cov_list,
                 spline_list=[],
                 inlier_percentage=1.0,
                 rr_random_slope=False):
        """
        Initialize the object and pass in the data, require
        - obs_mean: observations
        - obs_std: standard deviations for the observations
        - study_sizes: all study sizes in a list
        - x_cov_list: all x cov in a list
        - z_cov_list: all z cov in a list
        - spline_list: optional, all spline in a list
        - inlier_percentage: optional, used for trimming
        """
        # pass in data
        self.obs_mean = obs_mean
        self.obs_std = obs_std
        self.study_sizes = study_sizes
        self.num_studies = len(study_sizes)
        self.num_obs = sum(study_sizes)

        # construct x and z "covariates"
        self.spline_list = spline_list
        self.x_cov_list = x_cov_list
        self.z_cov_list = z_cov_list

        (self.F,
         self.JF,
         self.F_list,
         self.JF_list,
         self.id_beta_list,
         self.id_spline_beta_list,
         self.k_beta_list) = utils.constructXCov(x_cov_list,
                                                 spline_list=spline_list)
        (self.Z,
         self.Z_list,
         self.id_gamma_list,
         self.id_spline_gamma_list,
         self.k_gamma_list) = utils.constructZCov(z_cov_list,
                                                  spline_list=spline_list)

        self.k_beta = int(sum(self.k_beta_list))
        self.k_gamma = int(sum(self.k_gamma_list))
        self.k = self.k_beta + self.k_gamma

        self.id_beta = slice(0, self.k_beta)
        self.id_gamma = slice(self.k_beta, self.k)

        # if use the random slope model or not
        self.rr_random_slope = rr_random_slope
        if rr_random_slope:
            valid_x_cov_id = [i for i in range(len(x_cov_list)) if
                              x_cov_list[i]['cov_type'] in [
                                'log_ratio_spline',
                                'log_ratio_spline_integral']]
            if len(valid_x_cov_id) == 0:
                raise Exception(
                    "Error: no suitable x cov for random slope model.")
            if len(valid_x_cov_id) >= 2:
                raise Exception(
                    "Error: multiple x cov for random slope model.")

            x_cov = x_cov_list[valid_x_cov_id[0]]
            mat = x_cov['mat']
            if x_cov['cov_type'] == 'log_ratio_spline':
                scaling = mat[0] - mat[1]
            else:
                scaling = 0.5*(mat[0] + mat[1] - mat[2] - mat[3])
            self.Z *= scaling.reshape(scaling.size, 1)

        # create limetr object
        self.inlier_percentage = inlier_percentage
        self.lt = LimeTr(self.study_sizes,
                         self.k_beta,
                         self.k_gamma,
                         self.obs_mean,
                         self.F,
                         self.JF,
                         self.Z,
                         self.obs_std,
                         inlier_percentage=inlier_percentage)

    def addPriors(self, prior_list):
        """
        Add priors to the object, require prior_list contains priors
        """
        self.prior_list = prior_list
        
        (self.C, self.JC, self.c,
         self.H, self.JH, self.h,
         self.uprior, self.gprior, self.lprior,
         self.C_list, self.JC_list, self.c_list,
         self.H_list, self.JH_list, self.h_list,
         self.id_C_list, self.id_C_var_list,
         self.id_H_list, self.id_H_var_list,
         self.num_constraints_list,
         self.num_regularizers_list) = utils.constructPrior(prior_list, self)

        # renew uprior for gamma
        if self.uprior is None:
            self.uprior = np.array(
                [[-np.inf]*self.k_beta + [1e-7]*self.k_gamma, [np.inf]*self.k])
        else:
            uprior_beta = self.uprior[:, self.id_beta]
            uprior_gamma = self.uprior[:, self.id_gamma]
            uprior_gamma[0] = np.maximum(1e-7, uprior_gamma[0])
            uprior_gamma[1] = np.maximum(uprior_gamma[0], uprior_gamma[1])
            self.uprior = np.hstack((uprior_beta, uprior_gamma))

        self.lt.C, self.lt.JC, self.lt.c = self.C, self.JC, self.c
        self.lt.H, self.lt.JH, self.lt.h = self.H, self.JH, self.h
        (self.lt.uprior,
         self.lt.gprior,
         self.lt.lprior) = (self.uprior, self.gprior, self.lprior)

        self.lt = LimeTr(self.study_sizes,
                         self.k_beta,
                         self.k_gamma,
                         self.obs_mean,
                         self.F,
                         self.JF,
                         self.Z,
                         self.obs_std,
                         C=self.C, JC=self.JC, c=self.c,
                         H=self.H, JH=self.JH, h=self.h,
                         uprior=self.uprior,
                         gprior=self.gprior,
                         lprior=self.lprior,
                         inlier_percentage=self.inlier_percentage)


    def fitModel(self, x0=None,
                 outer_verbose=False, outer_max_iter=100,
                 outer_step_size=1.0, outer_tol=1e-6,
                 inner_print_level=0, inner_max_iter=20):

        # initialization with gamma set to be zero
        gamma_uprior = self.lt.uprior[:, self.lt.idx_gamma].copy()
        self.lt.uprior[:, self.lt.idx_gamma] = 1e-6
        self.lt.n = np.array([1]*self.num_obs)
        norm_z_col = np.linalg.norm(self.lt.Z, axis=0)
        self.lt.Z /= norm_z_col

        if x0 is None:
            if self.lprior is None:
                x0 = np.array([1.0]*self.k_beta + [1e-6]*self.k_gamma)
            else:
                x0 = np.array([1.0]*self.k_beta*2 + [1e-6]*self.k_gamma*2)
        else:
            if self.lprior is not None:
                beta0 = x0[:self.k_beta]
                gamma0 = x0[self.k_beta:self.k_beta + self.k_gamma]
                x0 = np.hstack((beta0, np.abs(beta0), gamma0, gamma0))

        (beta_0,
         gamma_0,
         self.w_soln) = self.lt.fitModel(x0=x0,
                                         outer_verbose=outer_verbose,
                                         outer_max_iter=outer_max_iter,
                                         outer_step_size=outer_step_size,
                                         outer_tol=outer_tol,
                                         inner_print_level=inner_print_level,
                                         inner_max_iter=inner_max_iter)
        # print("init obj", self.lt.objective(self.lt.soln))

        # fit the model from the initial point
        self.lt.uprior[:, self.lt.idx_gamma] = gamma_uprior
        self.lt.n = self.study_sizes

        if self.lprior is not None:
            x0 = np.hstack((beta_0, np.abs(beta_0), gamma_0, gamma_0))
        else:
            x0 = np.hstack((beta_0, gamma_0))

        self.lt.optimize(x0=x0,
                         print_level=inner_print_level,
                         max_iter=100)

        self.lt.Z *= norm_z_col
        self.lt.gamma /= norm_z_col**2

        self.beta_soln = self.lt.beta
        self.gamma_soln = self.lt.gamma

        # print("final obj", self.lt.objective(self.lt.soln))
        # print("------------------------------")

    def predictData(self, pred_x_cov_list, pred_z_cov_list, sample_size,
                    pred_study_sizes=None,
                    given_beta_samples=None,
                    given_gamma_samples=None,
                    ref_point=None,
                    include_random_effect=True):
        # sample solutions
        if given_beta_samples is None or given_gamma_samples is None:
            beta_samples, gamma_samples = LimeTr.sampleSoln(
                    self.lt,
                    sample_size=sample_size
                )
        else:
            beta_samples = given_beta_samples
            gamma_samples = given_gamma_samples

        # calculate the beta and gamma post cov
        # self.beta_samples_mean = np.mean(beta_samples, axis=0)
        # self.gamma_samples_mean = np.mean(gamma_samples, axis=0)

        # self.beta_samples_cov = \
        #     beta_samples.T.dot(beta_samples)/sample_size - \
        #     np.outer(self.beta_samples_mean, self.beta_samples_mean)
        # self.gamma_samples_cov = \
        #     gamma_samples.T.dot(gamma_samples)/sample_size - \
        #     np.outer(self.gamma_samples_mean, self.gamma_samples_mean)

        # create x cov
        (pred_F,
         pred_JF,
         pred_F_list,
         pred_JF_list,
         pred_id_beta_list) = utils.constructPredXCov(pred_x_cov_list, self)

        # create z cov
        (pred_Z,
         pred_Z_list,
         pred_id_gamma_list) = utils.constructPredZCov(pred_z_cov_list, self)

        # num of studies
        pred_num_obs = pred_Z.shape[0]

        # create observation samples
        y_samples = np.vstack([
            pred_F(beta) for beta in beta_samples
            ])

        if ref_point is not None:
            x_cov_spline_id = [x_cov['spline_id']
                               for x_cov in pred_x_cov_list
                               if 'spline' in x_cov['cov_type']]
            if len(x_cov_spline_id) == 0:
                raise Exception(
                    "Error: no spline x cov")
            if len(x_cov_spline_id) >= 2:
                raise Exception(
                    "Error: multiple spline x covs")

            spline = self.spline_list[x_cov_spline_id[0]]
            ref_risk = spline.designMat(np.array([ref_point])).dot(
                    beta_samples[:, 
                        self.id_spline_beta_list[x_cov_spline_id[0]]].T
                )

            y_samples /= ref_risk.reshape(sample_size, 1)

        pred_gamma = np.hstack([self.gamma_soln[pred_id_gamma_list[i]]
                                for i in range(len(pred_id_gamma_list))])

        if include_random_effect:
            if self.rr_random_slope:
                u = np.random.randn(sample_size, self.k_gamma)*\
                    np.sqrt(self.gamma_soln)
                # zu = np.sum(pred_Z*u, axis=1)
                zu = u[:, 0]

                valid_x_cov_id = [i for i in range(len(pred_x_cov_list)) if
                                  pred_x_cov_list[i]['cov_type'] == 'spline']

                if len(valid_x_cov_id) == 0:
                    raise Exception(
                        "Error: no suitable x cov for random slope model.")
                if len(valid_x_cov_id) >= 2:
                    raise Exception(
                        "Error: multiple x cov for random slope model.")

                mat = pred_x_cov_list[valid_x_cov_id[0]]['mat']
                if ref_point is None:
                    y_samples *= np.exp(np.outer(zu, mat - mat[0]))
                else:
                    y_samples *= np.exp(np.outer(zu, mat - ref_point))
            else:
                if pred_study_sizes is None:
                    pred_study_sizes = np.array([1]*pred_num_obs)
                else:
                    assert sum(pred_study_sizes) == pred_num_obs

                pred_num_studies = len(pred_study_sizes)

                pred_Z_sub = np.split(pred_Z, np.cumsum(pred_study_sizes)[:-1])
                u = [np.random.multivariate_normal(
                            np.zeros(pred_study_sizes[i]),
                            (pred_Z_sub[i]*pred_gamma).dot(pred_Z_sub[i].T),
                            sample_size
                        ) for i in range(pred_num_studies)]
                U = np.hstack(u)

                if np.any(['log_ratio' in self.x_cov_list[i]['cov_type']
                           for i in range(len(self.x_cov_list))]):
                    y_samples *= np.exp(U)
                else:
                    y_samples += U

        return y_samples, beta_samples, gamma_samples, pred_F, pred_Z


class MR_BeRT:
    def __init__(self, obs_mean, obs_std, study_sizes,
                 x_cov_list,
                 z_cov_list,
                 spline_list,
                 inlier_percentage=1.0,
                 rr_random_slope=False):
        """
        Initialize the object and pass in the data, require
        - obs_mean: observations
        - obs_std: standard deviations for the observations
        - study_sizes: all study sizes in a list
        - x_cov_list: all x cov in a list
        - z_cov_list: all z cov in a list
        - spline_list: all spline in a list (this is required)
        - inlier_percentage: optional, used for trimming
        """
        # create all the mr objects
        self.mr_list = []
        self.num_splines = len(spline_list)

        for i in range(self.num_splines):
            self.mr_list.append(
                MR_BRT(obs_mean, obs_std, study_sizes,
                       x_cov_list,
                       z_cov_list,
                       spline_list=[spline_list[i]],
                       inlier_percentage=inlier_percentage,
                       rr_random_slope=rr_random_slope))

        # pass in dimensions and data
        self.study_sizes = self.mr_list[0].study_sizes
        self.num_studies = self.mr_list[0].num_studies
        self.num_obs = self.mr_list[0].num_obs
        self.k_beta = self.mr_list[0].k_beta
        self.k_gamma = self.mr_list[0].k_gamma
        self.k = self.mr_list[0].k

        self.id_beta = self.mr_list[0].id_beta
        self.id_gamma = self.mr_list[0].id_gamma
        self.id_beta_list = self.mr_list[0].id_beta_list
        self.id_gamma_list = self.mr_list[0].id_gamma_list

        self.obs_mean = obs_mean
        self.obs_std = obs_std
        self.x_cov_list = x_cov_list
        self.z_cov_list = z_cov_list
        self.spline_list = spline_list
        self.inlier_percentage = inlier_percentage
        self.rr_random_slope = rr_random_slope

        self.F_list = [
            self.mr_list[i].F for i in range(self.num_splines)
        ]

    def addPriors(self, prior_list):
        """
        Add priors for each mr object
        """
        for i in range(self.num_splines):
            self.mr_list[i].addPriors(prior_list)

    def fitModel(self, x0=None,
                 outer_verbose=False, outer_max_iter=100,
                 outer_step_size=1.0, outer_tol=1e-6,
                 inner_print_level=0, inner_max_iter=20):

        for i in range(self.num_splines):
            self.mr_list[i].fitModel(
                x0=x0,
                outer_verbose=outer_verbose,
                outer_max_iter=outer_max_iter,
                outer_step_size=outer_step_size,
                outer_tol=outer_tol,
                inner_print_level=inner_print_level,
                inner_max_iter=inner_max_iter
                )

        beta_soln_list = [
            self.mr_list[i].beta_soln for i in range(self.num_splines)
        ]

        gamma_soln_list = [
            self.mr_list[i].gamma_soln for i in range(self.num_splines)
        ]

        self.beta_soln = np.vstack(beta_soln_list)
        self.gamma_soln = np.vstack(gamma_soln_list)

    def scoreModel(self,
                   scores_weights=np.array([1.0, 1.0]),
                   slopes=np.array([2.0, 10.0]),
                   quantiles=np.array([0.4, 0.4])):

        scores = np.zeros((2, self.num_splines))
        for i in range(self.num_splines):
            scores[0][i] = utils.scoreMR_datafit(self.mr_list[i])
            scores[1][i] = utils.scoreMR_TV(self.mr_list[i], n=3)

        weights = np.zeros(scores.shape)
        for i in range(2):
            weights[i] = utils.nonLinearTrans(
                    scores[i],
                    slope=slopes[i],
                    quantile=quantiles[i]
                )**scores_weights[i]

        weights = np.prod(weights, axis=0)
        self.weights = weights/np.sum(weights)

    def ensembleCurves(self, pred_x_cov_list, normalize_y_samples=False):
        y = []

        for mr in self.mr_list:
            (pred_F,
             pred_JF,
             pred_F_list,
             pred_JF_list,
             pred_id_beta_list) = utils.constructPredXCov(pred_x_cov_list, mr)

            y_sub = pred_F(mr.beta_soln)
            if normalize_y_samples:
                y_sub /= np.min(y_sub)

            y.append(y_sub)

        y = np.vstack(y)

        return y.T.dot(self.weights), y


    # def predictData(self, pred_x_cov_list, pred_z_cov_list, sample_size,
    #                 pred_study_sizes=None,
    #                 given_beta_samples=None,
    #                 given_gamma_samples=None,
    #                 normalize_y_samples=False,
    #                 include_random_effect=True):

    #     # determine the number of data points
    #     mat = pred_x_cov_list[0]['mat']
    #     if isinstance(mat, tuple):
    #         mat = mat[0]
    #     if mat.ndim == 1:
    #         pred_N = mat.size
    #     else:
    #         pred_N = mat.shape[1]

    #     # gathering all the dimension information
    #     k_beta = self.k_beta
    #     k_gamma = self.k_gamma

    #     y_samples = np.zeros((sample_size, self.num_splines, pred_N))

    #     if given_beta_samples is None or given_gamma_samples is None:
    #         beta_samples = np.zeros((sample_size, self.num_splines, k_beta))
    #         gamma_samples = np.zeros((sample_size, self.num_splines, k_gamma))

    #         # ensemble the gamma to sample the random effect
    #         beta_soln = self.beta_soln
    #         gamma_soln = self.gamma_soln
    #         gamma_soln_ensemble = gamma_soln.T.dot(self.weights)

    #         # create variance matrix
    #         lt = self.mr_list[0].lt
    #         varmat = limetr.utils.VarMat(lt.V, lt.Z, gamma_soln_ensemble, lt.n)
    #         D_blocks = varmat.varMatBlocks()

    #         # sample beta and gamma
    #         for i in range(sample_size):
    #             # sample the random effect and measurement error
    #             e = [np.random.multivariate_normal(
    #                     np.zeros(lt.n[i]), D_blocks[i]
    #                     ) for i in range(lt.m)]
    #             E = np.hstack(e)

    #             for j in range(self.num_splines):
    #                 lt = deepcopy(self.mr_list[j].lt)
    #                 lt.Y = lt.F(beta_soln[j]) + E
    #                 lt.optimize()
    #                 beta_samples[i][j] = lt.beta
    #                 gamma_samples[i][j] = lt.gamma
    #     else:
    #         beta_samples = given_beta_samples.copy()
    #         gamma_samples = given_gamma_samples.copy()

    #     # construct curve samples
    #     for i in range(self.num_splines):
    #         # construct the functions and covariates
    #         (pred_F,
    #          pred_JF,
    #          pred_F_list,
    #          pred_JF_list,
    #          pred_id_beta_list) =\
    #             utils.constructPredXCov(pred_x_cov_list, self.mr_list[i])

    #         for j in range(sample_size):
    #             y_samples[j][i] = pred_F(beta_samples[j][i])
    #             if normalize_y_samples:
    #                 y_samples[j][i] /= np.min(y_samples[j][i])

    #     # ensemble curves
    #     y_samples_ensemble = np.zeros((sample_size, pred_N))
    #     for i in range(sample_size):
    #         y_samples_ensemble[i] = y_samples[i].T.dot(self.weights)

    #     # include random effect
    #     if include_random_effect:
    #         (pred_Z,
    #          pred_Z_list,
    #          pred_id_gamma_list) =\
    #             utils.constructPredZCov(pred_z_cov_list, self.mr_list[0])
            
    #         pred_num_obs = pred_Z.shape[0]

    #         if pred_study_sizes is None:
    #             pred_study_sizes = np.array([1]*pred_num_obs)
    #         else:
    #             assert sum(pred_study_sizes) == pred_num_obs

    #         pred_num_studies = len(pred_study_sizes)
    #         pred_Z_sub = np.split(pred_Z, np.cumsum(pred_study_sizes)[:-1])

    #         pred_gamma = np.hstack([gamma_soln_ensemble[pred_id_gamma_list[i]]
    #                             for i  in range(len(pred_id_gamma_list))])

    #         for i in range(sample_size):
    #             u = [np.random.multivariate_normal(
    #                     np.zeros(pred_study_sizes[i]),
    #                     pred_Z_sub[i]*(pred_Z_sub[i].T*pred_gamma)
    #                     ) for i in range(pred_num_studies)]
    #             U = np.hstack(u)
    #             y_samples_ensemble[i] += U

    #     return y_samples_ensemble, y_samples, beta_samples, gamma_samples

    def predictData(self, pred_x_cov_list, pred_z_cov_list, sample_size,
                    pred_study_sizes=None,
                    given_beta_samples_list=None,
                    given_gamma_samples_list=None,
                    ref_point=None,
                    include_random_effect=True):

        sample_size_list = [0]*self.num_splines

        if sample_size <= 2*self.num_splines:
            valid_id = np.argsort(self.weights)[-(sample_size>>1):]
        else:
            valid_id = np.arange(self.num_splines)

        for i in valid_id:
            sample_size_list[i] = int(np.rint(sample_size*self.weights[i]))

        y_samples_list = []
        beta_samples_list = []
        gamma_samples_list = []
        F_list = []
        Z_list = []

        if given_beta_samples_list is None:
            given_beta_samples_list = [None for i in range(self.num_splines)]
        else:
            assert len(given_beta_samples_list) == self.num_splines

        if given_gamma_samples_list is None:
            given_gamma_samples_list = [None for i in range(self.num_splines)]
        else:
            assert len(given_gamma_samples_list) == self.num_splines

        for i in range(self.num_splines):
            mr = self.mr_list[i]
            if sample_size_list[i] == 0:
                beta_samples_list.append(None)
                gamma_samples_list.append(None)
                continue
            y_samples, beta_samples, gamma_samples, F, Z =\
                mr.predictData(
                    pred_x_cov_list,
                    pred_z_cov_list,
                    sample_size_list[i],
                    pred_study_sizes=pred_study_sizes,
                    ref_point=ref_point,
                    given_beta_samples=given_beta_samples_list[i],
                    given_gamma_samples=given_gamma_samples_list[i],
                    include_random_effect=include_random_effect
                )
            y_samples_list.append(y_samples)
            beta_samples_list.append(beta_samples)
            gamma_samples_list.append(gamma_samples)
            F_list.append(F)
            Z_list.append(Z)

        # calculate the beta and gamma post cov
        # beta_samples = np.vstack([beta for beta in beta_samples_list
        #                           if beta is not None])
        # gamma_samples = np.vstack([gamma for gamma in gamma_samples_list
        #                            if gamma is not None])

        # sample_size = beta_samples.shape[0]

        # self.beta_samples_mean = np.mean(beta_samples, axis=0)
        # self.gamma_samples_mean = np.mean(gamma_samples, axis=0)

        # self.beta_samples_cov = \
        #     beta_samples.T.dot(beta_samples)/sample_size - \
        #     np.outer(self.beta_samples_mean, self.beta_samples_mean)
        # self.gamma_samples_cov = \
        #     gamma_samples.T.dot(gamma_samples)/sample_size - \
        #     np.outer(self.gamma_samples_mean, self.gamma_samples_mean)

        return y_samples_list, beta_samples_list, gamma_samples_list,\
            F_list, Z_list


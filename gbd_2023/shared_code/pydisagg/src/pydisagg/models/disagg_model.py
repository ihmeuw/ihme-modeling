"""Module with main DisaggModel class and algorithmic implementation"""

import numpy as np
from numpy.typing import NDArray
from scipy.optimize import root_scalar

from pydisagg.transformations import Transformation


class DisaggModel:
    """Model for solving splitting/disaggregation problems

    transformation is a class attribute that defines the generalized
    proportionality assumption that we want to make
    It should be a Transformation object which is callable, has an
    inverse function, and has a derivative.

    Notes
    -----
    A possibly confusing code and notation choice is that we enforce
    multiplicativity in some space, but fit an additive parameter beta
    throughout

    We do this because it works better with the delta method calculations, and
    we assume that we have a log in the transormations so additive factors
    become multiplicative

    """

    def __init__(self, transformation: Transformation):
        """Initializes a dissaggregation model

        Parameters
        ----------
        transformation
            Transformation to apply to rate pattern values

        """
        self.T = transformation

    def predict_rate(self, beta: float, rate_pattern: NDArray) -> NDArray:
        """Predicts the rate in each bucket

        Parameters
        ----------
        beta : float
            beta paramter, log of rescaling parameter

        rate_pattern : NDArray
            rate pattern

        Returns
        -------
        NDArray
            estimated occurrences in each bucket

        """
        return self.T.inverse(beta + self.T(rate_pattern))

    def rate_diff_beta(self, beta: float, rate_pattern: NDArray) -> NDArray:
        """Computes the derivative of the predicted rates with respect to beta

        Parameters
        ----------
        beta : float
            beta paramter, log of rescaling parameter
        rate_pattern : NDArray
            rate pattern
        bucket_populations :NDArray
            Populations in each bucket
        Returns
        -------
        NDArray
            derivative of predicted rates in each bucket with respect to beta
        """
        return 1 / self.T.diff(self.T.inverse(beta + self.T(rate_pattern)))

    def predict_count(
        self,
        beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
    ) -> NDArray:
        """Generate a predicted count in each bucket assuming multiplicativity
        in the T-transformed space with the additive parameter beta

        Parameters
        ----------
        beta : float
            beta paramter, log of rescaling parameter
        rate_pattern : NDArray
            rate pattern
        bucket_populations :NDArray
            Populations in each bucket
        Returns
        -------
        NDArray
            Predicted rates in each bucket
        """
        return self.predict_rate(beta, rate_pattern) * bucket_populations

    def count_diff_beta(
        self,
        beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
    ) -> NDArray:
        """Computes the derivative of the predicted count in each bucket with
        respect to beta

        Parameters
        ----------
        beta
            beta paramter, log of rescaling parameter
        rate_pattern
            rate pattern
        bucket_populations
            Populations in each bucket

        Returns
        -------
        NDArray
            Derivative of predicted counts in each bucket with respect to beta

        """
        return self.rate_diff_beta(beta, rate_pattern) * bucket_populations

    def fit_beta(
        self,
        observed_total: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        lower_guess: float = -50.0,
        upper_guess: float = 50.0,
        verbose: int = 0,
    ) -> float:
        """Fits the parameter beta to the data

        Parameters
        ----------
        observed_total
            aggregated observation
        rate_pattern
            rate pattern to assume (generalized) proportionality to
        bucket_populations
            populations in each bucket
        lower_guess
            Lower bound for rootfinding (we use bracketing), by default -50
        upper_guess
            Upper bound for rootfinding, by default 50
        verbose
            how much to print, 1 prints the root value,
            2 prints the entire rootfinding output, by default 0

        Returns
        -------
        float
            computed value for beta

        """

        def beta_misfit(beta):
            return (
                self.H_func(
                    beta,
                    rate_pattern=rate_pattern,
                    bucket_populations=bucket_populations,
                )
                - observed_total
            )

        beta_results = root_scalar(
            beta_misfit, bracket=[lower_guess, upper_guess], method="toms748"
        )
        if verbose == 2:
            print(beta_results)
        elif verbose == 1:
            print(f"beta={beta_results.root}")
        fitted_beta = beta_results.root
        return fitted_beta

    def H_func(
        self,
        beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
    ) -> float:
        """Returns the predicted total function

        Parameters
        ----------
        beta
            beta paramter, log of rescaling parameter
        rate_pattern
            rate pattern
        bucket_populations
            Populations in each bucket

        Returns
        -------
        float
            Predicted total given beta, bucket populations, and rate pattern
            This is matched to observed_total with rootfinding to fit beta

        """
        return np.sum(
            self.predict_count(beta, rate_pattern, bucket_populations)
        )

    def H_diff_beta(
        self,
        beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
    ) -> float:
        """Returns the derivative of the predicted total function with respect
        to beta

        Parameters
        ----------
        beta : float
            beta paramter, log of rescaling parameter
        rate_pattern : NDArray
            rate pattern
        bucket_populations :NDArray
            Populations in each bucket

        Returns
        -------
        float
            derivative of H_func with respect to beta
        """
        return np.sum(
            self.count_diff_beta(beta, rate_pattern, bucket_populations)
        )

    def beta_standard_error(
        self,
        fitted_beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        observed_total_se: float,
    ) -> float:
        """Computes delta-method standard error estimate for beta

        Parameters
        ----------
        fitted_beta : float
            fitted value of beta. This being fitted is why we don't need what
            the original observation actually was
        rate_pattern : NDArray
            rate pattern
        bucket_populations : NDArray
            populations in each bucket
        observed_total_se : float
            standard error of the total observed quantity

        Returns
        -------
        float
            standard error of beta
        """
        error_inflation = 1 / self.H_diff_beta(
            fitted_beta, rate_pattern, bucket_populations
        )
        beta_standard_error = observed_total_se * error_inflation
        return beta_standard_error

    def split_to_rates(
        self,
        observed_total: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        observed_total_se: float | None = None,
        lower_guess: float = -50.0,
        upper_guess: float = 50.0,
        verbose: int = 0,
        reduce_output: bool = False,
    ) -> NDArray:
        """Splits the given total to rates

        Parameters
        ----------
        observed_total
            aggregated observation
        rate_pattern
            rate pattern to assume (generalized) proportionality to
        bucket_populations
            populations in each bucket
        observed_total_se
            standard error of the observed total
        lower_guess
            Lower bound for rootfinding (we use bracketing), by default -50
        upper_guess
            Upper bound for rootfinding, by default 50
        verbose
            how much to print, 1 prints the root value,
            2 prints the entire rootfinding output, by default 0
        reduce_output
            boolean for whether or not to set groups with zero population to have zero rate

        Returns
        -------
        if observed_total_se is not given, we return
            NDArray
                predicted counts
        Otherwise we return
            NDArray
                predicted counts
            NDArray
                standard error estimates

        """
        fitted_beta = self.fit_beta(
            observed_total,
            rate_pattern,
            bucket_populations,
            lower_guess,
            upper_guess,
            verbose=verbose,
        )
        rate_point_estimates = self.predict_rate(fitted_beta, rate_pattern)

        # This is some dirty type casting, if reduce output, we set all groups with population 0 to 0
        # Otherwise, we're multiplying everything True, which gets casted to 1
        output_multiplier = ((1 - 1 * reduce_output) + bucket_populations) > 0

        if observed_total_se is not None:
            standard_errors = self.rate_standard_errors(
                fitted_beta, rate_pattern, bucket_populations, observed_total_se
            )
            return (
                rate_point_estimates * output_multiplier,
                standard_errors * output_multiplier,
            )
        return rate_point_estimates * output_multiplier

    def rate_standard_errors(
        self,
        fitted_beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        observed_total_se: float,
    ) -> NDArray:
        """Computes standard errors of rate splitting outputs, only pushing
        forward uncertainty from observed_total_SE

        Parameters
        ----------
        fitted_beta
            fitted value of beta. This being fitted is why we don't need what
            the original observation actually was
        rate_pattern
            rate pattern
        bucket_populations
            populations in each bucket
        observed_total_se
            standard error of the total observed quantity

        Returns
        -------
        NDArray
            standard errors of rate splitting outputs

        """
        beta_SE = self.beta_standard_error(
            fitted_beta, rate_pattern, bucket_populations, observed_total_se
        )
        rate_standard_errors = (
            self.rate_diff_beta(fitted_beta, rate_pattern) * beta_SE
        )
        return rate_standard_errors

    def count_split_standard_errors(
        self,
        fitted_beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        observed_total_se: float,
    ) -> NDArray:
        """Computes standard errors of total count splitting outputs, only
        pushing forward uncertainty from observed_total_SE

        Parameters
        ----------
        fitted_beta
            fitted value of beta. This being fitted is why we don't need what
            the original observation actually was
        rate_pattern
            rate pattern
        bucket_populations
            populations in each bucket
        observed_total_se
            standard error of the total observed quantity

        Returns
        -------
        NDArray
            standard errors of count splitting outputs

        """
        rate_se = self.rate_standard_errors(
            fitted_beta,
            rate_pattern,
            bucket_populations=bucket_populations,
            observed_total_se=observed_total_se,
        )
        return rate_se * bucket_populations

    def split_to_counts(
        self,
        observed_total: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        observed_total_se: float | None = None,
        lower_guess: float = -50.0,
        upper_guess: float = 50.0,
        verbose: int = 0,
    ) -> NDArray:
        """Splits given total to counts

        Parameters
        ----------
        observed_total : float
            aggregated observation
        rate_pattern : NDArray
            rate pattern to assume (generalized) proportionality to
        bucket_populations : NDArray
            populations in each bucket
        observed_total_se: float, by default None
            standard error of the observed total
        lower_guess : float, optional
            Lower bound for rootfinding (we use bracketing), by default -50
        upper_guess : float, optional
            Upper bound for rootfinding, by default 50
        verbose : int, optional
            how much to print, 1 prints the root value,
            2 prints the entire rootfinding output, by default 0

        Returns
        -------
        if observed_total_se is not given, we return
            NDArray
                predicted rates
        Otherwise we return
            NDArray
                predicted rates
            NDArray
                standard error estimates

        """
        fitted_beta = self.fit_beta(
            observed_total,
            rate_pattern,
            bucket_populations,
            lower_guess,
            upper_guess,
            verbose=verbose,
        )

        fitted_beta = self.fit_beta(
            observed_total,
            rate_pattern,
            bucket_populations,
            lower_guess,
            upper_guess,
        )
        count_point_estimates = self.predict_count(
            fitted_beta, rate_pattern, bucket_populations
        )
        if observed_total_se is not None:
            standard_errors = self.count_split_standard_errors(
                fitted_beta, rate_pattern, bucket_populations, observed_total_se
            )
            return count_point_estimates, standard_errors
        return count_point_estimates

    def parameter_covariance(
        self,
        fitted_beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        observed_total_se: float,
        rate_pattern_cov: NDArray,
    ) -> NDArray:
        """Computes covariance matrix of beta and rate_pattern together

        Parameters
        ----------
        fitted_beta : float
            fitted value of beta. This being fitted is why we don't need what
            the original observation actually was
        rate_pattern : NDArray
            rate pattern
        bucket_populations : NDArray
            populations in each bucket
        observed_total_se : float
            standard error of the total observed quantity
        rate_pattern_cov : NDArray
            Covariance matrix of rate pattern estimates

        Returns
        -------
        NDArray
            Joint covariance matrix with beta and rate_pattern

        """
        rate_grad = self.Hinv_rate_grad(
            fitted_beta, rate_pattern, bucket_populations
        )
        beta_pattern_cov = (rate_pattern_cov @ rate_grad).reshape(-1, 1)

        beta_var = np.array(
            [
                [
                    self.beta_standard_error(
                        fitted_beta,
                        rate_pattern,
                        bucket_populations,
                        observed_total_se,
                    )
                    ** 2
                ]
            ]
        )

        full_parameter_cov = np.block(
            [
                [
                    beta_var + rate_grad.T @ rate_pattern_cov @ rate_grad,
                    beta_pattern_cov.T,
                ],
                [beta_pattern_cov, rate_pattern_cov],
            ]
        )

        return full_parameter_cov

    def parameter_fit_jacobian(
        self,
        fitted_beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
    ) -> NDArray:
        """Computes jacobian matrix of H_inv with respect to beta and
        rate_pattern together

        Parameters
        ----------
        fitted_beta : float
            fitted value of beta. This being fitted is why we don't need what
            the original observation actually was
        rate_pattern : NDArray
            rate pattern
        bucket_populations : NDArray
            populations in each bucket

        Returns
        -------
        NDArray
            Joint covariance matrix with beta and rate_pattern

        """

        rate_grad = self.Hinv_rate_grad(
            fitted_beta, rate_pattern, bucket_populations
        ).reshape(-1, 1)
        beta_diff = np.array(
            [
                [
                    (
                        1
                        / self.H_diff_beta(
                            fitted_beta, rate_pattern, bucket_populations
                        )
                    )
                ]
            ]
        )
        full_parameter_jac = np.block(
            [
                [beta_diff, rate_grad.T],
                [
                    np.zeros((len(rate_pattern), 1)),
                    np.identity(len(rate_pattern)),
                ],
            ]
        )

        return full_parameter_jac

    def Hinv_rate_grad(self, beta, rate_pattern, bucket_populations) -> NDArray:
        """Gradient of H inverse (inverted with respect to beta) with respect
        to rate_pattern. The inverse is handled implicitly, as beta is the
        input, not the total.

        Parameters
        ----------
        beta : float
            beta parameter, log of rescaling parameter
        rate_pattern : NDArray
            rate pattern
        bucket_populations :NDArray
            Populations in each bucket

        Returns
        -------
        NDArray
            gradient

        """
        denominator = self.H_diff_beta(beta, rate_pattern, bucket_populations)
        # Call this at the fitted beta for the inverse derivative of H at the observed_total
        # assuming that the observed_total is H(beta), the total that beta would give!

        return (
            -1
            * self.H_rate_grad(beta, rate_pattern, bucket_populations)
            / denominator
        )

    def H_rate_grad(self, beta, rate_pattern, bucket_populations) -> NDArray:
        """Gradient of H (inverted with respect to beta) with respect to
        rate_pattern

        Parameters
        ----------
        beta : float
            beta parameter, log of rescaling parameter
        rate_pattern : NDArray
            rate pattern
        bucket_populations :NDArray
            Populations in each bucket

        Returns
        -------
        NDArray
            gradient

        """
        return (bucket_populations * self.T.diff(rate_pattern)) / (
            self.T.diff(self.T.inverse(beta + self.T(rate_pattern)))
        )

    def rate_jacobian(
        self,
        beta: float,
        rate_pattern: NDArray,
    ) -> NDArray:
        """Computes the jacobian of the predicted output rates with respect to
        both beta and the pattern

        Parameters
        ----------
        beta : float
            fitted beta parameter
        rate_pattern : NDArray
            point estimate rate pattern

        Returns
        -------
        NDArray
            Jacobian, shaped d x (d+1)

        """

        diag_scaling = np.diag(
            1 / self.T.diff(self.T.inverse(beta + self.T(rate_pattern)))
        )
        dim = len(rate_pattern)
        right_portion = np.block(
            [np.ones((dim, 1)), np.diag(self.T.diff(rate_pattern))]
        )
        return diag_scaling @ right_portion

    def count_jacobian(
        self, beta: float, rate_pattern: NDArray, bucket_populations: NDArray
    ):
        """Computes the jacobian of the predicted output counts with respect to
        both beta and the pattern

        Parameters
        ----------
        beta : float
            fitted beta parameter
        rate_pattern : NDArray
            point estimate rate pattern
        bucket_populations :NDArray
            Populations in each bucket

        Returns
        -------
        NDArray
            Jacobian, shaped d x (d+1)

        """

        rate_jac = self.rate_jacobian(beta, rate_pattern)
        diag_pops = np.diag(bucket_populations)
        return diag_pops @ rate_jac

    def rate_split_covariance_uncertainty(
        self,
        fitted_beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        observed_total_se: float,
        rate_pattern_cov: NDArray,
    ) -> NDArray:
        """Computes covariance matrix of predicted rate outputs. Pushes forward
        uncertainty in both observed total, and in the rate pattern

        Parameters
        ----------
        fitted_beta : float
            fitted value of beta. This being fitted is why we don't need what
            the original observation actually was
        rate_pattern : NDArray
            rate pattern
        bucket_populations : NDArray
            populations in each bucket
        observed_total_se : float
            standard error of the total observed quantity
        rate_pattern_cov : NDArray
            Covariance matrix of rate pattern estimates

        Returns
        -------
        NDArray
            Covariance matrix of errors in splitting rate

        """
        param_covariance = self.parameter_covariance(
            fitted_beta=fitted_beta,
            rate_pattern=rate_pattern,
            bucket_populations=bucket_populations,
            observed_total_se=observed_total_se,
            rate_pattern_cov=rate_pattern_cov,
        )
        out_jac = self.rate_jacobian(
            fitted_beta,
            rate_pattern,
        )
        return out_jac @ param_covariance @ out_jac.T

    def count_split_covariance_uncertainty(
        self,
        fitted_beta: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        observed_total_se: float,
        rate_pattern_cov: NDArray,
    ) -> NDArray:
        """Computes covariance matrix of predicted splitting count outputs
        Pushes forward uncertainty in both observed total, and in the rate
        pattern

        Parameters
        ----------
        fitted_beta : float
            fitted value of beta. This being fitted is why we don't need what
            the original observation actually was
        rate_pattern : NDArray
            rate pattern
        bucket_populations : NDArray
            populations in each bucket
        observed_total_se : float
            standard error of the total observed quantity
        rate_pattern_cov : NDArray
            Covariance matrix of rate pattern estimates

        Returns
        -------
        NDArray
            Covariance matrix of errors in splitting counts

        """
        param_covariance = self.parameter_covariance(
            fitted_beta=fitted_beta,
            rate_pattern=rate_pattern,
            bucket_populations=bucket_populations,
            observed_total_se=observed_total_se,
            rate_pattern_cov=rate_pattern_cov,
        )
        out_jac = self.count_jacobian(
            fitted_beta, rate_pattern, bucket_populations
        )
        return out_jac @ param_covariance @ out_jac.T

    def rate_split_full_jac(
        self,
        observed_total: float,
        rate_pattern: NDArray,
        bucket_populations: NDArray,
        lower_guess: float = -50,
        upper_guess: float = 50,
        verbose: int = 0,
    ) -> NDArray:
        """

        Parameters
        ----------
        observed_total : float
            aggregated observation
        rate_pattern : NDArray
            rate pattern to assume (generalized) proportionality to
        bucket_populations : NDArray
            populations in each bucket
        lower_guess : float, optional
            Lower bound for rootfinding (we use bracketing), by default -50
        upper_guess : float, optional
            Upper bound for rootfinding, by default 50
        verbose : Optional[int], optional
            how much to print, 1 prints the root value,
            2 prints the entire rootfinding output, by default 0

        Returns
        -------
        NDArray
            jacobian of prediction output

        """
        beta = self.fit_beta(
            observed_total,
            rate_pattern,
            bucket_populations,
            lower_guess=lower_guess,
            upper_guess=upper_guess,
            verbose=verbose,
        )
        # denominator = self.T.diff(self.T.inverse(self.))
        # Check if this is accomplishing what i think it is - SA 4/19
        # previously not returning anything
        return self.rate_jacobian(beta, rate_pattern)

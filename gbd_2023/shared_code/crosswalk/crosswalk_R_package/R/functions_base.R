#
# functions_base.R
#
# January 2020
#
# Functions: CWData, CovModel, CWModel, adjust_alt_vals
#


#' Format the data for meta-regression
#'
#' \code{CWData} prepares the data as an input for \code{CWModel}
#'
#' @param df a data frame
#' @param obs column name for the dependent variable (note: must be difference in logs or difference in logits)
#' @param obs_se column name for the standard error of the dependent variable
#' @param alt_dorms name of the column specifying the alternative definition or method
#' @param ref_dorms name of the column specifying the reference definition or method
#' @param dorm_separator string indicating separator for multiple dorms in alt_dorms or ref_dorms
#' @param covs list of column names to be used as covariates
#' @param study_id name of the column for indicating group, for estimating between-group heterogeneity, optional
#' @return an object of type 'environment'
#' @examples
#' dat1 <- CWData(
#'   df = mydat,
#'   obs = "ratio",
#'   obs_se = "ratio_se",
#'   alt_dorms = "altvar",
#'   ref_dorms = "refvar",
#'   covs = list("x1", "x2")
#' )
#'
#'
CWData <- function(df, obs, obs_se, alt_dorms, ref_dorms, dorm_separator = NULL,
                   covs = list(), study_id = NULL, data_id = NULL, add_intercept = TRUE) {
  args <- as.list(match.call())
  f <- xwalk$CWData
  return(do.call("f", args[2:length(args)]))
}



#' Specify a covariate
#'
#' \code{CovModel} records information about the functional form of a covariate (e.g. monotonically increasing spline)
#'
#' @param cov_name name of the covariate
#' @param spline the object returned by an \code{XSpline} function call
#' @param spline_monotonicity may be "increasing" or "decreasing"; default NULL
#' @param spline_convexity may be "convex" or "concave"; default NULL
#' @param soln_name an alternative label for the covariate; default NULL
#' @examples
#' CovModel("x1")
#'
#'
CovModel <- function(
  cov_name, spline = NULL, spline_monotonicity = NULL, spline_convexity = NULL, soln_name = NULL,
  prior_beta_uniform = NULL, prior_beta_gaussian = NULL) {
  # args <- as.list(match.call())
  # f <- xwalk$CovModel
  # return(do.call("f", args[2:length(args)]))
  return(xwalk$CovModel(
    cov_name, spline, spline_monotonicity, spline_convexity,
    soln_name, prior_beta_uniform, prior_beta_gaussian
  ))
}



#' Specify a spline
#'
#' \code{XSpline} records information about the functional form of a spline (e.g. degree, knot locations)
#'
#' @param knots a vector of knot locations, including knots at <= min and >= max
#' @param degree polynomial degree (1=linear, 2=quadratic, 3=cubic); must be integer, e.g. \code{3L} or \code{as.integer(3)}
#' @param l_linear boolean indicating whether the leftmost segment is restricted to be linear
#' @param r_linear boolean indicating whether the rightmost segment is restricted to be linear
#' @examples
#' XSpline(knots = c(3,4,5,6,7), degree = 3L, l_linear = TRUE, r_linear = TRUE)
#'
#'
XSpline <- function(knots, degree, l_linear, r_linear) {
  args <- as.list(match.call())
  f <- xsp$XSpline
  return(do.call("f", args[2:length(args)]))
}




#' Fit a model
#'
#' \code{CWModel} fits a meta-regression model
#'
#' @param cwdata the object returned by a \code{CWData} function call
#' @param obs_type "diff_log" or "diff_logit" corresponding to whether the dependent variable is in log or logit space
#' @param cov_models a list of \code{CovModel} function calls;
#'   must include an element with \code{CovModel("intercept")} to run a model with an intercept
#' @param order_prior an optional list of 2-element vectors where the
#'   definition/method specified in the first element is required to
#'   yield a lower predicted value than the definition/method specified in the second element,
#'   all other things being equal
#' @param use_random_intercept If you set use_random_intercept=True in CWModel and NOT pass in study_id , a user warning will be printed out
#' @param gamma_bound Bounds for a uniform prior on gamma, e.g. \code(array(c(0.0, 0.4)))
#' @param max_iter the maximum number of interations allowed for the model to converge; default 100L;
#' must be integer, e.g. \code{100L} or \code{as.integer(100)}
#' @param inlier_pct a float in [0, 1] indicating the proportion of data points presumed to be inliers
#' @param outer_max_iter integer; default 100L
#' @param outer_step_size numeric; default 1.0
#' @return an object of type 'environment'
#' @examples
#' model1 <- CWModel(
#'   cwdata = dat1,
#'   obs_type = "diff_log",
#'   cov_models = list(CovModel("x1"), CovModel("x2")),
#'   gold_dorm = "measured"
#' )
#'
#'
CWModel <- function(
  cwdata, obs_type, cov_models, gold_dorm, order_prior = NULL, use_random_intercept = TRUE, prior_gamma_uniform = NULL,
  max_iter = 100L, inlier_pct = 1, outer_max_iter = 100L, outer_step_size = 1.0) {
  m <- xwalk$CWModel(cwdata, obs_type, cov_models, gold_dorm, order_prior, use_random_intercept, prior_gamma_uniform)
  xwalk$CWModel$fit(m, max_iter, inlier_pct, outer_max_iter, outer_step_size)
  return(m)
}



#' Adjust non-reference data points
#'
#' \code{adjust_orig_vals} adjusts data points that used an alternative (i.e. non-reference)
#' definition/method, based on the fit of the meta-regression, and leaves alone data points
#' that used the reference definition/method
#'
#' @param fit_object the object returned by a \code{CWModel} function call
#' @param df a data frame with alternative (i.e. non-reference) data points to be adjusted
#' @param orig_dorms name of the column specifying the definition/method
#' @param orig_vals_mean column name for the dependent variable in linear space (i.e. not log or logit space)
#' @param orig_vals_se column name for the standard error of the dependent variable in linear space
#' @param study_id column name; if specified, predicts out on the random effects estimated by CWModel for groups with the same study_id value
#' @param data_id column name with row-specific IDs that will be output with the predictions/adjusted values
#' @return a five-element list; mean and SE of the adjusted values, mean and SE of adjustment factor, and data ID
#' @examples
#' preds1 <- adjust_orig_vals(
#'   fit_object = fit1,
#'   df = df_orig1,
#'   orig_dorms = "obs_method",
#'   orig_vals_mean = "meanvar",
#'   orig_vals_se = "sdvar"
#' )
#'
#'
adjust_orig_vals <- function(fit_object, df, orig_dorms, orig_vals_mean, orig_vals_se, study_id = NULL, data_id = NULL, ref_dorms = NULL) {
  return(xwalk$CWModel$adjust_orig_vals(fit_object, df, orig_dorms, orig_vals_mean, orig_vals_se, study_id, data_id, ref_dorms))
}





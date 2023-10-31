#
# functions_utility.R
#
# January 2020
#
#


#' Delta method approximation
#'
#' \code{delta_transform} uses the delta method approximation to transform random variables to/from log/logit space
#'
#' @param mean a vector of means
#' @param sd a vector of standard deviations
#' @param transformation one of "linear_to_log", "log_to_linear", "linear_to_logit", or "logit_to_linear"
#' @return a data frame with the transformed means and standard deviations
#' @examples
#' df <- data.frame(meanvar = c(0.9, 1.0, 1,1), sdvar = (0.2, 0.2, 0.3))
#' delta_transform(mean = df$meanvar, sd = df$sdvar, transformation = "linear_to_log")
#'   cwdata = dat1,
#'   obs_type = "diff_log",
#'   cov_models = list(CovModel("x1"), CovModel("x2")),
#'   gold_dorm = "measured"
#' )
#'
#'
delta_transform <- function(mean, sd, transformation) {

  if (transformation == "linear_to_log") f <- xwalk$utils$linear_to_log
  if (transformation == "log_to_linear") f <- xwalk$utils$log_to_linear
  if (transformation == "linear_to_logit") f <- xwalk$utils$linear_to_logit
  if (transformation == "logit_to_linear") f <- xwalk$utils$logit_to_linear

  out <- do.call("cbind", f(mean = array(mean), sd = array(sd)))
  colnames(out) <- paste0(c("mean", "sd"), "_", strsplit(transformation, "_")[[1]][3])
  return(out)
}


#' Calculate differences between random variables
#'
#' \code{calculate_diff} calculates means and SDs for differences between random variables,
#' ensuring that the alternative defintion/method is in the numerator: log(alt/ref) = log(alt) - log(ref)
#'
#' @param df a data frame
#' @param alt_mean column name for the alternative (i.e. non-reference) means
#' @param alt_sd column name for the alternative standard deviations
#' @param ref_mean column name for the reference means
#' @param ref_sd column name for the reference standard deviations
#' @return a data frame with the differences and their standard deviations
#'
#'
calculate_diff <- function(df, alt_mean, alt_sd, ref_mean, ref_sd) {
  df <- as.data.frame(df)
  out <- data.frame(
    diff_mean =  df[, alt_mean] - df[, ref_mean],
    diff_sd = sqrt(df[, alt_sd]^2 + df[, ref_sd]^2)
  )
  return(out)
}




#' Logit Conversion Functions
#'
#' Convert between logit and inverse logit.
#' For certain qx conversions, convert by a factor of -.5 and -2 to ensure comparability with external estimates
#'
#' @param x numeric representing a logit or an inverse logit.
#'
#' @return numeric representing the calculated logit or inverse logit value.
#'
#' @examples
#' logit(0.25)
#' logit_qx(0.25)
#' 
#' @name logit_conversions
NULL

#' @rdname logit_conversions
#' @export
logit <- function(x) {
  # x is a probability
  result <- log(x/(1-x))
  return(result)
}

#' @rdname logit_conversions
#' @export
invlogit <- function(x) {
  # x is a logit probability
  result <- exp(x) / (1 + exp(x))
  return(result)
}

#' @rdname logit_conversions
#' @export
logit_qx <- function(x) {
  result <- -.5 * logit(x)
  return(result)
}

#' @rdname logit_conversions
#' @export
invlogit_qx <- function(x) {
  result <- invlogit(x * -2)
  return(result)
}

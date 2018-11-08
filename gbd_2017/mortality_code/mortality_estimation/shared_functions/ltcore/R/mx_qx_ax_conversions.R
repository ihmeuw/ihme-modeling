#' Convert between mx, qx, and ax values
#'
#' Convert between mx and qx values given a time for an age interval. 
#' If ax (the average number of years lived in the x to x + n age interval by those dying in the interval) exists, use the *_ax_to_* functions
#' The mx_to_qx and qx_to_mx functions assume smooth mortality across the interval.
#'
#' @param m numeric mx (mortality death rate) value to use in conversion
#' @param q numeric qx (probability of death) value to use in conversion
#' @param a numeric ax (average number of years lived in the age interval of those who died) value to use in conversion
#' @param t numeric representing the time length of the interval (e.g. 5 for age 5-9)
#'
#' @return Numeric value representing the converted mx, qx, or ax value.
#'
#' @examples
#' mx_to_qx(0.25, 45)
#' mx_ax_to_qx(.2, 3, 5)
#' qx_to_mx(.5, 5)
#' mx_qx_to_ax(.5, .25, 5)
#' qx_ax_to_mx(.1, 3, 5)
#'
#' @name mx_qx_ax_conversions
#'
NULL

#' @rdname mx_qx_ax_conversions
#' @export
mx_to_qx <- function(m, t) {
	result <- 1 - exp((-1 * t) * m)
	return(result)
}

#' @rdname mx_qx_ax_conversions
#' @export
mx_ax_to_qx <- function(m, a, t) {
    result <- (t*m)/(1+(t-a)*m)
    return(result)
}

#' @rdname mx_qx_ax_conversions
#' @export
qx_to_mx <- function(q, t) {
	result <- log(1-q) / (-1 * t)
	return(result)
}

#' @rdname mx_qx_ax_conversions
#' @export
mx_qx_to_ax <- function(m, q, t) {
    result <- (q+(m*t*(q-1)))/(m*q)
    return(result)
}

#' @rdname mx_qx_ax_conversions
#' @export
qx_ax_to_mx <- function(q, a, t){
  result <- q/(t-(t*q) + (a*q))
  return(result)
}

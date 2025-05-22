#' Check if qx is valid
#'
#' Assert that qx is in the range `(0, 1)`, or possibly `(0, 1]` if
#' `include_terminal = TRUE`.
#'
#' @param qx Values to check.
#' @param include_terminal Allow the possiblility of terminal age qx values,
#'   which can be 1.
#'
#' @return True or false.
#'
#' @export
is_valid_qx <- function(qx, include_terminal = FALSE) {

  if (isTRUE(include_terminal)) {
    qx > 0 & qx <= 1
  } else {
    qx > 0 & qx < 1
  }

}

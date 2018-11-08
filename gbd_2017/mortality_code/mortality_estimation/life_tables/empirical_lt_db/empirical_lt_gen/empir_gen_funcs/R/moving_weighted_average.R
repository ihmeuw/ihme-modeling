#' Generate a moving weighted average
#'
#' @param x vector of data to be smoothed
#' @param weights: a list of weights to use in smoothing

#' @return returns vector of smoothed data
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

moving_weighted_average <- function(x, weights) {

  # Set window
  window_padding <- (length(weights) - 1) / 2
  weight_center <- window_padding + 1

  # Set
  index_start = 1
  index_end = length(x)

  # Loop over
  output_data <- list()
  for (i in index_start:index_end) {
    window_start = i - window_padding
    window_end = i + window_padding

    # Make sure the windows don't go outside of allowed indicies
    if (window_start < index_start) {
      window_start <- index_start
    }
    if (window_end > index_end) {
      window_end <- index_end
    }

    # Determine window for weights
    weight_window_start <- weight_center - (i - window_start)
    weight_window_end <- weight_center + (window_end - i)

    # Get the actual weights to use
    actual_weights <- weights[weight_window_start:weight_window_end]

    m <- sum(x[window_start:window_end] * actual_weights) / sum(actual_weights)
    output_data[[i]] <- m
  }
  output_data <- do.call("rbind", output_data)
  return(output_data)
}

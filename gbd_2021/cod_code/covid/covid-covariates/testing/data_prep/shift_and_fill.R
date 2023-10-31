### This function takes a vector of cumulative values and coverts to daily.
##   -assumes they are evenly spaced by day with "NA" for missing days of data
##   -redistribute == TRUE ->
##     -If a value is missing, it will go back in time and calculate the average daily count over the
##      "missing" data time period, putting that value in the middle of the "missing" series.
##     -Essentially taking the average slope
##   -redistribute == FALSE ->
##     -The function will just take a simple diff from the last day of observed data

shift_and_fill <- function(x, redistribute) {
  out <- rep(as.numeric(NA), length(x))
  # if the first element is missing replace with zero
  if (is.na(x[1])) {
    x[1] <- 0
  }
  # match first element of out vector to first element of in vector
  out[1] <- x[1]

  if (redistribute) {
    for (i in 2:length(out)) {
      j <- i - 1
      while (is.na(x[j])) {
        j <- j - 1
      }
      if (j == i - 1) {
        out[i] <- x[i] - x[j]
      } else {
        out[ceiling((i + j) / 2)] <- (x[i] - x[j]) / (i - j)
      }
    }
  } else if (!redistribute) {
    for (i in 2:length(out)) {
      j <- i - 1
      while (is.na(x[j])) {
        j <- j - 1
      }
      out[i] <- (x[i] - x[j])
    }
  }
  return(out)
}

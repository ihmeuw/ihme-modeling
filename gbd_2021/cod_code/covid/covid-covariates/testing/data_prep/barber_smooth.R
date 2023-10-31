
#' Kernel smooth as average of X neighbors
#' 
#' Substitute in linear extension of first neighbor using average diff of nearest 3 
#' for first and last date
#' 
#' @param vec Vector of positive values
#' @param n_neighbors Integer of number of nieghbors to smooth with
#' @param times Integer number of times to run the smoother
barber_smooth <- function(vec, n_neighbors, times) {
  if(any(vec <= 0)) {
    stop("Must pass positive values only")
  }
  X <- n_neighbors
  N <- length(vec)
  avg_mat <- diag(1, nrow = N)
  for (i in (seq(X) + 1)) {
    # Before
    avg_mat[i:N, 1:(N - i + 1)] <- avg_mat[i:N, 1:(N - i + 1)] + diag(1, nrow = (N - i + 1))

    # After
    avg_mat[1:(N - i + 1), i:N] <- avg_mat[1:(N - i + 1), i:N] + diag(1, nrow = (N - i + 1))
  }
  avg_mat <- avg_mat / rowSums(avg_mat)
  if (!all(rowSums(avg_mat) == 1)) {
    stop("Messed up kernel")
  }
  for (i in seq(times)) {
    smooth_vec <- avg_mat %*% log(vec)

    smooth_vec[N] <- smooth_vec[N - 1] + mean(smooth_vec[(N - 2):N] - smooth_vec[(N - 3):(N - 1)])
    smooth_vec[1] <- smooth_vec[2] - mean(smooth_vec[2:4] - smooth_vec[1:3])

    vec <- exp(smooth_vec)
  }

  return(vec)
}

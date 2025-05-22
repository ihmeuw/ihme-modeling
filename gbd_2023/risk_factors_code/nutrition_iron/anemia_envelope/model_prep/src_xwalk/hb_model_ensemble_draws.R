# ensemble distribution functions -----------------------------------------

#' Hemoglobin model ensemble distribution function used for imputing the
#' 2.5th and 97.5th percentiles of draws for missing `alt_risk_lower` and
#' `ref_risk_upper` values in BoP datasets, respectively
#'
#' @param num_draws Number of draws to be run with given mean and variance
#' @param mn Mean of hemoglobin distribution
#' @param vr Variance of hemoglobin distribution
#'
#' @note The draws are weighted 40% gamma and 60% mirrored Gumbel
#'
#' @return Sorted list of draws
#'
#' @export
hb_me_ensemble_draws <- function(num_draws, mn, vr) {
  gamma_w <- 0.4
  m_gum_w <- 0.6
  xmax <- 250

  params_gamma <- gamma_mv2p(mn, vr)
  params_mgumbel <- mgumbel_mv2p(mn, vr, xmax)

  gamma_dist <- stats::rgamma(
    n = num_draws,
    shape = params_gamma$shape,
    rate = params_gamma$rate
  )

  gumbel_dist <- rmgumbel(
    n = num_draws,
    alpha = params_mgumbel$alpha,
    scale = params_mgumbel$scale,
    xmax = xmax
  )
  gumbel_dist <- mirror_gumble_dist(
    mn = mn,
    dist_draws = gumbel_dist
  )

  hb_dist <- gamma_w * sort(gamma_dist) + m_gum_w * sort(gumbel_dist)

  return(hb_dist)
}

gamma_mv2p <- function(mn, vr) {
  return(list(shape = mn^2 / vr, rate = mn / vr))
}

mgumbel_mv2p <- function(mn, vr, xmax) {
  x <- list(
    alpha = xmax - mn + digamma(1) * sqrt(vr) * sqrt(6) / pi,
    scale = sqrt(vr) * sqrt(6) / pi
  )
  return(x)
}

rmgumbel <- function(n, alpha, scale, xmax) {
  mn <- alpha - scale * digamma(1)
  x <- VGAM::rgumbel(n, alpha + xmax - 2 * mn, scale)
  return(x)
}

mirror_gumble_dist <- function(mn, dist_draws) {
  diff_vec <- dist_draws - mn
  diff_vec <- diff_vec * (-1)
  return(mn + diff_vec)
}

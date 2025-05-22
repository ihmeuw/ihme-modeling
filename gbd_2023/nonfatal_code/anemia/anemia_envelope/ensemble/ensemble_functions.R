# Adapted from IHME's pdf_families.R script


# Gamma ---------------------------------------------------------------------------

pdf_gamma <- function(x, m, v) {
  stats::dgamma(x, shape = m^2 / v, rate = m / v)
}

# Inverse Gamma -------------------------------------------------------------------

sse_invgamma <- function(p, m, v) {
  (p[2] / (p[1] - 1) - m)^2 + (p[2]^2 / ((p[1] - 2) * (p[1] - 1)^2) - v)^2
}
pdf_invgamma <- function(x, m, v) {
  p <- abs(stats::optim(c(m, m * sqrt(v)), sse_invgamma, m = m, v = v)$par)
  actuar::dinvgamma(x, shape = p[1], scale = p[2])
}
pdf_invgamma <- purrr::possibly(pdf_invgamma, 0)

# Normal --------------------------------------------------------------------------

pdf_norm <- function(x, m, v) {
  stats::dnorm(x, mean = m, sd = sqrt(v))
}

# Log Normal ----------------------------------------------------------------------

pdf_lnorm <- function(x, m, v) {
  stats::dlnorm(
    x,
    meanlog = log(m / sqrt(1 + (v / (m^2)))),
    sdlog = sqrt(log(1 + (v / m^2)))
  )
}

# Exponential ---------------------------------------------------------------------

pdf_exp <- function(x, m, v) {
  stats::dexp(x, rate = 1 / m)
}

# Weibull -------------------------------------------------------------------------

sse_weibull <- function(p, m, v) {
  a <- gamma(1 + 1 / p[2])
  (p[1] * a - m)^2 + (p[1]^2 * (gamma(1 + 2 / p[2]) - a^2) - v)^2
}
pdf_weibull <- function(x, m, v) {
  p <- abs(stats::optim(c(m / sqrt(v), m), sse_weibull, m = m, v = v)$par)
  stats::dweibull(x, shape = p[2], scale = p[1])
}
pdf_weibull <- purrr::possibly(pdf_weibull, 0)

# Loglogistic ---------------------------------------------------------------------

sse_llogis <- function(p, m, v) {
  p <- abs(p)
  b <- pi / p[2]
  (p[1] * b / sin(b) - m)^2 + ((p[1]^2 * b) * (2 / sin(2 * b) - b / (sin(b)^2)) - v)^2
}
pdf_llogis <- function(x, m, v) {
  p <- abs(stats::optim(c(m, max(2, m)), sse_llogis, m = m, v = v)$par)
  actuar::dllogis(x, shape = p[2], scale = p[1])
}
pdf_llogis <- purrr::possibly(pdf_llogis, 0)

# Gumbel --------------------------------------------------------------------------

pdf_gumbel <- function(x, m, v) {
  scale <- sqrt(v) * sqrt(6) / pi
  actuar::dgumbel(
    x,
    alpha = m - 0.577215664901533 * scale, # 0.577... is  Euler's constant
    scale = scale
  )
}

# Main functions ------------------------------------------------------------------

get_pdf <- function(name, x, m, v, xmax) {
  pdf <- switch(
    name,
    exp = pdf_exp(x, m, v),
    gamma = pdf_gamma(x, m, v),
    invgamma = pdf_invgamma(x, m, v),
    llogis = pdf_llogis(x, m, v),
    gumbel = pdf_gumbel(x, m, v),
    weibull = pdf_weibull(x, m, v),
    lnorm = pdf_lnorm(x, m, v),
    norm = pdf_norm(x, m, v),
    mgamma = pdf_gamma(xmax - x, xmax - m, v), # Mirrored Gamma
    mgumbel = pdf_gumbel(xmax - x, xmax - m, v), # Mirrored Gumbel
    stop("Invalid distribution name")
  )
  pdf[is.infinite(pdf)] <- 0
  return(pdf)
}

get_cdf <- function(mean_val, variance, weights, xmin, xmax, x) {
  weighted_density <- 0
  for (name in names(weights)) {
    if(weights[[name]] > 0) {
      weighted_density <- weighted_density +
        weights[[name]] * get_pdf(name, x, mean_val, variance, xmax)
    }
  }
  return(cumsum(weighted_density / sum(weighted_density)))
}

get_sd_sse <- function(std_dev, draw_vals, weights, x, xmin, xmax,
                       threshold_weights, indices) {
  cdf <- get_cdf(draw_vals$mean_hemoglobin, std_dev^2, weights, xmin, xmax, x)
  sse <- 0
  for(i in names(indices)) {
    curr_prev <- max(cdf[indices[[i]]])
    sse <- sse +
      threshold_weights[[i]] * ((curr_prev - draw_vals[[i]]) ^ 2)
  }
  return(sse)
}

get_prev <- function(cdf, prev_name, indices) {
  value <- cdf[max(indices[[prev_name]])]
  if (is.nan(value)) {
    value <- 0
  }
  return(value)
}

get_ensemble_draw <- function(draw, weights, xmin, xmax, x, min_sd, max_sd, threshold_weights, indices) {
  std_dev <- stats::optim(
    runif(1, min = min_sd, max = max_sd),
    fn = get_sd_sse,
    draw_vals = draw,
    weights = weights,
    x = x,
    xmin = xmin,
    xmax = xmax,
    threshold_weights = threshold_weights,
    indices = indices,
    method = "Brent",
    lower = min_sd,
    upper = max_sd
  )$par
  cdf <- get_cdf(draw$mean, std_dev^2, weights, xmin, xmax, x)
  return(data.table::data.table(
    meanval = draw$mean_hemoglobin,
    stdev = std_dev,
    stgpr_total_anemia_prev = draw$total_anemia,
    stgpr_modsev_anemia_prev = draw$mod_sev_anemia,
    stgpr_severe_anemia_prev = draw$severe_anemia,
    est_prev_total = get_prev(cdf, "total_anemia", indices),
    est_prev_modsev = get_prev(cdf, "mod_sev_anemia", indices),
    est_prev_sev = get_prev(cdf, "severe_anemia", indices)
  ))
}

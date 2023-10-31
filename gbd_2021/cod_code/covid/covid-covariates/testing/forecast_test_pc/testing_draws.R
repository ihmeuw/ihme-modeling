### Project testing per capita

## Setup
Sys.umask("002")
Sys.setenv(MKL_VERBOSE = 0)
suppressMessages(library(data.table))
suppressMessages(library(parallel))
suppressMessages(library(forecast))
suppressMessages(library(MASS))
setDTthreads(1)

## install ihme.covid (always use newest version)
tmpinstall <- system(SYSTEM_COMMAND)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T)
##

## Arguments
if (interactive()) {
  user <- Sys.info()["user"]
  code_dir <- paste0("FILEPATH", user, "FILEPATH")
  release <- "2020_06_05.08"
  lsvid <- 680
  output_dir <- file.path("FILEPATH", release)
  seir_covariates_version <- "FILEPATH"
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--lsvid", type = "integer", required = TRUE, help = "location set version id to use")
INTERNAL_COMMENT)
  parser$add_argument("--seir-covariates-version", default = "best", help = "Version of seir-covariates. Defaults to 'best'. Pass a full YYYY_MM_DD.VV or 'latest' if you provide")
  args <- parser$parse_args()

  code_dir <- ihme.covid::get_script_dir()

  lsvid <- args$lsvid
  output_dir <- args$outputs_directory
  seir_covariates_version <- args$seir_covariates_version
  release <- basename(output_dir)
}
ncores <- 10
n_draws <- 1000
logit <- F
frontier <- 500
censor <- F
censor_date <- as.Date("2020-04-01")

## Paths
seir_testing_reference <- file.path(seir_covariates_version, "testing_reference.csv")
input_data_path <- file.path(output_dir, "data_smooth.csv")
first_case_path <- file.path(output_dir, "first_case_date.csv")
detailed_out_path <- file.path(output_dir, "forecast_raked_test_pc_detailed.csv")
simple_out_path <- file.path(output_dir, "forecast_raked_test_pc_simple.csv")
scenario_dict_out_path <- file.path(output_dir, "testing_scenario_dict.csv")
plot_scenarios_out_path <- file.path(output_dir, "forecast_test_pc.pdf")
plot_prod_comp_out_path <- file.path(output_dir, "plot_comp_w_previous.pdf")
plot_draws_path <- file.path(
  output_dir, 
  paste0(
    "plot_testing_draws_", 
    ifelse(logit, "logit", "linear"),
    ifelse(censor, "_censored", ""),
    ".pdf"
  )
)

## Functions
source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path(code_dir, "forecast_all_locs.R"))
source(file.path(code_dir, "rake_vals.R"))
source(file.path(code_dir, "gen_better_worse.R"))
source(file.path(code_dir, "diagnostics.R"))

## Tables
hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = 723)

## Prep testing data
dt <- fread(input_data_path)
dt[, date := as.Date(date)]
in_dt <- copy(dt)
dt <- dt[, .(location_id, location_name, date, daily_total, pop)]
dt[, total_pc_100 := daily_total / pop * 1e5]
dt <- merge(dt, hierarchy[, .(location_id, super_region_name, region_name)])



fcast_loc_draws <- function(loc_dt, n_draws = 1000, out_date = "2021-01-01") {
  loc_dt <- loc_dt[!is.na(total_pc_100)]
  loc_dt[, day := as.integer(date - min(date))]
  out_n <- as.integer(as.Date(out_date) - max(loc_dt$date))
  if(logit) {
    # Convert to proportion of frontier and logit transform
    loc_dt[, prop := total_pc_100 / frontier]
    loc_dt[, total_pc_100 := log(prop / (1 - prop))]
  }
  if(censor) {
    loc_dt <- loc_dt[date >= censor_date]
  }
  # Fit a linear model
  lm_fit <- lm(total_pc_100 ~ day, loc_dt)
  # Sample from the variance-covariance matrix
  vcov_mat <- vcov(lm_fit)
  # Ensure that expected slope is not negative
  if(lm_fit$coefficients[2] < 0) {
    lm_fit$coefficients[1] <- lm_fit$coefficients[1] + max(loc_dt$day) * lm_fit$coefficients[2]
    lm_fit$coefficients[2] <- 0
  }
  coef_mat <- MASS::mvrnorm(n = n_draws, mu = lm_fit$coefficients, Sigma = vcov_mat)
  # Generate a matrix of linear predictions
  X <- matrix(c(rep(1, out_n), seq((max(loc_dt$day) + 1), (max(loc_dt$day) + out_n))), nrow = out_n)
  pred_mat <- X %*% t(coef_mat)
  pred_mat <- sweep(pred_mat, 2, (pred_mat[1, ] - mean(pred_mat[1, ])))
  # Run a RW1 on the residuals (should these be w.r.t. the sampled predictions?)
  loc_dt[, resid := lm_fit$residuals]
  rw1 <- arima(loc_dt$resid, order = c(0, 1, 0), include.mean = T)
  rw1$x <- loc_dt$resid
  # Simulate from the RW
  fcast <- replicate(n_draws, simulate(rw1, nsim = out_n))
  # Package for output
  fcast_dt <- data.table(pred_mat + fcast)
  if(logit) {
    fcast_dt <- (exp(fcast_dt) / (1  + exp(fcast_dt))) * frontier
  }
  names(fcast_dt) <- paste0("draw_", 0:(n_draws - 1))
  fcast_dt[, date := max(loc_dt$date) + seq(out_n)]
  fcast_dt[, location_id := unique(loc_dt$location_id)]
  
  rep_past <- matrix(rep(loc_dt$total_pc_100, n_draws), ncol = n_draws)
  past_dt <- cbind(loc_dt[, .(location_id, date)], rep_past)
  names(past_dt)[3:(3 + n_draws - 1)] <- paste0("draw_", 0:(n_draws - 1))

  fcast_dt <- rbind(past_dt, fcast_dt)
  
  coef_dt <- data.table(
    location_id = unique(loc_dt$location_id),
    beta = lm_fit$coefficients[2], 
    sigma = rw1$sigma2
  )
  
  return(list(fcast_dt[], coef_dt))
}
loc_dt_list <- split(dt, by = "location_id")
message("Forecasting draws...")
fcast_list <- mclapply(loc_dt_list, fcast_loc_draws, n_draws = n_draws, mc.cores = ncores)

fcast_locs_dt <- rbindlist(
  lapply(fcast_list, `[[`, 1)
)

coef_dt <- rbindlist(
  lapply(fcast_list, `[[`, 2)
)

# Use national values for missing subnationals
missing_children <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(fcast_locs_dt$location_id))
parents <- unique(hierarchy[location_id %in% missing_children]$parent_id)
missing_children_dt <- rbindlist(lapply(parents, function(p) {
  parent_dt <- copy(fcast_locs_dt[location_id == p])
  all_children <- setdiff(hierarchy[parent_id == p]$location_id, p)
  impute_locs <- intersect(missing_children, all_children)
  rbindlist(lapply(impute_locs, function(i) {
    child_dt <- copy(parent_dt)
    child_dt[, location_id := i]
  }))
}))
fcast_locs_dt <- rbind(fcast_locs_dt, missing_children_dt)

## Generate regional/superregional average locations
coef_dt <- merge(coef_dt, hierarchy[, .(location_id, region_name, super_region_name)])
coef_summ <- coef_dt[, 
  .(
    beta_mean = mean(beta),
    beta_sd = sd(beta),
    sigma_mean = mean(sigma),
    sigma_sd = sd(sigma),
    N = .N
  ), by = .(region_name, super_region_name)
]
# Use super if there are less than two locations in a region
coef_summ_super <- coef_dt[, 
  .(
    beta_mean_super = mean(beta),
    beta_sd_super = sd(beta),
    sigma_mean_super = mean(sigma),
    sigma_sd_super = sd(sigma)
  ), by = super_region_name
]
region_dt <- merge(coef_summ, coef_summ_super)
region_dt[
  N < 2, 
  c("beta_mean", "beta_sd", "sigma_mean", "sigma_sd") :=
  .(beta_mean_super, beta_sd_super, sigma_mean_super, sigma_sd_super)
]

message("Imputing missing locations with a regional average...")
missing_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(fcast_locs_dt$location_id))
first_case_dt <- fread(first_case_path)
setnames(first_case_dt, "first_case_date", "date")
first_case_dt[, date := as.Date(date)]
regions <- unique(hierarchy[location_id %in% missing_locs]$region_name)
missing_dt <- rbindlist(lapply(regions, function(r) {
  print(r)
  all_region_locs <- hierarchy[region_name == r]$location_id
  region_locs <- setdiff(all_region_locs, missing_locs)
  region_missing_children <- hierarchy[region_name == r & location_id %in% missing_locs]$location_id
  region_coef <- region_dt[region_name == r]
  rbindlist(lapply(region_missing_children, function(c) {
    child_dt <- first_case_dt[location_id == c]
    if(nrow(child_dt) == 0) { # No first case date => min date of region
      child_dt <- data.table(
        location_id = c,
        date = min(first_case_dt[location_id %in% all_region_locs]$date)
      )
    }
    first_date <- child_dt$date
    extend_date <- as.Date("2021-01-01") - first_date + 1 # Make this date an argument
    beta_sample <- rnorm(n_draws, region_coef$beta_mean, 0)
    pred <- matrix(rep(seq(extend_date), n_draws), ncol = n_draws) %*% diag(beta_sample)
    # Get last day of data for region
    max_data_date <- max(dt[location_id %in% all_region_locs]$date)
    uncertain_dates <- seq(max_data_date, as.Date("2021-01-01"), by = "1 day")
    sigma_sample <- rnorm(n_draws * length(uncertain_dates), 0, region_coef$sigma_mean)
    rw_mat <- matrix(sigma_sample, ncol = n_draws)
    rw_mat <- apply(rw_mat, 2, cumsum)
    past_n <- dim(pred)[1] - dim(rw_mat)[1]
    zero_mat <- matrix(0, nrow = past_n, ncol = n_draws)
    rw_mat <- rbind(zero_mat, rw_mat)
    fcast_dt <- data.table(pred + rw_mat)
    names(fcast_dt) <- paste0("draw_", 0:(n_draws - 1))
    fcast_dt[, location_id := c]
    fcast_dt[, date := seq(as.Date(first_date), as.Date("2021-01-01"), by = "1 day")]

    return(fcast_dt)
  }))
}))

fcast_locs_dt <- rbind(fcast_locs_dt, missing_dt)

melt_dt <- melt.data.table(fcast_locs_dt, id.vars= c("date", "location_id"))

# Truncate at zero and frontier
melt_dt[value < 0, value := 0]
melt_dt[value > frontier & date > max(dt$date), value := frontier]

# Summarize
summ_dt <- melt_dt[, .(mean_value = mean(value, na.rm = T), lower = quantile(value, 0.025), upper = quantile(value, 0.975)), by = .(location_id, date)]

message("Plotting...")
pdf(plot_draws_path, width = 12, height = 8)
sorted <- ihme.covid::sort_hierarchy(hierarchy)[location_id %in% summ_dt$location_id]
pb <- txtProgressBar(max = length(sorted$location_id), style = 3)
for(i in seq(length(sorted$location_id))) {
  setTxtProgressBar(pb, i)
  loc <- sorted$location_id[i]
  gg <- ggplot() + 
        geom_point(data = dt[location_id == loc & !is.na(total_pc_100)], aes(x = date, y = total_pc_100)) +
        geom_ribbon(data = summ_dt[location_id == loc], aes(x = date, ymin = lower, ymax = upper), alpha = 0.5) +
        geom_line(data = summ_dt[location_id == loc], aes(x = date, y = mean_value), color = "red") +
        ggtitle(unique(hierarchy[location_id == loc]$location_name)) + 
        xlab("Date") + ylab("Tests per 100k") + 
        theme_bw()
  print(gg)
}
invisible(dev.off())
message(paste("\nPlots:", plot_draws_path))
message(paste("\nDone! Output directory:", output_dir))
pacman::p_load(data.table, openxlsx, ggplot2, plyr, parallel, dplyr, RMySQL, stringr)
library(msm, lib.loc = paste0("FILEPATH"))

summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

hazard_dt <- data.table(status = c("positive", "negative"),
                        mean = c(2.57, 1.33),
                        lower = c(1.11, 0.73),
                        upper = c(5.94, 2.43),
                        num = c(569.328, 1792.672))
draws <- paste0("draw_", 0:999)
hazard_dt[, se := (upper-lower)/3.92]

get_normal_draws <- function(n, dt){
  mean_n <- dt[, mean][n]
  log_mean <- log(mean_n)
  se_n <- dt[, se][n]
  log_se <- deltamethod(~log(x1), mean_n, se_n^2)
  draw_vector <- exp(rnorm(1000, mean = log_mean, sd = log_se))
  draw_dt <- as.data.table(t(as.matrix(draw_vector)))
  setnames(draw_dt, names(draw_dt), draws)
  return(draw_dt)
}

draws_dt <- rbindlist(lapply(1:nrow(hazard_dt), function(x) get_normal_draws(n = x, dt = hazard_dt)))
draws_dt <- cbind(hazard_dt, draws_dt)
draws_dt[, total_num := sum(num)]
draws_dt[, (draws) := lapply(.SD, function(x) x * num/total_num), .SDcols = draws]
final_dt <- summaries(draws_dt, draws)

sundstrom_dt <- data.table(status = c("negative", "positive"),
                           mean = c(0.9, 5.2), 
                           lower = c(0.4, 2.0),
                           upper = c(1.8, 14),
                           num = c(361, 182))
sundstrom_dt[, se := (upper-lower)/3.92]

sundstrom_draws <- rbindlist(lapply(1:nrow(sundstrom_dt), function(x) get_normal_draws(n = x, dt = sundstrom_dt)))
sundstrom_draws <- cbind(sundstrom_dt, sundstrom_draws)
sundstrom_draws[, total_num := sum(num)]
sundstrom_draws[, (draws) := lapply(.SD, function(x) sum(x * num/total_num)), .SDcols = draws]
final_sund_dt <- summaries(sundstrom_draws, draws)

tison_dt <- data.table(status = c("household", "inst"), 
                       mean = c(9.26, 1.29),
                       lower = c(2.2, 0.6),
                       upper = c(38.6, 18.9),
                       num = c(712, 229))

get_draws_better <- function(n, dt){
  mean_n <- dt[, mean][n]
  lower_n <- dt[, lower][n]
  upper_n <- dt[, upper][n]
  log_mean <- log(mean_n)
  log_lower <- log(lower_n)
  log_upper <- log(upper_n)
  log_se <- (log_upper-log_lower)/3.92
  draw_vector <- exp(rnorm(1000, mean = log_mean, sd = log_se))
  draw_dt <- as.data.table(t(as.matrix(draw_vector)))
  setnames(draw_dt, names(draw_dt), draws)
  return(draw_dt)
}

tison_draws <- rbindlist(lapply(1:nrow(tison_dt), function(x) get_draws_better(n = x, dt = tison_dt)))
tison_draws <- cbind(tison_dt, tison_draws)
tison_draws[, total_num := sum(num)]
tison_draws[, (draws) := lapply(.SD, function(x) sum(x * num/total_num)), .SDcols = draws]
final_tison_dt <- summaries(tison_draws, draws)


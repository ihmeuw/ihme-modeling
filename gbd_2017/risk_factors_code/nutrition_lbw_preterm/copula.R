
rm(list=ls())

print(Sys.time())

start.time <- proc.time()

os <- .Platform$OS.type
if (os=="windows") {
  j<- "J:/"
  h <-"H:/"
  
} else {
  j<- "/home/j/"
  h<-"/homes/hpm7/"
  
}

args <- commandArgs(trailingOnly = TRUE)

loc <- args[1]

library(data.table)
library(stringr)
library(copula)
library(VineCopula)
library(ggplot2)
library(parallel)
library(haven)
library(methods)
library(actuar)


source("FILEPATH/ihmeDistList.R")


dmgamma = function(x, shape, rate, XMAX)
{
  XMAX = XMAX 
  dgamma(XMAX - x, shape=shape, rate=rate)
}

pmgamma = function(q, shape, rate, XMAX, lower.tail=T) 
{ 
  1 - pgamma(XMAX - q, shape = shape, rate = rate)
}

qmgamma = function(p, shape, rate, XMAX) 
{ 
  XMAX - qgamma(1 - p, shape = shape, rate = rate)
} 


d_ensemble = function(x, 
                      exp_rate,	
                      gamma_rate,	gamma_shape,
                      invgamma_scale,	invgamma_shape,
                      lnorm_meanlog,	lnorm_sdlog,	
                      norm_mean,	norm_sd,	
                      weibull_scale,	weibull_shape,
                      weights, allXMIN, allXMAX){
  
  clean_weights <- copy(weights)
  setnames(clean_weights, names(weights), substr(names(weights), 4, 20))

  betasr_XMIN = 0
  
    clean_weights[["exp"]] * dexp(x, rate = exp_rate) +
    clean_weights[["gamma"]] * dgamma(x, shape = gamma_shape, rate = gamma_rate) +
    clean_weights[["invgamma"]] * dinvgamma(x, shape = invgamma_shape, scale = invgamma_scale) +
    clean_weights[["weibull"]] * dweibull(x, shape = weibull_shape, scale = weibull_scale) +
    clean_weights[["lnorm"]] * dlnorm(x, meanlog = lnorm_meanlog, sdlog = lnorm_sdlog) +
    clean_weights[["norm"]] * dnorm(x, mean = norm_mean, sd= norm_sd) 
  
  
  
}

p_ensemble = function(x, 
                      exp_rate, 
                      gamma_rate, gamma_shape,
                      invgamma_scale, invgamma_shape,
                      lnorm_meanlog,  lnorm_sdlog,  
                      norm_mean,  norm_sd,  
                      weibull_scale,  weibull_shape,
                      weights, allXMIN, allXMAX){
  
  clean_weights <- copy(weights)
  setnames(clean_weights, names(weights), substr(names(weights), 4, 20))
    
    clean_weights[["exp"]] * pexp(x, rate = exp_rate) +
    clean_weights[["gamma"]] * pgamma(x, shape = gamma_shape, rate = gamma_rate) +
    clean_weights[["invgamma"]] * pinvgamma(x, shape = invgamma_shape, scale = invgamma_scale) +
    clean_weights[["weibull"]] * pweibull(x, shape = weibull_shape, scale = weibull_scale) +
    clean_weights[["lnorm"]] * plnorm(x, meanlog = lnorm_meanlog, sdlog = lnorm_sdlog) +
    clean_weights[["norm"]] * pnorm(x, mean = norm_mean, sd= norm_sd) 
  
}

q_ensemble = function(x, 
                      exp_rate, 
                      gamma_rate, gamma_shape,
                      invgamma_scale, invgamma_shape,
                      lnorm_meanlog,  lnorm_sdlog,  
                      norm_mean,  norm_sd,  
                      weibull_scale,  weibull_shape,
                      weights, allXMIN, allXMAX){
  
  
  clean_weights <- copy(weights)
  setnames(clean_weights, names(weights), substr(names(weights), 4, 20))
  
    clean_weights[["exp"]] * qexp(x, rate = exp_rate) +
    clean_weights[["gamma"]] * qgamma(x, shape = gamma_shape, rate = gamma_rate) +
    clean_weights[["invgamma"]] * qinvgamma(x, shape = invgamma_shape, scale = invgamma_scale) +
    clean_weights[["weibull"]] * qweibull(x, shape = weibull_shape, scale = weibull_scale) +
    clean_weights[["lnorm"]] * qlnorm(x, meanlog = lnorm_meanlog, sdlog = lnorm_sdlog) +
    clean_weights[["norm"]] * qnorm(x, mean = norm_mean, sd= norm_sd) 
}


Corner_text <- function(text, location="topright"){
  legend(location,legend=text, bty ="n", pch=NA) 
}


stretch_optim_37 <- function(par, sim, ga_u_28, ga_u_37){
  
  ga_par <- par
  
  ga_threshold <- 37
  min_ga <- min(sim[, "ga"])
  ga_vector_u_37 <- sim[ga<ga_threshold, ga]
  
  ga_vector_u_37 <- unique(sim[ga<ga_threshold, ga])
  
  ga_vector_u_37 <- sort(ga_vector_u_37, decreasing = FALSE)
  
  ga_scalars <- ((ga_vector_u_37 - min_ga)/(ga_threshold - min_ga))^(ga_par)
  
  ga_dt <- data.table(ga = sort(ga_vector_u_37, decreasing = FALSE),
                      ga_scalars = sort(ga_scalars, decreasing = FALSE))
  
  sim <- merge(sim, ga_dt, all.x = T, by = c("ga"))
  
  sim[, old_ga := ga]
  sim[ga < ga_threshold, ga := ga * ga_scalars]
  
  e_ga_u_28 <- (nrow(sim[ga < 28, ]) / nrow(sim))
  
  sum(log(e_ga_u_28/ga_u_28))^2
  
}

stretch_return_37 <- function(par, sim){
  
  ga_par <- par
  
  ga_threshold <- 37
  min_ga <- min(sim[, "ga"])
  ga_vector_u_37 <- sim[ga<ga_threshold, ga]
  
  ga_vector_u_37 <- unique(sim[ga<ga_threshold, ga])
  
  ga_vector_u_37 <- sort(ga_vector_u_37, decreasing = FALSE)
  
  ga_scalars <- ((ga_vector_u_37 - min_ga)/(ga_threshold - min_ga))^(ga_par)
  
  ga_dt <- data.table(ga = sort(ga_vector_u_37, decreasing = FALSE),
                      ga_scalars = sort(ga_scalars, decreasing = FALSE))
  
  sim <- merge(sim, ga_dt, all.x = T, by = c("ga"))
  
  sim[, old_ga := ga]
  sim[ga < ga_threshold, ga := ga * ga_scalars]
  
  sim <- sim[, -c("ga_scalars")]
  
  return(sim)
  
}

stretch_optim_2500 <- function(par, sim, bw_u_2500){
  
  bw_par <- par
  
  bw_threshold <- 2500
  bw_stretch_pt <- 500
  
  min_bw <- min(sim[, "bw"])
  
  bw_vector_u_2500 <- sim[bw<bw_threshold, bw]
  
  bw_vector_u_2500 <- unique(sim[bw<bw_threshold, bw])
  
  bw_vector_u_2500 <- sort(bw_vector_u_2500, decreasing = FALSE)
  
  bw_scalars <- ((bw_vector_u_2500 - min_bw)/(bw_threshold - min_bw))^(bw_par)
  
  bw_dt <- data.table(bw = sort(bw_vector_u_2500, decreasing = FALSE),
                      bw_scalars = sort(bw_scalars, decreasing = FALSE))
  
  sim <- merge(sim, bw_dt, all.x = T, by = c("bw"))
  
  sim[, old_bw := bw]
  sim[bw < bw_threshold, bw := bw * bw_scalars]
 
  ratio_500_2500 <- 0.01634069  
  

  e_ratio_500_2500 <- nrow(sim[bw < bw_stretch_pt, ]) / nrow(sim[bw < 2500, ])
  
  
  
  sum(log(e_ratio_500_2500/ratio_500_2500))^2
  
  
}

stretch_return_2500 <- function(par, sim){
  
  bw_par <- par
  
  bw_threshold <- 2500
  bw_stretch_pt <- 500
  
  min_bw <- min(sim[, "bw"])
  
  bw_vector_u_2500 <- sim[bw<bw_threshold, bw]
  
  bw_vector_u_2500 <- unique(sim[bw<bw_threshold, bw])
  
  bw_vector_u_2500 <- sort(bw_vector_u_2500, decreasing = FALSE)
  
  bw_scalars <- ((bw_vector_u_2500 - min_bw)/(bw_threshold - min_bw))^(bw_par)
  
  bw_dt <- data.table(bw = sort(bw_vector_u_2500, decreasing = FALSE),
                      bw_scalars = sort(bw_scalars, decreasing = FALSE))
  
  sim <- merge(sim, bw_dt, all.x = T, by = c("bw"))
  
  sim[, old_bw := bw]
  sim[bw < bw_threshold, bw := bw * bw_scalars]
  
  sim <- sim[, -c("bw_scalars")]
  
  return(sim)
  
}


 

param_vars <- c("exp_rate",	"gamma_rate",	"gamma_shape", "invgamma_scale",	"invgamma_shape", "lnorm_meanlog",	"lnorm_sdlog",	"norm_mean",	"norm_sd",	"weibull_scale",	"weibull_shape")
weight_vars <- c("exp",	"gamma",	"invgamma",	"weibull",	"lnorm",	"norm")

params_template <- setNames(data.table(matrix(ncol = length(param_vars), nrow = 0)), param_vars)

ga_params <- copy(params_template)
setnames(ga_params, param_vars, paste0("ga_", param_vars))

bw_params <- copy(params_template)
setnames(bw_params, param_vars, paste0("bw_", param_vars))

weight_template <- setNames(data.table(matrix(ncol = length(weight_vars), nrow = 0)), weight_vars)

ga_weight_template <- copy(weight_template)
setnames(ga_weight_template, weight_vars, paste0("ga_", weight_vars))

bw_weight_template <- copy(weight_template)
setnames(bw_weight_template, weight_vars, paste0("bw_", weight_vars))

ga_weight_vars <- paste0("ga_", weight_vars)
bw_weight_vars <- paste0("bw_", weight_vars)

  
ga_ensemble <- readRDS(FILEPATH)
bw_ensemble <- readRDS(FILEPATH)
 
ga_ensemble <- rbindlist(list(ga_ensemble, ga_params), use.names = T, fill = T)
ga_ensemble[is.na(ga_ensemble), ] <- 1
 
ga_ensemble <- rbindlist(list(ga_ensemble, ga_weight_template), use.names = T, fill = T)
ga_ensemble[is.na(ga_ensemble), ] <- 0
 

 
bw_ensemble <- rbindlist(list(bw_ensemble,bw_params), use.names = T, fill = T)
bw_ensemble[is.na(bw_ensemble), ] <- 1

 
bw_ensemble <- rbindlist(list(bw_ensemble, bw_weight_template), use.names = T, fill = T)
bw_ensemble[is.na(bw_ensemble), ] <- 0

ga_bw_ensemble <- merge(ga_ensemble, bw_ensemble, by = c("location_id", "sex_id", "year_id", "age_group_id", "draw"), all = T)
 
ga_bw_ensemble[, draw := as.integer(draw)]
 

ga_bw_ensemble[, draw := seq_len(.N), by = list(sex_id, year_id, age_group_id, location_id)]

ga_bw_ensemble[, draw := draw - 1]

ga_bw_ensemble <- ga_bw_ensemble[draw < 100 ]

row_list <- split(ga_bw_ensemble, 1:nrow(ga_bw_ensemble))



simulate_copula <- function(location_id, sex_id, age_group_id, year_id, draw, ga_u_28, ga_u_32, ga_u_37, ga_mean,
                            ga_exp_rate,	
                            ga_gamma_rate,	ga_gamma_shape,
                            ga_invgamma_scale,	ga_invgamma_shape,
                            ga_lnorm_meanlog,	ga_lnorm_sdlog,	
                            ga_norm_mean,	ga_norm_sd,	
                            ga_weibull_scale,	ga_weibull_shape,
                            ga_weights, ga_XMIN, ga_XMAX,
                            bw_u_2500, bw_mean, 
                            bw_exp_rate,	
                            bw_gamma_rate,	bw_gamma_shape,
                            bw_invgamma_scale,	bw_invgamma_shape,
                            bw_lnorm_meanlog,	bw_lnorm_sdlog,	
                            bw_norm_mean,	bw_norm_sd,	
                            bw_weibull_scale,	bw_weibull_shape,
                            bw_weights, bw_XMIN, bw_XMAX){
  
  
  
  if(draw == 0){ print(paste(location_id, sex_id, year_id, draw)) }
  
  min_bw <- 0
  max_bw <- 6000
  min_ga <- 0
  max_ga <- 60
  
  copula_param <- 1.75  
  copula_param2 <- 1 
  copula_sample <- 100000  
  
  
  copula_dist <- mvdc(copula = surBB8Copula(param = c(copula_param, copula_param2)), margins = c("_ensemble", "_ensemble"),
                      paramMargins = list(list(exp_rate = ga_exp_rate,
                                               gamma_rate = ga_gamma_rate,
                                               gamma_shape = ga_gamma_shape,
                                               invgamma_scale = ga_invgamma_scale,
                                               invgamma_shape = ga_invgamma_shape,
                                               lnorm_meanlog = ga_lnorm_meanlog,
                                               lnorm_sdlog = ga_lnorm_sdlog,
                                               norm_mean = ga_norm_mean,	
                                               norm_sd = ga_norm_sd,	
                                               weibull_scale = ga_weibull_scale,
                                               weibull_shape = ga_weibull_shape,
                                               weights = ga_weights, allXMIN = ga_XMIN, allXMAX = ga_XMAX),
                                          list(exp_rate = bw_exp_rate,
                                               gamma_rate = bw_gamma_rate,
                                               gamma_shape = bw_gamma_shape,
                                               invgamma_scale = bw_invgamma_scale,
                                               invgamma_shape = bw_invgamma_shape,
                                               lnorm_meanlog = bw_lnorm_meanlog,
                                               lnorm_sdlog = bw_lnorm_sdlog,
                                               norm_mean = bw_norm_mean,	
                                               norm_sd = bw_norm_sd,	
                                               weibull_scale = bw_weibull_scale,
                                               weibull_shape = bw_weibull_shape,
                                               weights = bw_weights, allXMIN = bw_XMIN, allXMAX = bw_XMAX)))
  
  
   
  sim <- data.table(rMvdc(copula_sample, copula_dist))
  setnames(sim, c("V1", "V2"), c("ga", "bw"))
  
  t <- try (
    ga_par <- optim(0.5, fn = stretch_optim_37, sim = sim, ga_u_28 = ga_u_28, ga_u_37 = ga_u_37, method = "Brent", lower = 0.01, upper = 2)$par, silent = T)

  if(class(t) == "try-error"){
    ga_par = 0
  }
  
  t2 <- try (
    bw_par  <- optim(1, fn = stretch_optim_2500, sim = sim, bw_u_2500 = bw_u_2500, method = "Brent", lower = 0.01, upper = 3)$par, silent = T)

  if(class(t2) == "try-error"){
    bw_par = 0
  }

   
  sim <- stretch_return_37(par = ga_par, sim)
  
  sim <- stretch_return_2500(par = bw_par, sim)
   
  sim <- sim[, -c("old_bw", "old_ga")]
  
  sim_info <- data.table(location_id = location_id, sex_id = sex_id, age_group_id = age_group_id, year_id = year_id, draw = draw)
  
  sim <- cbind(sim_info, sim)
  
  sim <- data.table(sim)
  
  return(sim)
  
  
  
}


slots <-15
if (os=="windows") { cores_to_use = 1  } else {    cores_to_use = ifelse(grepl('Intel', system("cat /proc/cpuinfo | grep \'name\'| uniq", inter = T)), floor(slots * .86), floor(slots*.64)) }


sim_all <- mclapply( row_list, function(x) simulate_copula(location_id = x$location_id, sex_id = x$sex_id, age_group_id = x$age_group_id, year_id = x$year_id, draw = x$draw,
                                                                     ga_u_28 = x$ga_u_28_, ga_u_32 = x$ga_u_32_, ga_u_37 = x$ga_u_37_, ga_mean = x$ga_mean_, 
                                                                     ga_exp_rate = x$ga_exp_rate,
                                                                     ga_gamma_rate = x$ga_gamma_rate, ga_gamma_shape = x$ga_gamma_shape,
                                                                     ga_invgamma_scale = x$ga_invgamma_scale, ga_invgamma_shape = x$ga_invgamma_shape,
                                                                     ga_lnorm_meanlog = x$ga_lnorm_meanlog, ga_lnorm_sdlog = x$ga_lnorm_sdlog,
                                                                     ga_norm_mean = x$ga_norm_mean, ga_norm_sd = x$ga_norm_sd,
                                                                     ga_weibull_scale = x$ga_weibull_scale, ga_weibull_shape = x$ga_weibull_shape,
                                                                     ga_weights = x[, ga_weight_vars, with = F],
                                                                     ga_XMIN = x$ga_XMIN, ga_XMAX = x$ga_XMAX,
                                                                     bw_u_2500 = x$bw_u_2500_, bw_mean = x$bw_mean_, 
                                                                     bw_exp_rate = x$bw_exp_rate,
                                                                     bw_gamma_rate = x$bw_gamma_rate, bw_gamma_shape = x$bw_gamma_shape,
                                                                     bw_invgamma_scale = x$bw_invgamma_scale, bw_invgamma_shape = x$bw_invgamma_shape,
                                                                     bw_lnorm_meanlog = x$bw_lnorm_meanlog, bw_lnorm_sdlog = x$bw_lnorm_sdlog,
                                                                     bw_norm_mean = x$bw_norm_mean, bw_norm_sd = x$bw_norm_sd,
                                                                     bw_weibull_scale = x$bw_weibull_scale, bw_weibull_shape = x$bw_weibull_shape,
                                                                     bw_weights = x[, bw_weight_vars, with = F],
                                                                     bw_XMIN = x$bw_XMIN, bw_XMAX = x$bw_XMAX),
                                                                     mc.cores = cores_to_use ) %>% try %>% rbindlist(use.names = T, fill =T)


 
saveRDS(sim_all, FILEPATH)
 
print(Sys.time())

job.runtime <- proc.time() - start.time
job.runtime <- job.runtime[3]

print(paste0("Time elapsed:", job.runtime))

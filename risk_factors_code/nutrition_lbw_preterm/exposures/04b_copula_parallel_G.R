## Joint distribution modeling using copulas


rm(list=ls())

print(Sys.time())

start.time <- proc.time()

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  
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



###############################################################################
## DEFINE D/P/Q FUNCTIONS FOR EACH COMPONENT DISTRIBUTION
## Source the d, p, and q functions for each distribution. Write out the d/p/q functions for those that require XMIN or XMAX

source("FILEPATH")

dgumbel <- function(x,alpha,scale) {
  z <- (x - alpha)/scale
  1/scale*exp( -(z + exp(-z)) )
}


pgumbel <- function(q,alpha,scale) exp(-exp(-(q-alpha)/scale))

qgumbel <- function(p,alpha,scale) alpha-(scale*log(-log(p)))

dbetasr = function(x, shape1, shape2, XMIN, XMAX)
{ 

  dbeta((x-XMIN)/(XMAX-XMIN), shape1, shape2)/(XMAX-XMIN) 
}

pbetasr = function(q, shape1, shape2, lt, XMIN, XMAX) 
{ 
  pbeta((q-XMIN)/(XMAX-XMIN), shape1, shape2, lower.tail=TRUE) 
}

qbetasr = function(p, shape1, shape2, XMIN, XMAX)
{
  
  qbeta(abs(p-XMIN)/XMAX, shape1, shape2)/(XMAX-XMIN) 
}


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

dmgumbel = function(x, alpha, scale, XMAX)
{ 
  
  XMAX = XMAX 
  dgumbel(XMAX - x, alpha = alpha, scale = scale)
}

pmgumbel = function(q, alpha, scale, lower.tail = T, XMAX) ## Set lower tail = T
{ 

  1 - pgumbel(XMAX - q, alpha = alpha, scale = scale)
}

qmgumbel = function(p, alpha, scale, XMAX)
{ 
  XMAX - qgumbel(1 - p, alpha = alpha, scale = scale)
} 



###############################################################################
## DEFINE D/P/Q FUNCTION FOR THE ENSEMBLE DISTRIBUTION
## weight * d/p/q of component distribution


d_ensemble = function(x, betasr_XMAX, betasr_XMIN,	betasr_shape1,	betasr_shape2,	
                      exp_rate,	gamma_rate,	gamma_shape,
                      gumbel_alpha,	gumbel_scale,	invgamma_scale,	invgamma_shape,
                      llogis_scale,	llogis_shape,	lnorm_meanlog,	lnorm_sdlog,	
                      mgamma_XMAX,	mgamma_XMIN,	mgamma_rate,	mgamma_shape,	
                      mgumbel_XMAX,	mgumbel_XMIN,	mgumbel_alpha,	mgumbel_scale,	
                      norm_mean,	norm_sd,	weibull_scale,	weibull_shape,
                      invweibull_scale, invweibull_shape, weights, allXMIN, allXMAX){
  
  clean_weights <- copy(weights)
  setnames(clean_weights, names(weights), substr(names(weights), 4, 20))
  
  betasr_XMIN = 0
  
  clean_weights[["exp"]] * dexp(x, rate = exp_rate) +
    clean_weights[["gamma"]] * dgamma(x, shape = gamma_shape, rate = gamma_rate) +
    clean_weights[["invgamma"]] * dinvgamma(x, shape = invgamma_shape, scale = invgamma_scale) +
    clean_weights[["llogis"]] * dllogis(x,shape=llogis_shape, scale=llogis_scale) +
    clean_weights[["gumbel"]] * dgumbel(x, alpha = gumbel_alpha, scale=gumbel_scale) +
    clean_weights[["invweibull"]] * dinvweibull(x,shape=invweibull_shape,scale=invweibull_scale) +
    clean_weights[["weibull"]] * dweibull(x, shape = weibull_shape, scale = weibull_scale) +
    clean_weights[["lnorm"]] * dlnorm(x, meanlog = lnorm_meanlog, sdlog = lnorm_sdlog) +
    clean_weights[["norm"]] * dnorm(x, mean = norm_mean, sd= norm_sd) +
    clean_weights[["betasr"]] * dbetasr(x, shape1 = betasr_shape1, shape2 = betasr_shape2, XMIN = betasr_XMIN, XMAX = betasr_XMAX) +
    clean_weights[["mgamma"]] * dmgamma(x, shape = mgamma_shape, rate = mgamma_rate, XMAX = mgamma_XMAX) +
    clean_weights[["mgumbel"]] * dmgumbel(x, alpha = mgumbel_alpha, scale = mgumbel_scale, XMAX = mgumbel_XMAX)
  
  
  
}

p_ensemble = function(x, betasr_XMAX, betasr_XMIN,	betasr_shape1,	betasr_shape2,	
                      exp_rate,	gamma_rate,	gamma_shape,
                      gumbel_alpha,	gumbel_scale,	invgamma_scale,	invgamma_shape,
                      llogis_scale,	llogis_shape,	lnorm_meanlog,	lnorm_sdlog,	
                      mgamma_XMAX,	mgamma_XMIN,	mgamma_rate,	mgamma_shape,	
                      mgumbel_XMAX,	mgumbel_XMIN,	mgumbel_alpha,	mgumbel_scale,	
                      norm_mean,	norm_sd,	weibull_scale,	weibull_shape,
                      invweibull_scale, invweibull_shape, weights, allXMIN, allXMAX){
  
  clean_weights <- copy(weights)
  setnames(clean_weights, names(weights), substr(names(weights), 4, 20))
  
  betasr_XMIN = 0
  
  clean_weights[["exp"]] * pexp(x, rate = exp_rate) +
    clean_weights[["gamma"]] * pgamma(x, shape = gamma_shape, rate = gamma_rate) +
    clean_weights[["invgamma"]] * pinvgamma(x, shape = invgamma_shape, scale = invgamma_scale) +
    clean_weights[["llogis"]] * pllogis(x,shape=llogis_shape, scale=llogis_scale) +
    clean_weights[["gumbel"]] * pgumbel(x, alpha = gumbel_alpha, scale=gumbel_scale) +
    clean_weights[["invweibull"]] * pinvweibull(x,shape=invweibull_shape,scale=invweibull_scale) +
    clean_weights[["weibull"]] * pweibull(x, shape = weibull_shape, scale = weibull_scale) +
    clean_weights[["lnorm"]] * plnorm(x, meanlog = lnorm_meanlog, sdlog = lnorm_sdlog) +
    clean_weights[["norm"]] * pnorm(x, mean = norm_mean, sd= norm_sd) +
    clean_weights[["betasr"]] * pbetasr(x, shape1 = betasr_shape1, shape2 = betasr_shape2, XMIN = betasr_XMIN, XMAX = betasr_XMAX) +
    clean_weights[["mgamma"]] * pmgamma(x, shape = mgamma_shape, rate = mgamma_rate, XMAX = mgamma_XMAX) +
    clean_weights[["mgumbel"]] * pmgumbel(x, alpha = mgumbel_alpha, scale = mgumbel_scale, XMAX = mgumbel_XMAX)
  
}

q_ensemble = function(x, betasr_XMAX, betasr_XMIN,	betasr_shape1,	betasr_shape2,	
                      exp_rate,	gamma_rate,	gamma_shape,
                      gumbel_alpha,	gumbel_scale,	invgamma_scale,	invgamma_shape,
                      llogis_scale,	llogis_shape,	lnorm_meanlog,	lnorm_sdlog,	
                      mgamma_XMAX,	mgamma_XMIN,	mgamma_rate,	mgamma_shape,	
                      mgumbel_XMAX,	mgumbel_XMIN,	mgumbel_alpha,	mgumbel_scale,	
                      norm_mean,	norm_sd,	weibull_scale,	weibull_shape,
                      invweibull_scale, invweibull_shape, weights, allXMIN, allXMAX){
  
  
  clean_weights <- copy(weights)
  setnames(clean_weights, names(weights), substr(names(weights), 4, 20))
  
  betasr_XMIN = 0
  
  clean_weights[["exp"]] * qexp(x, rate = exp_rate) +
    clean_weights[["gamma"]] * qgamma(x, shape = gamma_shape, rate = gamma_rate) +
    clean_weights[["invgamma"]] * qinvgamma(x, shape = invgamma_shape, scale = invgamma_scale) +
    clean_weights[["llogis"]] * qllogis(x,shape=llogis_shape, scale=llogis_scale) +
    clean_weights[["gumbel"]] * qgumbel(x, alpha = gumbel_alpha, scale=gumbel_scale) +
    clean_weights[["invweibull"]] * qinvweibull(x,shape=invweibull_shape,scale=invweibull_scale) +
    clean_weights[["weibull"]] * qweibull(x, shape = weibull_shape, scale = weibull_scale) +
    clean_weights[["lnorm"]] * qlnorm(x, meanlog = lnorm_meanlog, sdlog = lnorm_sdlog) +
    clean_weights[["norm"]] * qnorm(x, mean = norm_mean, sd= norm_sd) +
    clean_weights[["betasr"]] * qbetasr(x, shape1 = betasr_shape1, shape2 = betasr_shape2, XMIN = betasr_XMIN, XMAX = betasr_XMAX) +
    clean_weights[["mgamma"]] * qmgamma(x, shape = mgamma_shape, rate = mgamma_rate, XMAX = mgamma_XMAX) +
    clean_weights[["mgumbel"]] * qmgumbel(x, alpha = mgumbel_alpha, scale = mgumbel_scale, XMAX = mgumbel_XMAX)
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
  
  ratio_500_2500 <- 0.01634069 ## Using ratio of 500 / 2500 from microdata
  
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




#############################################
##
## RUN CODE
##
############################################

## Create template of all parameters and weights



param_vars <- c("betasr_XMAX", "betasr_XMIN", "betasr_shape1",	"betasr_shape2",	"exp_rate",	"gamma_rate",	"gamma_shape",	"gumbel_alpha",	"gumbel_scale",	"invgamma_scale",	"invgamma_shape",	"invweibull_scale",	"invweibull_shape", "llogis_scale",	"llogis_shape",	"lnorm_meanlog",	"lnorm_sdlog",	"mgamma_XMAX",	"mgamma_XMIN",	"mgamma_rate",	"mgamma_shape", "mgumbel_XMAX",	"mgumbel_XMIN",	"mgumbel_alpha",	"mgumbel_scale",	"norm_mean",	"norm_sd",	"weibull_scale",	"weibull_shape")
weight_vars <- c("exp",	"gamma",	"invgamma", "llogis",	"gumbel",	"invweibull",	"weibull",	"lnorm",	"norm",	"betasr",	"mgamma",	"mgumbel")

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


ga_ensemble <- readRDS("FILEPATH")
bw_ensemble <- readRDS("FILEPATH")


############################################
## Load Gestational Age Parameters & clean

## Add missing params & set to 1
ga_ensemble <- rbindlist(list(ga_ensemble, ga_params), use.names = T, fill = T)
ga_ensemble[is.na(ga_ensemble), ] <- 1

## Add missing weights & set to 0
ga_ensemble <- rbindlist(list(ga_ensemble, ga_weight_template), use.names = T, fill = T)
ga_ensemble[is.na(ga_ensemble), ] <- 0


############################################
## Load Birthweight Parameters & clean

## Add missing params & set to 1
bw_ensemble <- rbindlist(list(bw_ensemble,bw_params), use.names = T, fill = T)
bw_ensemble[is.na(bw_ensemble), ] <- 1

## Add missing weights & set to 0
bw_ensemble <- rbindlist(list(bw_ensemble, bw_weight_template), use.names = T, fill = T)
bw_ensemble[is.na(bw_ensemble), ] <- 0

ga_bw_ensemble <- merge(ga_ensemble, bw_ensemble, by = c("location_id", "sex_id", "year_id", "age_group_id", "draw"), all = T)


ga_bw_ensemble[, draw := as.integer(draw)]



row_list <- split(ga_bw_ensemble, 1:nrow(ga_bw_ensemble))



#######################################################



simulate_copula <- function(location_id, sex_id, age_group_id, year_id, draw, ga_u_28, ga_u_32, ga_u_37, ga_mean,
                            ga_betasr_XMAX, ga_betasr_XMIN,	ga_betasr_shape1,	ga_betasr_shape2,	
                            ga_exp_rate,	ga_gamma_rate,	ga_gamma_shape,
                            ga_gumbel_alpha,	ga_gumbel_scale,	ga_invgamma_scale,	ga_invgamma_shape,
                            ga_llogis_scale,	ga_llogis_shape,	ga_lnorm_meanlog,	ga_lnorm_sdlog,	
                            ga_mgamma_XMAX,	ga_mgamma_XMIN,	ga_mgamma_rate,	ga_mgamma_shape,	
                            ga_mgumbel_XMAX,	ga_mgumbel_XMIN,	ga_mgumbel_alpha,	ga_mgumbel_scale,	
                            ga_norm_mean,	ga_norm_sd,	ga_weibull_scale,	ga_weibull_shape,
                            ga_invweibull_scale, ga_invweibull_shape, ga_weights, ga_XMIN, ga_XMAX,
                            bw_u_2500, bw_mean, 
                            bw_betasr_XMAX, bw_betasr_XMIN,	bw_betasr_shape1,	bw_betasr_shape2,	
                            bw_exp_rate,	bw_gamma_rate,	bw_gamma_shape,
                            bw_gumbel_alpha,	bw_gumbel_scale,	bw_invgamma_scale,	bw_invgamma_shape,
                            bw_llogis_scale,	bw_llogis_shape,	bw_lnorm_meanlog,	bw_lnorm_sdlog,	
                            bw_mgamma_XMAX,	bw_mgamma_XMIN,	bw_mgamma_rate,	bw_mgamma_shape,	
                            bw_mgumbel_XMAX,	bw_mgumbel_XMIN,	bw_mgumbel_alpha,	bw_mgumbel_scale,	
                            bw_norm_mean,	bw_norm_sd,	bw_weibull_scale,	bw_weibull_shape,
                            bw_invweibull_scale, bw_invweibull_shape, bw_weights, bw_XMIN, bw_XMAX){
  
  
  
  if(draw == 0){ print(paste(location_id, sex_id, year_id, draw)) }
  
  min_bw <- 0
  max_bw <- 6000
  min_ga <- 0
  max_ga <- 60
  
  copula_param <- 1.75
  copula_param2 <- 1 
  copula_sample <- 100000 
  
  
  
  copula_dist <- mvdc(copula = surBB8Copula(param = c(copula_param, copula_param2)), margins = c("_ensemble", "_ensemble"),
                      paramMargins = list(list(betasr_XMAX = ga_betasr_XMAX,
                                               betasr_XMIN = ga_betasr_XMIN,
                                               betasr_shape1 = ga_betasr_shape1,
                                               betasr_shape2 = ga_betasr_shape2,
                                               exp_rate = ga_exp_rate,
                                               gamma_rate = ga_gamma_rate,
                                               gamma_shape = ga_gamma_shape,
                                               gumbel_alpha = ga_gumbel_alpha,
                                               gumbel_scale = ga_gumbel_scale,
                                               invgamma_scale = ga_invgamma_scale,
                                               invgamma_shape = ga_invgamma_shape,
                                               llogis_scale = ga_llogis_scale,
                                               llogis_shape = ga_llogis_shape,
                                               lnorm_meanlog = ga_lnorm_meanlog,
                                               lnorm_sdlog = ga_lnorm_sdlog,
                                               mgamma_XMAX = ga_mgamma_XMAX,
                                               mgamma_XMIN = ga_mgamma_XMIN,
                                               mgamma_rate = ga_mgamma_rate,
                                               mgamma_shape = ga_mgamma_shape,
                                               mgumbel_XMAX = ga_mgumbel_XMAX,
                                               mgumbel_XMIN = ga_mgumbel_XMIN,
                                               mgumbel_alpha = ga_mgumbel_alpha,	mgumbel_scale = ga_mgumbel_scale,
                                               norm_mean = ga_norm_mean,	norm_sd = ga_norm_sd,	weibull_scale = ga_weibull_scale,
                                               weibull_shape = ga_weibull_shape,
                                               invweibull_scale = ga_invweibull_scale, invweibull_shape = ga_invweibull_shape, weights = ga_weights, allXMIN = ga_XMIN, allXMAX = ga_XMAX),
                                          list(betasr_XMAX = bw_betasr_XMAX,
                                               betasr_XMIN = bw_betasr_XMIN,
                                               betasr_shape1 = bw_betasr_shape1,
                                               betasr_shape2 = bw_betasr_shape2,
                                               exp_rate = bw_exp_rate,
                                               gamma_rate = bw_gamma_rate,
                                               gamma_shape = bw_gamma_shape,
                                               gumbel_alpha = bw_gumbel_alpha,
                                               gumbel_scale = bw_gumbel_scale,
                                               invgamma_scale = bw_invgamma_scale,
                                               invgamma_shape = bw_invgamma_shape,
                                               llogis_scale = bw_llogis_scale,
                                               llogis_shape = bw_llogis_shape,
                                               lnorm_meanlog = bw_lnorm_meanlog,
                                               lnorm_sdlog = bw_lnorm_sdlog,
                                               mgamma_XMAX = bw_mgamma_XMAX,
                                               mgamma_XMIN = bw_mgamma_XMIN,
                                               mgamma_rate = bw_mgamma_rate,
                                               mgamma_shape = bw_mgamma_shape,
                                               mgumbel_XMAX = bw_mgumbel_XMAX,
                                               mgumbel_XMIN = bw_mgumbel_XMIN,
                                               mgumbel_alpha = bw_mgumbel_alpha,	mgumbel_scale = bw_mgumbel_scale,
                                               norm_mean = bw_norm_mean,	norm_sd = bw_norm_sd,	weibull_scale = bw_weibull_scale,
                                               weibull_shape = bw_weibull_shape,
                                               invweibull_scale = bw_invweibull_scale, invweibull_shape = bw_invweibull_shape, weights = bw_weights, allXMIN = bw_XMIN, allXMAX = bw_XMAX)))
  
  
  
  
  
  store_sim_template <- fread("FILEPATH")
  store_sim <- store_sim_template[, list(modelable_entity_name)]
  
  gestweeklabels <- c("[0, 20)", "[20, 22)", "[22, 24)", "[24, 26)", "[26, 28)", "[28, 30)", "[30, 32)", "[32, 34)", "[34, 36)", "[36, 37)","[37, 38)", "[38, 40)", "[40, 42)", "[42, )")
  
  gestweeklevels <- c("-1000", "20", "22", "24", "26", "28", "30", "32", "34", "36", "37", "38", "40", "42", "100")
  
  bwlabels <-  c("[0, 500)", "[500, 1000)", "[1000, 1500)",
                 "[1500, 2000)", "[2000, 2500)", "[2500, 3000)", "[3000, 3500)",
                 "[3500, 4000)", "[4000, 4500)", "[4500, 5000)","[5000 )")
  
  bwlevels <- c("-5000", "500", "1000", "1500", "2000", "2500", "3000", "3500", "4000", "4500", "5000", "10000")
  
  ## 100000 draws Output is two columns of gestational ages and birthweights
  sim <- data.table(rMvdc(copula_sample, copula_dist))
  setnames(sim, c("V1", "V2"), c("ga", "bw"))
  
  
  t <- try (
    ga_par <- optim(0.5, fn = stretch_optim_37, sim = sim, ga_u_28 = ga_u_28, ga_u_37 = ga_u_37, method = "Brent", lower = 0.01, upper = 2)$par
    , silent = T)
  
  
  if(class(t) == "try-error"){
    ga_par = 0
  }
  
  
  
  t2 <- try (
    bw_par <- optim(1, fn = stretch_optim_2500, sim = sim, bw_u_2500 = bw_u_2500, method = "Brent", lower = 0.01, upper = 3)$par
    , silent = T)
  
  
  if(class(t2) == "try-error"){
    bw_par = 0
  }
  
  
  
  
  sim <- stretch_return_37(par = ga_par, sim)
  
  

  sim <- stretch_return_2500(par = bw_par, sim)
  
  
  
  if(sex_id == 1 & draw == 0 & year_id == 1990){
    
    ga_curve <- data.table(x = seq(min_ga, max_ga, 0.1), y = d_ensemble(seq(min_ga, max_ga, 0.1),
                                                                        ga_betasr_XMAX, ga_betasr_XMIN,	ga_betasr_shape1,	ga_betasr_shape2,
                                                                        ga_exp_rate,	ga_gamma_rate,	ga_gamma_shape,
                                                                        ga_gumbel_alpha,	ga_gumbel_scale,	ga_invgamma_scale,	ga_invgamma_shape,
                                                                        ga_llogis_scale,	ga_llogis_shape,	ga_lnorm_meanlog,	ga_lnorm_sdlog,
                                                                        ga_mgamma_XMAX,	ga_mgamma_XMIN,	ga_mgamma_rate,	ga_mgamma_shape,
                                                                        ga_mgumbel_XMAX,	ga_mgumbel_XMIN,	ga_mgumbel_alpha,	ga_mgumbel_scale,
                                                                        ga_norm_mean,	ga_norm_sd,	ga_weibull_scale,	ga_weibull_shape,
                                                                        ga_invweibull_scale, ga_invweibull_shape, ga_weights, ga_XMIN, ga_XMAX))
    
    
    
    bw_curve <- data.table(x = seq(min_bw, max_bw, 1), y = d_ensemble(seq(min_bw, max_bw, 1),
                                                                      bw_betasr_XMAX, bw_betasr_XMIN,	bw_betasr_shape1,	bw_betasr_shape2,
                                                                      bw_exp_rate,	bw_gamma_rate,	bw_gamma_shape,
                                                                      bw_gumbel_alpha,	bw_gumbel_scale,	bw_invgamma_scale,	bw_invgamma_shape,
                                                                      bw_llogis_scale,	bw_llogis_shape,	bw_lnorm_meanlog,	bw_lnorm_sdlog,
                                                                      bw_mgamma_XMAX,	bw_mgamma_XMIN,	bw_mgamma_rate,	bw_mgamma_shape,
                                                                      bw_mgumbel_XMAX,	bw_mgumbel_XMIN,	bw_mgumbel_alpha,	bw_mgumbel_scale,
                                                                      bw_norm_mean,	bw_norm_sd,	bw_weibull_scale,	bw_weibull_shape,
                                                                      bw_invweibull_scale, bw_invweibull_shape, bw_weights, bw_XMIN, bw_XMAX))
    
    
    
    
    pdf("FILEPATH")
    
    plot(x = ga_curve$x, y = ga_curve$y, col = "red", type = "l", xlab = "Gestational Age", ylab = "Density", xlim =c(0,60), ylim =c(0, 0.3))
    
    title(paste("Gestational Age - Location:", location_id, ",", year_id, "Sex:", sex_id, "Draw:", draw))
    Corner_text(text=paste("DisMod <28 prev:", round(ga_u_28, 5),
                           "\nEnsemble <28 prev:" , round(integrate(d_ensemble, lower = 0, upper = 28,
                                                                    betasr_XMAX = ga_betasr_XMAX,
                                                                    betasr_XMIN = ga_betasr_XMIN,
                                                                    betasr_shape1 = ga_betasr_shape1,
                                                                    betasr_shape2 = ga_betasr_shape2,
                                                                    exp_rate = ga_exp_rate,
                                                                    gamma_rate = ga_gamma_rate,
                                                                    gamma_shape = ga_gamma_shape,
                                                                    gumbel_alpha = ga_gumbel_alpha,
                                                                    gumbel_scale = ga_gumbel_scale,
                                                                    invgamma_scale = ga_invgamma_scale,
                                                                    invgamma_shape = ga_invgamma_shape,
                                                                    llogis_scale = ga_llogis_scale,
                                                                    llogis_shape = ga_llogis_shape,
                                                                    lnorm_meanlog = ga_lnorm_meanlog,
                                                                    lnorm_sdlog = ga_lnorm_sdlog,
                                                                    mgamma_XMAX = ga_mgamma_XMAX,
                                                                    mgamma_XMIN = ga_mgamma_XMIN,
                                                                    mgamma_rate = ga_mgamma_rate,
                                                                    mgamma_shape = ga_mgamma_shape,
                                                                    mgumbel_XMAX = ga_mgumbel_XMAX,
                                                                    mgumbel_XMIN = ga_mgumbel_XMIN,
                                                                    mgumbel_alpha = ga_mgumbel_alpha,	mgumbel_scale = ga_mgumbel_scale,
                                                                    norm_mean = ga_norm_mean,	norm_sd = ga_norm_sd,	weibull_scale = ga_weibull_scale,
                                                                    weibull_shape = ga_weibull_shape,
                                                                    invweibull_scale = ga_invweibull_scale, invweibull_shape = ga_invweibull_shape, weights = ga_weights, allXMIN = ga_XMIN, allXMAX = ga_XMAX)$value, 5),
                           "\nDisMod <32 prev:", round(ga_u_32, 5),
                           "\nEnsemble <32 prev:" , round(integrate(d_ensemble, lower = 0, upper = 32,
                                                                    betasr_XMAX = ga_betasr_XMAX,
                                                                    betasr_XMIN = ga_betasr_XMIN,
                                                                    betasr_shape1 = ga_betasr_shape1,
                                                                    betasr_shape2 = ga_betasr_shape2,
                                                                    exp_rate = ga_exp_rate,
                                                                    gamma_rate = ga_gamma_rate,
                                                                    gamma_shape = ga_gamma_shape,
                                                                    gumbel_alpha = ga_gumbel_alpha,
                                                                    gumbel_scale = ga_gumbel_scale,
                                                                    invgamma_scale = ga_invgamma_scale,
                                                                    invgamma_shape = ga_invgamma_shape,
                                                                    llogis_scale = ga_llogis_scale,
                                                                    llogis_shape = ga_llogis_shape,
                                                                    lnorm_meanlog = ga_lnorm_meanlog,
                                                                    lnorm_sdlog = ga_lnorm_sdlog,
                                                                    mgamma_XMAX = ga_mgamma_XMAX,
                                                                    mgamma_XMIN = ga_mgamma_XMIN,
                                                                    mgamma_rate = ga_mgamma_rate,
                                                                    mgamma_shape = ga_mgamma_shape,
                                                                    mgumbel_XMAX = ga_mgumbel_XMAX,
                                                                    mgumbel_XMIN = ga_mgumbel_XMIN,
                                                                    mgumbel_alpha = ga_mgumbel_alpha,	mgumbel_scale = ga_mgumbel_scale,
                                                                    norm_mean = ga_norm_mean,	norm_sd = ga_norm_sd,	weibull_scale = ga_weibull_scale,
                                                                    weibull_shape = ga_weibull_shape,
                                                                    invweibull_scale = ga_invweibull_scale, invweibull_shape = ga_invweibull_shape, weights = ga_weights, allXMIN = ga_XMIN, allXMAX = ga_XMAX)$value, 5),
                           "\nDisMod <37 prev:", round(ga_u_37, 4),
                           "\nEnsemble <37 prev:" , round(integrate(d_ensemble, lower = 0, upper = 37,
                                                                    betasr_XMAX = ga_betasr_XMAX,
                                                                    betasr_XMIN = ga_betasr_XMIN,
                                                                    betasr_shape1 = ga_betasr_shape1,
                                                                    betasr_shape2 = ga_betasr_shape2,
                                                                    exp_rate = ga_exp_rate,
                                                                    gamma_rate = ga_gamma_rate,
                                                                    gamma_shape = ga_gamma_shape,
                                                                    gumbel_alpha = ga_gumbel_alpha,
                                                                    gumbel_scale = ga_gumbel_scale,
                                                                    invgamma_scale = ga_invgamma_scale,
                                                                    invgamma_shape = ga_invgamma_shape,
                                                                    llogis_scale = ga_llogis_scale,
                                                                    llogis_shape = ga_llogis_shape,
                                                                    lnorm_meanlog = ga_lnorm_meanlog,
                                                                    lnorm_sdlog = ga_lnorm_sdlog,
                                                                    mgamma_XMAX = ga_mgamma_XMAX,
                                                                    mgamma_XMIN = ga_mgamma_XMIN,
                                                                    mgamma_rate = ga_mgamma_rate,
                                                                    mgamma_shape = ga_mgamma_shape,
                                                                    mgumbel_XMAX = ga_mgumbel_XMAX,
                                                                    mgumbel_XMIN = ga_mgumbel_XMIN,
                                                                    mgumbel_alpha = ga_mgumbel_alpha,	mgumbel_scale = ga_mgumbel_scale,
                                                                    norm_mean = ga_norm_mean,	norm_sd = ga_norm_sd,	weibull_scale = ga_weibull_scale,
                                                                    weibull_shape = ga_weibull_shape,
                                                                    invweibull_scale = ga_invweibull_scale, invweibull_shape = ga_invweibull_shape, weights = ga_weights, allXMIN = ga_XMIN, allXMAX = ga_XMAX)$value, 4)), location= "topleft")
    
    plot(x = bw_curve$x, y = bw_curve$y, col = "red", type = "l", xlab = "Birthweight", ylab = "Density", xlim =c(0,6000), ylim =c(0, 0.0015))
    
    title(paste("Birthweight - Location:", location_id, ",", year_id, "Sex:", sex_id, "Draw:", draw))
    
    Corner_text(text=paste("DisMod <2500 prev:", round(bw_u_2500, 4),
                           "\nEnsemble <2500 prev:" , round(integrate(d_ensemble, lower = 0, upper = 2500,
                                                                      betasr_XMAX = bw_betasr_XMAX,
                                                                      betasr_XMIN = bw_betasr_XMIN,
                                                                      betasr_shape1 = bw_betasr_shape1,
                                                                      betasr_shape2 = bw_betasr_shape2,
                                                                      exp_rate = bw_exp_rate,
                                                                      gamma_rate = bw_gamma_rate,
                                                                      gamma_shape = bw_gamma_shape,
                                                                      gumbel_alpha = bw_gumbel_alpha,
                                                                      gumbel_scale = bw_gumbel_scale,
                                                                      invgamma_scale = bw_invgamma_scale,
                                                                      invgamma_shape = bw_invgamma_shape,
                                                                      llogis_scale = bw_llogis_scale,
                                                                      llogis_shape = bw_llogis_shape,
                                                                      lnorm_meanlog = bw_lnorm_meanlog,
                                                                      lnorm_sdlog = bw_lnorm_sdlog,
                                                                      mgamma_XMAX = bw_mgamma_XMAX,
                                                                      mgamma_XMIN = bw_mgamma_XMIN,
                                                                      mgamma_rate = bw_mgamma_rate,
                                                                      mgamma_shape = bw_mgamma_shape,
                                                                      mgumbel_XMAX = bw_mgumbel_XMAX,
                                                                      mgumbel_XMIN = bw_mgumbel_XMIN,
                                                                      mgumbel_alpha = bw_mgumbel_alpha,	mgumbel_scale = bw_mgumbel_scale,
                                                                      norm_mean = bw_norm_mean,	norm_sd = bw_norm_sd,	weibull_scale = bw_weibull_scale,
                                                                      weibull_shape = bw_weibull_shape,
                                                                      invweibull_scale = bw_invweibull_scale, invweibull_shape = bw_invweibull_shape, weights = bw_weights, allXMIN = bw_XMIN, allXMAX = bw_XMAX)$value, 4)), location= "topleft")
    
    
    
    g1 <- ggplot(data = sim, aes(x = ga, ..density..)) + geom_freqpoly(bins = 25) + xlim(20,50) + ylim(0,0.4)
    
    g2 <- ggplot(data = sim, aes(x = old_ga, ..density..)) + geom_freqpoly(bins = 25) + xlim(20,50) + ylim(0,0.4)
    
    g3 <- ggplot() + stat_ecdf(data = sim, aes(x = ga), geom = "step", col = "red") + stat_ecdf(data = sim, aes(x = old_ga), geom = "step", col = "blue") +
      ggtitle(paste("Gestational Age Cumulative Prevalence"))
    
    g4 <- ggplot() + stat_ecdf(data = sim, aes(x = bw), geom = "step", col = "red") + stat_ecdf(data = sim, aes(x = old_bw), geom = "step", col = "blue") +
      ggtitle(paste("Birthweight Cumulative Prevalence"))
    
    g5 <- ggplot(data = sim, aes(x = bw, y = ga)) + geom_point(alpha = 0.04, col = "red") + xlim(0,6000) + ylim(0,60) +
      ggtitle(paste("Copula - Par Adjust")) + xlab("Birthweight (g)") + ylab("Gestational Age (wk)") +
      geom_point(data = sim, aes(x = old_bw, y = old_ga), alpha = 0.04, col = "grey")
    
    print(g1)
    print(g2)
    print(g3)
    Corner_text(text=paste("Par:", round(ga_par, 4)), location= "topright")
    print(g4)
    Corner_text(text=paste("Par:", round(bw_par, 4)), location= "topright")
    print(g5)
    
    Corner_text(text=paste("New Copula Prevalence <28wk:", nrow(sim[ga < 28, "ga"]) / nrow(sim),
                           "\nDisMod <28 prev:", round(ga_u_28, 4),
                           "\nNew Copula Prevalence <37wk:", nrow(sim[ga < 37, "ga"]) / nrow(sim),
                           "\nDisMod <37 prev:", round(ga_u_37, 4),
                           "\nNew Copula Prevalence <=2500g:", nrow(sim[bw < 2500, "bw" ]) / nrow(sim),
                           "\nDisMod <2500 prev:", round(bw_u_2500, 4),
                           "\nNew ratio 500/2500:", round(nrow(sim[bw<500,]) / nrow(sim[bw<2500,]), 4),
                           "\nNew Copula Prevalence <=500g:", nrow(sim[bw < 500, "bw" ]) / nrow(sim),
                           "\nNew Copula Prevalence <=1500g:", nrow(sim[bw < 1500, "bw" ]) / nrow(sim)), location= "bottomright")
    
    
    dev.off()
    
  }
  
  sim <- sim[, -c("old_bw", "old_ga")]
  
  ## Set each point to a level/label of gestational age or birthewight
  sim[, ga := cut(ga, breaks = gestweeklevels, labels = gestweeklabels, include.lowest = T, right = F)]
  sim[, bw := cut(bw, breaks = bwlevels, labels = bwlabels, include.lowest = T)]
  
  ## Create a third column combining the gestational age and birthweight
  sim[, modelable_entity_name := paste0("Birth prevalence - ", ga, " wks, ", bw, " g")]
  
  ## Calculate prevalence by dividing # of rows per gestational age/birthweight combo by the total number of row (100,000)
  sim[, total := .N]
  sim[, ME_prop := .N/total, by = "modelable_entity_name"]
  
  sim <- unique(sim[, -c("bw", "ga", "total")])
  
  sim_info <- data.table(location_id = location_id, sex_id = sex_id, age_group_id = age_group_id, year_id = year_id, draw = draw)
  
  sim <- cbind(sim_info, sim)
  
  sim <- data.table(sim)
  
  return(sim)
  
  
  
}


slots <-45
if (os=="windows") { cores_to_use = 1  } else {    cores_to_use = ifelse(grepl('Intel', system("cat /proc/cpuinfo | grep \'name\'| uniq", inter = T)), floor(slots * .86), floor(slots*.64)) }


sim_all <- rbindlist(mclapply( row_list, function(x) simulate_copula(location_id = x$location_id, sex_id = x$sex_id,
                                                                     age_group_id = x$age_group_id,
                                                                     year_id = x$year_id,
                                                                     draw = x$draw,
                                                                     ga_u_28 = x$ga_u_28_,
                                                                     ga_u_32 = x$ga_u_32_,
                                                                     ga_u_37 = x$ga_u_37_,
                                                                     ga_mean = x$ga_mean_,
                                                                     ga_betasr_XMAX = x$ga_betasr_XMAX,
                                                                     ga_betasr_XMIN = x$ga_betasr_XMIN,
                                                                     ga_betasr_shape1 = x$ga_betasr_shape1,
                                                                     ga_betasr_shape2 = x$ga_betasr_shape2,
                                                                     ga_exp_rate = x$ga_exp_rate,
                                                                     ga_gamma_rate = x$ga_gamma_rate,
                                                                     ga_gamma_shape = x$ga_gamma_shape,
                                                                     ga_gumbel_alpha = x$ga_gumbel_alpha,
                                                                     ga_gumbel_scale = x$ga_gumbel_scale,
                                                                     ga_invgamma_scale = x$ga_invgamma_scale,
                                                                     ga_invgamma_shape = x$ga_invgamma_shape,
                                                                     ga_llogis_scale = x$ga_llogis_scale,
                                                                     ga_llogis_shape = x$ga_llogis_shape,
                                                                     ga_lnorm_meanlog = x$ga_lnorm_meanlog,
                                                                     ga_lnorm_sdlog = x$ga_lnorm_sdlog,
                                                                     ga_mgamma_XMAX = x$ga_mgamma_XMAX,
                                                                     ga_mgamma_XMIN = x$ga_mgamma_XMIN,
                                                                     ga_mgamma_rate = x$ga_mgamma_rate,
                                                                     ga_mgamma_shape = x$ga_mgamma_shape,
                                                                     ga_mgumbel_XMAX = x$ga_mgumbel_XMAX,
                                                                     ga_mgumbel_XMIN = x$ga_mgumbel_XMIN,
                                                                     ga_mgumbel_alpha = x$ga_mgumbel_alpha,
                                                                     ga_mgumbel_scale = x$ga_mgumbel_scale,
                                                                     ga_norm_mean = x$ga_norm_mean,
                                                                    ga_norm_sd = x$ga_norm_sd,
                                                                    ga_weibull_scale = x$ga_weibull_scale,
                                                                    ga_weibull_shape = x$ga_weibull_shape,
                                                                    ga_invweibull_scale = x$ga_invweibull_scale,
                                                                    ga_invweibull_shape = x$ga_invweibull_shape,
                                                                    ga_weights = x[, ga_weight_vars, with = F],
                                                                    ga_XMIN = x$ga_XMIN,
                                                                    ga_XMAX = x$ga_XMAX,
                                                                    bw_u_2500 = x$bw_u_2500_,
                                                                    bw_mean = x$bw_mean_,
                                                                    bw_betasr_XMAX = x$bw_betasr_XMAX,
                                                                    bw_betasr_XMIN = x$bw_betasr_XMIN,
                                                                    bw_betasr_shape1 = x$bw_betasr_shape1,
                                                                    bw_betasr_shape2 = x$bw_betasr_shape2,
                                                                    bw_exp_rate = x$bw_exp_rate,
                                                                    bw_gamma_rate = x$bw_gamma_rate,
                                                                    bw_gamma_shape = x$bw_gamma_shape,
                                                                    bw_gumbel_alpha = x$bw_gumbel_alpha,
                                                                    bw_gumbel_scale = x$bw_gumbel_scale,
                                                                    bw_invgamma_scale = x$bw_invgamma_scale,
                                                                    bw_invgamma_shape = x$bw_invgamma_shape,
                                                                    bw_llogis_scale = x$bw_llogis_scale,
                                                                    bw_llogis_shape = x$bw_llogis_shape,
                                                                    bw_lnorm_meanlog = x$bw_lnorm_meanlog,
                                                                    bw_lnorm_sdlog = x$bw_lnorm_sdlog,
                                                                    bw_mgamma_XMAX = x$bw_mgamma_XMAX,
                                                                    bw_mgamma_XMIN = x$bw_mgamma_XMIN,
                                                                    bw_mgamma_rate = x$bw_mgamma_rate,
                                                                    bw_mgamma_shape = x$bw_mgamma_shape,
                                                                    bw_mgumbel_XMAX = x$bw_mgumbel_XMAX,
                                                                    bw_mgumbel_XMIN = x$bw_mgumbel_XMIN,
                                                                    bw_mgumbel_alpha = x$bw_mgumbel_alpha,
                                                                    bw_mgumbel_scale = x$bw_mgumbel_scale,
                                                                    bw_norm_mean = x$bw_norm_mean,
                                                                    bw_norm_sd = x$bw_norm_sd,
                                                                    bw_weibull_scale = x$bw_weibull_scale,
                                                                    bw_weibull_shape = x$bw_weibull_shape,
                                                                    bw_invweibull_scale = x$bw_invweibull_scale,
                                                                    bw_invweibull_shape = x$bw_invweibull_shape,
                                                                    bw_weights = x[, bw_weight_vars, with = F],
                                                                    bw_XMIN = x$bw_XMIN,
                                                                    bw_XMAX = x$bw_XMAX),
                                                                    mc.cores = cores_to_use ), use.names = T, fill =T)



saveRDS(sim_all, "FILEPATH")

print(Sys.time())

job.runtime <- proc.time() - start.time
job.runtime <- job.runtime[3]

print(paste0("Time elapsed:", job.runtime))

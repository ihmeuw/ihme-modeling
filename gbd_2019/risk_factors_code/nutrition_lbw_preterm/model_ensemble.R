rm(list=ls())

Sys.umask(mode = 002)

os <- .Platform$OS.type
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH"  )

args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
param_map <- fread(param_map_filepath)
  
location_ids = param_map[task_id, location_ids]
decomp_step = param_map[task_id, decomp_step]
dimension = param_map[task_id, dimension]
mean_meid = param_map[task_id, mean_meid]
prev_meid = param_map[task_id, prev_meid]
min_allowed_sd = param_map[task_id, min_allowed_sd]
max_allowed_sd = param_map[task_id, max_allowed_sd]
threshold = param_map[task_id, threshold]
repo = param_map[task_id, repo]
outdir = param_map[task_id, outdir]
weights_filepath <- param_map[task_id, weights_filepath]
num_draws = param_map[task_id, num_draws]
  
year_ids = get_demographics(gbd_team = "epi", gbd_round_id = 6)$year_id
sex_ids = list(1,2)
age_group_ids = 2
  
weights <- fread(weights_filepath)
weights <- weights[1, -c("location_id", "age_group_id", " sex_id", "year_id")]

source("FILEPATH")


source("FILEPATH/get_draws.R"  )
source("FILEPATH/edensity.R")
source("FILEPATH/pdf_families.R")

Rcpp::sourceCpp("FILEPATH/scale_density_simpson.cpp")
dlist <- c(classA, classB, classM)

createMeanPrevParamsList <- function(mean_meid, prev_meid, location_id, sex_id, year_id, age_group_id, num_draws, decomp_step){
  
  prev_draws <- get_draws(gbd_id_type = 'modelable_entity_id', source = "epi", 
                          gbd_id = prev_meid, 
                          location_id = location_id, sex_id = sex_id, year_id = year_id, age_group_id = age_group_id, 
                          decomp_step = decomp_step)
  
  mean_draws <- get_draws(gbd_id_type = 'modelable_entity_id', source = "epi", 
                          gbd_id = mean_meid, 
                          location_id = location_id, sex_id = sex_id, year_id = year_id, age_group_id = age_group_id,
                          decomp_step = decomp_step)
  
  mean_draws <- fread(file.path("FILEPATH"))
  

  prev_draws <- melt(prev_draws, id.vars = c("location_id", "sex_id", "year_id", "age_group_id"), measure.vars =  paste0("draw_", 0:999), variable.name = "draw", value.name = "prev" )
  mean_draws <- melt(mean_draws, id.vars = c("location_id", "sex_id", "year_id", "age_group_id"), measure.vars =  paste0("draw_", 0:999), variable.name = "draw", value.name = "mean" )
  params.list <- merge(prev_draws, mean_draws)
  
  params.list <- params.list[draw %in% paste0("draw_", 0:(num_draws-1))]
  params.list <- split(params.list, by = c("location_id", "sex_id", "year_id", "draw"), drop = TRUE)
   
  return(params.list)
  
}

getOptimalSDs <- function(meanprev_list, min_allowed_sd, max_allowed_sd, weights, threshold) {
  
  meanprev_list <- lapply(meanprev_list, function(meanprev){
    
    mean = meanprev$mean
    obs_prev = meanprev$prev
    
    optim_sd <- try(optim(par = runif(1, min = min_allowed_sd, max = max_allowed_sd),
                          fn = calcEstimDensityError,
                          mean = mean,
                          obs_prev = obs_prev,
                          weights = weights,
                          threshold = threshold,
                          method = "Brent",
                          lower = min_allowed_sd,
                          upper = max_allowed_sd,
                          control = list(maxit = 250))$par, "try-error")
    
    if(inherits(optim_sd, what = "try-error")) optim_sd = as.numeric(NA)
    
    meanprev$sd <- optim_sd
    
    return(meanprev)
    
  })
  
  return(meanprev_list)
  
}


calcPrevFromMeanSD <- function(mean, sd, weights, threshold){
  
  mu <- log(mean/sqrt(1 + (sd^2/(mean^2))))
  sdlog <- sqrt(log(1 + (sd^2/mean^2)))
  
  XMIN <- qlnorm(0.001, mu, sdlog)
  XMAX <- qlnorm(0.999, mu, sdlog)
  
  ensemble_dens <- get_edensity(weights = weights, mean = mean, sd = sd) 
  
  ensemble_dens_func <- approxfun(x = ensemble_dens$x, y = ensemble_dens$fx, yleft = 0, yright = 0)
  prev <- try(integrate(ensemble_dens_func, lower = 0, upper = threshold)$value)
  
  # Highly penalize any ensembles that are bimodal 
  if(class(prev) == "try-error" | ensemble_dens$fx[995] > 2*min(ensemble_dens$fx[500:750])){
    
    message("Bad ensemble")
    prev <- 0.999999 
    
  }
  
  return(prev)
  
}

calcEstimDensityError <- function(par, mean, obs_prev, weights, threshold) {
  
  sd = par
  
  estim_prev <- calcPrevFromMeanSD(mean = mean, sd = sd, weights = weights, threshold = threshold)

  sqr_error <- sum((obs_prev - estim_prev)^2)

  return(sqr_error)
  
}



# ----- 

print(Sys.time())
message("Creating Mean & Prev Param List")
meanprev_list <- createMeanPrevParamsList(mean_meid = mean_meid, 
                                          prev_meid = prev_meid, 
                                          location_id = location_ids, 
                                          sex_id = sex_ids, 
                                          year_id = year_ids, 
                                          age_group_id = age_group_ids,
                                          num_draws = num_draws,
                                          decomp_step = decomp_step)

# -----

print(Sys.time())
message("Optimizing Standard Deviations")
meanprev_list <- getOptimalSDs(meanprev_list = meanprev_list, 
                               min_allowed_sd = min_allowed_sd, 
                               max_allowed_sd = max_allowed_sd, 
                               weights = weights, 
                               threshold = threshold)

meanprev_list <- rbindlist(meanprev_list)
meanprev_list[, dimension := dimension]


# -----

print(Sys.time())
message("Saving")
saveRDS(meanprev_list, file = file.path(outdir, dimension, paste0(location_ids, ".rds")))

print(Sys.time())
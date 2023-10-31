
#' Purpose: Find optimal variance that produces an ensemble distribution characterized by mean/variance that minimizes weighted squared error at thresholds
#' Inputs: 
#' - GBD Demographics: location_ids, sex_ids, age_group_ids, year_ids
#' - GBD Round IDs: decomp_step, gbd_round_id
#' - num_draws: between 1-1000
#' - ME_ID of Mean model
#' - prev_meids - ME_IDs of Prevalence models in comma delimited list (i.e. "5" or "1,2,3")
#' - thresholds - thresholds associated with the prev_meids (in same order) (i.e, "28,37" for first threshold at 28 units and second threshold at 37 units)
#' - threshold_weights - threshold weights associated with the prev_meids (in same order)
#' - min_allowed_sd - the minimum standard deviation allowed; a good rule of thumb is mean / 15. The greater the range between min_allowed_sd and max_allowed_sd the longer the optimization will take.
#' - max_allowed_sd - the maximum standard deviation allowed; a good rule of thumb is mean / 3.  The greater the range between min_allowed_sd and max_allowed_sd the longer the optimization will take.
#' - outdir - where to save outputs
#' - weights_filepath - filepath to weights saved. Please see xx for example of weights
#' How to run: Either run this script with "run_interactively = 1" or run model_ens_variance_launch.R with "run_interactively = 0" to launch this script in parallel
# --------------

rm(list=ls())

Sys.umask(mode = 002)

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_demographics.R"  )
source(paste0("FILEPATH/ens_variance_functions.R"))

run_interactively = 0

if(run_interactively == 1){
  
  decomp_step = "iterative"
  gbd_round_id = 7
  var = "ga"
  version = "VERSION"
  mean_meid = 15802
  prev_meids = "24448,24449"
  thresholds = "28,37"
  threshold_weights = "0,1"
  outdir = "FILEPATH"
  num_draws = 100
  location_ids = 533
  year_ids = 2002
  sex_ids = c(1)
  age_group_ids = 2
  
  microdata_filepath = "FILEPATH/ga_ensemble.rds"
  
  dir.create(file.path(outdir, var, version), recursive = T)
  
  if(microdata_filepath %like% ".rds"){
    microdata <- readRDS(microdata_filepath)  
  } else if(microdata_filepath %like% "csv") {
    microdata <- fread(microdata_filepath)
  } else{
    stop("Microdata needs to be saved in .csv or .rds format")
  }
  
  
  # For CGF only - need to transform data to the same space the weights were fit in 
  if(var %in% c("HAZ", "WAZ", "WHZ")){
    microdata[, data := (data + 10)]
  }
  
  XMIN = quantile(microdata[, .(min = min(data)), by = .(nid, location_id, year_id)]$min, 0.01)
  XMAX = quantile(microdata[, .(max = max(data)), by = .(nid, location_id, year_id)]$max, 0.99)
  
  min_allowed_sd = 0.50 * min(microdata[, .(sd = sd(data)), by = .(nid, location_id, year_id)]$sd)
  max_allowed_sd = 1.50 * max(microdata[, .(sd = sd(data)), by = .(nid, location_id, year_id)]$sd)
  
  write.csv(microdata, file.path(outdir, var, version, "microdata.csv"), row.names = F, na = "")
  
  rm(microdata)
  
  
} else {
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  ## Retrieving array task_id
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  decomp_step = param_map[task_id, decomp_step]
  gbd_round_id = param_map[task_id, gbd_round_id]
  var = param_map[task_id, var]
  version = param_map[task_id, version]
  
  mean_meid = param_map[task_id, mean_meid]
  prev_meids = param_map[task_id, prev_meids]
  min_allowed_sd = param_map[task_id, min_allowed_sd]
  max_allowed_sd = param_map[task_id, max_allowed_sd]
  XMIN = param_map[task_id, XMIN]
  XMAX = param_map[task_id, XMAX]
  
  thresholds = param_map[task_id, thresholds]
  threshold_weights = param_map[task_id, threshold_weights]
  outdir = param_map[task_id, outdir]
  num_draws = param_map[task_id, num_draws]
  
  location_ids = param_map[task_id, location_ids]
  year_ids = param_map[task_id, year_ids]
  sex_ids = param_map[task_id, sex_ids]
  age_group_ids = param_map[task_id, age_group_ids]
  
}

print(file.path(outdir, var, version, "weights.csv"))
weights <- fread(file.path(outdir, var, version, "weights.csv"))

thresholds <- as.character(thresholds)
threshold_weights <- as.character(threshold_weights)
prev_meids <- as.character(prev_meids)

if(thresholds %like% ","){thresholds <- as.numeric(unlist(strsplit(thresholds, split=",")))}
if(threshold_weights %like% ","){threshold_weights <- as.numeric(unlist(strsplit(threshold_weights, split=",")))}
if(prev_meids %like% ","){prev_meids <- as.numeric(unlist(strsplit(prev_meids, split=",")))}
meids <- c(mean_meid, prev_meids)

# Demographics
if(is.na(location_ids)){
  location_ids<-get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$location_id
}
if(is.na(sex_ids)){
  sex_ids<-get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$sex_id
}
if(is.na(age_group_ids)){
  age_group_ids<-get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$age_group_id
}
if(is.na(year_ids)){
  year_ids<-get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$year_id
}


# ----- 

print(Sys.time())
message("Creating Mean & Prev Param List")
meanprev_list <- createMeanPrevList(meids = meids, 
                                    location_ids = location_ids, sex_ids = sex_ids, year_ids = year_ids, age_group_ids = age_group_ids,
                                    num_draws = num_draws, decomp_step = decomp_step, gbd_round_id = gbd_round_id)



# -----

print(Sys.time())
message("Optimizing Standard Deviations")

meanprev_list <- getOptimalSDs(meanprev_list = meanprev_list,
                               mean_meid = mean_meid,
                               prev_meids = prev_meids,
                               min_allowed_sd = min_allowed_sd, 
                               max_allowed_sd = max_allowed_sd, 
                               weights = weights, 
                               thresholds = thresholds,
                               threshold_weights = threshold_weights, 
                               XMIN = XMIN, 
                               XMAX = XMAX)

meanprev_list <- rbindlist(meanprev_list)
meanprev_list[, var := var]
meanprev_list[, mean := get(paste0("meid_", mean_meid))]


print(Sys.time())
message("Saving")

label = "meanSD"

if(length(location_ids)==1){label <- paste0(label, "_", location_ids)}
if(length(age_group_ids)==1){label <- paste0(label, "_", age_group_ids)}
if(length(sex_ids)==1){label <- paste0(label, "_", sex_ids)}
if(length(year_ids)==1){label <- paste0(label, "_", year_ids)}

dir.create(file.path(outdir, var, version, "meanSD"))
saveRDS(meanprev_list, file = file.path(outdir, var, version, "meanSD", paste0(label, ".rds")))


# ----- vetting pdf

print(Sys.time())
message("Making vetting pdf")

rows_to_vet <- meanprev_list[draw==paste0("draw_0") & year_id %in% c(1990,2010,2019)]
rows_to_vet <- split(rows_to_vet, by = c("location_id", "sex_id", "year_id", "age_group_id"))

dir.create(file.path(outdir, var, version, "vetting"))

if(nrow(rows_to_vet) > 0){
  
  pdf(file.path(outdir, var, version, "vetting", paste0(label, ".pdf")), width = 12, height = 6 )
  
  colors <- c("red", "orange", "yellow", "green", "turquoise", "blue", "purple", "pink", "brown", "grey")
  
  for(row in rows_to_vet){
    
    plot_weights <- copy(weights)
    plot_weights[, (names(weights))  := 0 ]
    
    x <- seq(from = XMIN, to = XMAX, length = 1000)  
    ens_dens <- get_ensemble_density(weights = weights, mean = row$mean, sd = row$sd, XMIN = XMIN, XMAX = XMAX)  
    plot(x, ens_dens(x), col = "black")
    
    for(i in 1:length(names(weights))){
      
      distr <- names(weights)[i]
      color <- colors[i]
      
      plot_weights[, (distr) := 1] 
      
      distr_dens <- get_ensemble_density(weights = plot_weights, mean = row$mean, sd = row$sd, XMIN = XMIN, XMAX = XMAX)  
      lines(x,distr_dens(x), col = color)
      
      plot_weights[, (names(weights))  := 0 ]
      
    }
    
    title(main = paste(var, "location_id:", row$location_id, "sex_id:", row$sex_id, "age_group_id", row$age_group_id, "year_id", row$year_id),
          sub = paste("mean", round(row$mean, 5), "sd", round(row$sd, 5)))
    
  }
  
  dev.off()
  
}


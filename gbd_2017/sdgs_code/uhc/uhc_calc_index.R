##########################################################
# Description: Create index
##########################################################
## DEFINE ROOT AND LIBRARIES
rm(list=ls())
library(argparse)
library(data.table)
library(plyr)
library(parallel)
library(foreach)
library(feather)
if (Sys.info()[1] == "Linux"){
  j <- "/home/j/"
  h <- paste0("/homes/", Sys.info()[7])
}else if (Sys.info()[1] == "Windows"){
  j <- "J:/"
  h <- "H:/"
}else if (Sys.info()[1] == "Darwin"){
  j <- "/Volumes/snfs/"
  h <- paste0("/Volumes/", Sys.info()[6], "/")
}

##########################################################
## PARSE ARGUMENTS
parser <- ArgumentParser()
parser$add_argument("--prog_dir", help="Program directory",
                    default=paste0(h, "FILEPATH"), type="character")
parser$add_argument("--data_dir", help="Space where results are to be saved",
                    default="FILEPATH", type="character")
parser$add_argument("--intervention_data_dir", help="Space where prepped interventions are saved",
                    default="FILEPATH", type="character")
parser$add_argument("--lsid", help="Location set",
                    default=35, type="integer")
parser$add_argument("--agg_lsids", help="Aggregate location set",
                    default=40, type="integer")
parser$add_argument("--lid", help="Location id",
                    default=113, type="integer")
parser$add_argument("--interids", help = "intervention ids",
                    default = c(194,197,200,203,206,209,212,215,284),
                    nargs="+", type = "integer")
parser$add_argument("--yids", help="Years to run",
default=1990:2017, nargs="+", type="integer")
args <- parser$parse_args()
list2env(args, .GlobalEnv)
rm(args)

##########################################################
## DEFINE FUNCTIONS
source(paste0(prog_dir, "/utilities.R"))

#Read in files for deaths and MI ratios
compileAmenable <- function(lids, yids, data_dir, cause) {
  # Load all specified risk-standardized mortality summaries
  args <- expand.grid(location_id = lids, year_id = yids)
  amendf <- rbindlist(mcmapply(function(data_dir, yid, lid)  {
    if (file.exists(paste0(data_dir, "FILEPATH"))) {
      amenabledf<-fread(paste0(data_dir, "FILEPATH"))
      amenabledf<-amenabledf[!(cause_id %in% c(429,432,435,441,468,484,487,849)),]
    } 
 },
  yid = args$year_id,
  lid = args$location_id,
  MoreArgs = list(data_dir = data_dir),
  SIMPLIFY = FALSE,
  mc.cores = 24))
  return(amendf)
}


compileAmenable_MI <- function(lids, yids, data_dir) {
  # Load all specified MI ratios
  args <- expand.grid(location_id = lids, year_id = yids)
  amendf <- rbindlist(mcmapply(function(data_dir, yid, lid)  {
    if (file.exists(paste0(data_dir, "FILEPATH"))) {
      amenabledf<-fread(paste0(data_dir, "FILEPATH"))
    }
  },
  yid = args$year_id,
  lid = args$location_id,
  MoreArgs = list(data_dir = data_dir),
  SIMPLIFY = FALSE,
  mc.cores = 24))
  return(amendf)
}

#Get 9 interventions outside of HAQ analysis
compileInterventions <- function(interids, intervention_data_dir){
  interventdf <- mclapply(interids, function(interid){
    if (file.exists(paste0(intervention_data_dir, interid, "FILEPATH"))) {
      interventdf<-read_feather(paste0(intervention_data_dir, interid, "FILEPATH"))
    }
  },
  mc.cores = 24)
  interventdf <- rbindlist(interventdf)
  interventdf <- interventdf[, c('measure_id', 'metric_id') := NULL]
  interventdf <- melt(interventdf, id.vars = c('location_id','year_id', 'indicator_component_id', 'age_group_id', 'sex_id'),
  value.name = 'val', variable.name = 'draw')
  interventdf$draw <- gsub("draw_", "", interventdf$draw)
  setnames(interventdf, c("val", "indicator_component_id"), c("rsval", "cause_id"))
  interventdf$val <- NA
  return(interventdf)
}



calcIndex <- function(base_lsid, agg_lsids, yids, prog_dir, data_dir) {
  # Get requisite datasets
  print("Getting locations")
  load(paste0(data_dir, "/locsdf_", lsid,".RData"))
  locsdf <- locsdf[!location_type %in% c('global', 'superregion', 'region')]

  # Load GBD 2017 age-standardized, risk standardized values and MI ratios
  print("Compiling Risk-Standardized Deaths")
  amenabledf <- compileAmenable(lids = locsdf$location_id, yids, data_dir)
  gc(T)
  
  #Pull in MI ratios
  print("Compiling MI Ratios")
  mi_ratiosdf <-  compileAmenable_MI(lids = locsdf$location_id, yids, data_dir)
  gc(T)
  
  # #Pull in prepped intervention measures
  print("Pulling in Interventions")
  interventiondf <- compileInterventions(interids, intervention_data_dir)
 
  
  #Combine amenable deaths, MI ratios and interventions
  componentsdf <- rbindlist(list(amenabledf, mi_ratiosdf, interventiondf), use.names =T)
  componentsdf$age_group_id <- 22 
  componentsdf$sex_id <- 3
  
  # Make sure we have all 41 indicators (32 amenable mort + 3 vaccines + 4 maternal + met need + ART)
  if (length(unique(componentsdf$cause_id)) != 41) {
    print(unique(componentsdf$cause_id))
    stop("Missing indicator(s), should be 32 amenable mort + 3 vaccines + 4 maternal + met need + ART")
  }
  
  rm(amenabledf, mi_ratiosdf, interventiondf)
  gc(T)
  
  #Create min/maxdf for all components adding log offset to all values, only using level 3 locations for scaling
  minmaxdf <- componentsdf[location_id %in% locsdf[level == 3, location_id], .(min_rsval = quantile(rsval+ 1e-6, .01), max_rsval = quantile(rsval+ 1e-6, .99)), by = .(cause_id, draw)]
  
  gc(T)
  
  # Scale amenabledf to min/max
  uhcdf <- merge(componentsdf, minmaxdf,
                   by = c("cause_id", "draw"))
  rm(componentsdf)
  gc(T)
  uhcdf <- uhcdf[rsval < min_rsval, rsval := min_rsval][rsval > max_rsval, rsval := max_rsval]
  
    #Save unscaled draws 
  print("Saving unscaled draws")
  write_feather(uhcdf, path = paste0(data_dir, "FILEPATH"))
  
  # #Remove interventions and scale differently depending
  interdf <- uhcdf[cause_id %in% interids]
  uhcdf <- uhcdf[!cause_id %in% interids]

  interdf <- interdf[, log_index_cause := (((log(rsval) - log(min_rsval)) / (log(max_rsval) - log(min_rsval)))) * 100]
  uhcdf <- uhcdf[, log_index_cause := (1 - ((log(rsval) - log(min_rsval)) / (log(max_rsval) - log(min_rsval)))) * 100]
  uhcdf <- rbindlist(list(uhcdf, interdf), use.names = T)
  rm(interdf)
  uhcdf <- uhcdf[log_index_cause < 0, log_index_cause := 0][log_index_cause > 100, log_index_cause := 100]

  # Save draws of index
  print("Saving scaled component draws")
  write_feather(uhcdf,
       path = paste0(data_dir, "FILEPATH"))
  
  #Take mean of all indicators
  
  # Summarize
  componentsindex <- uhcdf[, .(index_mean = mean(log_index_cause),
                         index_lval = quantile(log_index_cause, 0.025),
                         index_uval = quantile(log_index_cause, 0.975)),
                         by = .(location_id, year_id, age_group_id, cause_id, sex_id)]

  #Compile index draws as mean of all scaled values
  aggdf <- uhcdf[, .(log_index_cause = mean(log_index_cause)),
                 by = .(location_id, year_id, age_group_id, sex_id, draw)]
  aggdf$cause_id <- 188
  
  print("Saving scaled index draws")
  write_feather(aggdf,
                path = paste0(data_dir, "FILEPATH"))
  
  #Cast draws wide for forecasting/diagnostics
  widedrawsdf <- copy(aggdf)
  widedrawsdf <- widedrawsdf1[, log_index_cause := (log_index_cause/100)]
  widedrawsdf <- dcast(widedrawsdf, location_id + year_id + age_group_id + sex_id + cause_id ~
                         draw, value.var = "log_index_cause")
  setnames(widedrawsdf, old = paste0('0':'999'), new = paste0('draw_', 0:999))
  
  write_feather(widedrawsdf,
                path = "FILEPATH")

  #Find mean, upper, lower of index draws
  aggdf <- aggdf[, .(index_mean = mean(log_index_cause), 
                     index_lval = quantile(log_index_cause, 0.025), 
                     index_uval = quantile(log_index_cause, 0.975)), 
                 by = .(location_id, year_id, age_group_id, sex_id, cause_id)]
  write.csv(aggdf,
                file = "FILEPATH")
 
  #Combine
  indexdf <- rbindlist(list(componentsindex, aggdf), use.names = T, fill = TRUE)
  write_feather(indexdf,
       path = paste0(data_dir, "FILEPATH"))
  
}


##########################################################
## RUN PROGRAM
uhcdf <- calcIndex(base_lsid, agg_lsids, yids, prog_dir, data_dir)

##########################################################
## END SESSION
quit("no")

##########################################################



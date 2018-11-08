##########################################################
# Description: Make MI ratios for Non-melanoma skin cancer (squamous-cell carcinoma)
##########################################################
## DEFINE ROOT AND LIBRARIES
rm(list=ls())
library(argparse)
library(data.table)
library(plyr)
if (Sys.info()[1] == "Linux"){
  j <- "/home/j/"
  h <- paste0("/homes/", Sys.info()[8])
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
                    default= "FILEPATH", type="character")
parser$add_argument("--data_dir", help="Space where results are to be saved",
                    default="FILEPATH", type="character")
parser$add_argument("--lsid", help="Location set",
                    default=35, type="integer")
parser$add_argument("--lid", help="Location",
                    default=6, type="integer")
parser$add_argument("--cc_vers", help="codcorrect version",
                    default=89, type="integer")
parser$add_argument("--yid", help="Year for current job",
                    default=1990, type="integer")
args <- parser$parse_args()
list2env(args, .GlobalEnv)
rm(args)

##########################################################
## DEFINE FUNCTIONS
source(paste0(prog_dir, "/utilities.R"))

makeAgeWeights <- function(cause_list){
  ageweightdf <- getAgeGroups()
  cs_ageweightdf <- data.table()
  for (cid in cause_list$cause_id) {
    start <- cause_list[cause_id == cid]$age_group_years_start
    end <- cause_list[cause_id == cid]$age_group_years_end
    cause_weights <- data.table(ageweightdf)
    cause_weights$cause_id <- cid
    cause_weights <- cause_weights[age_group_years_start >= start & age_group_years_start < end]
    cause_weights <- cause_weights[, age_group_weight_value := age_group_weight_value / sum(age_group_weight_value)]
    cs_ageweightdf <- rbind(cs_ageweightdf,
                            cause_weights[, c("cause_id", "age_group_id", "age_group_weight_value"), with = FALSE])
  }
  return(cs_ageweightdf)
}


makeMIratios <- function(data_dir, yid, lid) {
  
  # Pull raw counts of deaths at draw level, for each cause
  causes <- c(849, 429, 432, 435, 441, 468, 484, 487)
  deathsdf<-get_draws(rep("cause_id",length(causes)), gbd_id = causes, source = 'codcorrect', measure_id = 1,
                      location_id = lid, year_id = yid, sex_id = c(1,2), gbd_round_id = 5, metric_id = 1, version_id = cc_vers)
  
   # Make it long
  deathsdf<-melt(deathsdf, id.vars = c('location_id','year_id','sex_id','age_group_id','cause_id', 'measure_id', 'metric_id'),
                 value.name = 'deaths', variable.name = 'draw')
  deathsdf$draw<-gsub("draw_","", deathsdf$draw)
  
  #Pull incidence 
  cancerincidence <- c(1466, 1469, 1472, 1475, 1478, 1481, 1484, 1487)

  #Made this flat file from a sql query 
  cancerincidence_ids <- fread(paste0(data_dir, "FILEPATH")) 
  cancerincidence_ids <- cancerincidence_ids[best_start %like% "2018-05-29"] 
  
  # Use this to pass version id for component inputs
  my_import_func <- function(incidence_id) {
    incidence_version <- cancerincidence_ids[indicator_component_id == incidence_id, indicator_component_version_id]
    pafdf <- get_draws(gbd_id_type = 'indicator_component_id', gbd_id = incidence_id, source = 'sdg', location_id = lid,
                       year_id = yid, version_id = incidence_version)
  }
  
  incidencedf <- rbindlist(mclapply(cancerincidence, my_import_func, mc.cores = 5))
  
  #Make it long
  incidencedf<-melt(incidencedf, id.vars = c('location_id','year_id','sex_id','age_group_id','cause_id', 'measure_id', 'indicator_component_id', 'indicator_component_version_id'),
                    value.name = 'incidence', variable.name = 'draw')
  
  incidencedf$draw<-gsub("draw_","", incidencedf$draw)
 
  #Multiply incidence by population to get counts instead of incident rates
  load(paste0(data_dir, "FILEPATH"))
  incidencedf <- merge(incidencedf, popsdf, by = c('location_id', 'age_group_id', 'sex_id', 'year_id'), all.x = T)
  incidencedf <- incidencedf[, incidence_count := incidence * population, by = .(location_id, age_group_id, sex_id, draw, cause_id)]
  setnames(incidencedf, "incidence", "incidence_rate")
  
  #Merge dataframes
  inputdf <- merge(incidencedf[,list(location_id, year_id, sex_id, age_group_id, cause_id, draw, incidence_count)],
                   deathsdf[,list(location_id, year_id, sex_id, age_group_id, cause_id, draw, deaths)],
                   by = c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", "draw"))
  
  #For age groups that there are relevant
  
  # Only keep Nolte/McKee causes and ages
  cause_list <- fread(paste0("FILEPATH"))
  causesdf <- prepCauseHierarchy(data_dir)
  
  # Aggregate to Nolte/McKee list, attach to main data frame
  inputdf <- merge(inputdf,
                   cause_list[, c("cause_id", "age_group_id_start", "age_group_id_end"), with = FALSE],
                   by = "cause_id")
  inputdf <- inputdf[age_group_id >= age_group_id_start & age_group_id <= age_group_id_end,]
  

  # Collapse to both sexes
  inputdf<-inputdf[,list(deaths = sum(deaths), incidence = sum(incidence_count)), by = c('location_id','year_id','age_group_id','cause_id','draw')]
  
  # Aggregate sexes
  inputdf <- inputdf[, sex_id := 3]
 
  # Age-standardize
  ageweightdf <- makeAgeWeights(cause_list)
  load(paste0(data_dir, "FILEPATH")) # population file
  inputdf <- merge(inputdf,
                   popsdf,
                   by = c("location_id", "year_id", "age_group_id", "sex_id"), all.x = T)
  inputdf <- merge(inputdf,
                   ageweightdf,
                   by = c("cause_id", "age_group_id"),
                   all.x = TRUE)
  if (nrow(inputdf[is.na(age_group_weight_value)]) > 0) stop("Problem with age weight merge")
  inputdf <- inputdf[, age_group_id := 27]
  inputdf <- inputdf[, .(deaths = sum((deaths / population) * age_group_weight_value),
                         incidence = sum((incidence / population) * age_group_weight_value)), 
                     by = .(location_id, year_id, age_group_id, sex_id, cause_id, draw)]
  
  
  
  ## Compute aggregated, all ages both sex MI ratio
  inputdf[,rsval := deaths/incidence]
  inputdf[,val := NA] # Needed to keep shape of dfs uniform
  inputdf$draw<-as.numeric(inputdf$draw)
  inputdf<-inputdf[order(cause_id,draw)]

  # Return data frame
  write.csv(inputdf[, c("location_id", "year_id", "age_group_id", "sex_id", "cause_id", "draw", "rsval", "val"), with = FALSE],
            file = paste0(data_dir, "FILEPATH"),
            row.names = FALSE)
}

##########################################################
## RUN PROGRAM

makeMIratios(data_dir, yid, lid)

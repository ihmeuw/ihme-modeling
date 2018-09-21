###########################################################################################################################################
# Date created: 6/12/17
# Description: Script for calculating attainment of SDG goals attained by country, super region, and SDI quintile
#
# How to use:
#
# source("FILEPATH")
# get_goal_met(17)
#
###########################################################################################################################################

## Clear environment

rm(list = ls())

## Load packages

require(pacman)
pacman::p_load(data.table, reshape2, RMySQL, ini, magrittr, ggplot2)

#set root

if (.Platform$OS.type=="unix") {
  root <- 'FILEPATH'
} else {
  root <- 'FILEPATH'
}

#function gets sourced in aggregation script

get_goal_met <- function(version=17, attainment_year = 2030, coverage_threshold_version = "resubmission") {
  
  # Goal:       
  #             Pull data for indicators with targets and compute the target goal based on its target type.
  # 
  #             Compare the computed target value to the projected data (2020 for Road Inj & 2030 for the rest)
  # 
  #             Return dataframe with goal_met column added to it
  # 
  # Parameters: 
  #             version
  # 
  #             class(version) = "numeric"
  #             dUSERt for version is 17 (was used for resubmission)
  #             indicator thresholds are pulled from FILEPATH
  #             attainment_year toggles the year for which goal attainment is calculated
  #             coverage_threshold_version toggles the targets that are used to calculate attainment (resubmission dUSERt)
  #             different versions represent different attainment thresholds - resubmission was the final version (<= 0.5 for 0-100%, <= .005 per 1000, <=.5 per 100,000, >=99% universal access and coverage (<=1% for SEVs)
  
  
  query <- function(query,conn_def) {
    odbc <- read.ini(paste0(root,"FILEPATH"))
    conn <- dbConnect(RMySQL::MySQL(), 
                      host = ADDRESS, 
                      username = USERNAME, 
                      password = PASSWORD)
    dt <- dbGetQuery(conn,query) %>% data.table
    dbDisconnect(conn)
    return(dt)
  }
  
  indicators_with_targets <- c(1044,1045,1032,1033,1034,1040,1041,1000,1001,1002,1004,1020,1025,1026,1035,1037,1061,1047,1062,1049,1051,1052,1053,1064,1057)
  
  ## load unscaled data
 
  sdg_unscaled<- fread(paste0("FILEPATH",version,".csv"))
  
  # subset to years needed and indicators with targets
  
  sdg_unscaled<-sdg_unscaled[year_id %in% c(2015,2016,2020,2030),]
  sdg_unscaled<-sdg_unscaled[indicator_id %in% indicators_with_targets,]
  
  #rescale (indicator id's by multiplier type)
  
  by_hundred <- c(1044, 1045, 1032,1034, 1035, 1037, 1061, 1047, 1049, 1051, 1052, 1053, 1064, 1057, 1004)
  by_thousand <- c(1040, 1041, 1000, 1002)
  by_hundred_thousand <- c(1001, 1020, 1025, 1026)
  
  #accordingly (indicators by multiplier)
  
  sdg_unscaled[indicator_id %in% by_hundred_thousand, mean_val:= mean_val*100000]
  sdg_unscaled[indicator_id %in% by_thousand, mean_val:= mean_val*1000]
  sdg_unscaled[indicator_id %in% by_hundred, mean_val:= mean_val*100]
  
  ## Get target goals
  
  targets <- fread(paste0("FILEPATH", coverage_threshold_version,".csv"))
  targets <- targets[,c('indicator_id', 'target_type','target', 'target_notes')]
  names(targets)[names(targets) == 'target'] <- 'target_value'
  
  ## Merge on SDI quintiles, super regions
  
  sdi_quintiles<-fread("FILEPATH")
  sdi_quintiles<-sdi_quintiles[, list(location_id, sdi_quintile)]
  
  sdg_unscaled<-merge(sdg_unscaled, sdi_quintiles)
  sdg_unscaled<-merge(sdg_unscaled, targets, by = c('indicator_id'), all.x = TRUE)
  
  #relative targets need special computation for their target value (helper function to compute relative reduction)
  
  reduced_by <- function(mean,denom){
    mean <- mean * (1-1/denom)
    return(mean)
  }
  
  #relative target indicators. 2020 for Road inj and 2030 for the others
  
  NCD_Mort <- sdg_unscaled[indicator_id == 1020 & year_id == 2015,]
  NCD_Mort_2030 <- sdg_unscaled[indicator_id == 1020 & year_id == attainment_year,]
  NCD_Mort_2030[,target_value:=NULL]
  NCD_Mort_final <- merge(NCD_Mort_2030,
                          NCD_Mort[,target_value := lapply(mean_val, function(x) reduced_by(x,3))][,c("location_id", "target_value")],
                          by ="location_id", all.x = TRUE)
  NCD_Mort_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  Suicide <- sdg_unscaled[indicator_id == 1025 & year_id == 2015,]
  Suicide_2030 <- sdg_unscaled[indicator_id == 1025 & year_id == attainment_year,]
  Suicide_2030[,target_value:=NULL]
  Suicide_final <- merge(Suicide_2030,
                         Suicide[,target_value := lapply(mean_val, function(x) reduced_by(x,3))][,c("location_id", "target_value")],
                         by ="location_id", all.x = TRUE)
  Suicide_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #calculate attainment rates for road inj in 2020 if 'attainment_year' == 2030 for other indicators, calculate for year specified in 'attainment_year' if != 2030
  
  if(attainment_year == 2030){
  
    Road_Inj <- sdg_unscaled[indicator_id == 1026 & year_id == 2015,]
    Road_Inj_2020 <- sdg_unscaled[indicator_id == 1026 & year_id == 2020,]
    Road_Inj_2020[,target_value:=NULL]
    Road_Inj_final <- merge(Road_Inj_2020,
                            Road_Inj[,target_value := lapply(mean_val, function(x) reduced_by(x,2))][,c("location_id", "target_value")],
                            by ="location_id", all.x = TRUE)
    Road_Inj_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  }else if(attainment_year !=2030){
    
    Road_Inj <- sdg_unscaled[indicator_id == 1026 & year_id == 2015,]
    Road_Inj_2020 <- sdg_unscaled[indicator_id == 1026 & year_id == attainment_year,]
    Road_Inj_2020[,target_value:=NULL]
    Road_Inj_final <- merge(Road_Inj_2020,
                            Road_Inj[,target_value := lapply(mean_val, function(x) reduced_by(x,2))][,c("location_id", "target_value")],
                            by ="location_id", all.x = TRUE)
    Road_Inj_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  }
  

  MMR_final <- sdg_unscaled[indicator_id == 1033 & year_id == attainment_year,]
  MMR_final[, goal_met := ifelse(mean_val < target_value, 1, 0)]
  
  #mean_val should be higher than the targets for these indicators
  
  goal_higher <- c(1057, 1035, 1037, 1061, 1034)
  
  #compute the goal_met for indicators with absolute target type
  
  twenty_thirty_higher <- sdg_unscaled[year_id == attainment_year & target_type == "absolute" & indicator_id %in% goal_higher,]
  twenty_thirty_higher[, goal_met := ifelse(mean_val >= target_value, 1, 0)]
  
  twenty_thirty_lower <- sdg_unscaled[year_id == attainment_year & target_type == "absolute" & !(indicator_id %in% goal_higher) & indicator_id != 1033,] #1033 is MMR
  twenty_thirty_lower[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #bind them all together.
  
  data <- list(NCD_Mort_final,Suicide_final,Road_Inj_final,MMR_final,twenty_thirty_higher,twenty_thirty_lower)
  final_df <- do.call(rbind,data)
  
  # attach location metadata 
  
  locs <- query("SELECT location_id, location_name, super_region_id, super_region_name FROM ADDRESS 
                where location_set_id = 35 and location_set_version_id = 59;","gbd")
  
  final_df <- merge(final_df ,locs)
  
  #reshape from long to wide
  
  test<-dcast(final_df, location_id+ihme_loc_id+location_name + super_region_id + super_region_name + sdi_quintile ~indicator_stamp, value.var = "goal_met")
  
  return(test)  
}

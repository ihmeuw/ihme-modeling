###########################################################################################################################################
# Date created: 6/12/17
# Description: Script for calculating attainment of SDG goals attained by country, super region, and SDI quintile using 
# conservative goal thresholds
#
# How to use:
#
# source("FILEPATH")
# get_conservative_goal_met(17)
#
###########################################################################################################################################
## Clear environment

rm(list = ls())

## Load packages

require(pacman)
pacman::p_load(data.table, reshape2, RMySQL, ini,magrittr)

#set root
if (.Platform$OS.type=="unix") {
  root <- 'FILEPATH'
} else {
  root <- 'FILEPATH'
}

#function gets sourced in aggregation script

get_conservative_goal_met <- function(version=17, attainment_year = 2030, eliminate_start_year = 2015, eliminate_denom = .8, coverage_threshold_version = "resubmission") {
  
  # Goal:       
  #             Pull data for indicators with targets and compute the conservative target goal based on its target type.
  #
  #             Indicators with elimination as their targets get assigned new targets, which is 80% reduction from its 2015 mean_val
  #
  #             If conservative target < original target, original target does not get replaced
  # 
  #             Compare the computed target value to the forecasted data (2020 for Road Inj & 2030 for the rest)
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
  #             eliminate_start_year toggles the year from which relative targets are calculated (2015 dUSERt)
  #             eliminate_denom toggles the amount that eliminate targets must be reduced by (80% dUSERt - .8)
  #             coverage_threshold_version toggles the targets that are used to calculate attainment (resubmission dUSERt)
  #             different versions represent different attainment thresholds - resubmission was the final version (80% reduction for eliminate; >= 90% universal access or coverage (<=10% for SEVs))
  
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
  
  }else if(attainment_year != 2030){
    
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
  
  #helper function to calculate target thresholds for eliminate targets
  
  eliminate_reduced_by <- function(mean) {
    mean <- mean * (1-eliminate_denom)
    return(mean)
  }
  
  #mean_val should be higher than the targets for these indicators
  
  goal_higher <- c(1057, 1035, 1037, 1061, 1034)
  
  #compute the goal_met for indicators
  
  twenty_thirty_higher <- sdg_unscaled[year_id == attainment_year & target_type == "absolute" & indicator_id %in% goal_higher,]

  #create list of indicators that require relative reductions to calculate conservative attainment
  
  conservative_goal <- c(1044,1045,1032,1000,1001,1002,1004,1047,1064) 
  
  #subset all remaining indicators
  
  twenty_thirty_lower <- sdg_unscaled[year_id == attainment_year & target_type == "absolute" & !(indicator_id %in% goal_higher) & !(indicator_id %in% conservative_goal) & indicator_id != 1033,]
  
  ## Relative reductions for all "eliminate" targets.
  
  #i_221
  Stunting <- sdg_unscaled[indicator_id == conservative_goal[1] & year_id == eliminate_start_year,]
  Stunting_2030 <- sdg_unscaled[indicator_id == conservative_goal[1] & year_id == attainment_year,]
  Stunting_2030[,target_value:=NULL]
  Stunting_final <- merge(Stunting_2030,
                          Stunting[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                          by ="location_id", all.x = TRUE)
  Stunting_final[, target_value := ifelse(target_value <= 0.5, 0.5, target_value)]
  Stunting_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #i_221a
  Wasting <- sdg_unscaled[indicator_id == conservative_goal[2] & year_id == eliminate_start_year,]
  Wasting_2030 <- sdg_unscaled[indicator_id == conservative_goal[2] & year_id == attainment_year,]
  Wasting_2030[,target_value:=NULL]
  Wasting_final <- merge(Wasting_2030,
                         Wasting[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                         by ="location_id", all.x = TRUE)
  Wasting_final[, target_value := ifelse(target_value <= 0.5, 0.5, target_value)]
  Wasting_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #i_221b
  Overweight <- sdg_unscaled[indicator_id == conservative_goal[3] & year_id == eliminate_start_year,]
  Overweight_2030 <- sdg_unscaled[indicator_id == conservative_goal[3] & year_id == attainment_year,]
  Overweight_2030[,target_value:=NULL]
  Overweight_final <- merge(Overweight_2030,
                            Overweight[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                            by ="location_id", all.x = TRUE)
  Overweight_final[, target_value := ifelse(target_value <= 0.5, 0.5, target_value)]
  Overweight_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #i_331
  HIV <- sdg_unscaled[indicator_id == conservative_goal[4] & year_id == eliminate_start_year,]
  HIV_2030 <- sdg_unscaled[indicator_id == conservative_goal[4] & year_id == attainment_year,]
  HIV_2030[,target_value:=NULL]
  HIV_final <- merge(HIV_2030,
                     HIV[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                     by ="location_id", all.x = TRUE)
  HIV_final[, target_value := ifelse(target_value <= 0.005, 0.005, target_value)]
  HIV_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #i_332
  TB <- sdg_unscaled[indicator_id == conservative_goal[5] & year_id == eliminate_start_year,]
  TB_2030 <- sdg_unscaled[indicator_id == conservative_goal[5] & year_id == attainment_year,]
  TB_2030[,target_value:=NULL]
  TB_final <- merge(TB_2030,
                    TB[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                    by ="location_id", all.x = TRUE)
  TB_final[, target_value := ifelse(target_value <= 0.5, 0.5, target_value)]
  TB_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #i_333
  Malaria <- sdg_unscaled[indicator_id == conservative_goal[6] & year_id == eliminate_start_year,]
  Malaria_2030 <- sdg_unscaled[indicator_id == conservative_goal[6] & year_id == attainment_year,]
  Malaria_2030[,target_value:=NULL]
  Malaria_final <- merge(Malaria_2030,
                         Malaria[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                         by ="location_id", all.x = TRUE)
  Malaria_final[, target_value := ifelse(target_value <= 0.005, 0.005, target_value)]
  Malaria_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #i_335
  NTDs <- sdg_unscaled[indicator_id == conservative_goal[7] & year_id == eliminate_start_year,]
  NTDs_2030 <- sdg_unscaled[indicator_id == conservative_goal[7] & year_id == attainment_year,]
  NTDs_2030[,target_value:=NULL]
  NTDs_final <- merge(NTDs_2030,
                      NTDs[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                      by ="location_id", all.x = TRUE)
  NTDs_final[, target_value := ifelse(target_value <= 0.5, 0.5, target_value)]
  NTDs_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #i_521
  IPV <- sdg_unscaled[indicator_id == conservative_goal[8] & year_id == eliminate_start_year,]
  IPV_2030 <- sdg_unscaled[indicator_id == conservative_goal[8] & year_id == attainment_year,]
  IPV_2030[,target_value:=NULL]
  IPV_final <- merge(IPV_2030,
                     IPV[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                     by ="location_id", all.x = TRUE)
  IPV_final[, target_value := ifelse(target_value <= 0.5, 0.5, target_value)]
  IPV_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  #i_1623
  CBA <- sdg_unscaled[indicator_id == conservative_goal[9] & year_id == eliminate_start_year,]
  CBA_2030 <- sdg_unscaled[indicator_id == conservative_goal[9] & year_id == attainment_year,]
  CBA_2030[,target_value:=NULL]
  CBA_final <- merge(CBA_2030,
                     CBA[,target_value := lapply(mean_val, function(x) eliminate_reduced_by(x))][,c("location_id", "target_value")],
                     by ="location_id", all.x = TRUE)
  CBA_final[, target_value := ifelse(target_value <= 0.5, 0.5, target_value)]
  CBA_final[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  ## Compute attainment
  
  twenty_thirty_higher[, goal_met := ifelse(mean_val >= target_value, 1, 0)]
  twenty_thirty_lower[, goal_met := ifelse(mean_val <= target_value, 1, 0)]
  
  data <- list(NCD_Mort_final,Suicide_final,Road_Inj_final,MMR_final,twenty_thirty_higher,
               twenty_thirty_lower, CBA_final,IPV_final, NTDs_final,Malaria_final,TB_final,
               HIV_final,Overweight_final,Wasting_final, Stunting_final)
  
  ## Bind them all
  
  final_df <- do.call(rbind,data)
  
  # attach location metadata 
  locs <- query("SELECT location_id, location_name, super_region_id, super_region_name FROM ADDRESS
                where location_set_id = 35 and location_set_version_id = 59;","gbd")
  
  final_df <- merge(final_df, locs)
  
  #reshape from long to wide
  
  test<-dcast(final_df, location_id+ihme_loc_id+location_name + super_region_id + super_region_name +sdi_quintile~indicator_stamp, value.var = "goal_met")
  
  return(test)
}

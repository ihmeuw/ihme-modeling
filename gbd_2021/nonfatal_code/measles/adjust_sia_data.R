#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  USERNAME
# Date:    
# Purpose: Correct bias in administrative measles SIA coverage using admin bias in DTP3 coverage estimates
# Path:    FILEPATH
#***********************************************************************************************************************


#----OPEN FUNCTION------------------------------------------------------------------------------------------------------
adjust_sia_data <- function(...) {
  #***********************************************************************************************************************
  
  
  #----CONFIG-------------------------------------------------------------------------------------------------------------
  ### runtime configuration
  if (Sys.info()["sysname"] == "Linux") {
    j_root <- "FILEPATH" 
    h_root <- "FILEPATH"
  } else { 
    j_root <- "FILEPATH"
    h_root <- "FILEPATH"
  }
  model_root <- paste0("FILEPATH/vaccines")
  
  ### load packages
  pacman::p_load(magrittr, foreign, stats, MASS, data.table, dplyr, plyr, lme4, reshape2, parallel, readxl)
  
  ### load shared functions
  source("/FILEPATH/utility.r")
  source("/FILEPATH/get_covariate_estimates.R") 
  source("/FILEPATH/get_location_metadata.R") 
  source("/FILEPATH/get_population.R") 
  
  ### load custom functions
  "/FILEPATH/read_excel.R" %>% source
  
  ### set objects
  acause <- "measles"
  age_start <- 2             ## age "early neonatal"
  age_end <- 17              ## age 60-64 years
  a <- 3                     ## birth cohort years before 1980
  
  if(gbd_round ==  7) gbd_cycle <- "gbd2020"
  
  ### directories
  home <- file.path("FILEPATH/00_documentation")
  #***********************************************************************************************************************
  
  
  #----GET DATA-----------------------------------------------------------------------------------------------------------
  ### get location data,  drop US subnational Georgia to avoid confusion
  locations2 <- locations[location_id != 533, ]
  
  ### prep WHO supplementary immunization activity data, 1987 through 1999
  # SIA data prior to 2010 obtained from personal communication
  sia_1987_1999 <- fread("FILEPATH/Standardized_measles_SIAs_1987_1999.csv")
  setnames(sia_1987_1999, c("yr", "iso", "reached_pct", "country"), c("year_id", "ihme_loc_id", "prop_reached", "location_name"))
  sia_1987_1999[is.na(reached) & !is.na(prop_reached) & !is.na(target), reached := prop_reached * target]
  sia_1987_1999 <- sia_1987_1999[!(is.na(reached) | reached==0) &
                                   year_id < 2000, ]
  
  # fix location names
  sia_1987_1999[location_name=="CAR", location_name := "Central African Republic"]
  sia_1987_1999[location_name=="DRCongo", location_name := "Democratic Republic of the Congo"]
  sia_1987_1999[location_name=="Hong Kong", location_name := "Hong Kong Special Administrative Region of China"]
  sia_1987_1999[location_name=="Saint Kitts & Nevis", location_name := "Saint Kitts and Nevis"]
  sia_1987_1999[location_name=="Sao Tome & Principe", location_name := "Sao Tome and Principe"]
  sia_1987_1999[location_name=="UAE", location_name := "United Arab Emirates"]
  sia_1987_1999[location_name=="VietNam", location_name := "Viet Nam"]
  sia_1987_1999[location_name=="DPRKorea", location_name := "Democratic People's Republic of Korea"]
  sia_1987_1999[location_name=="UK", location_name := "United Kingdom"]
  sia_1987_1999[location_name=="Bolivia", location_name := "Bolivia (Plurinational State of)"] #loc name changes below this row new GBD 2020
  sia_1987_1999[location_name=="Iran", location_name := "Iran (Islamic Republic of)"]
  sia_1987_1999[location_name=="Swaziland", location_name := "Eswatini"]
  sia_1987_1999[location_name=="Syria", location_name := "Syrian Arab Republic"]
  sia_1987_1999[location_name=="Tanzania", location_name := "United Republic of Tanzania"]
  sia_1987_1999[location_name=="Venezuela", location_name := "Venezuela (Bolivarian Republic of)"]
  # merge ihme location ids
  sia_1987_1999 <- merge(sia_1987_1999, locations2[, .(location_id, location_name)], by="location_name", all.x=TRUE)
  #drop locs that aren't ones we estimate covariates for (ie french polynesia, new caledonia, zanzibar)
  if (length(sia_1987_1999[is.na(location_id), location_name] %>% unique) > 0) print(paste0("dropping locations with missing location_id: ", paste(sia_1987_1999[is.na(location_id), location_name] %>% unique, collapse=", ")))
  sia_1987_1999 <- sia_1987_1999[!is.na(location_id), ]
  # prep only necessary columns
  sia_1987_1999 <- sia_1987_1999[, c("location_id", "year_id", "target", "reached", "prop_reached"), with=FALSE]
  
  ### post-2000 SIA data downloaded from http://www.who.int/immunization/monitoring_surveillance/data/en/
  sia_2 <- read_excel(file.path("FILEPATH"), skip=1, sheet="SIAs_Jan2000_Dec2020") %>% as.data.table
  setnames(sia_2, c("Country", "ISO", "Year", "Target population", "Reached population", "% Reached", "Age group", "Survey results"), c("location_name", "ihme_loc_id", "year_id", "target", "reached", "prop_reached", "ages", "survey_coverage"))  # from "Age Group" to "Age"
  # fill in number reached from prop and target when possible
  sia_2[is.na(reached) & !is.na(prop_reached) & !is.na(target), reached := prop_reached * target]
  # impute population reached for recent surveys missing number from last survey with available number, if there is a survery with available reached data
  missing_cov <- sia_2[`Imp status` == "done" & is.na(reached) & !is.na(target) & year_id %in% (year_end_data-2):year_end_data,]
  for(loc in unique(missing_cov$ihme_loc_id)){
    completed <- sia_2[ihme_loc_id==loc & `Imp status` == "done" & !is.na(target) & !is.na(reached),]
    if(nrow(completed)>0){
      index <- nrow(completed)
      sia_2[`Imp status` == "done" & is.na(reached) & !is.na(target) & year_id %in% (year_end_data-2):year_end_data & ihme_loc_id==loc, reached := target*(completed[index, reached]/completed[index, target])] 
    }
    
  }
  # drop where number of administered vaccines is missing
  sia_2 <- sia_2[Intervention %in% c("Measles", "MMR", "MR", "measles") &
                   !(is.na(reached) | reached==0), ]
  # keep survey data
  sia_2_survey <- sia_2[!is.na(survey_coverage), .(ihme_loc_id, year_id, survey_coverage, prop_reached)] %>% unique
  sia_2_survey[, survey_coverage := survey_coverage / 100]
  # collapse by country-year
  sia_2 <- sia_2[!duplicated(sia_2), ]
  sia_2 <- sia_2[, .(target=sum(target), reached=sum(reached)), by=c("ihme_loc_id", "year_id")] %>% .[, .(ihme_loc_id, year_id, reached, target)] %>% unique
  # merge ihme location ids
  #first fix hong kong ihme_loc_id
  sia_2[ihme_loc_id=="HKG", ihme_loc_id := "CHN_354"]
  sia_2 <- merge(sia_2, locations2[, .(ihme_loc_id, location_id)], by="ihme_loc_id", all.x=TRUE)
  #dropping locs we don't estimate (should drop MAC)
  if (length(sia_2[is.na(location_id), ihme_loc_id] %>% unique) > 0) print(paste0("dropping locations with missing location_id: ", paste(sia_2[is.na(location_id), ihme_loc_id] %>% unique, collapse=", ")))
  sia_2 <- sia_2[!is.na(location_id), ]
  # add on survey coverage
  sia_2 <- merge(sia_2, sia_2_survey, by=c("ihme_loc_id", "year_id"), all.x=TRUE)
  #***********************************************************************************************************************
  
  
  #----BIAS ADJUSTMENT----------------------------------------------------------------------------------------------------
  ### SIA bias correction
  # bind together all years of SIA data
  who_sia <- rbind(sia_1987_1999, sia_2, fill=TRUE)
  
  # prep SIA as a continuous measure representing the proportion of the population under 15 reached
  if (gbd_round < 7) {
    sum_pop_all_sia <- population[age_group_id <= 8, .(target_pop=sum(population)), by=c("location_id", "year_id")] %>% unique
    who_sia <- merge(who_sia, sum_pop_all_sia, by=c("location_id", "year_id"), all.x=TRUE)
  } else if (gbd_round == 7) {
    sum_pop_all_sia <- population[age_group_id %in% c(2,3,388,389,238,34,6,7,8), .(target_pop=sum(population)), by=c("location_id", "year_id")] %>% unique
    who_sia <- merge(who_sia, sum_pop_all_sia, by=c("location_id", "year_id"), all.x=TRUE)
  }
  # if reported target population is missing or zero, replace with target_pop from under 15 GBD population
  who_sia[is.na(target) | target==0, target := target_pop]
  
  # calculate administrative coverage from reported doses and target population
  who_sia[, administrative_coverage := reached / target] # target_pop
  
  # get DTP3 coverage modeled administrative bias, used as a proxy for bias ratio of post-campaign survey coverage to administrative reported coverage
  if(bias_model_run_date == 20200515){
    RUNS <-  fread("FILEPATH/bias_run_log.csv")[me_name=="vacc_dpt3" & date_version == bias_model_run_date, run_id] 
    setwd(model_root)
    source("init.r")
    
    source("/FILEPATH/utility.r")
    dtp3_bias <- rbindlist(lapply(RUNS, function(x) model_load(x, obj="raked") %>% data.table %>% .[, run_id := x]))[, .(location_id, year_id, gpr_mean)]
    setnames(dtp3_bias, "gpr_mean", "cv_admin_bias_ratio")
    
    } else if(bias_model_run_date == "2020_08_14_3"){
      
      RUNS <-  fread("/FILEPATH/bias_run_log.csv")[me_name=="vacc_dpt3" & date_version == bias_model_run_date, run_id] 
      setwd(model_root)
      source("init.r")
      
      dtp3_bias <- rbindlist(lapply(RUNS, function(x) model_load(x, obj="raked") %>% data.table %>% .[, run_id := x]))[, .(location_id, year_id, gpr_mean)]
      setnames(dtp3_bias, "gpr_mean", "cv_admin_bias_ratio")
      
      print("STGPR bias loaded")
      
      # hybridize with MR-BRT modeled outputs!
      df.adjust <- readRDS("FILEPATH/bias_correction_data_pairs.rds")
      ### NEW: "hybridize" MR-BRT cascade output with ST-GPR output depending on if a data location or not
      data_locs <- unique(df.adjust$location_id)
      # grab subnationals associated with national data locs and append to data_locs vector only if parent_id already in data_locs
      locations_2 <- get_location_metadata(location_set_id = 22, gbd_round_id = 7, decomp_step = "iterative")
      has_subs <- locations_2[level==3 & most_detailed != 1, location_id]
      sub_ids1 <- locations_2[level >3 & parent_id %in% has_subs & parent_id %in% data_locs, location_id]
      sub_ids2 <- locations_2[level >3 & parent_id %in% sub_ids1, location_id]  
      sub_ids3 <- locations_2[level >3 & parent_id %in% sub_ids2, location_id]
      sub_ids4 <- locations_2[level >3 & parent_id %in% sub_ids3, location_id]
      data_locs <- c(data_locs, sub_ids1, sub_ids2, sub_ids3, sub_ids4) %>% unique
      # global_locs to pull from mrbrt is everything else!
      global_locs <- setdiff(locations_2[level >= 3, location_id], data_locs)  # setdiff from in mrbrt to pull out "global" locs 
      
      print("Idenitifed locations to pull from mrbrt")
      
      # read in mrbrt cascade results
      work_dir <- paste0("/FILEPATH/2020-08-14-3")
      mrbrt_results <- lapply(c("vacc_dpt3", "vacc_mcv1", "vacc_polio3", "vacc_bcg"), function(x) {
        mrbrt <- fread(file.path(work_dir, x, paste0("mrbrt_", x, "_results.csv")))
      }) %>% rbindlist
      # subset down to global_locs
      mrbrt_results <- mrbrt_results[location_id %in% global_locs & me_name=="vacc_dpt3"]
      setnames(mrbrt_results, "pred", "cv_admin_bias_ratio")
      #remove unnecessary columns for merge
      mrbrt_results <- mrbrt_results[, `:=` (mean_value=NULL, me_name=NULL, mrbrt_bias_est=1)]
      
      # remove global_locs from existing bias object from stgpr
      dtp3_bias <- dtp3_bias[!location_id %in% unique(mrbrt_results$location_id)]
      # rbind global_locs bias from mrbrt
      dtp3_bias <- rbind(dtp3_bias, mrbrt_results, fill=TRUE)
      dtp3_bias[is.na(mrbrt_bias_est), mrbrt_bias_est := 0]
    } else {
      # read in from combined stgpr mrbrt file  started saving after the august 14th run
      dtp3_bias <- readRDS(file=paste0("FILEPATH/admin_bias/hybridized_bias.rds"))
    }
  
  # use DTP3 administrative bias to predict bias in measles SIAs in location-years without post-campaign surveys
  who_sia <- merge(who_sia, dtp3_bias, by=c("location_id", "year_id"), all.x=TRUE)
  who_sia[is.na(survey_coverage), survey_coverage := administrative_coverage * cv_admin_bias_ratio]
  
  # calculate bias ratio of post-campaign survey coverage to administrative reported coverage
  who_sia[, post_campaign_survey_bias_ratio := survey_coverage / administrative_coverage]
  
  # coverage variable of interest: corrected number of doses administered divided by the population under age 15 (target pop)
  who_sia[, corrected_target_coverage := (reached * post_campaign_survey_bias_ratio) / target_pop]
  
  # save SIA data 
  fwrite(who_sia, file.path(j.version.dir.inputs, "prepped_sia_data.csv"), row.names = FALSE)
  
  ### ignore other columns
  who_sia <- who_sia[year_id < year_end, c("location_id", "year_id", "corrected_target_coverage"), with=FALSE]
  #***********************************************************************************************************************
  
  
  #----END FUNCTION-------------------------------------------------------------------------------------------------------
  return(who_sia)
  
}
#***********************************************************************************************************************
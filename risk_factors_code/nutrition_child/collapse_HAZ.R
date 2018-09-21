
###########################################################
### Project: ubCov
### Purpose: Collapse ubcov extraction output for HAZ
###########################################################

###################
### Setting up ####
###################

rm(list=ls())

#Load libraries using pacman 

options(java.parameters = "-Xmx8000m")
library(pacman)
p_load(data.table, readstata13, haven, dplyr, survey, binom, RMySQL, xlsx, ggplot2)
p_load(data.table, readstata13, haven, dplyr, survey, binom, RMySQL, ggplot2)

## Set root filepaths
input_root <- FILEPATH
output_root <- FILEPATH

######################################################################################################################
#Load functions
ubcov_central_repo <- ifelse(Sys.info()["user"] == "USERNAME", "FILEPATH/ubcov_central", "FILEPATH/ubcov_central")
source(paste0(ubcov_central_repo, "/collapse.R"))
source(paste0(ubcov_central_repo, "/format_epi.R"))
source("FILEPATH/format_epi.R")
source(paste0(ubcov_central_repo, "/collapse_master.R"))
source("FILEPATH/collapse_master.R")

######################################################################################################################
## Indicator(s) of interest
vars <- c("HAZ", "HAZ_b1", "HAZ_b2", "HAZ_b3")
runtime <- system.time(
  #Call function 
  out <- collapse_ubcov(
    
    #REQUIRED 
    vars = vars,
    
    stratify_by = c("nid", "ihme_loc_id", "year_start", "year_end", "age_start", "age_end", "sex"),
    calc.sd = TRUE, stdev_format = "long",
    cut_custom_age = TRUE, cut_ages = c(0,0.01917808,0.07945205,1,2,5), missing_age = "drop", #7/365 = 0.01917808 #29/365 = 0.07945205
    aggregate_under5 = FALSE, tabulate_pregnant = FALSE,
    allow_missing_vars = FALSE, drop_lonely = TRUE,
    #Formatting options (major)
    prev_or_prop = "proportion",
    validate = FALSE, save_combined = TRUE,
    #output_name = output_name,
    save_xlsx = FALSE, save_warnings = FALSE, 
    #Formatting options (minor)
    case_name = "", case_definition = "", case_diagnostics = "",
    recall_type = "Point", source_type = "Survey - cross-sectional", note_SR = "", keep_metadata = FALSE,
    test_file = "" 
  )
)

upload <- out$upload
warnings <- out$warnings

csv <- read.csv(paste0(output_root,"/ubcov_tabulation_extractor.csv"), stringsAsFactors = FALSE)

vars <- c("HAZ_b1", "HAZ_b2", "HAZ_b3", "HAZ", "HAZ_standard_deviation")
for (varx in vars) {
  file <- subset(csv, csv$var==varx)
  file$age_end <- file$age_end + 1
  file$age_demographer <- 0
  file$smaller_site_unit[file$nid == 156268] <- 1
  file$site_memo[file$nid == 156268] <- "Kosovo"
  file$representative_name[file$nid == 150871] <- "Unknown"
  file$representative_name[file$nid == 156276] <- "Unknown"
  file$representative_name[file$nid == 265153] <- "Unknown"
  file$representative_name[file$nid == 106158] <- "Unknown"
  file$cv_who_2006_standard <- 0
  file$sex <- ifelse(file$sex == 1, "Male", "Female")
  file$representative_name[is.na(file$representative_name)] <- "Unknown"
  file$urbanicity_type[is.na(file$urbanicity_type)] <- "Unknown"
  
  if (varx == "HAZ") {
    file$mean <- (file$mean+10)/10
    file$is_outlier[file$mean > 1.3] <- 1
  }
  
  if (varx == "HAZ_standard_deviation") {
    file$mean <- file$mean/10
  }
  
  if (varx == "HAZ_b1" | varx == "HAZ_b2" | varx == "HAZ_b3"){
    file$measure <- "prevalence"
  }
  output_name <- output_name <- paste("ubcov_tabulation", varx, Sys.info()["user"], Sys.Date(), sep = "_")
  write.csv(file, paste0(output_root, "/", output_name, ".csv"), row.names = FALSE)
}

warnings



# change when files are named after bundle 
vars <- c("stuntingminus1", "stuntingminus2", "stuntingminus3","underweightminus1", "underweightminus2", "underweightminus3",
         "wastingminus1", "wastingminus2_prevalence", "wastingminus2_proportion", "wastingminus3")
for (varx in vars){

  data1 <- read.csv(FILEPATH, stringsAsFactors = FALSE)
  data1$measure <- "prevalence"
  datakeep <- subset(data1, data1$extractor != "USERNAME")
  datakeep$group[!is.na(datakeep$group_review)] <- 1
  datakeep$specificity[!is.na(datakeep$group_review)] <- "undefined"
  datadelete <- subset(data1, data1$extractor == "USERNAME")
  datadelete <- datadelete[,"seq", drop = FALSE]
  write.csv(datakeep, FILEPATH, row.names = FALSE)
  write.csv(datadelete, FILEPATH, row.names = FALSE)
}

vars <- c("stuntingmean", "stuntingsd", 
          "underweightmean", "underweightsd",
          "wastingmean", "wastingsd")

for (varx in vars){
  data1 <- read.csv(FILEPATH, stringsAsFactors = FALSE)
  datadelete <- subset(data1, data1$extractor == "USERNAME")
  datadelete <- datadelete[FILEPATH,"seq", drop = FALSE]
  write.csv(data1, FILEPATH, row.names = FALSE)
}




#####
file <- read.csv(FILEPATH)
file$age_end <- file$age_end + 1
vars_to_keep <- c("var","nid", "ihme_loc_id", "sex", "year_start", "age_start", "age_end", "mean", "sample_size", "effective_sample_size")
file <- file[vars_to_keep]
file <- dcast(file, nid + ihme_loc_id + sex + year_start + age_start + age_end + sample_size + effective_sample_size ~ var, value.var = "mean")


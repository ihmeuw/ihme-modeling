
###########################################################
### Project: ubCov
### Purpose: Collapse ubcov extraction output for WAZ
###########################################################

###################
### Setting up ####
###################

rm(list=ls())

#Load libraries using pacman 

options(java.parameters = "-Xmx8000m")
library(pacman)
p_load(data.table, readstata13, haven, dplyr, survey, binom, RMySQL, xlsx)
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
source("FILEPATH/collapse_master.R")

######################################################################################################################
## Indicator(s) of interest
vars <- c("WAZ", "WAZ_b1", "WAZ_b2", "WAZ_b3")

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

vars <- c("WAZ", "WAZ_b1", "WAZ_b2", "WAZ_b3", "WAZ_standard_deviation")
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
  
  if (varx == "WAZ") {
    file$mean <- (file$mean+10)/10
    file$is_outlier[file$mean > 1.3] <- 1
  }
  
  if (varx == "WAZ_standard_deviation") {
    file$mean <- file$mean/10
  }

  if (varx == "WAZ_b1" | varx == "WAZ_b2" | varx == "WAZ_b3"){
    file$measure <- "prevalence"
  }
  
  output_name <- output_name <- paste("ubcov_tabulation", varx, Sys.info()["user"], Sys.Date(), sep = "_")
  write.csv(file, paste0(output_root, "/", output_name, ".csv"), row.names = FALSE)
}

warnings

file <- read.csv(FILEPATH)
file$age_end <- file$age_end + 1
file <- subset(file, !is.na(file$effective_sample_size))
vars_to_keep <- c("var","nid", "ihme_loc_id", "sex", "year_start", "age_start", "age_end", "mean", "sample_size", "effective_sample_size")
file <- file[vars_to_keep]
file <- dcast(file, nid + ihme_loc_id + sex + year_start + age_start + age_end + sample_size + effective_sample_size ~ var, value.var = "mean")

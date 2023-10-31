##Script to upload MI survivors into post-MI model


suppressMessages(library(R.utils))
library(openxlsx)
library(haven)
library(data.table)

date <- gsub("-", "_", Sys.Date())

suppressMessages(sourceDirectory(paste0("/FILEPATH/")))
source("/FILEPATH/get_demographics.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/validate_input_sheet.R")
source("/FILEPATH/upload_bundle_data.R")
source("/FILEPATH/save_bundle_version.R")

demographics <- get_demographics(gbd_team="epi")
locations <- unlist(demographics$location_id, use.names=F)

args <- commandArgs(trailingOnly = TRUE)
decomp_step <- args[1]
decomp_step <- "iterative"
gbd_round_id <- 7
years=c(1990, 1995, 2000, 2005, 2010, 2015, 2020)

#Delete current data
 #current <- data.table(get_bundle_data(bundle_id=583, decomp_step = 'iterative', gbd_round_id = 7))
 #current <- subset(current, measure=="incidence" & nid==239851)
 #current <- current[,c("bundle_id", "seq")]

 #filename <- paste0("/FILEPATH/del_all_", date, ".xlsx")
 #write.xlsx(current, filename, sheetName="extraction", na="")
 #validate_input_sheet(bundle_id=583, filename, logs)
 #upload_bundle_data(bundle_id=583, gbd_round = 7, decomp_step = "iterative", filename)

#Load ages
ages <- data.frame(get_ids("age_group"))
ages$age_start <- sapply(strsplit(ages$age_group_name, " to "), "[", 1)
ages$age_start <- as.numeric(with(ages, ifelse(age_group_id==235, 95, age_start)))
ages$age_end <- sapply(strsplit(ages$age_group_name, " to "), "[", 2)
ages$age_end <- as.numeric(with(ages, ifelse(age_group_id==235, 99, age_end)))
ages <- subset(ages, age_group_id %in% c(8:20, 30, 31, 32, 235))

#Set working directory
setwd(paste0("/FILEPATH/"))

#Set log path
logs <- "/FILEPATH"
#Set timestamp
date <- gsub("-", "_", Sys.Date())

#Pull in files from parallel jobs
df <- list()
for (i in 1:length(locations))
{
  df[[i]] <- readRDS(paste0("mi_survivors_", locations[i], ".rds"))
  # df[[i]] <- data.frame(read_dta(paste0("mi_survivors_", locations[i], ".dta")))
}

all <- do.call(rbind, df)
names(all)[5:7] <- paste0(names(all)[5:7], ".new")

all <- merge(all, ages[,c("age_group_id", "age_start", "age_end")], by="age_group_id")
all$year_start <- all$year_id
all$year_end <- all$year_id
all$sex <- with(all, ifelse(sex_id==1, "Male", ifelse(sex_id==2, "Female", NA)))


#Format for upload
iter <- 2  # DATE - first upload, decomp stage 1
#DATE - second round; changed iter to 2
#DATE - problem with missing locations; deleted all survivor data and will upload again

if (iter==1) {
  
  all$seq <- NA
  all$modelable_entity_id <- 15755
  all$modelable_entity_name <- "Post-MI IHD"
  all$bundle_id <- 583
  all$nid <- 239851 #need to fix
  all$field_citation_value <- "IHME GBD DisMod Ischemic Heart Disease Excess Mortality Estimates"
  all$source_type <- "Mixed or estimation"
  all$smaller_site_unit <- 0
  
  all$age_demographer <- 1
  all$age_issue <- 0
  
  all$sex_issue <- 0
  
  all$year_issue <- 0
  
  all$unit_type <- "Person"
  all$unit_value_as_published <- 1
  all$measure_adjustment <- 0
  all$measure_issue <- 0
  all$measure <- "incidence"
  all$case_definition <- "incidence*30-day cfr"
  all$note_modeler <- "30-day MI survivors"
  all$extractor <- "USERNAME"
  all$is_outlier <- 0
  
  all$underlying_nid <- NA
  all$sampling_type <- NA
  all$representative_name <- "Unknown"
  all$urbanicity_type <- "Unknown"
  all$recall_type <- "Not Set"
  all$uncertainty_type <- NA
  all$input_type <- NA
  all$standard_error <- NA
  all$effective_sample_size <- NA
  all$design_effect <- NA
  all$site_memo <- NA
  all$case_name <- NA
  all$case_diagnostics <- NA
  all$response_rate <- NA
  all$note_SR <- NA
  all$uncertainty_type_value <- 95
  all$seq_parent <- NA
  all$recall_type_value <- NA
  all$cases <- NA
  all$sample_size <- NA
  
  all$mean <- all$mean.new
  all$lower <- all$lower.new
  all$upper <- all$upper.new
  
} else {
  old <- data.frame(get_bundle_data(583, decomp_step="iterative", gbd_round_id = 7))
  old <- subset(old, measure=="incidence" & nid==239851)
  all <- merge(all, old, by=c("location_id", "age_start", "age_end", "year_start", "year_end", "sex"), all.x=T) # , "age_group_id", "sex_id", "year_id"
  all$mean <- all$mean.new
  all$lower <- all$lower.new
  all$upper <- all$upper.new
  all$response_rate <- NA
}

all <- all[,c("sex", "bundle_id", "measure", "location_id", "year_start", "year_end",
              "age_start", "age_end", "nid", "representative_name", "sample_size", "source_type", "urbanicity_type",
              "recall_type", "unit_type", "unit_value_as_published", "cases", "is_outlier", "seq", "underlying_nid", 
              "sampling_type", "recall_type_value", "uncertainty_type", "uncertainty_type_value", "input_type", 
              "standard_error", "effective_sample_size", "design_effect", "response_rate", "extractor", "mean", "lower", "upper")]

incidence <- all[measure == "incidence"]

filename <- paste0("/FILEPATH/survivors_2021_02_01.xlsx")
write.xlsx(all, filename, sheetName="extraction", na="")
validate_input_sheet(bundle_id=583, filename, logs)
upload_bundle_data(bundle_id=583, filename, decomp_step="iterative", gbd_round_id = 7)

#------
bv <- save_bundle_version(bundle_id=583, decomp_step= "iterative", gbd_round_id = 7, include_clinical = 'None')
bunv <- get_bundle_version(37364, fetch = 'all') #previous version 36788 37364
unique(bunv$measure)
bunv$crosswalk_parent_seq <- NA
bunv <- bunv[measure!="cfr"]
csmr <- bunv[measure=="mtspecific"]
csmr <- bunv[measure=="mtspecific" & year_start %in% years]
incidence <- bunv[measure=="incidence" & nid == 239851 & year_start %in% years]

bunv <- bunv[measure!="mtspecific"]
bunv <- bunv[nid!=239851]
bunv <- rbind(bunv, incidence)
bunv <- rbind(bunv, csmr)

filename <- paste0("/FILEPATH/s2_csmr_survivors_date_2021_02_01.xlsx")
write.xlsx(bunv, filename, sheetName="extraction", na="")

source("/FILEPATH/save_bundle_version.R")

bundle_version_id <- 37271
description <- "Step 2 CSMR with survivors from 618212" 
result <- save_crosswalk_version(bundle_version_id=bundle_version_id, data_filepath=filename, description=description)

bundle_version_id <- 37364
description <- "Step 2 CSMR with survivors from 618212, fixing Canada"
result <- save_crosswalk_version(bundle_version_id=bundle_version_id, data_filepath=filename, description=description)


##Script to split CSMR in to acute and chronic proportions
.libPaths("FILEPATH")
suppressMessages(library(R.utils))
library(openxlsx)

#Load shared functions
suppressMessages(sourceDirectory(paste0(jpath, "FILEPATH")))

demographics <- get_demographics(gbd_team="epi")
locations <- unlist(demographics$location_id, use.names=F)

ages <- data.frame(get_age_metadata(age_group_set_id=12))
names(ages) <- gsub("_group_years", "", names(ages))
ages <- subset(ages, age_group_id %in% c(8:20, 30, 31, 32, 235))

#Set working directory
setwd("FILEPATH")

#Set timestamp
date <- gsub("-", "_", Sys.Date())

#Set log path
logs <- "FILEPATH"

#Pull in files from parallel jobs
df <- list()
for (i in 1:length(locations))
	{
		df[[i]] <- readRDS(paste0("chronic_", locations[i], ".rds"))
	}

all <- do.call(rbind, df)
names(all)[5:6] <- paste0(names(all)[5:6], ".new")

all <- merge(all, ages[,c("age_group_id", "age_start", "age_end")], by="age_group_id")
all$year_start <- all$year_id
all$year_end <- all$year_id
all$sex <- with(all, ifelse(sex_id==1, "Male", ifelse(sex_id==2, "Female", NA)))

iter <- 1

if (iter==1) {
	all$seq <- NA
	all$bundle_id <- 583
	all$bundle_name <- "Post-MI IHD"
	all$nid <- 239850
	all$field_citation_value <- "IHME GBD 2015 DisMod Ischemic Heart Diseases Excess Mortality Estimates"
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
	all$measure <- "mtspecific"
	all$case_definition <- "(1-MI/IHD)*IHD deaths"
	all$note_modeler <- "custom IHD csmr"
	all$extractor <- "USERNAME"
	all$is_outlier <- 0

	all$underlying_nid <- NA
	all$sampling_type <- NA
	all$representative_name <- "Unknown"
	all$urbanicity_type <- "Unknown"
	all$recall_type <- "Not Set"
	all$uncertainty_type <- "Sample size"
	all$input_type <- NA
	all$upper <- NA
	all$lower <- NA
	all$standard_error <- NA
	all$effective_sample_size <- NA
	all$design_effect <- NA
	all$site_memo <- NA
	all$case_name <- NA
	all$case_diagnostics <- NA
	all$response_rate <- NA
	all$note_SR <- NA
	all$uncertainty_type_value <- NA
	all$seq_parent <- NA
	all$recall_type_value <- NA
	all$cases <- NA

	all$mean <- all$mean.new
	all$sample_size <- all$sample_size.new
	all$lower <- NA
	all$upper <- NA

} else {
	print("getting data")
	old <- get_epi_data(114)
	old <- subset(old, measure=="mtspecific" & nid==239850)
	all <- merge(all, old, by=c("location_id", "age_start", "age_end", "year_start", "year_end", "sex"), all.X=T)
	all$mean <- all$mean.new
	all$sample_size <- all$sample_size.new
	all$upper <- NA
	all$lower <- NA
	all$uncertainty_type_value <- NA
	all$response_rate <- NA
	all$standard_error <- NA
	all$effective_sample_size <- NA

}

all <- subset(all, mean<1)
all <- subset(all, sample_size!=0)
all <- all[,c("sex", "bundle_name", "measure", "location_id", "year_start", "year_end", 
           "age_start", "age_end", "nid", "representative_name", "sample_size", "source_type", "urbanicity_type",
           "recall_type", "unit_type", "unit_value_as_published", "cases", "is_outlier", "seq", "underlying_nid",
           "sampling_type", "recall_type_value", "uncertainty_type", "uncertainty_type_value", "input_type", 
           "standard_error", "effective_sample_size", "design_effect", "response_rate", "extractor", "mean", "lower", "upper")]

filename <- paste0(jpath, "FILEPATH/ihd_csmr_", date, ".xlsx")
write.xlsx(all, filename, sheetName="extraction", na="")
validate_input_sheet(bundle_id=583, filename, logs)
upload_epi_data(bundle_id=583, filename)

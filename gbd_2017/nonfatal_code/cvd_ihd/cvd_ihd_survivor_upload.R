##Script to upload MI survivors into post-MI model
rm(list = setdiff(ls(), c(lsf.str(), "jpath", "hpath", "os")))
.libPaths("FILEPATH")
suppressMessages(library(R.utils))
library(openxlsx)
library(haven)

suppressMessages(sourceDirectory(paste0(jpath, "FILEPATH")))

demographics <- get_demographics(gbd_team="epi")
locations <- unlist(demographics$location_id, use.names=F)

#Delete current data
# current <- data.frame(get_epi_data(bundle_id=583))
# current <- subset(current, measure=="incidence" & nid==239851)
# current <- current[,c("bundle_id", "seq")]

# filename <- paste0(jpath, "WORK/12_bundle/cvd_ihd/583/03_review/02_upload/del_all_", date, ".xlsx")
# write.xlsx(current, filename, sheetName="extraction", na="")
# validate_input_sheet(bundle_id=583, filename, logs)
# upload_epi_data(bundle_id=583, filename)

#Load ages
ages <- data.frame(get_ids("age_group"))
ages$age_start <- sapply(strsplit(ages$age_group_name, " to "), "[", 1)
ages$age_start <- with(ages, ifelse(age_group_id==235, 95, age_start))
ages$age_end <- sapply(strsplit(ages$age_group_name, " to "), "[", 2)
ages$age_end <- with(ages, ifelse(age_group_id==235, 99, age_end))
ages <- subset(ages, age_group_id %in% c(8:20, 30, 31, 32, 235))

#Set working directory
setwd("FILEPATH")

#Set log path
logs <- "FILEPATH"
#Set timestamp
date <- gsub("-", "_", Sys.Date())

#Pull in files from parallel jobs
df <- list()
for (i in 1:length(locations))
	{
		#df[[i]] <- readRDS(paste0("mi_survivors_", locations[i], ".rds"))
		df[[i]] <- data.frame(read_dta(paste0("mi_survivors_", locations[i], ".dta")))
	}

all <- do.call(rbind, df)
names(all)[5:7] <- paste0(names(all)[5:7], ".new")

all <- merge(all, ages[,c("age_group_id", "age_start", "age_end")], by="age_group_id")
all$year_start <- all$year_id
all$year_end <- all$year_id
all$sex <- with(all, ifelse(sex_id==1, "Male", ifelse(sex_id==2, "Female", NA)))


#Format for upload
iter <- 1 #03/08/2018 - second round; changed iter to 2
		  #07/23/2018 - problem with missing locations; deleted all survivor data and will upload again

if (iter==1) {

	all$seq <- NA
	all$modelable_entity_id <- 15755
	all$modelable_entity_name <- "Post-MI IHD"
	all$bundle_id <- NA
	all$nid <- 239851 #need to fix
	all$field_citation_value <- "Institute for Health Metrics and Evaluation (IHME). IHME DisMod Output as Input Data 2016 (IHD CSMR)"
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
	old <- data.frame(get_epi_data(583))
	old <- subset(old, measure=="incidence" & nid==239851)
	all <- merge(all, old, by=c("location_id", "age_start", "age_end", "year_start", "year_end", "sex", "age_group_id", "sex_id", "year_id"), all.x=T)
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

filename <- paste0(jpath, "FILEPATH/survivors_", date, ".xlsx")
write.xlsx(all, filename, sheetName="extraction", na="")
validate_input_sheet(bundle_id=583, filename, logs)
upload_epi_data(bundle_id=583, filename)


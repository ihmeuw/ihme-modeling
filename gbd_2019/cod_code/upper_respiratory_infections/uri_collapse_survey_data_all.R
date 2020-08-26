######################################################################################
## URI surveys are collapsed using methods from MRI.
## Results are pulled from the LRI survey cleaning from UbCov
## and tabulated at the survey-location-age-sex-year level accounting for the
## survey weights provided in the surveys. We assume that cough without difficulty
## breathing is the definition of upper respiratory infection. This means that this
## defintion is mutually exclusive from the LRI case definition which requires
## difficulty breathing. However, there are necessarily some children from these
## surveys with cough that are not counted as either LRI or URI because children with
## cough and difficulty breathing but without symptoms in the chest or fever are not
## counted as either. This should be revisted in future GBD iterations.
######################################################################################

#Toggle for whether we drop 0-case rows for which SE cannot be calculated, & keep SE
	dropping_zeros = FALSE

### PREP ###
	library(data.table)
	library(survey)
#Set filepaths
	root <- "filepath"
	currentdata <- "filepath"
	j_root <- "filepath"

#Read in supplementary information
	locs <- fread("filepath")
	locs <- locs[,c("location_name",
	                "location_id",
	                "super_region_name",
	                "super_region_id",
	                "region_name",
	                "region_id",
	                "parent_id", 
	                "ihme_loc_id",
	                "exists_subnational"
	                )]

	duration <- 5 # duration in days of a URI episode

#Read in dataset, rename columns
	load(paste0(currentdata))
	uri <- as.data.table(all)
	rm(all)
	setnames(uri, old = c('survey_name', 'iso3', 'year_start', 'year_end'), 
	         new = c('survey_series', 'ihme_loc_id', 'start_year', 'end_year'))

#Merge locs & seasonality scalars onto dataset
	uri <- join(uri, locs, by = "ihme_loc_id")

## Try to add information to map to GBD subnationals ##
	firstup <- function(x) {
	  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
	  x
	}

## Import a file created manually that helps to match the survey
## extractions to GBD locations based on admin_1 and iso3
# 'subnat' in this CSV means that there is a matched subnational location in GBD
	subnat_map <- read.csv("filepath")
	setnames(subnat_map, c("parent_id", "location"), c("iso3","admin_1"))

	uri$iso3 <- substr(uri$ihme_loc_id,1,3)
	uri <- join(uri, subnat_map, by=c("admin_1","iso3"))
	uri$subnat[is.na(uri$subnat)] <- 0

## Identify known subnationals ##
	uri$subname <- ifelse(uri$subnat==1, as.character(uri$ihme_loc_id_mapped), uri$iso3)

	uri$nid.new <- ifelse(uri$subnat==1, paste0(uri$nid,"_",uri$subname), uri$nid)

	nid.new <- unique(uri$nid.new)

### CLEAN UP DATA ###
#Change NA's to 9's in symptom columns
	uri$had_fever[is.na(uri$had_fever)] = 9
	uri$had_cough[is.na(uri$had_cough)] = 9
	uri$diff_breathing[is.na(uri$diff_breathing)] = 9
	uri$chest_symptoms[is.na(uri$chest_symptoms)] = 9

#Drop rows w/o sex, round ages, drop over 5s
	setnames(uri, old = 'sex_id', new = 'child_sex')
	uri$child_sex[is.na(uri$child_sex)] = 3
	uri$age_year <- floor(uri$age_year)
	uri <- subset(uri, age_year <= 5)

#Reassign pweight = 1 if missing or 0
#Include pwt_adj indicator if reassigned to 1
	uri <- uri[, pwt_adj := 0]
	uri <- uri[is.na(pweight) | pweight == 0, pwt_adj := 1]
	uri <- uri[is.na(pweight), pweight := 1]
	uri <- uri[pweight == 0, pweight := 1]

	uri <- uri[, start_n := .N, by = c("nid", "ihme_loc_id", "start_year")]

#Drop if missing PSU
	uri <- subset(uri, !is.na(psu))
	uri <- uri[,psudrop_n := .N, by = c("nid", "ihme_loc_id", "start_year")]

	uri$has_uri <- ifelse(uri$had_cough==1 & uri$diff_breathing!=1, 1, 0)

### COLLAPSE TO COUNTRY-YEAR-AGE-SEX ###
#Data frame for outputs
	df <- data.frame()

#################################################################################
# Loop over surveys to apply surveydesign & collapse to country-year-age-sex
	num <- 1
for(n in nid.new) {
  print(paste0("On survey number ",num," of ",length(nid.new)))
  temp <- subset(uri, nid.new == n)
  if(length(unique(temp$psu))>1){
    temp$obs <- 1

	#Apply survey design & collapse
    dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
    prev <- svyby(~has_uri, ~child_sex + age_year, dclus, svymean, na.rm=T)
    prev$sample_size <- svyby(~has_uri, ~child_sex + age_year, dclus, unwtd.count, na.rm=T)$count

    prev$cases <- prev$has_uri * prev$sample_size
    prev$ihme_loc_id <- unique(temp$subname)
    prev$location <- unique(temp$location_name)
    prev$start_year <- unique(temp$start_year)
    prev$end_year <- unique(temp$end_year)
    prev$nid <- unique(temp$nid)
    prev$sex <- ifelse(prev$child_sex==2,"Female",ifelse(prev$child_sex==1,"Male","Both"))
    prev$age_start <- prev$age_year
    prev$age_end <- prev$age_year + 1
    prev$recall_period <- unique(temp$recall_period_weeks)

    df <- rbind.data.frame(df, prev)
  }
  num <- num + 1
}

##########################################################
## Convert to point prevalence ##
##########################################################
	df$recall <- as.numeric(df$recall_period) * 7
	df$mean <- (df$has_uri*(5/(5-1+df$recall)))
	df$standard_error <- df$se * (5/(5-1+df$recall))

##########################################################
## prep upload fields ##
	df$year_start <- df$start_year
	df$year_end <- df$end_year
	df$measure <- "prevalence"
	df$source_type <- "Survey - cross-sectional"
	df$extractor <- "EXTRACTOR"

	df <- join(df, locs[,c("ihme_loc_id","location_id","location_name")], by="ihme_loc_id")


	template <- read.csv("filepath")

	output <- rbind.fill(template, df)

	output[is.na(output)] <- ""

	output$unit_value_as_published <- 1
	output$recall_type <- "Point"
	output$representative_name <- "Representative for subnational location only"
	output$unit_type <- "Person"
	output$urbanicity_type <- "Mixed/both"
	output$is_outlier <- 0
	output$step2_location_year <- "These data were re-extracted so that all surveys extracted consistently"

#Export for further prep #
	write.csv(output, "filepath", row.names=F)

###########################################################################

## Pull where these data are subnational (not extracted in previous GBD) ##
	df <- join(output, locs[,c("location_id","level","parent_id")], by="location_id")
	df <- subset(df, level >= 4)

## Find where NIDs exist, keep those ##
#uri_data <- get_bundle_data(bundle_id=25, decomp_step="step2")
	nid_exist <- unique(uri_data$nid)

	keep_subs <- subset(df, nid %in% nid_exist)

  keep_subs$step2_location_year <- "These data were re-extracted at the subnational level from existing surveys"

	national_nids <- unique(keep_subs$nid)

	keep_national <- subset(uri_data, !(nid %in% national_nids))

	output <- rbind.fill(keep_national, keep_subs)

## Need to sex split the data ##
	source("filepath/sex_split_mrbrt_weights.R")

	output$crosswalk_parent_seq <- ""
	output <- duplicate_sex_rows(output)

	output[is.na(output)] <- ""

	write.xlsx(output, "filepath", sheetName="extraction")

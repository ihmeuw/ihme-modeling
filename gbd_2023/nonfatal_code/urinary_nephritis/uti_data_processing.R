################################################################################################################
################################################################################################################
## Purpose: this script does 5 things, which are 1) retrieves new data, 2) applies previous
##          crosswalk coefficients,  3) systematically mark all (old + new) data as outliers based on MAD,
##          4) upload MAD-outliered new data as a new crosswalk version, 5) change outlier status of old  
##          data 
################################################################################################################
################################################################################################################

rm(list=ls())

## Source central functions
pacman::p_load(data.table, openxlsx, ggplot2, readr, RMySQL, openxlsx, readxl, stringr, tidyr, plyr, dplyr, gtools)
library(msm, lib.loc = FILEPATH)
library(Hmisc, lib.loc = FILEPATH)
library(metafor, lib.loc=FILEPATH)

base_dir <- FILEPATH
temp_dir <- FILEPATH
functions <- c( "get_bundle_version", "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version",
                "get_age_metadata", "save_bulk_outlier")
lapply(paste0(base_dir, functions, ".R"), source)

date <- Sys.Date()
date <- gsub("-", "_", date)


#########################################################################################
##STEP 1: Retrieve new data
#########################################################################################
bundle_version_id= BUNDLE_VERSION_ID 	 
df_new = get_bundle_version(bundle_version_id, fetch='new', export = FALSE)

#########################################################################################
##STEP 2: Set objects 
#########################################################################################
#For crosswalk
df_new = within(df_new, {cv_marketscan = ifelse(nid==433114, 1, 0)})
df_new = within(df_new, {cv_hospital = ifelse(cv_marketscan==0, 1, 0)})
dt <- copy(as.data.table(df_new))
xwalk_coef <- read.csv(paste0(FILEPATH,"/model_summaries.csv"))
cov_names <- c("cv_marketscan") 
xwalk_type<-"logit"

#For MAD outlier
output_filepath1 <- FILEPATH
output_filepath2 <- FILEPATH
MAD <- 2
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) 
byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 


#########################################################################################
##STEP 3: Apply crosswalk coefficients
#########################################################################################
dt_xwalk <- subset(dt, cv_marketscan ==1)
dt_no_xwalk <- subset(dt, cv_marketscan ==0)


if (xwalk_type=="logit") {
  dt_xwalk$mean_logit <- log(dt_xwalk$mean / (1-dt_xwalk$mean))
  dt_xwalk$se_logit <- sapply(1:nrow(dt_xwalk), function(i) {
    mean_i <- dt_xwalk[i, mean]
    se_i <- dt_xwalk[i, standard_error]
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  
  predicted <- as.data.table(subset(xwalk_coef, X_cv_marketscan == 1 & X_cv_ms2000==0))
  setnames(predicted, c("X_cv_marketscan"), c("cv_marketscan"))
  predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
  predicted[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2))), by=1:nrow(predicted)] 
  
  
  dt_xwalk <- merge(dt_xwalk, predicted, by=cov_names)
  setnames(dt_xwalk, c("mean", "standard_error"), c("mean_orig", "standard_error_orig"))
  dt_xwalk[, `:=` (mean_logit = mean_logit - Y_mean, se_logit = sqrt(se_logit^2 + Y_se^2))]
  dt_xwalk[, `:=` (mean= inv.logit(mean_logit), standard_error = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]
  dt_xwalk[(mean_orig!=0), `:=` (lower = NA, upper = NA)]  
  dt_xwalk[(mean_orig==0), `:=` (mean= mean_orig, standard_error = sqrt(standard_error_orig^2 + Y_se_norm^2))]
  dt_xwalk[, (c( "X_cv_ms2000", "Y_mean", "Y_se", "mean_logit", "se_logit", "Y_se_norm", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp", "Z_intercept")) := NULL]
  
  
} else if (xwalk_type=="log") {
  predicted <- as.data.table(subset(xwalk_coef, X_cv_marketscan == 1 & X_cv_ms2000==0))
  setnames(predicted, c("X_cv_marketscan"), c("cv_marketscan"))
  predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
  
  dt_xwalk <- merge(dt_xwalk, predicted, by=cov_names)
  setnames(dt_xwalk, c("mean", "standard_error"), c("mean_orig", "standard_error_orig"))
  
  dt_xwalk[, `:=` (log_mean = log(mean_orig), log_se = deltamethod(~log(x1), mean_orig, standard_error_orig^2)), by = c("mean_orig", "standard_error_orig")]
  dt_xwalk[, `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
  dt_xwalk[, `:=` (mean = exp(log_mean), standard_error = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
  dt_xwalk[, `:=` (cases = NA, lower = NA, upper = NA)]
  dt_xwalk[, (c( "X_cv_ms2000", "Y_mean", "Y_se", "log_mean", "log_se", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp", "Z_intercept")) := NULL]
  
}


dt_xwalk[is.na(lower), uncertainty_type_value := NA]
dt_xwalk$crosswalk_parent_seq <- dt_xwalk$seq

dt_no_xwalk$crosswalk_parent_seq <- ""

dt_total <- rbind.fill(dt_xwalk, dt_no_xwalk)
dt_total$standard_error[dt_total$standard_error>1] <-1
dt_total$new <- 1 #this will allow us to distinguish later which data is new in GBD2020 and which is not
write.xlsx(dt_total, output_filepath1, sheetName = "extraction", col.names=TRUE)

#########################################################################################
##STEP 4: Upload crosswalked step 2 new data as a new crosswalk version
#########################################################################################

# Upload the xwalk version
path_to_data1 <- output_filepath1
description1 <- paste0('crosswalked step 2 new data, updated')
result <- save_crosswalk_version( bundle_version_id, path_to_data1, description=description1)

print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
print(sprintf('Crosswalk version ID from best model: %s', result$previous_step_crosswalk_version_id))


#########################################################################################
##STEP 5: MAD outlier
#########################################################################################
crosswalk_version_id <- CROSSWALK_VERSION_ID
new_xwalk <- get_crosswalk_version(crosswalk_version_id, export=FALSE)

new_xwalk[is.na(new), new:=0]  

new_xwalk_not_MAD <- subset(new_xwalk, measure!="incidence" & measure != "prevalence")
new_xwalk<- subset(new_xwalk, measure=="incidence" | measure == "prevalence")

unique(new_xwalk$is_outlier)

new_xwalk$is_outlier_GBD2019 <- new_xwalk$is_outlier
new_xwalk$is_outlier <- NULL
new_xwalk$note_modeler_GBD2019 <- new_xwalk$note_modeler
new_xwalk$note_modeler <- ""


## GET AGE WEIGHTS
fine_age_groups <- "no" #specify how you want to get your age group weights

if (fine_age_groups=="yes") {
  #---option 1: if all data are at the most detailed age groups
  all_fine_ages <- as.data.table(get_age_metadata())
  setnames(all_fine_ages, c("age_group_years_start"), c("age_start"))
  
  
} else if (fine_age_groups=="no") {
  #---option 2: if <1 years is aggregated, change that age group id to 28 and aggregate age weights
  all_fine_ages <- as.data.table(get_age_metadata()) 
  setnames(all_fine_ages, c("age_group_years_start"), c("age_start"))
  not_babies <- all_fine_ages[!age_group_id %in% c(2:3, 388, 389)]
  not_babies[, age_group_years_end := age_group_years_end-1]
  not_babies <- not_babies[, c("age_start", "age_group_years_end", "age_group_id", "age_group_weight_value")]
  group_babies1 <- all_fine_ages[age_group_id %in% c(2:3, 388, 389)]
  group_babies1$age_group_id <- 28
  group_babies1$age_group_years_end <- 0.999
  group_babies1$age_start <- 0
  group_babies1[, age_group_weight_value := sum(age_group_weight_value)]
  group_babies1 <- group_babies1[, c("age_start", "age_group_years_end", "age_group_id", "age_group_weight_value")]
  group_babies1 <- unique(group_babies1)
  all_fine_ages <- rbind(not_babies, group_babies1)  
  
}

## Delete rows with emtpy means
new_xwalk<- new_xwalk[!is.na(mean)]


##merge age table map and merge on to dataset
new_xwalk <- as.data.table(merge(new_xwalk, all_fine_ages, by = c("age_start")))

#calculate age-standardized prevalence/incidence below:
##create new age-weights for each data source
new_xwalk <- new_xwalk[, sum1 := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
new_xwalk <- new_xwalk[, new_weight1 := age_group_weight_value/sum1, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their locaiton-age-sex-nid group 

##age standardizing per location-year by sex
#add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
new_xwalk[, as_mean := mean * new_weight1] #initially just the weighted mean for that AGE-location-year-sex-nid
new_xwalk[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series

##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
new_xwalk[as_mean == 0, is_outlier := 1] 
new_xwalk$note_modeler <- as.character(new_xwalk$note_modeler)
new_xwalk[as_mean == 0, note_modeler := paste0(note_modeler, " | outliered this location-year-sex-NID age-series because age standardized mean is 0")]

## log-transform to pick up low outliers
new_xwalk[as_mean != 0, as_mean := log(as_mean)]

# calculate median absolute deviation
new_xwalk[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
new_xwalk[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
new_xwalk[,median:=median(as_mean,na.rm = T),by=c("sex")]


# outlier based on MAD
new_xwalk[as_mean>((MAD*mad)+median), is_outlier := 1]
new_xwalk[as_mean>((MAD*mad)+median), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is higher than ", MAD, " MAD above median")]
new_xwalk[as_mean<(median-(MAD*mad)), is_outlier := 1]
new_xwalk[as_mean<(median-(MAD*mad)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than ", MAD, " MAD below median")]
with(new_xwalk, table(sex, mad))
with(new_xwalk, table(sex, median))
with(new_xwalk, table(sex, exp(median)))



new_xwalk[, c("age_group_name",	"age_group_years_start",	"age_group_years_end",	"most_detailed",	"sum1",	"new_weight1","sum", "new_weight", "as_mean","age_group_weight_value", "age_group_id") := NULL]

new_xwalk[is.na(is_outlier), is_outlier:=0]

# separate out new data from old data
new_xwalk_new <- subset(new_xwalk, new ==1)
new_xwalk_old <- subset(new_xwalk, new ==0)

write.xlsx(new_xwalk_new, output_filepath2, sheetName = "extraction", col.names=TRUE)

########################################################################################
#STEP 6: Upload MAD-outliered new data
#########################################################################################
path_to_data2 <- output_filepath2
description2 <- DESCRIPTION
result <- save_crosswalk_version( bundle_version_id, path_to_data2, description=description2)

print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
 print(sprintf('Crosswalk version ID from best model: %s', result$previous_step_crosswalk_version_id))

#########################################################################################
#STEP 7: Updated outlier_status of old data using save_bulk_outlier
 #########################################################################################
new_xwalk_old <- subset(new_xwalk_old, (is_outlier_GBD2019 ==0 & is_outlier==1) |  (is_outlier_GBD2019 ==1 & is_outlier==0))
table(new_xwalk_old$is_outlier)
table(new_xwalk_old$is_outlier_GBD2019)

new_xwalk_old<- new_xwalk_old[, c("seq", "is_outlier")]
outlier_update<- FILEPATH
write.xlsx(new_xwalk_old, outlier_update, sheetName = "extraction", col.names=TRUE)

crosswalk_version_id <- CROSSWALK_VERSION_ID
decomp_step <- 'iterative'
filepath <- outlier_update
description3 <- paste0('xv',crosswalk_version_id, 'update MAD outlier status of old data')
result <- save_bulk_outlier(
  crosswalk_version_id=crosswalk_version_id,
  filepath=filepath,
  description=description3
)

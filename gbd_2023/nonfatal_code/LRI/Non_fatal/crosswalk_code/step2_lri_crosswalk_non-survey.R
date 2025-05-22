#########################################################################################
## CONVERT INCIDENCE TO PREVALENCE FOR NON-CLINICAL DATA TYPES
#########################################################################################

## set up
rm(list = ls())

pacman::p_load(plyr, openxlsx, ggplot2, metafor, msm, lme4, scales, data.table, boot, readxl, magrittr)
# Source all GBD shared functions at once
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Pull in crosswalk packages
Sys.setenv("RETICULATE_PYTHON" = "/FILEPATH") 
library(reticulate)
reticulate::use_python("/FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")

# Set-up variables
date <- gsub("-", "_", Sys.Date())
user <- Sys.info()["user"]
release <- 16

years <- get_demographics(gbd_team = 'epi',
                          release_id = release)[["year_id"]]

cov_years <- years

# Save directory
save_dir <- paste0("FILEPATH", date, "/")
dir.create(paste0(save_dir), recursive = TRUE)

# Source utilities
source(paste0("/FILEPATH/", user,"FILEPATH/bundle_crosswalk_collapse.R")) 
source(paste0("/FILEPATH/", user,"FILEPATH/convert_inc_prev_function.R")) 
source(paste0("/FILEPATH/", user,"FILEPATH/sex_split_mrbrt_weights.R")) 
source(paste0("/FILEPATH/", user,"FILEPATH/map_dismod_input_function.R")) 
source(paste0("/FILEPATH/", user,"/FILEPATH/input_data_scatter_sdi_function.R")) 

# Pull locations
locs <- as.data.table(get_location_metadata(location_set_id = 35, release_id = release))

# Pull age groups
age_groups <- get_ids("age_group")


`%notin%` <- Negate(`%in%`)
################################################
## STEP 1 -- PULL BUNDLE VERSION AND GET DATA
################################################

## Check for most recent bundle
bundle_meta <- get_version_quota(bundle_id = 19)

## Get bundle version
bundle_version_id <- 47889 
dt_lri <- as.data.table(get_bundle_version(bundle_version_id = bundle_version_id))

########################################################################
## STEP 2 -- CONVERT INCIDENCE TO PREVALENCE FOR (NON-CLINICAL DATA) 
##           LITERATURE DATA
########################################################################

inc <- dt_lri[measure == "incidence" & clinical_data_type == ""]

# meta-analysis for duration of LRI
duration <- read.csv("FILEPATH") 

# Convert everything to duration (divide by 1 year)
duration_val   <- duration$mean/365.2422

duration <- as.data.table(duration)
test <- duration[, c(paste0("duration_", 1:1000))]
test <- test/365.2422
duration[,  c(paste0("duration_", 1:1000)) := test[, c(paste0("duration_", 1:1000))]]

duration[, duration_lower:= apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = (paste0("duration_", 1:1000))]
duration[, duration_upper:= apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = (paste0("duration_", 1:1000))]

duration_upper <- duration$duration_upper
duration_lower <- duration$duration_lower

duration_se <- (duration_upper - duration_lower) / (2*1.96)

inc$prev <- inc$mean * duration_val
inc$standard_error <- inc$prev*sqrt((inc$standard_error/inc$mean)^2+(duration_se/duration_val)^2)


inc$mean <- inc$prev
inc$cases <- inc$mean * inc$sample_size
inc <- subset(inc, select = c(-prev))
inc$measure <- "prevalence"
inc$note_modeler <- paste0("Converted from incidence to prevalence using mean duration ", duration_val)

# Compute upper, lower UIs
inc$lower <- inc$mean - (1.96 * inc$standard_error)
inc$upper <- inc$mean + (1.96 * inc$standard_error)

# remove rows that were converted from overall bundle variable
dt_lri_without_inc <- dt_lri[!(measure == "incidence" & clinical_data_type == "")]

# combine together
dt_lri <- rbind(dt_lri_without_inc, inc)

################################################################
## STEP 3 -- RECODE NEW DATA, CLINICAL DATA, and GBD ROUND
################################################################

## CLINICAL recode ##

dt_lri <- dt_lri[clinical_data_type == "inpatient", cv_inpatient := 1]
dt_lri <- dt_lri[clinical_data_type == "claims", cv_marketscan := 1]
dt_lri <- dt_lri[clinical_data_type == "claims - flagged", cv_marketscan := 1]

# Save out
write.csv(dt_lri, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"), row.names=F)

# Move to step 3
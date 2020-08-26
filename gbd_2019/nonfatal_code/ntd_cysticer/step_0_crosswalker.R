# NCC crosswalking

## SET UP FOCAL DRIVES

rm(list = ls())

os <- .Platform$OS.type
if (os=="windows") {
  j <-"FILEPATH"
  h <-"FILEPATH"
} else {
  j <-"FILEPATH"
  h <-paste0("FILEPATH", Sys.info()[7], "/")
}


library(metafor, lib.loc = "FILEPATH")
library(msm)
library(data.table)
library(ggplot2)
library(openxlsx)
source(paste0(j, "FILEPATH"))
source(paste0(j, "FILEPATH"))
source(paste0(j, "FILEPATH"))
source(paste0(j, "FILEPATH"))
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source("FILEPATH")

# MR-BRT

repo_dir <- paste0(j, "FILEPATH")
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))

# Custom

source("FILEPATH")
source("FILEPATH")

#############################################################################################
###                                      Set-Up                                           ###
#############################################################################################

#' [Set-up run directory / tracker]

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")

# Helper function

update_tracker <- function(bid, bvid, cwid, type, note){
  
  if (is.na(note)){ stop("must include note")}
  
  tracker <- fread(paste0(crosswalks_dir, "FILEPATH"))
  row     <- data.table(
    bid = bid,
    bvid = bvid,
    cwid = cwid,
    type = type,
    note = note
  )
  
  tracker <- rbind(tracker, row)
  fwrite(tracker, paste0(crosswalks_dir, "FILEPATH"))
  
}

#' [Set up age-split bundle]

tracker <- fread(paste0(crosswalks_dir, "FILEPATH"))
fit1 <- readRDS(paste0(crosswalks_dir, "FILEPATH"))

as_datax <- get_bundle_version(ADDRESS)
input_data <- get_bundle_version(ADDRESS)

# cleaning
as_data <- as_data[!(is.na(sample_size))]
locs <- get_location_metadata(35)
locs <- locs[, .(location_id, ihme_loc_id)]
as_data <- merge(as_data, locs, by = "location_id")
as_data[, c("group", "group_review", "specificity") := NA]
as_data[, c("group", "group_review", "specificity") := .(as.logical(group), as.logical(group_review), as.logical(specificity))]

# read in crosswalked dataset
locs <- get_location_metadata(35)
locs <- locs[, .(location_id, ihme_loc_id)]
xw_data <- fread("FILEPATH")
xw_data[, c("mean", "lower", "upper", "standard_error") := NULL]
setnames(xw_data, c("mean_adjusted", "lo_adjusted", "hi_adjusted", "se_adjusted"), c("mean", "lower", "upper", "standard_error"))
as_data <- xw_data[age_end - age_start <= 25]
as_data <- as_data[!(is.na(sample_size))]
as_data <- merge(as_data, locs, by = "location_id")
as_data_ss <- apply_sex_crosswalk(mr_brt_fit_obj = fit1, all_data = as_data, decomp_step = "iterative")

input_data <- input_data[!(is.na(sample_size))]
locs <- get_location_metadata(35)
locs <- locs[, .(location_id, ihme_loc_id)]
input_data <- merge(input_data, locs, by = "location_id")
input_data[, c("group", "group_review", "specificity") := NA]
input_data[, c("group", "group_review", "specificity") := .(as.logical(group), as.logical(group_review), as.logical(specificity))]

input_data_ss <- apply_sex_crosswalk(mr_brt_fit_obj = fit1, all_data = input_data, decomp_step = "iterative")
fwrite(input_data_ss, paste0(crosswalks_dir, "FILEPATH"))

as_data_ss[, crosswalk_parent_seq := 2]
as_data_ss[, c("group_review", "group", "specificity") := NA]
as_data_ss[is.na(upper), upper := 0]
as_data_outfile <- paste0(crosswalks_dir, "FILEPATH")
as_data_ss[, cv_definitive := NULL]

write.xlsx(as_data_ss, as_data_outfile, sheetName = "extraction")
as_data_description <- "binomial sex split -- 2 matches"

as_data_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS, 
                                         data_filepath = as_data_outfile,
                                         description = as_data_description)

update_tracker(bid = NaN, 
               bvid = ADDRESS, 
               cwid = as_data_cw_md$crosswalk_version_id, 
               type= "agesplit", 
               note = "sex split with only the two matches")

#'[ DX crosswalk]

#' [real Split]
locs <- get_location_metadata(35)
locs <- locs[, .(location_id, ihme_loc_id)]
xw_data <- fread("FILEPATH")
xw_data[, c("mean", "lower", "upper", "standard_error") := NULL]
setnames(xw_data, c("mean_adjusted", "lo_adjusted", "hi_adjusted", "se_adjusted"), c("mean", "lower", "upper", "standard_error"))
input_data <- xw_data[age_end - age_start <= 25]
input_data <- xw_data[!(is.na(sample_size))]
input_data <- merge(input_data, locs, by = "location_id")
input_data[, cases := mean * sample_size]

input_data_ss <- apply_sex_crosswalk(mr_brt_fit_obj = fit1, all_data = input_data, decomp_step = "iterative")
input_data_as <- apply_age_split(data = input_data_ss,
                                 dismod_meid = ADDRESS,
                                 dismod_mvid = ADDRESS,
                                 loc_pattern = 1,
                                 decomp_step_pop = "iterative",
                                 decomp_step_meid = "iterative")

input_data_outfile <- paste0(crosswalks_dir, "FILEPATH")
input_data_as[, c("group", "group_review", "specificity") := NA]
input_data_as <- input_data_as[sample_size > cases]
input_data_as[, c("lower", "upper", "uncertainty_type_value") := NA]
input_data_as[, cv_definitive := NULL]

write.xlsx(input_data_as, input_data_outfile, sheetName = "extraction")
input_data_description <- "binomial sex split -- 2 matches, age split 23901-391760"

input_data_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS, 
                                         data_filepath = input_data_outfile,
                                         description = input_data_description)

update_tracker(bid = NaN, 
               bvid = ADDRESS, 
               cwid = input_data_cw_md$crosswalk_version_id, 
               type= "input", 
               note = "sex split with only the two matches; age split 23901 - 391760")

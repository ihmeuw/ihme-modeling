#--------------------------------------------------------------
# Project: CKD data processing pipeline
# Purpose: Process and upload crosswalk versions for CKD bundles
#--------------------------------------------------------------

# setup -------------------------------------------------------------------
user <- Sys.getenv('USER')

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

# load packages
require(data.table)
library(tidyverse)
library(writexl)

# source functions
ckd_repo<- paste0("FILEPATH", user,"FILEPATH")
shared_code_repo<- paste0("FILEPATH", user,"FILEPATH")
source(paste0(ckd_repo,"general_func_lib.R"))
source(paste0(ckd_repo,"FILEPATH/data_processing_functions.R"))
source(paste0(ckd_repo,"FILEPATH/data_processing_plots.R"))
source(paste0(shared_code_repo,"sex_split.R"))
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# set objects
args<-commandArgs(trailingOnly = T)
if(length(args)==0) {
  args <- pass_args
}
bid<-as.numeric(args[1])
bvid<-as.numeric(args[2])
acause<-c(as.character(args[3]))
release_id<-as.numeric(args[4])
output_file_name<-c(as.character(args[5]))
cv_ref_map_path<-c(as.character(args[6]))
save_cx_pre_age_split<-as.numeric(args[7])
save_cx_age_spec<-as.numeric(args[8])
sex_split_ref_map_path<-c(as.character(args[9]))
process_cv_ref_map_path<-c(as.character(args[10]))


message(paste("Bundle version id is", bvid))

# set data table threads
setDTthreads(10)

# load bundle version data
bdt <- get_bundle_version(bundle_version_id = bvid,export = F,transform = T, fetch = "all")

# set output path 
bdt_xwalk_path<-paste0(j_root,"FILEPATH")

# dt for rows that get dropped 
drop_name<-paste0("dropped_data_bid_", bid, "_bvid_",bvid)
dropped_dt <- data.table()

# dt for tracking # datapoints involved in each processing step
tracking_dt <- data.table()
tracking_dt[, bundle_id := bid]
tracking_dt[, bundle_version_id := bvid]

# deal with weird slashes in albuminuria cv_cols
col_fix<-any(grepl("/",names(bdt)))
if (col_fix){
  message("renaming any columns with a / in them")
  fix_col_names<-grep("/",names(bdt),value=T)
  new_col_names<-sub(pattern = "/",replacement = "_",fix_col_names)
  setnames(bdt,fix_col_names,new_col_names)
}

#  -----------------------------------------------------------------------

#  within-study age-sex split -------------------------------------
message("within-study age-sex split ---------------------")

if ("age_sex_lit" %in% names(bdt)){
  # running split
  bdt <- age_sex_split_lit(dt=bdt, split_indicator = "age_sex_lit",
                           bid = bid, output_path = bdt_xwalk_path)
  
  # add to tracking dt
  dt_split <- as.data.table(read.csv(paste0(bdt_xwalk_path,"FILEPATH/csvs/bundle_",bid, "_pre_age_sex_lit_split.csv")))
  
  tracking_dt[, age_sex_split_input_datapoints := nrow(dt_split)]
  tracking_dt[, age_sex_split_output_datapoints := nrow(bdt[!is.na(parent_seq_age), ])]
  
  # assign the original age seq as the crosswalk_parent_seq
  bdt[!is.na(parent_seq_age),crosswalk_parent_seq:=parent_seq_age]
  
  if (bid == 186) {
    bdt[nid == 269881 & is.na(mean), mean := 0]
  }
  
  # run scatter plot function
  age_sex_split_lit_plot(bid = bid, bvid = bvid, output_path = bdt_xwalk_path, release_id = release_id)
  
}

#   -----------------------------------------------------------------------


# CKD Stage V: aggregate age groups for subnational India and Bashkortostan 
#   to stabilize age pattern --------------------------------------------------------

if (bid %in% c(186)) {
  bdt2 <- bdt[(nid==279526 | nid==282076 | nid==276450 | nid==280192 | nid==133397 | nid==293226 | nid==304148)&specificity!="age_combined",]
  bdt2 <- bdt2[!(nid == 279526 & age_start == 20 & age_end == 29), ]
  bdt2 <- bdt2[!(nid == 279526 & age_start == 70 & age_end == 99), ]
  bdt3 <- copy(bdt2)
  bdt3$age_start <- bdt3$age_start-bdt3$age_start%%20
  bdt3$age_end <- bdt3$age_start+19
  
  # selecting columns for grouping
  bdt3 <- subset(bdt3, select = c(sex, age_start, age_end, cases, sample_size, nid))
  bdt3[, cases := sum(cases), by = c("sex", "age_start", "age_end", "nid")]
  bdt3[, sample_size := sum(sample_size), by = c("sex", "age_start", "age_end", "nid")]
  bdt3 <- unique(bdt3)
  bdt3[, mean := cases/sample_size]
  
  # creating df to merge back onto with the rest of the columns
  bdt2 <- subset(bdt2, select = -c(mean, lower, upper, standard_error, cases, sample_size,
                                   uncertainty_type_value, design_effect, effective_sample_size))
  
  # left join
  bdt3[nid == 276450 & age_end == 99, age_end := 89]
  bdt3[nid == 280192 & age_end == 79, age_end := 69]
  bdt3[nid == 133397 & age_end == 99 & sex == "Male", age_end := 124]
  bdt3[nid == 133397 & age_end == 99 & sex == "Female", age_end := 94]
  bdt3[nid == 293226 & age_end == 99 & sex == "Male", age_end := 94]
  bdt3[nid == 293226 & age_end == 99 & sex == "Female", age_end := 89]
  bdt3[nid == 304148 & age_end == 99, age_end := 94]
  
  bdt4 <- left_join(bdt3, bdt2, by = c("age_end", "sex", "nid"))
  
  bdt4[, age_start := age_start.x]
  bdt4[, age_start.y := NULL]
  bdt4[, age_start.x := NULL]
  
  bdt4$note_sr <- paste(bdt4$note_sr, " | Data aggregated into 20-year age groups for larger sample sizes")
  
  write.csv(bdt4, paste0(bdt_xwalk_path, "FILEPATH/bid_", bid, "_bvid_", bvid, "_vetting_StageV_agg_ages_IND_Bashkort.csv"))
  
  bdt <- bdt[!nid %in% c(279526, 282076, 276450, 280192, 133397, 293226, 304148), ]
  
  bdt <- rbind(bdt, bdt4, fill = TRUE)
  
  write.csv(bdt, paste0(bdt_xwalk_path, "FILEPATH/bid_", bid, "_bvid_", bvid, "_vetting_StageV_agg_ages_appended.csv"))
}


# All CKD Stages: aggregate age groups for China National Health Survey NID 200838
#   to stabilize age pattern --------------------------------------------------------

if (bid %in% c(182, 183, 186, 760)) {
  bdt2 <- bdt[nid==200838,]
  bdt3 <- copy(bdt2)
  bdt3$age_start <- bdt3$age_start-bdt3$age_start%%20
  bdt3$age_end <- bdt3$age_start+19
  
  # selecting columns for grouping
  bdt3 <- subset(bdt3, select = c(sex, age_start, age_end, cases, sample_size))
  bdt3[, cases := sum(cases), by = c("sex", "age_start", "age_end")]
  bdt3[, sample_size := sum(sample_size), by = c("sex", "age_start", "age_end")]
  bdt3 <- unique(bdt3)
  bdt3[, mean := cases/sample_size]
  
  # creating df to merge back onto with the rest of the columns
  bdt2 <- subset(bdt2, select = -c(mean, lower, upper, standard_error, cases, sample_size,
                                   uncertainty_type_value, design_effect, effective_sample_size))
  
  # left join
  bdt3[age_end == 99, age_end := 94]
  
  bdt4 <- left_join(bdt3, bdt2, by = c("age_end", "sex"))
  
  bdt4[, age_start := age_start.x]
  bdt4[, age_start.y := NULL]
  bdt4[, age_start.x := NULL]
  
  bdt4$note_sr <- paste(bdt4$note_sr, " | Data aggregated into 20-year age groups for larger sample sizes")
  
  write.csv(bdt4, paste0(bdt_xwalk_path, "FILEPATH/bid_", bid, "_bvid_", bvid, "_vetting_agg_ages_China_national_health_survey.csv"))
  
  bdt <- bdt[!nid == 200838, ]
  
  # outlier data points for age 0-19 because still small sample size and skews age pattern at young ages implausibly high
  bdt4$is_outlier[bdt4$age_start==0] <- 1
  
  bdt <- rbind(bdt, bdt4, fill = TRUE)
  
  write.csv(bdt, paste0(bdt_xwalk_path, "FILEPATH/bid_", bid, "_bvid_", bvid, "_vetting_agg_ages_China_survey_appended.csv"))
}


#   -----------------------------------------------------------------------

# apply sex splits --------------------------------------------------------
# split into both-sex and sex-specific data sets first
message("separating data into sex-specific and both-sex datasets")

tosplit_dt <- bdt[sex == "Both" & (measure %in%c("prevalence","incidence", "proportion")), ]
sex_specific_dt <- bdt[sex %in% c("Male", "Female") & (measure %in%c("prevalence","incidence", "proportion")) & is.na(crosswalk_parent_seq), ]

# writing both-sex and sex-specific data sets
write.csv(tosplit_dt, paste0(bdt_xwalk_path,"FILEPATH/bid_", bid, "_to_split.csv"))
write.csv(sex_specific_dt, paste0(bdt_xwalk_path,"FILEPATH/bid_", bid, "_sex_specific.csv"))

# removing to be sex split data from bdt
bdt <- setdiff(bdt, tosplit_dt)

# set measures that need splitting
split_measure <- unique(tosplit_dt$measure)

# read in sex_split_ref_map for sex split model settings
sex_split_ref_map <- as.data.table(read.csv(sex_split_ref_map_path))
sex_split_ref_map <- sex_split_ref_map[bundle_id==bid, ]

# BIRDS team sex split function
message("running sex split function")

date <- strtrim(gsub(":", "_", gsub(" ", "__", gsub("-", "_", Sys.time()))), 17)
sex_split_path <- paste0(bdt_xwalk_path,"FILEPATH/", bid, "/sex_split_", date, "/")
topic_name <- paste0(acause, "_bid_", bid)

sex_split(topic_name = topic_name, 
          output_dir = paste0(bdt_xwalk_path,"FILEPATH/", bid, "/"), 
          bundle_version_id = NULL, 
          data_all_csv = NULL, 
          data_to_split_csv = paste0(bdt_xwalk_path,"FILEPATH/bid_", bid, "_to_split.csv"), 
          data_raw_sex_specific_csv = paste0(bdt_xwalk_path,"FILEPATH/bid_", bid, "_sex_specific.csv"), 
          nids_to_drop = c(), 
          cv_drop = c(), 
          mrbrt_model = NULL, 
          mrbrt_model_age_cv = sex_split_ref_map$mrbrt_model_age_cv, 
          mrbrt_model_linear_age_cv = sex_split_ref_map$mrbrt_model_linear_age_cv,
          mrbrt_model_age_spline_knots = as.numeric(unlist(strsplit(sex_split_ref_map$mrbrt_model_age_spline_knots,","))),
          mrbrt_model_age_spline_degree = sex_split_ref_map$mrbrt_model_age_spline_degree,
          mrbrt_model_age_spline_linear_tail_left = sex_split_ref_map$mrbrt_model_age_spline_linear_tail_left,
          mrbrt_model_age_spline_linear_tail_right = sex_split_ref_map$mrbrt_model_age_spline_linear_tail_right,
          release_id = release_id, 
          measure = split_measure,
          vetting_plots = TRUE)

# reading in post sex split data
postsplit_dt <- read.csv(paste0(sex_split_path, "sex_split_", date, "_", topic_name, "_post_sex_split_data.csv"))
postsplit_dt <- postsplit_dt[, intersect(colnames(bdt), colnames(postsplit_dt))]

postsplit_dt <- as.data.table(postsplit_dt)

# binding onto bdt, fixing seqs
postsplit_dt[, crosswalk_parent_seq := seq]

if ("date_added_to_bundle" %in% colnames(bdt)) {
  bdt[, date_added_to_bundle := as.character(date_added_to_bundle)]
}
if ("date_to_added_bundle" %in% colnames(bdt)) {
  bdt[, date_to_added_bundle := as.character(date_to_added_bundle)]
}

bdt <- rbind(bdt, postsplit_dt, fill = TRUE)

# drop any data that is still sex specific in bundle (measure is not prev, inc, or prop) 
message("dropping rows that could not be sex split due to measure")
dropped_dt<-rbindlist(
  list(dropped_dt,copy(
    bdt[sex == "Both", ])[,drop_reason:="could not be sex split due to measure"]),
  use.names=T,fill=T)
bdt<-bdt[!sex == "Both", ]

# add to tracking dt
sex_split_vetting_dt <- as.data.table(read.csv(paste0(sex_split_path, "sex_split_", date, "_", topic_name, "_vetting_data.csv")))

tracking_dt[, sex_split_sex_matches_datapoints := sex_split_vetting_dt[Statement == "Sex matches found", ]$Number]
tracking_dt[, sex_split_input_datapoints := sex_split_vetting_dt[Statement == "Observations to be sex split", ]$Number]
tracking_dt[, sex_split_output_datapoints := sex_split_vetting_dt[Statement == "Number of sex split observations", ]$Number]


#   -----------------------------------------------------------------------


# apply crosswalks -------------------------------------------------------
if(bid==670 | bid==182 | bid==183 | bid==186 | bid==760) {
  message("apply crosswalks")
  cv_ref_map<-fread(cv_ref_map_path)[bundle_id==bid]
  for (a in cv_ref_map[,alt]){
    bdt<-apply_mrbrt_xwalk(input_dt = bdt, bid = bid, alt_def = a, 
                           cv_ref_map_path = cv_ref_map_path, 
                           output_path = bdt_xwalk_path)
  }
  apply_mrbrt_xwalk_plot(input_dt = bdt, bid = bid, bvid = bvid, output_path = bdt_xwalk_path, 
                         release_id = release_id, cv_ref_map_path = cv_ref_map_path)
  
  # add to tracking dt
  file<-paste0(bdt_xwalk_path,"FILEPATH/csvs/")
  filenames <- list.files(file, pattern=paste0("bundle_",bid, "_*"), full.names=TRUE)
  dt_xwalk <- as.data.table(rbindlist(lapply(filenames, fread), fill=T))
  
  for (a in cv_ref_map[,alt]){
    tracking_dt[, paste0("crosswalk_", a, "_datapoints") := nrow(dt_xwalk[orig_alt_def==a])]
  }
}


#   -----------------------------------------------------------------------


# clean up ----------------------------------------------------------------
message("clean up")

orig_rows_bdt <- nrow(bdt)
orig_rows_bdt_dropped <- nrow(dropped_dt)

# drop any data marked with group review 0 
message("dropping rows where group_review is marked 0")
dropped_dt<-rbindlist(
  list(dropped_dt,copy(
    bdt[group_review%in%c(0)])[,drop_reason:="group_review value of 0"]),
  use.names=T,fill=T)
bdt<-bdt[!(group_review%in%c(0))]

# clear effective sample size where design effect is null or where effective sample size is less than cases
message("clearing effective_sample_size where design effect is NA or where effective sample size is less than cases")
bdt[is.na(design_effect),effective_sample_size:=NA]
bdt[measure %in% c("prevalence", "proportion") & (effective_sample_size < cases),effective_sample_size:=NA]

# reset weird slashes in cv_ cols for albuminuria
if (col_fix){
  message("resetting col names with /s in them to their original titles")
  setnames(bdt, new_col_names, fix_col_names)
}

# drop any loc ids that can't be uploaded to dismod
message("dropping locations not in the DisMod hierarchy")
dismod_locs<-get_location_metadata(location_set_id = 9, release_id = release_id)[,c(location_id, location_name)]
dropped_dt<-rbindlist(
  list(dropped_dt,copy(
    bdt[!(location_id%in%dismod_locs)])[,drop_reason:="loc_id not supported in Dismod modelling"]),
  use.names=T,fill=T)
bdt<-bdt[location_id%in%dismod_locs]


bdt$unit_value_as_published[is.na(bdt$unit_value_as_published)] <- 1

# drop any data points with sample size==1
dropped_dt<-rbindlist(
  list(dropped_dt,copy(
    bdt[bdt$sample_size==1])[,drop_reason:="sample size is 1"]),
  use.names=T,fill=T)
bdt <- bdt[(!bdt$sample_size==1 | is.na(bdt$sample_size)),]

# erase any standard errors>1
bdt$standard_error[bdt$standard_error>=1] <- NA
message(paste0("Number of rows missing uncertainty info: ", nrow(bdt[is.na(bdt$standard_error) & is.na(bdt$lower) & is.na(bdt$sample_size),])))
dropped_dt<-rbindlist(
  list(dropped_dt,copy(
    bdt[is.na(bdt$standard_error) & is.na(bdt$lower) & is.na(bdt$sample_size)])[,drop_reason:="Unable to calculate uncertainty with given information"]),
  use.names=T,fill=T)
bdt <- bdt[!(is.na(bdt$standard_error) & is.na(bdt$lower) & is.na(bdt$sample_size)),]

# assign crosswalk_parent_seq, clear seq for crosswalk upload
bdt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
bdt[, seq := NA]

if (nrow(bdt[is.na(crosswalk_parent_seq)|crosswalk_parent_seq == "",])>0) {
  stop("There is a missing crosswalk_parent_seq. Crosswalk version validations will not pass. Please check.")
}

# fix group_review for validations
bdt[is.na(group)|group == ''|is.na(specificity)|specificity == '', `:=` (group = NA, specificity = NA, group_review = NA)]

# fix uncertainty for validations
bdt[is.na(lower)|is.na(upper), `:=` (lower = NA, upper = NA, uncertainty_type_value = NA)]

# fix mean > 1 proportions for validations (affecting one datapoint in bid 554, unclear the reason)
bdt[measure == "proportion" & mean > 1, mean := 1]

# limit note_sr to below length 2000 to pass validations
if ("note_SR" %in% colnames(bdt)) {
  bdt[, note_sr := note_SR]
  bdt[, note_SR := NULL]
}

bdt[nchar(note_sr) > 2000, note_sr := strtrim(note_sr, 1999)]

#   -----------------------------------------------------------------------

# outliering problematic age specific data before age split model
if (bid == 670) {
  bdt[nid == 401058, is_outlier := 1]
  bdt[nid == 237986 & age_start %in% c(10, 15), is_outlier := 1]
}

# save only age-specific data to estimate age pattern for age-splitting
bdt_agespec <- bdt
bdt_agespec$age_range <- bdt_agespec$age_end-bdt_agespec$age_start

# keep only data with age range <=25 years
bdt_agespec <- bdt_agespec[bdt_agespec$age_range<= 25,]

# keep only data with sample size > 50
bdt_agespec <- bdt_agespec[bdt_agespec$sample_size>50,]

# keep only data with measure incidence, prevalence, or proportion
bdt_agespec <- bdt_agespec[bdt_agespec$measure %in% c("prevalence", "incidence", "proportion"),]

# add to tracking dt
tracking_dt[, age_split_dismod_input_datapoints := nrow(bdt_agespec)]

# plot age spec data 
age_splitting_input_plot(input_dt=bdt_agespec, bid=bid, bvid=bvid, output_path=bdt_xwalk_path, 
                         release_id=release_id)

# write age spec data, save crosswalk if specified
message("writing age-specific dataset after processing")
output_file_name2<-paste0("age_specific_sex_split_and_crosswalked_bid_", bid, "_bvid_",bvid)
write.csv(bdt_agespec, paste0(bdt_xwalk_path,"FILEPATH/", output_file_name2, ".csv"))

if(save_cx_age_spec==1) {
  writexl::write_xlsx(list(extraction=bdt_agespec),paste0("FILEPATH", bid, "FILEPATH", output_file_name2, ".xlsx"))
  description <- "Age-specific data only to model age pattern, age range <=25 years and sample size > 50"
  result <- save_crosswalk_version(bvid,data_filepath = paste0("FILEPATH", bid, "FILEPATH",output_file_name2, ".xlsx"),
                                   description = description)
  
  bvids<-fread(process_cv_ref_map_path)
  bvids$cvid_age_specific[bvids$bundle_id==bid] <- result$crosswalk_version_id
  fwrite(bvids, file = process_cv_ref_map_path, append = FALSE)
}

# write all data
if ((nrow(bdt)+nrow(dropped_dt)) != (orig_rows_bdt+orig_rows_bdt_dropped)){
  stop("The number of dropped rows plus the number of crosswalk version rows does not equal the expected number of rows. Please check.")
}

message("writing full dataset after processing, not age split")
write.csv(bdt, paste0(bdt_xwalk_path,"crosswalk_versions/", output_file_name, ".csv"))
write.csv(dropped_dt, paste0(bdt_xwalk_path,"dropped_data/", drop_name, ".csv"))

# save full crosswalk version
if(save_cx_pre_age_split==1) {
  writexl::write_xlsx(list(extraction=bdt),paste0("FILEPATH", bid, "FILEPATH",output_file_name, ".xlsx"))
  description <- "Processed but not age split"
  result <- save_crosswalk_version(bvid,data_filepath = paste0("FILEPATH", bid, "FILEPATH",output_file_name, ".xlsx"),
                                   description = description)
  
  bvids<-fread(process_cv_ref_map_path)
  bvids$cvid_not_age_split[bvids$bundle_id==bid] <- result$crosswalk_version_id
  fwrite(bvids, file = process_cv_ref_map_path, append = FALSE)
}

# save tracking dt
tracking_dt_all<-as.data.table(read.csv(paste0(bdt_xwalk_path, "data_processing_tracking.csv")))
tracking_dt_all <- tracking_dt_all[!bundle_id == bid, ]
tracking_dt_all <- rbind(tracking_dt_all, tracking_dt, fill = TRUE)
fwrite(tracking_dt_all, file = paste0(bdt_xwalk_path, "data_processing_tracking.csv"), append = FALSE)


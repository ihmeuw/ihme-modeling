# Introduction ------------------------------------------------------------
# Purpose: (1) Add newly extracted data to smoking bundle
#          (2) For each component, a. split into good and bad data
#                                  b. run ST-GPR on good data
#                                  c. age- and sex-split bad data
#                                  d. combine with good data
#                                  e. run ST-GPR on all data
# Date Modified: 2024-10-16


# Setup -------------------------------------------------------------------
if (TRUE){
  rm(list=ls())
  user <- Sys.getenv("USER")
  
# Model specifications
me_name            <- "smoking_prev_current"  # [smoking_prev, smoking_prev_current_any, smoking_prev_former_any] <- main modeling indicators for GBD
num.draws          <- 100      # Number of draws. Typically set 100 for practice runs, and 1000 for final runs
adding_data        <- T        # If TRUE, will add newly imported data to bundle; if FALSE, will move forward with existing bundle data                    
using_bundles      <- F        # If TRUE, will save new bundle data, bundle, and xwalk versions and run ST-GPR from xwalk; if FALSE, will not save and will run ST-GPR from flat file
using_outliers     <- F        # If TRUE, will import outliers from GBD 2021 and past; should be FALSE unless there's a reason to use those
make_plots         <- T        # If TRUE, automatically creates vetting plots, saved in /tobacco/smoking_direct_prev/exposure/modeling/GBD2024/outputs/visuals/

# If setting manually -- New cleaned and collapsed data (to be added to bundle if adding_data==T)
if (F){
dataname <- "all_data_long_2024-08-01.csv"
}

# GBD cycle-related variables
release <- 16
gbd <- 9
gbd_prior <- 9 # Previous round, from which we are loading bundles and to which we are adding data
main_bundle <- 4886

# Cluster/ST-GPR related variables
cluster_project <- 'proj_team'
nparallel <- 50 # Number of parallelizations! More parallelizations --> faster (if cluster is emtpy). I usually do 50-100.
holdouts <- 0 # Keep at 0 unless you want to run cross-validation

# Set Paths
share_path <- FILEPATH
# Where functions are stored
code_path <- FILEPATH
# These are inputs that may change every cycle and used in both smoking and smokeless models
input_path <- FILEPATH 
# These are constant reference files that do not generally change and are thus stored on J. 
reference_files <- FILEPATH
# Main directory where intermediate files will be saved to from this script
outpath <- FILEPATH
# Where data is stored for this GBD cycle
data_path <- FILEPATH
# New cleaned and collapsed dataset
filedata <- file.info(list.files(FILEPATH, pattern="long", full.names = T))
new_raw_data <- rownames(filedata[with(filedata, order(as.POSIXct(ctime))), ])[1]
if(exists("dataname")){new_raw_data <- paste0(FILEPATH)}
# Where ST-GPR model draws are saved (central location)
stgpr_output <- FILEPATH
# Where the data and config for st-gpr will be saved
stgpr_input <- FILEPATH
# Path to a valid directory for storing logfiles
logs <- FILEPATH
# Path to bundle files for uploading and saving
bundle_path <- FILEPATH
# The file containing data to be outliered
outlier_file <- FILEPATH

# Loading packages and files
date <- Sys.Date()
pkgs <- c("data.table", "tidyverse", "parallel")
invisible(lapply(pkgs, library, character.only=TRUE))
# source(paste0(code_path, "functions/useful_functions.R"))
source(FILEPATH)
# source_functions(code_path)

# Mapping to bundle IDs/config IDs/age-sex splitting runs/etc. 
map <- fread(paste0(FILEPATH, "me_to_var_map.csv"))
best_old_bundles <- as.data.table(fread(paste0(FILEPATH, "best_bundle_ids.csv")))[gbd_id == gbd_prior]
bundle_id <- best_old_bundles$bundle_id[best_old_bundles$me_name==me_name]
best_old_runs <- as.data.table(fread(paste0(FILEPATH, "best_age_sex_ids.csv")))[gbd_id == gbd_prior]

# Read in relevant demographic data
ages <- get_age_spans()[age_group_id %in% c(7:20, 30:32, 235)]
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages <- ages %>% mutate(age_end=age_end-1, 
                        age_group=paste(age_start, "to", age_end))
ages[age_end==124, age_group := "95 plus"]
suppressWarnings(locs <- get_location_metadata(location_set_id=22, release_id=release)[level > 2])
pops <- get_population(location_set_id=22, year_id=1980:2024, sex_id=-1,
                       age_group_id=c(7:20, 30:32, 235), release_id=release, location_id=locs[,location_id])
pops <- left_join(pops, ages, by="age_group_id")
ids <- c("nid","location_id","year_id","age_start","age_end","sex_id")

message("=== SETUP DONE ===")
}


# Step 1: Data Import and Processing  ---------------------------------------------------------
if (TRUE){
# Read in existing data
best_datasets <- fread(paste0(FILEPATH, "best_data.csv"))
bestdf <- best_datasets$filepath[best_datasets$me_name==me_name & best_datasets$num==max(best_datasets$num[best_datasets$me_name==me_name])]
old_bundle <- as.data.table(fread(bestdf))
old_bundle$tobdf <- "old_bundle"

# Clean the new data (removing duplicates from extractions, overlapping age or year sets, etc.)
tobdf <- "old_bundle"
source(paste0(FILEPATH, "01.1_smoking_data_cleaning.R"))

# Check duplicates (fix and run until no duplicates found)
# If anything found, add fixes to cleaning script above
checkdups("old_bundle")

if(adding_data){
  # Read in new cleaned and collapsed data
  new_data <- fread(new_raw_data) %>% 
              drop_na(var) %>% mutate(tobdf="new_data")
  
 # Mexico patch fix 
  mexss <- new_data %>% filter(nid==8672 & var=="smoked_current_any_var") %>% dplyr::select(nid, age_start, sample_size, sex_id) %>% dplyr::mutate(var="smoked_former_any_var")
  new_data <- rows_patch(new_data, mexss, by=c("nid", "age_start", "var", "sex_id"))
  
  # Filter to outcome of interest
  new_data <- new_data[!(var %like% "chew" | var %like% "slt" | var %like% "ever")]
  if (me_name=="smoking_prev_current_any"){
    new_data <- new_data %>% filter(var %like% "current")
  } else if (me_name=="smoking_prev_former_any"){
    new_data <- new_data %>% filter(var %like% "former")
  } else if (me_name=="smoking_prev"){
    new_data <- new_data %>% filter(var %like% "current_daily")
  } 
  
  # Clean the new data (removing duplicates from extractions, overlapping age or year sets, etc.)
  tobdf <- "new_data"
  source(paste0(FILEPATH, "01.1_smoking_data_cleaning.R"))
  
  # Check duplicates (fix and run until no duplicates found)
      # If anything found, add fixes to cleaning script above
  checkdups("new_data")

    # Combine existing bundle and new_data into new dataset
      # Subset old data by nid-location so that updated new data replaces it (if any)
  setnames(old_bundle, "mean", "val", skip_absent=T)
  suppressWarnings(new_data[, nidloc := paste(nid, location_id, sep="-")])
  old_bundle <- old_bundle %>% mutate(nidloc=paste(nid, location_id, sep="-")) %>% filter(!nidloc %in% new_data$nidloc)
      # Bind old and new data
  df <- rbind(new_data, old_bundle, use.names=T, fill=T)
      # Make necessary data changes
  df <- df %>% mutate(seq=NA,                              is_outlier=0,
                      age_group_id=999,                    orig_age_start=age_start,
                      orig_age_end=age_end,                orig_year_id=year_id, 
                      orig_year_start=year_start,          orig_year_end=year_end,
                      year_start=year_id,                  year_end=year_id,
                      underlying_nid=NA,                   measure="proportion",
                      sex=ifelse(sex_id==1, "Male",ifelse(sex_id==2, "Female", "Both")),
                      variance=case_when(val==0 ~ 0.001,
                                         is.na(variance) & (is.na(design_effect) | design_effect==0) & is.na(sample_size) ~ 0.01,
                                         is.na(variance) & (is.na(design_effect) | design_effect==0) ~ (val*(1-val))/sample_size,
                                         is.na(variance) ~ ((val*(1-val))/sample_size)*design_effect,
                                         TRUE ~ variance),
                      variance=ifelse(variance<0.001, 0.001, variance)) %>% 
               filter(str_detect(var, "^smoked") | str_detect(var, "^cig") | str_detect(var, "^hook") | str_detect(var, "^smk"), !is.na(location_id))

  df <- make_bundle_data_changes(df)
  setnames(df, "val", "mean", skip_absent=T)
  
  checkdups("df")
  
}

# Reshape dataset wide by descriptors
    # This is used in crosswalking below
out <- dcast.data.table(df, nid + survey_name + location_id + year_id + age_start + age_end + sex_id + source + imputed_ss + extractor ~ var, 
                        value.var=c("mean", "variance", "sample_size","seq","is_outlier"))
    # If this line gives message "Aggregate function missing, defaulting to 'length'", you have duplicates: Fix and try again
    
# Eswatini fix to wide data
replace <- out %>% filter(nid==250033, sex_id==2, age_start==45, extractor=="")
out <- out %>% filter(!(nid==250033 & sex_id==2 & age_start==45 & extractor=="")) %>% 
               rows_patch(replace, by=c("nid", "age_start", "sex_id"))

# Bundles!
if (using_bundles){
  epi_bundle_iterative <- get_bundle_data(bundle_id)
  if(nrow(epi_bundle_iterative) > 0){
    clear_data <- epi_bundle_iterative[,.(seq)]
    cleared_data <- paste0(FILEPATH, "iterative_clear_bundle_smoking_", me_name, "_", date,".xlsx")
    openxlsx::write.xlsx(clear_data, cleared_data, sheetName="extraction", rowNames=F)
  # Clear the bundle:
    result_upload <- upload_bundle_data(bundle_id, filepath=cleared_data)
  }
  
  # Upload new data
  in_stgpr_path_excel <- paste0(FILEPATH, "smoking_in_stgpr_", me_name, "_", date, ".xlsx")
  openxlsx::write.xlsx(df, in_stgpr_path_excel, sheetName="extraction", rowNames=F)
  new_bundle_version <- upload_bundle_data(bundle_id=bundle_id, filepath=in_stgpr_path_excel)
  new_bundle_version_id <- save_bundle_version(bundle_id=bundle_id)[,2]
  message(paste(me_name, "Bundle Version ID:", new_bundle_version_id))
  df <- get_bundle_version(new_bundle_version_id)
}

message("=== DATA IMPORT AND PROCESSING DONE ===")
}

# Step 2: Crosswalk -------------------------------------------------------
if(TRUE){
gold_standard <- map[me==me_name, unique(gold_standard)]

message("Xwalk Begins")
post_xwalk_path <- paste0(FILEPATH, "smoking_post_xwalk_", gold_standard,"_",date, ".csv")
xwalked <- linear_xwalk(out, df, gold_standard=gold_standard, from_bundle=T)

# Outlier illogical data
xwalked[sample_size > 1000000, `:=` (is_outlier=1, outlier_reason="Sample size too large")] # Surveys with incorrect survey sizes, i.e. that seem to extrapolate to whole kingdom in case of Thailand (i.e. samples of 35,000,000 people) bwmorgan 2024/5/3
xwalked <- xwalked[!(is.na(mean) | mean>0.9)]  # Drop obs with missing mean

write_csv(xwalked, post_xwalk_path)

# Age-sex splitting
xwalked[, me_name := me_name]
datasets <- divide_data(xwalked, round.ages=F) # From age_sex_split_functions.R; returns training set with correctly split ages and sexes, and test set that doesn't have these correct splits
good     <- datasets$train[,split := 0]
bad      <- datasets$test

# Prep good data for STGPR
setnames(good, "mean", "val", skip_absent = T)
good[, `:=` (year_start = year_id,
             year_end = year_id, 
             orig_age_start = age_start, 
             orig_age_end = age_end, 
             orig_year_id = year_id,
             underlying_nid = NA, 
             crosswalk_parent_seq = NA,
             survey_id = NULL,
             measure = "proportion",
             is_outlier = 0,
             age_group_id = case_when(age_start<80 ~ (age_start+25)/5, # Define GBD age group ids
                                      between(age_start, 80, 94) ~ ((age_start+25)/5)+9,
                                      age_start==95 ~ 235),
             sex = ifelse(sex_id == 1, "Male", ifelse(sex_id == 2, "Female", "Both")))]
good[val == 0 & between(age_group_id, 8, 17), `:=` (is_outlier=1, 
                                                    outlier_reason="Zero value")]
good[variance<0.0001, variance := 0.0001] # Set lower bounds for variance
bad[variance<0.0001, variance := 0.0001]

# Outlier age-sex data if necessary (to avoid bad data being used in establishing splitting patterns)
type <- "Before" 
source(paste0(FILEPATH, "tobacco_outliers.R"))
good <- good[(is.na(good$is_outlier) | good$is_outlier==0),]

# Save good data to use in ST-GPR
split_train_path <- paste0(FILEPATH, "smoking_", gold_standard, "_", date,".csv")
split_train_path_excel <- paste0(FILEPATH, "smoking_", gold_standard, "_", date,".xlsx")
write.csv(good, split_train_path)
openxlsx::write.xlsx(good, split_train_path_excel, sheetName="extraction", rowNames=F)

message("=== CROSSWALKING DONE ===")
}


# Step 3: Age-Sex Split ST-GPR -------------------------------------------------------
if(TRUE){
# Establish ST-GPR configuration and launch model
split_config_id <- map[me==me_name, unique(split_id)]
# central_root <- "/ihme/code/st_gpr/central/stgpr"
# setwd(central_root)

draws <- 100 

if (TRUE){ # Set up the ST-GPR configuration
  config_path_temp <- FILEPATH
  config <- fread(config_path_temp)
  
  choice_draws <- draws
  bundle_id_choice <- b_id
  
  config[model_index_id==split_config_id, gpr_draws := choice_draws]
  config[model_index_id==split_config_id, holdouts := holdouts]
  config[model_index_id==split_config_id, metric_id := 3]
  config[, year_end := 2024]
  config$prediction_year_ids <- NA
  config$bundle_id <- b_id
  config$release_id <- release
  config$description <- "2023 age-sex split ST-GPR with 2021 tools, updated params and ages"
  config[model_index_id==split_config_id, crosswalk_version_id := NA]
  config$path_to_data <- as.character(config$path_to_data)
  config[model_index_id==split_config_id, path_to_data := new_data_path]
  config[model_index_id==split_config_id, bundle_id := NA]
  
  if (me_name=="smoking_prev_former_any"){
    config$path_to_custom_covariates <- as.character(config$path_to_custom_covariates)
    config[model_index_id==split_config_id, path_to_custom_covariates := FILEPATH]
    config[model_index_id==split_config_id, me_name := "smoking_prev_former_any_raw"]
    config[model_index_id==split_config_id, modelable_entity_id := 24542]
  } else if (me_name %in% c("smoking_prev_current_any","smoking_prev")){
    config[,stage_1_model_formula := "data ~ log(cigarettes_pc) + as.factor(age_group_id) + (1 | level_1/level_2/level_3)"]
    config[,gbd_covariates := "cigarettes_pc"]
    config[,path_to_custom_covariates := NA]
  } 
  
  new_path <- paste0(gsub(".csv","",config_path_temp), "_",me_name,"_",date,".csv")
  write.csv(config, new_path, fileEncoding="UTF-8")
}

# Register ST-GPR
id.agesex <- register_stgpr_model(path_to_config=new_path,
                                  model_index_id=split_config_id)

# Submit ST-GPR
stgpr_sendoff(run_id=id.agesex, project=cluster_project, nparallel=nparallel, log_path=logs)

# Hold until model finishes
checkmod(id.agesex)

message("=== AGE-SEX SPLIT ST-GPR FINISHED ===")
}


# Step 4: Age-Sex Splitting Data -------------------------------------------------------
if (TRUE){
draw.files <- paste0(FILEPATH, locs[, location_id], ".csv", collapse=" ")
finished.file <- paste0(FILEPATH,"/model_complete.csv")

ages <- get_age_spans()[age_group_id %in% c(7:20, 30:32, 235)]
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages[, age_end := age_end - 1]
ages[, age_group := paste(age_start, "to", age_end)]
ages[age_end==124, age_group := "95 plus"]

# Check if there is any "bad" data to be split
if (nrow(bad) > 0){
  bad[is.na(variance),variance := mean(good$variance)] # Fill in missing variance with ~mean variance
  bad <- as.data.table(bad)
  
  split <- new_split_points(bad, id.agesex, region=F) # Split bad rows into correct age- and sex-grouped data
  split <- split[, names(bad), with=F][, split := 1]
  
  # Combine the good data (correctly age-sex split) with the newly split bad data
  split$underlying_nid <- NA
  split$is_outlier <- NULL # This will be filled in again later for the final model
  good <- as.data.table(good)
  setnames(good, "val", "mean", skip_absent=T)
  good$crosswalk_parent_seq <- NA

  if(using_bundles){
    split[,crosswalk_parent_seq := seq]
    split$seq <- NA
    good <- good %>% dplyr::select(!any_of(c("year_start", "year_end", "orig_age_start", "orig_age_end", "orig_year_id", "sex", "age_group_id", "measure", "is_outlier", "crosswalk_parent_seq")))
  } else{
    good <- good %>% dplyr::select(!any_of(c("year_start", "year_end", "orig_age_start", "orig_age_end", "orig_year_id", "sex", "age_group_id", "measure", "is_outlier")))
  }
  
  # Combine to create full dataset!
  out <- rbind(good, split, fill=T)
} else{
  message("No bad age groups, so no age-sex splitting needed!")
  out <- good
}

# Begin prepping for Final ST-GPR
nrow(out[is.na(mean)]) # Count missing means (shouldn't be any)
out <- merge(out, ages, by=c("age_start", "age_end"), all=T)

# Check for duplicates and fix in raw data
checkdups("out")

# Define a few algorithmic outliers
out$is_outlier <- 0
out[age_start > 19 & age_end < 65 & mean ==0,  `:=` (is_outlier=1, outlier_reason="Zero value")] # 0 is improbable data for adult ages
out[mean>0.9 | sample_size<10,  `:=` (is_outlier=1, outlier_reason="Sample size <10")]

setnames(out, c("mean"), c("val"))

if(using_bundles){ # Select columns needed for modeling
  out <- out[, .(nid, age_group_id, sex_id, location_id, year_id, val, variance, sample_size, split, xwalk, seq, crosswalk_parent_seq)]
} else{
  out <- out[, .(nid, age_group_id, sex_id, location_id, year_id, val, variance, sample_size, split, xwalk)]
}

ss_in <- out[!is.na(sample_size), unname(quantile(sample_size, .05))]
out[is.na(sample_size), sample_size:=ss_in] # Replace missing sample size with 5th percentile
out[val<.001, `:=`(val=.001, offset=1)] # Replace extremely low values with really low values
out[is.na(offset), offset:=0]
out[variance < .0009, variance:=.0009] # Replace extremely low variance with really low variance
out <- out[!is.na(variance)] # Drop those with missing variance altogether
out[, me_name:= me_name]
out$measure <- "proportion"
out <- out %>% dplyr::mutate(across(any_of(c("nid", "location_id", "year_id", "age_group_id", "sex_id", "val", "variance", "sample_size", "seq", "crosswalk_parent_seq")), as.numeric))
out$original_identifier <- with(out, ifelse(is.na(seq), crosswalk_parent_seq, seq))
final <- merge(out, locs[, .(location_name, ihme_loc_id, location_id)], by="location_id")

# Define pathways
in_stgpr_path <- paste0(outpath, "inputs/", "smoking_in_stgpr_", gold_standard, "_", date, ".csv")
in_stgpr_path_excel <- paste0(outpath, "inputs/", "smoking_in_stgpr_", gold_standard, "_", date, ".xlsx")

# Outlier if using old outliers
if (using_outliers){
outliers[, c("age_range", "location_name") := NULL]
outliers <- outliers[keep_outlier==1]
outliers_batch <- unique(outliers[batch_outlier==1, nid])
final <- final[!(nid %in% outliers_batch)]# just get rid of these bad NIDs - probably should make the is_outlier indicator eventually, though.
outliers <- outliers[!(nid %in% outliers_batch)]
outliers$norm_outlier <- 1
final <- merge(final, outliers[,c("nid","location_id","year_id","age_group_id","sex_id","norm_outlier")], by=c("nid","location_id","year_id","age_group_id","sex_id"), all.x=T)
final[, is_outlier := norm_outlier]
final$norm_outlier <- NULL
}

final[is.na(is_outlier),is_outlier := 0]
final[,sex := ifelse(sex_id==1, "Male","Female")]
final$underlying_nid <- NA
final$unit_value_as_published <- 1
final <- final %>% drop_na(age_group_id)

# Outlier based on review of vetting plots
type <- "After"
source(paste0(FILEPATH, "tobacco_outliers.R"))

if (using_bundles==F){
  final <- final %>% dplyr::mutate(measure_id=18) %>% dplyr::select(year_id, val, location_id, is_outlier, nid, sex_id, sample_size, age_group_id, measure_id, variance)
}
writexl::write_xlsx(list(extraction=final), in_stgpr_path_excel)

# Upload to crosswalk version before running final models
if (using_bundles){
  # Write new data to file to save xwalk version
  in_stgpr_path_excel_xwalk <- paste0(FILEPATH, "smoking_in_stgpr_xwalk_", gold_standard, "_", date, ".xlsx")
  openxlsx::write.xlsx(final, in_stgpr_path_excel_xwalk, sheetName="extraction", rowNames=F)

  message("Uploading crosswalk and new NIDs")
  description <- paste0("Post-outliering xwalk for GBD 2023")
  result <- save_crosswalk_version(
    bundle_version_id=new_bundle_version_id,
    data_filepath=in_stgpr_path_excel_xwalk,
    description=description)
  xwalk_version_final <- result$crosswalk_version_id
  message(paste(me_name, "Crosswalk Version ID:", xwalk_version_final))
}

message("=== AGE-SEX SPLITTING DONE ===")
}

# Step 5: Final ST-GPR -------------------------------------------------------
if (TRUE){
#' Launch the final ST-GPR model for final estimates
message("Launching Final STGPR model")
final_config_id <- map[me==me_name, unique(final_id)]

draws <- num.draws
holdouts <- 0

config <- fread(FILEPATH)
config <- config[model_index_id==final_config_id]
config <- config %>% mutate(release_id = release, 
                            gbd_round_id = gbd,
                            bundle_id = b_id,
                            gpr_draws = draws,
                            holdouts = holdouts,
                            year_end = 2024,
                            author_id = "USER",
                            description = "2023 age-sex split ST-GPR with 2021 tools")

# If using bundles, specify xwalk version; if not, specify path to flat file
if (using_bundles){
    config <- config %>% mutate(bundle_id = as.numeric(bundle_id),
                                crosswalk_version_id = xwalk_version_final,
                                path_to_data = NA)
  } else{
    config <- config %>% mutate(path_to_data = as.character(in_stgpr_path),
                                bundle_id = NA, 
                                crosswalk_version_id = NA)
}

# Define custom covariates if model requires them
if (me_name=="smoking_prev_former_any"){
    config <- config %>% mutate(path_to_custom_covariates = FILEPATH,
                                me_name = "smoking_prev_former_any_raw")
 } else if (me_name %in% c("smoking_prev_current_any", "smoking_prev")){
   config <- config %>% mutate(stage_1_model_formula = "data ~ log(cigarettes_pc) + as.factor(age_group_id) + (1 | level_1/level_2/level_3)",
                                 gbd_covariates = "cigarettes_pc")
}

new_path <- paste0(gsub(".csv","",config_path_temp), "_",me_name,"_final_",date,".csv")
write.csv(config,new_path)

# Register and submit model!
final.id <- register_stgpr_model(path_to_config=new_path,
                                 model_index_id=final_config_id)

stgpr_sendoff(run_id=final.id, project=cluster_project, nparallel=nparallel, log_path=logs)

message(paste(me_name, "Bundle Version ID:", new_bundle_version_id))
message(paste(me_name, "Crosswalk Version ID:", xwalk_version_final))
message(paste(me_name, "ST-GPR Run ID:", final.id))

if (make_plots){
  checkmod(final.id)
  modname <- switch(me_name,
                    smoking_prev_current_any="current",
                    smoking_prev_former_any="former",
                    smoking_prev="daily")
  tobacco_plot(me_name=modname,
               runid=final.id)
}

}
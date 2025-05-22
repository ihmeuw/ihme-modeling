# Introduction ------------------------------------------------------------
# Purpose: (1) Add newly extracted data to smokeless tobacco bundle
#          (2) For each component, a. split into good and bad data
#                                  b. run ST-GPR on good data
#                                  c. age- and sex-split bad data
#                                  d. combine with good data
#                                  e. run ST-GPR on all data
# Date Modified: 

# Setup -------------------------------------------------------------------
if (TRUE){
rm(list=ls())
user <- Sys.getenv("USER")

## Model specifications
draws              <- 100      # Number of draws; 100 for practice runs and 1000 for final runs
me_name            <- "smokeless_tobacco"              
adding_data        <- T        # If TRUE, will add newly imported data to bundle; if FALSE, will move forward with existing bundle data                    
using_bundles      <- F        # If TRUE, will save new bundle data, bundle, and xwalk versions and run ST-GPR from xwalk; if FALSE, will not save and will run ST-GPR from flat file
using_outliers     <- F        # If TRUE, will import outliers from GBD 2021 and past; should be FALSE unless there's a reason to use those

# If setting manually -- New cleaned and collapsed data (to be added to bundle if adding_data==T)
if (F){
dataname <- "all_data_long.csv"
}

# GBD cycle-related variables
release <- 16
gbd_prior <- 9 # Previous round, from which we are loading bundles and to which we are adding data
gbd <- 9       # Current modeling round
main_bundle <- "BUNDLE_ID"

# Cluster/ST-GPR related variables
cluster_project <- "PROJECT"
nparallel <- 50 # Number of parallelizations! More parallelizations --> faster (if cluster is emtpy). I usually do 50-100.
holdouts <- 0   # Keep at 0 unless you want to run cross-validation

# Filepaths 
share_path <- "FILEPATH"
# Where functions are stored
code_path <- "FILEPATH"
# These are inputs that may change every cycle and used in both smoking and smokeless models
input_path <- "FILEPATH" 
# These are constant reference files that do not generally change and are thus stored on J. 
reference_files <- "FILEPATH" 
# Main directory where intermediate files will be saved to from this script
outpath <- "FILEPATH"
# Where data is stored for this GBD cycle
data_path <- "FILEPATH"
# New cleaned and collapsed dataset
filedata <- file.info(list.files("FILEPATH", pattern="long", full.names = T))
new_raw_data <- rownames(filedata[with(filedata, order(as.POSIXct(ctime))), ])[1]
if(exists("dataname")){new_raw_data <- paste0("FILEPATH", dataname)}
# Where ST-GPR model draws are saved (central location)
stgpr_output <- "FILEPATH"
# Where the data and config for st-gpr will be saved
stgpr_input <- "FILEPATH"
# Path to a valid directory for storing logfiles
logs <- "FILEPATH"
# Path to bundle files for uploading and saving
bundle_path <- "FILEPATH"
# The file containing data to be outliered
outlier_file <- "FILEPATH"

# Loading packages and files
date <- gsub(" |:", "-", Sys.time())
pkgs <- c("data.table", "tidyverse", "parallel")
invisible(lapply(pkgs, library, character.only = TRUE))

if (TRUE){
source("FILEPATH")
}

# Mapping to bundle IDs/config IDs/age-sex splitting runs/etc. 
map <- fread(paste0("FILEPATH", "me_to_var_map.csv"))
best_old_bundles <- as.data.table(fread("FILEPATH"))[gbd_id == gbd_prior]
best_old_runs <- as.data.table(fread("FILEPATH"))[gbd_id == gbd_prior]

if (using_outliers){
outliers <- fread(outlier_file)
outliers[, c("location_name", "age_range") := list(NULL, NULL)]
outliers <- outliers[keep_outlier == 1]
full_outliers <- copy(outliers)
}

# Demographic information
ages <- get_age_spans()[age_group_id %in% c(7:21)]
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages <- ages %>% mutate(age_end=age_end-1, 
                        age_group=paste(age_start, "to", age_end))
ages[age_end==124, age_group := "80 plus"]
suppressWarnings(locs <- get_location_metadata(location_set_id=22, release_id=release)[level > 2])
pops <- get_population(location_set_id=22, year_id=1980:2024, sex_id=-1, 
                       age_group_id=c(7:21), release_id=release, location_id=locs[, location_id])

message("=== SETUP DONE ===")
}

# Step 1: Data Import and Processing -------------------------------------------------------------
if (TRUE){
# Read in existing data
old_bundle <- get_bundle_data(4886)
old_bundle <- old_bundle %>% mutate(sex_id=ifelse(sex=="Male", 1, ifelse(sex=="Female", 2, 3)), 
                                    age_start = orig_age_start, 
                                    age_end = orig_age_end) %>% 
              distinct(nid, location_id, year_id, sex, var, val, age_start, .keep_all = T)

# Clean the existing data (removing duplicates from extractions, overlapping age or year sets, etc.)
tobdf <- "old_bundle"
source("FILEPATH")

# Check duplicates (fix and run until no duplicates found)
# If anything found, add fixes to cleaning script above
checkdups("old_bundle")

if(adding_data){
  # Pull in best version of the new data, post-collapse & append:
  new_data <- fread(new_raw_data) %>% filter(var %like% "chew" | var %like% "slt" | var %like% "snus")
  
  # Theoretically, only new_data is needed. For GBD 2023, we sourced an old collapsed file as well to ensure all new NIDs were included. 
  #   This can be deleted for next round
  if (FALSE){
  data622 <- fread(paste0("FILEPATH", "all_data_long_2023-06-22.csv")) %>% filter(!nid %in% new_data$nid)
  data622 <- data622 %>% filter(var %like% "chew" | var %like% "slt")
  new_data <- rbind(new_data, data622) %>%
    mutate(id=paste(nid, location_id, sex_id, age_start, year_id, sep="-"), 
           val2=round(val, 5)) %>%
    distinct(nid, location_id, sex_id, age_start, year_id, var, val2, .keep_all = T) %>%
    dplyr::select(-val2)
  }
  
  # Clean the new data (removing duplicates from extractions, overlapping age or year sets, etc.)
  tobdf <- "new_data"
  source(paste0("FILEPATH", "01.1_smokeless_data_cleaning.R"))
  
  # Check duplicates (fix and run until no duplicates found)
  # If anything found, add fixes to cleaning script above
  checkdups("new_data")
  
  # Append old and new data
  new_data <- new_data %>% mutate(file_date := as.character(file_date), 
                                  type="new")
  
  old_bundle <- old_bundle %>% mutate(sex_id = case_when(sex=="Male" ~ 1, 
                                                         sex=="Female" ~ 2, 
                                                         TRUE ~ 3), 
                                      type="old")
  
  # Filter out NIDs in old data that match NIDs in new data in case data has been updated (e.g. new microdata vs. old tabs)
  old_bundle <- old_bundle %>% filter(!nid %in% new_data$nid) %>% dplyr::select(-file_date)
  
  # Bind new and old data together
  df <- rbindlist(list(old_bundle, new_data), use.names = T, fill = T) %>% arrange(type)

  # Prep dataset
  df <- df %>% mutate(seq=NA, 
                      is_outlier=0, 
                      age_group_id=999, 
                      orig_age_start=age_start, 
                      orig_age_end=age_end, 
                      orig_year_id=year_id, 
                      orig_year_start=year_start, 
                      orig_year_end=year_end, 
                      year_start=year_id, 
                      year_end=year_id, 
                      sex=ifelse(sex_id == 1, "Male", ifelse(sex_id == 2, "Female", "Both")), 
                      underlying_nid=NA, 
                      measure="proportion", 
                      variance=ifelse(variance < 0.0009, 0.0009, variance))
  
  # Check duplicates (fix and run until no duplicates found)
  checkdups("df")
}

# Step 1.5: Save data (OPTIONAL) -------------------------------------------------------
# If you've added new data and wish to save a new bundle, run the chunk below
if (FALSE){
  # Upload empty version of the bundle to clear old data
  bundle_save(purpose = "empty", bun_id = b_id, gbd_id = gbd, release_id = release, raw_data = df, fp_to_bundle = bundle_path, date = date, me_name = "smokeless_tobacco")
  
  # Upload new data to the bundle
  bundle_save(purpose = "upload", bun_id = b_id, gbd_id = gbd, release_id = release, raw_data = df, fp_to_bundle = bundle_path, date = date, me_name = "smokeless_tobacco")
  
  # Save the bundle version
  bundle_version_id <- bundle_save(purpose = "save", bun_id = b_id, gbd_id = gbd, release = release, raw_data = df, fp_to_bundle = bundle_path, date = date, me_name = "smokeless_tobacco")
  print(bundle_version_id)
  
  # For certainty, download the new data from the bundle and keep working with it
  bundle_draw <- get_bundle_data(bundle_id = b_id, export = FALSE)
  
  df <- make_bundle_data_changes(bundle_draw)
}

message("=== DATA IMPORT AND PROCESSING DONE ===")
}


# Step 2: Crosswalking -------------------------------------------
if (TRUE){
# Cast wide by var (which determines which model we're using)
out <- dcast.data.table(df, nid + survey_name + location_id + year_id + age_start + age_end + sex_id + source + imputed_ss + extractor ~ var, 
                        value.var = c("val", "variance", "sample_size", "seq", "is_outlier"))

# Check for illogical data
wide_data <- check_logicals(df = out, smokeless_only = T, margin_of_error = 0.05)
illogics <- wide_data[illogical==1] %>% invisible
if(nrow(illogics) > 0){
  to_fix <- illogics[, .(nid, source, extractor, illogic_reason)] %>% unique # Makes a record of the illogical variables
}

# Reshape long
var_cols <- names(wide_data)[names(wide_data) %like% "snus" | names(wide_data) %like% "slt" | names(wide_data) %like% "chew"]
wide_data[, (var_cols):= lapply(.SD, as.numeric), .SDcols = var_cols]
melt <- melt.data.table(wide_data, measure.vars=patterns("val_", "variance_", "sample_size_", "is_outlier_", "seq_"), 
                                     value.name = c("val", "variance", "sample_size", "is_outlier", "seq"), 
                                     variable.name = "var", variable.factor = F)

melt$var <- as.integer(melt$var)
relvars <- grep("var", names(wide_data), value = T)
namem <- data.table(numvar = relvars)
namem[, var:=seq(.N)] %>% invisible
melt <- merge(melt, namem, by = "var", all.x = T)
melt$var <- NULL
names(melt)[names(melt) == "numvar"] <- "var"
melt$var <- str_replace(melt$var, "val_", "")
melt <- melt[!is.na(val)]
setnames(melt, "mean", "val", skip_absent=T)
melt[, standard_error := sqrt(variance)]
melt[, standard_error := ifelse(standard_error < 0.03, 0.03, standard_error)]
melt[is.na(variance) & !is.na(val) & !is.na(sample_size) & !(val == 0), variance:=(val*(1-val))/sample_size] %>% invisible
melt[val == 0 & is.na(variance) & !is.na(sample_size) & sample_size > 3, pretend_val:=3/sample_size ] %>% invisible
melt[val == 0 & is.na(variance) & !is.na(sample_size), variance:=(pretend_val*(1-pretend_val))/sample_size] %>% invisible
melt[is.na(variance)] %>% nrow
melt[, c("pretend_val"):=NULL] %>% invisible
melt[, variance := ifelse(variance < 0.0009, 0.0009, variance)]
melt <- merge(melt, locs[, .(location_id, region_id)], by = "location_id", all.x = TRUE)

message("=== CROSSWALKING DONE ===")
}


# Step 3: Age-Sex Splitting ST-GPR -------------------------------------------
if (TRUE){
gold_standard_mei <- "chew_current_any_var"
age_sex_ids <- c()

for (var_of_interest in c("slt_current_any_var", "chew_current_any_var", "non_chew_current_any_var")){
# var_of_interest <- "slt_current_any_var"  
message(paste0("Working on ", var_of_interest))
  
# Create a modified subset of melt with variable of interest
xwalked <- melt %>% filter(var == var_of_interest, val<0.9) %>% 
           mutate(age_end=ifelse(age_end>=84, 84, age_end), 
                  me_name=me_name)

# Split data into "good" (data are correctly age-sex split) and "bad" (data need to be age- or sex-split) dataframes
datasets <- divide_data(xwalked, round.ages = F)
good     <- datasets$train[, split := 0]
fwrite(good, paste0("FILEPATH", "temp_split_files/", var_of_interest, "_good.csv"))
bad      <- datasets$test
fwrite(bad, paste0("FILEPATH", "temp_split_files/", var_of_interest, "_bad.csv"))

good$measure <- "proportion"
good$is_outlier <- 0

# Check duplicates in good data before running ST-GPR
checkdups("good")
    
if (using_outliers){
  outliers_batch <- unique(full_outliers[batch_outlier == 1, nid])
  good <- good %>% mutate(is_outlier=ifelse(nid %in% outliers_batch, 1, 0),
                          outlier_reason=ifelse(nid %in% outliers_batch, "Existing batch outlier", NA))
  outliers <- outliers[!(nid %in% outliers_batch)]
  outliers$norm_outlier <- 1
  good <- merge(good, outliers[, c("nid", "location_id", "year_id", "age_group_id", "sex_id", "norm_outlier")], by=c("nid", "location_id", "year_id", "age_group_id", "sex_id"), all.x=T)
  good <- good[nid != "NID" & nid != "NID" & nid != "NID" & nid != "NID" & nid != "NID"]
  good[, is_outlier := norm_outlier]
  good$norm_outlier <- NULL
  good[is.na(is_outlier), is_outlier := 0]
}

# Save the good data in the format necessary for ST-GPR. This good data will be used to train the ST-GPR model.
split_train_path <- paste0("FILEPATH", "sep_slt_models/", model, "_", var_of_interest, "_", date, ".csv")
good <- prep_good_data(good)
good <- distinct(good)
good$seq <- NULL

## Outliering via plot review 10/2024
type <- "Before"
me_name <- var_of_interest
source(paste0("FILEPATH", "/tobacco_outliers/tobacco_outliers.R"), local=T)

fwrite(good, split_train_path) # Save the training (good) data into a place to be used by ST-GPR

# Launching Age-Sex Split ST-GPR
message("Launching Age-Sex Split ST-GPR") 
split_config_id <- map[me == "smokeless_tobacco", unique(split_id)]

if(var_of_interest == "slt_current_any_var"){
    me_id <- "ID"
} else if(var_of_interest == "chew_current_any_var"){
    me_id <- "ID"
} else if(var_of_interest == "non_chew_current_any_var"){
    me_id <- "ID"
}

# Load config file
config_path_temp <- paste0("FILEPATH", "configs/stgpr_from_bundle_GBD2023.csv")
config <- as.data.table(fread("FILEPATH"))

config[model_index_id == split_config_id, gpr_draws := draws]
config[model_index_id == split_config_id, holdouts := holdouts]
config$path_to_data <- as.character(config$path_to_data)
config[model_index_id == split_config_id, path_to_data := split_train_path]
config[model_index_id == split_config_id, bundle_id := NA]
config[model_index_id == split_config_id, author_id := "USER"]
config$release_id <- release
config[model_index_id == split_config_id, modelable_entity_id := me_id]
config$year_end <- 2024
config[model_index_id == split_config_id, description:=paste0("age-sex split for ", var_of_interest)]

# Save config
new_path <- paste0(gsub(".csv", "", config_path_temp), me_name, ".csv")
fwrite("FILEPATH", new_path)

# Register ST-GPR model
id.agesex <- register_stgpr_model(path_to_config = new_path, 
                                  model_index_id = split_config_id)
# Submit ST-GPR model
stgpr_sendoff(run_id=id.agesex, project=cluster_project, nparallel=nparallel, log_path=logs)

age_sex_ids <- c(age_sex_ids, id.agesex)
message(paste0("Age-sex split ST-GPR for ", var_of_interest, " submitted!"))
}

message("All age-sex split ST-GPR models have been submitted.")
message("SLT model run_id is ", age_sex_ids[1])
message("Chew model run_id is ", age_sex_ids[2])
message("Non-Chew model run_id is ", age_sex_ids[3])

checkmod(age_sex_ids[1])
checkmod(age_sex_ids[2])
checkmod(age_sex_ids[3])

message("=== AGE-SEX SPLIT ST-GPRS COMPLETE ===")
}


# Step 4: Final Component Models ------------------------------------------------------
if (TRUE){
final_ids <- c()
for (var_of_interest in c("slt_current_any_var", "chew_current_any_var", "non_chew_current_any_var")){
   # var_of_interest <- "slt_current_any_var" # For testing purposes
message(paste0("Working on ", var_of_interest))

if (var_of_interest == "slt_current_any_var"){
  id.agesex <- age_sex_ids[1]
  # id.agesex <- "ID"
} else if (var_of_interest == "chew_current_any_var"){
  id.agesex <- age_sex_ids[2]
  # id.agesex <- "ID"
} else if (var_of_interest == "non_chew_current_any_var"){
  id.agesex <- age_sex_ids[3]
  # id.agesex <- "ID"
}

# Load draw files from age-sex ST-GPR
draw.files <- paste0("FILEPATH")

# Load good and bad datasets created earlier
good <- fread("FILEPATH")
bad <- fread("FILEPATH")

good$measure <- "proportion"
good$is_outlier <- 0

if(var_of_interest %like% "slt"){bad[nid=="NID" & location_id==163 & age_start==15, extractor:="drop"]} # Overlapping age-sex groups; will remove these tagged rows below

# Check duplicates
checkdups("good")
checkdups("bad")

if (using_outliers){
  outliers_batch <- unique(full_outliers[batch_outlier == 1, nid])
  good <- good %>% mutate(is_outlier=ifelse(nid %in% outliers_batch, 1, 0),
                          outlier_reason=ifelse(nid %in% outliers_batch, "Existing outlier batch", NA))
  outliers <- outliers[!(nid %in% outliers_batch)]
  outliers$norm_outlier <- 1
  good <- merge(good, outliers[, c("nid", "location_id", "year_id", "age_group_id", "sex_id", "norm_outlier")], by=c("nid", "location_id", "year_id", "age_group_id", "sex_id"), all.x=T)
  good <- good[nid != "NID" & nid != "NID" & nid != "NID" & nid != "NID" & nid != "NID"]
  good[, is_outlier := norm_outlier]
  good$norm_outlier <- NULL
  good[is.na(is_outlier), is_outlier := 0]
}
 
# Split the tabulated data by age- and sex-sets 
if (nrow(bad) > 0){
  message("Splitting Points for Age Sex")
  setnames(bad, "val", "mean", skip_absent = T) # Need "mean" for this function
  split <- new_split_points(bad, id.agesex, region = T)
  split <- split[, names(bad), with=F][, split := 1] 
  setnames(split, "mean", "val", skip_absent = T)
  
  if(var_of_interest %like% "slt"){split <- split[!(nid=="NID" & extractor=="drop")]} # Fix for overlapping age-sex strata mentioned above
  
  # Check duplicates in split data
  checkdups("split")
  
  # Combine the good data (already correctly age-sex split) with the newly split bad data
  good$measure <- NULL
  good$is_outlier <- 0
  setDT(good)
  setDT(split)
  out <- rbindlist(list(good, split), use.names = T, fill = T)
  
} else{
  message("No bad age groups, so no age-sex splitting needed!")
  out <- good
}


# Final ST-GPR Prep 
out <- dplyr::select(out, -any_of(c("age_group_id")))
out <- merge(out, ages, by = c("age_start", "age_end"), all = T)

# Dealing with seqs
if(using_bundles == T){
  out <- out[split > 0, crosswalk_parent_seq := seq]
  out <- out[split > 0, seq := NA]
  out <- out[split == 0, crosswalk_parent_seq := NA]
}

vars <- c("val", "variance", "sample_size", "split")
me_of_interest <- gsub("_var", "", var_of_interest)

if (using_bundles==F){out$crosswalk_parent_seq=NA}
final <- prep_gpr(out, bundle_bool, me_name = me_of_interest)
final <- merge(final, locs[, .(location_name, ihme_loc_id, location_id)], by = "location_id")

# Check duplicates
checkdups("final")

  
# Launching Final ST-GPR
  
# Create the path for saving the PRE-ST-GPR data, the cleaned dataset
in_stgpr_path <- paste0("FILEPATH")
# Inflate the variance points if they need to be changed at all
final[, variance := ifelse(variance < 0.0009, 0.0009, variance)]
setnames(final, "val", "data", skip_absent = T)
if (using_bundles==T){
    final <- final[, c("nid", "location_id", "year_id", "age_group_id", "sex_id", "data", "variance", "sample_size", "measure", "is_outlier", "seq", "crosswalk_parent_seq")]
}
final <- final[!is.na(variance)]

if (using_outliers){
full_outliers <- full_outliers[keep_outlier==1]
outliers_batch <- unique(full_outliers[batch_outlier == 1, nid])
final <- final[!(nid %in% outliers_batch)]
outliers <- outliers[!(nid %in% outliers_batch)]
outliers$norm_outlier <- 1
final_out <- merge(final, outliers[, c("nid", "location_id", "year_id", "age_group_id", "sex_id", "norm_outlier")], by=c("nid", "location_id", "year_id", "age_group_id", "sex_id"), all.x=T)
final_out <- final_out[!(nid %in% outliers_batch)]
final_out[, is_outlier := norm_outlier]
final_out$norm_outlier <- NULL
final <- final_out
}

# Outliering via plot review
type <- "After"
me_name <- var_of_interest
source("FILEPATH")
final <- final_out
  
# Saving bundles
if(using_bundles == T){
   message("Saving xwalk version")
   bundle_version_id <- get(paste0("bversion_", var_of_interest))
   xwalk_id <- bundle_save("xwalk", bun_id = bundle_component, gbd_id = gbd, release_id = release, raw_data = final, fp_to_bundle = in_stgpr_path, date = date, me_name = var_of_interest, bundle_version = bundle_version_id, description = description_of_run)
   print(xwalk_id)
} else {
  message("Writing in ST-GPR data to path")
  write.csv(final, "FILEPATH")
}
message("Done Writing in ST-GPR data")
  
# Launch the final component ST-GPR models
message("Launching Final ST-GPR model")
final_config_id <- map[gold_standard == gold_standard_mei, unique(final_id)]

if(var_of_interest == "slt_current_any_var"){
    me_id <- "ID"
} else if(var_of_interest == "chew_current_any_var"){
    me_id <- "ID"
} else if(var_of_interest == "non_chew_current_any_var"){
    me_id <- "ID"
}
  
# Load config
config <- fread("FILEPATH")

# Set params    
config[model_index_id == final_config_id, `:=` (gpr_draws = draws,
                                                holdouts = holdouts,
                                                description = paste0(var_of_interest, " final component model"),
                                                me_name = paste0(me_of_interest),
                                                modelable_entity_id := me_id)]
config$release_id <- release
config$year_end <- 2024
    
if(using_bundles == T){
  config$crosswalk_version_id <- as.numeric(config$crosswalk_version_id)
  config$bundle_id <- as.numeric(config$bundle_id)
  config[model_index_id == final_config_id, `:=` (crosswalk_version_id = xwalk_id,
                                                  path_to_data = NA,
                                                  bundle_id = as.numeric(bundle_component))]
} else {
  config[model_index_id == final_config_id, `:=` (crosswalk_version_id := NA,
                                                  path_to_data := "FILEPATH",
                                                  bundle_id := NA)]
  config$path_to_data <- as.character(config$path_to_data)
}

# Write new config
new_path <- paste0("FILEPATH")
write.csv(config, "FILEPATH", row.names = F)

# Register ST-GPR model
final.id <- register_stgpr_model(path_to_config = "FILEPATH", 
                                 model_index_id = final_config_id)

# Submit ST-GPR model
stgpr_sendoff(run_id=final.id, project=cluster_project, nparallel=nparallel, log_path=logs)
message(paste0("Final ST-GPR for ", var_of_interest, " submitted!"))

final_ids <- c(final_ids, final.id)
}

message("All final ST-GPR models have been submitted.")
message("SLT model run_id is ", final_ids[1])
message("Chew model run_id is ", final_ids[2])
message("Non-Chew model run_id is ", final_ids[3])

checkmod(final_ids[1])
if (make_plots){
   tobacco_plot(me_name="slt",
               runid=final_ids[1])
}

checkmod(final_ids[2])
if (make_plots){
   tobacco_plot(me_name="chew",
               runid=final_ids[2])
}

checkmod(final_ids[3])
if (make_plots){
   tobacco_plot(me_name="nonchew",
               runid=final_ids[3])
}

message("=== FINAL COMPONENT ST-GPRS COMPLETE ===")
}

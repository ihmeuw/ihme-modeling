#####################################INTRO#############################################
#' Author: 
#' 5/17/18
#' Purpose: Wait for all files to finish, then combine them all into a single .xlsx and upload to the Epi database for dismod
#'          1) Wipe the current database (only data will be from previous model upload)
#'          2) Wait for files to finish
#'          3) Read in all files and combine into one with needed data cols filled out
#'          4) Upload to Epi database
#'
#'
#####################################INTRO#############################################

library("ihme", lib.loc = "FILEPATH")
ihme::setup()

library(data.table)
library(magrittr)
library(openxlsx)
library(dplyr)
library(rjson)

source_functions(get_epi_data = T, upload_epi_data = T, get_location_metadata = T, get_best_model_versions = T,
                 get_ids = T)

# Read in commandline args and setup ------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
me_id    <- args[1]
main_dir <- args[2]
root_dir <- args[3]
# me_id <- 1635

# setup the bundle_id, acause name, and NID based on the ME id
# throws an error if given an unexpected me id
me_id_setup <- function(me_id) {
  if (me_id == 1629) {
    # chlamydia
    assign("bundle_id", 3794,         envir = .GlobalEnv)
    assign("acause", "std_chlamydia", envir = .GlobalEnv)
    assign("NID", 350632,             envir = .GlobalEnv)
  } else if (me_id == 1635) {
    # gonorrhea
    assign("bundle_id", 3791,         envir = .GlobalEnv)
    assign("acause", "std_gonnorhea", envir = .GlobalEnv)
    assign("NID", 350631,             envir = .GlobalEnv)
  } else {
    # unknown me_id, fail gracefully
    stop(paste(Sys.time(), "Modelable entity id", me_id, "does not match chlamydia (1629) or gonorrhea (1635)"))
  }
}

me_id_setup(me_id)

model_version_id <- (get_best_model_versions("modelable_entity", me_id))$model_version_id

# Wipe the current database -----------------------------------------------
# we do this step first as we'll probably have to wait for file writing to
# finish anyway, so its more efficient to do what we can already

message(paste(Sys.time(), "Wiping the current database for", acause, "in bundle id", bundle_id))

wipe_db <- function(bundle, acause) {
  cat(paste0("Deleting old data for ", acause, "\n"))
  date <- gsub("-", "_", Sys.Date())
  
  dt <- get_epi_data(bundle)
  dt <- dt[, .(seq)]
  
  output_file <- paste0("OUTPUT_FILEPATH")
  
  write.xlsx(dt, output_file, sheetName = "extraction")
  upload_epi_data(bundle_id = bundle, filepath = output_file)
}

wipe_db(bundle_id, acause)


# Wait for epididymo incidence files to write -----------------------------

message(paste(Sys.time(), "Waiting for incidence calculations to complete for", acause))

loc_data <- get_location_metadata(35)
locations <- unique(loc_data[level >= 3, location_id])

# source job submission function (submits 02) and wait_for function
source(paste0(root_dir, "submit_and_wait_functions.R"))

# WAIT FOR FILES TO FINISH: every time (default 60) seconds, checks for files label with me_id in the files_in directory
# if it sits at 95% of files for too long (30 repeats here), it'll relaunch the missing locations using the script specified by
# relaunch_code.
wait_for(locations, files_in = paste0(main_dir, "EX"), pattern = paste0(me_id, "_eo_"), 
         relaunch_code = paste0(root_dir, "02_epididymo_incidence.R"), time = 60, repeat_limit = 30)


# Combine all files for an ME ---------------------------------------------

message(paste(Sys.time(), "Combining files for", acause, "into one file for upload"))


file_list <- list.files(paste0(main_dir, "EX", "/"), pattern = paste0(me_id, ".*csv$"))

# drops where incidence isnt allowed in models:
# under 10 and > 60ish (currently)
fread_plus <- function(filepath) {
  print(filepath)
  dt <- fread(filepath)
  dt <- dt[mean != 0, ] 
} 

# combine all location-specific csvs into one
eo_incidence <- rbindlist(parallel::mclapply(paste0(main_dir, "EX/", file_list), fread_plus))

# turn age groups into age start and ends
ages <- get_ids("age_group")
ages <- ages[age_group_id %in% 6:20, ] %>% 
  mutate(age_start = gsub(" to.*", "", age_group_name),
         age_end = as.numeric(age_start) + 4)

eo_incidence <- left_join(eo_incidence, ages, by = "age_group_id")

# Add/edit columns we need and upload to database -------------------------

# create necessary cols we need
col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0("FILEPATH/upload_order.csv"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (!name %in% names(dt)){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epi_order <- c(epi_order, extra_cols)
  setcolorder(dt, new_epi_order)
  return(dt)
}
eo_incidence <- col_order(as.data.table(eo_incidence))

# tack on the right values for required columns
final <- eo_incidence %>% 
  mutate(bundle_id  = bundle_id,
         nid        = NID,
         sex_issue  = 0,
         sex        = "Male",
         year_issue = 0,
         year_start  = year_id,
         year_end    = year_id,
         age_issue  = 0,
         measure    = "incidence",
         unit_type  = "Person*year",
         unit_value_as_published = 1,
         measure_issue           = 0,
         uncertainty_type_value  = 95,
         recall_type             = "Not Set",
         representative_name     = "Representative for subnational location only",
         source_type             = "Unidentifiable",
         urbanicity_type         = "Mixed/both",
         note_modeler = paste0("Incidence from ", acause, " (MVID ", model_version_id,  ") uploaded on ", Sys.Date(), " by ", user),
         is_outlier   = 0,
         extractor    = user) %>% 
  select(-c(year_id:age_group_name))

upload_file <- paste0("FILEPATH/EX", Sys.Date(), ".xlsx")

message(paste(Sys.time(), "Saving extraction file here:", upload_file))

write.xlsx(final, upload_file, sheetName = "extraction")

# UPLOAD DATA
# try catch to send success/fail message to slack
tryCatch({
  upload_epi_data(bundle_id, upload_file)
  assign("message", paste0("Epididymo-orchitis incidence for ", acause, " uploaded successfully to bundle_id ", bundle_id, 
                           ". View file here: "), .GlobalEnv)
}, error = function(e) {
  assign("message", paste0("Epididymo-orchitis incidence for ", acause, " uploaded *failed* to bundle_id ", bundle_id, 
                           ". View file here. Error: ", e), .GlobalEnv)
})


# Make a message ----------------------------------------------------------
json <- rjson::fromJSON(file = paste0(h_root, "private_url.json"))

post_slack <- function(message, url) {
  system(paste0("curl -X POST -H \'Content-type: application/json' --data \'{\"text\": \"", message, "\"}\' ", url))
}

# post message
post_slack(message = message, url = json$pipeline_updates)








#' @author
#' @date 2019/05/15
#' @description GBD 2019 Acute Otitis Media sex-split and xwalk

rm(list=ls())

library(openxlsx)
library(reticulate)

# SOURCE FUNCTIONS --------------------------------------------------------
# Source all GBD shared functions at once
shared.dir <- "filepath"
files.sources <- list.files(shared.dir)
files.sources <- paste0(shared.dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source)) # "invisible suppresses the sapply output

helper_dir <- "filepath"
files.sources <- list.files(helper_dir)
files.sources <- paste0(helper_dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source))

repo_dir <- "filepath"
source(paste0(repo_dir, "mr_brt_functions.R"))

# SET OBJECTS -------------------------------------------------------------
bundle <- 26 # bundle_id for acute otitis
acause <- "otitis"
ds <- 'iterative'   # decomp step
gbd_round_id <- 7

# get bundle version id from bv tracking sheet
bundle_version_dir <- "filepath"
bv_tracker <- fread(paste0(bundle_version_dir, 'bundle_version_tracking.csv'))
bv_row <- bv_tracker[bundle_id == bundle & current_best == 1]

bv_id <- bv_row$bundle_version
print(paste("Crosswalking on bundle version", bv_id, "BV description", bv_row$description))

date <- gsub("-", "_", Sys.Date())

model_objects_dir <- "filepath"
out_dir <- "filepath"

# SEX SPLIT ---------------------------------------------------------------
# get bundle version
bundle_version_dt <- get_bundle_version(bv_id, transform = T, fetch = "all")

# from RDS 
sex_split_model <- readRDS(paste0(model_objects_dir, "2019_05_28_AOM_sexsplit.RDS"))

# only sex split if there is both sex data
if ("Both" %in% unique(bundle_version_dt$sex)){
  sex_split_final <- sex_split_data(bundle_version_dt, sex_split_model)
  sex_split_final_dt <- sex_split_final[[1]]
} else if (!"Both" %in% unique(bundle_version_dt$sex)) {
  sex_split_final_dt <- bundle_version_dt
}

# REMOVE INPATIENT DATA AND GROUP REVIEW 0 DATA ---------------------------
# Remove inpatient data since it was not used
dt <- sex_split_final_dt[clinical_data_type != "inpatient"]
xwalk_dt <- dt[group_review != 0 | is.na(group_review)]

# SAVE CROSSWALK VERSION --------------------------------------------------
desc <- "description"
sex_model_name <- "name"

wb <- createWorkbook()
addWorksheet(wb, "extraction")
writeData(wb, "extraction", xwalk_dt)
saveWorkbook(wb, paste0(out_dir, sex_model_name, ".xlsx"), overwrite = T)

result <- save_crosswalk_version(bundle_version_id = bv_id, 
                                 data_filepath = paste0(out_dir, sex_model_name, ".xlsx"), 
                                 description = desc)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

if (result$request_status %like% "Successful") {
  df_tmp <- data.table(bundle_id = bundle,
                       bundle_version_id = bv_id,
                       crosswalk_version = result$crosswalk_version_id, 
                       is_bulk_outlier = 0,
                       date = date,
                       description = desc, 
                       filepath = paste0(out_dir, sex_model_name, ".xlsx")
                       # current_best = 1,
                       )
  
  cv_tracker <- as.data.table(read.xlsx(paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx')))
  # cv_tracker[bundle_id == bundle,`:=` (current_best = 0)]
  cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
  write.xlsx(cv_tracker, paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
}


#' @author
#' @date 
#' @description GBD 2019 Chronic Otitis Media sex-split and xwalk

rm(list=ls())
library(openxlsx)

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
bundle <- 27 # bundle_id for chronic otitis
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

# from RDS
sex_split_model <- readRDS(paste0(model_objects_dir, "2019_06_19_COM_sexsplit.RDS"))

# FUNCTIONS ------------------------------------------------------------------
# COPIES BOTH SEX REMISSION DATA AS MALE AND FEMALE REMISSION FOR XWALK VERSION
get_sex_remission <- function(sexsplit_dt) {
  dt <- copy(sexsplit_dt)
  not_remission_dt <- dt[measure != "remission"]
  male_dt <- copy(dt[measure == "remission"])
  male_dt[, `:=`(sex = "Male",
                 specificity = "sex",
                 group_review = 1,
                 group = 1)]
  male_dt[, crosswalk_parent_seq := seq]
  male_dt[, seq := NA]
  female_dt <- copy(dt[measure == "remission"])
  female_dt[, `:=`(sex = "Female",
                   specificity = "sex",
                   group_review = 1,
                   group = 1)]
  female_dt[, crosswalk_parent_seq := seq]
  female_dt[, seq := NA]
  dt <- rbindlist(list(male_dt, female_dt, not_remission_dt))
}

# SEX SPLIT ---------------------------------------------------------------
bundle_version_dt <- get_bundle_version(bv_id, transform = T, fetch = "all")
sex_split <- sex_split_data(bundle_version_dt, sex_split_model)
sex_split_dt <- sex_split[[1]]

sex_split_dt <- sex_split_dt[group_review != 0 | is.na(group_review)]
final_xwalk_dt <- find_nondismod_locs(sex_split_dt)
final_xwalk_dt <- get_sex_remission(final_xwalk_dt)

if ("inpatient" %in% unique(final_xwalk_dt$clinical_data_type)){
  # drop rows with massive SE - they would not contribute much to model anyway
  final_xwalk_dt <- final_xwalk_dt[standard_error < 1]
}

# SAVE CROSSWALK VERSION --------------------------------------------------
desc <- paste(ds, "sex-split", bv_row$description)
sex_model_name <- paste0(date, "_chronic_otitis_sex_split")

wb <- createWorkbook()
addWorksheet(wb, "extraction")
writeData(wb, "extraction", final_xwalk_dt)
saveWorkbook(wb, paste0(out_dir, sex_model_name, ".xlsx"), overwrite = T)

result <- save_crosswalk_version(bundle_version_id = bv_id,
                                 data_filepath = paste0(out_dir, sex_model_name, ".xlsx"),
                                 description = desc)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

if (result$request_status == "Successful") {
  df_tmp <- data.table(bundle_id = bundle,
                       bundle_version_id = bv_id,
                       crosswalk_version = result$crosswalk_version_id,
                       is_bulk_outlier = 0,
                       date = date,
                       description = desc,
                       filepath = paste0(out_dir, sex_model_name, ".xlsx"),
                       current_best = 1)

  cv_tracker <- read.xlsx(paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
  cv_tracker <- data.table(cv_tracker)
  cv_tracker[bundle_id == bundle,`:=` (current_best = 0)]
  cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
  write.xlsx(cv_tracker, paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
}

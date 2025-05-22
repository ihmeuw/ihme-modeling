library(readr)
library(data.table)
library(assertable)

library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

if(!interactive()){
  parser <- argparse::ArgumentParser()
  parser$add_argument("--version_id", type = "integer", required = TRUE,
                      help = "5q0 data version id")
  parser$add_argument("--mark_best", type = "character", required = TRUE,
                      help = "TRUE/FALSE mark run as best")

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

}else{
  version_id <- "NUMERIC_RUN_ID_HERE"
  mark_best <- F
}

new_run_id <- version_id
source_dir <- "FILEPATH"

adult_path <- paste0("FILEPATH")
## Creating additional 45q15 upload file to preserve 45q15_data file
adult_path_upload <- paste0("FILEPATH")

locs <- get_locations(gbd_year = 2023, gbd_type = "ap_old")
eth_locs <- locs[ihme_loc_id %like% "ETH" & location_id < 6e4, location_id]

adult <- fread(adult_path)

## read flat file for adult version
adult_prev_version <- get_proc_version("45q15 data", gbd_year = 2021)
adult_prev <- fread(gsub(new_run_id, adult_prev_version, adult_path))
adult_prev[, age_group_id := 199]

## Retrieve NIDs for data to keep
new_nid <- fread(glue::glue("FILEPATH"))
archived_new_nid <- fread(glue::glue("FILEPATH"))
new_nids <- c(unique(new_nid$nid), unique(archived_new_nid$nid))

adult[method_id != 5 |(nid %in% new_nid$nid & !location_id %in% eth_locs), keep := T]
adult <- adult[(keep)]
adult[, keep := NULL]

adult <- rbind(adult, adult_prev[method_id == 5], fill = T)

adult$exposure <- sapply(adult$exposure, as.numeric)

assert_values(adult, "exposure", "not_na", warn_only = T)

adult[is.na(exposure), exposure := 0]

write_csv(adult, adult_path_upload)

mortdb::upload_results(filepath=adult_path_upload, model_name="45q15", model_type="data", run_id=new_run_id)

if(mark_best){
  update_status(model_name="45q15", model_type="data", run_id=new_run_id, new_status="best", send_slack = T)
}

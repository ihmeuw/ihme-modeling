## set directories
## h: mort-data root dir
if (Sys.info()[1] == 'Windows') {
  library(argparse)
  library(mortdb)
  library(readr)
  library(data.table)
  h <- "FILEPATH"
} else {
  library(argparse)
  library(data.table)
  library(readr)
  library(RMySQL)
  library(assertable)
  library(mortdb, lib = "FILEPATH")
  library(mortcore, lib= "FILEPATH")
  h <- Sys.getenv("HOME")
}

parser <- argparse::ArgumentParser()
parser$add_argument("--version_id", type = "integer", required = TRUE,
                    help = "5q0 data version id")
parser$add_argument("--mark_best", type = "character", required = TRUE,
                    help = "TRUE/FALSE mark run as best")

args <- parser$parse_args()
list2env(args, .GlobalEnv)

new_run_id <- version_id

master_folder <- paste0("FILEPATH")
input_folder <- paste0("FILEPATH")
output_folder <- paste0("FILEPATH")

under_five_path <- paste0("FILEPATH")
under_five <- fread(paste0("FILEPATH"))

under_five$adjustment <- 0
under_five$reference <- 0
under_five <- under_five[!is.na(nid),]

# remove duplicate census for ZAF locs (different nids, same underlying source)
zaf_locs <- fread(paste0("FILEPATH"))[grepl("ZAF", ihme_loc_id)]$location_id
under_five <- under_five[!(location_id %in% zaf_locs & (nid == 12142 | nid == 140966))]

# remove negative values
under_five <- under_five[mean >= 0]

# final assertions
assert_values(under_five, "mean", test = "gte", test_val = 0)
assert_values(under_five, "mean", test = "lt", test_val = 1)

write_csv(under_five, under_five_path)

previous_complete_run_id <- get_proc_version(model_name = "5q0", model_type = "data", run_id = "recent_completed")
upload_results(filepath = under_five_path, model_name = "5q0", model_type = "data", run_id = new_run_id)

if(mark_best){
  update_status("5q0", "data", run_id = new_run_id, new_status = "best")
}

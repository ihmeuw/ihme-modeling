library(readr)
library(data.table)
library(assertable)

library(mortdb, lib = 'FILEPATH')
library(mortcore, lib = 'FILEPATH')

parser <- argparse::ArgumentParser()
parser$add_argument("--version_id", type = "integer", required = TRUE,
                    help = "5q0 data version id")
parser$add_argument("--mark_best", type = "character", required = TRUE,
                    help = "TRUE/FALSE mark run as best")

args <- parser$parse_args()
list2env(args, .GlobalEnv)

new_run_id <- version_id

adult_path <- paste0("FILEPATH")

adult <- fread(adult_path)
adult$exposure <- sapply(adult$exposure, as.numeric)

assert_values(adult, "exposure", "not_na", warn_only = T)

# Add for no 2020+ data runs
# adult <- adult[year_id < 2020]

adult[is.na(exposure), exposure := 0]
adult[, c("dup") := NULL]
write_csv(adult, adult_path)

previous_complete_run_id <- get_proc_version(model_name = "45q15", model_type = "data", run_id = "recent_completed")
upload_results(filepath=adult_path, model_name="45q15", model_type="data", run_id=new_run_id)

if(mark_best){
  update_status(model_name="45q15", model_type="data", run_id=new_run_id, new_status="best", send_slack = T)
}

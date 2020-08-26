library(readr)
library(data.table)
library(assertable)

library(mortdb, lib = "FILEPATH")
library(mortcore, lib= "FILEPATH")

args <- commandArgs(trailingOnly = T)

new_run_id <- as.numeric(args[1])
mark_as_best <- as.character(args[2])

adult_path <- "FILEPATH"

adult <- fread(adult_path)
adult$exposure <- sapply(adult$exposure, as.numeric)

assert_values(adult, "exposure", "not_na", warn_only = T)

adult[is.na(exposure), exposure := 0]
adult[, c("dup") := NULL]
write_csv(adult, adult_path)

previous_complete_run_id <- get_proc_version(model_name = "45q15", model_type = "data", run_id = "recent_completed")
upload_results(filepath=adult_path, model_name="45q15", model_type="data", run_id=new_run_id)

if(mark_as_best){
  update_status(model_name="45q15", model_type="data", run_id=new_run_id, new_status="best", send_slack = T)
}
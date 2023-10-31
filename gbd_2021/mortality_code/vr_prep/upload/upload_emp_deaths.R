
library(mortdb)
library(mortcore)

library(ggplot2)
library(data.table)

args <- commandArgs(trailingOnly = T)
new_run_id <- as.numeric(args[1])
recent_completed_run_id <- as.numeric(args[2])
mark_as_best <- as.logical(args[3])
upload <- as.logical(args[4])

print(new_run_id)
print(mark_as_best)

run_folder <- paste0()
datapath <- paste0()
output_folder <- paste0()

if (upload) {
  upload_results(filepath = datapath, model_name = "death number empirical", model_type = "data", run_id = new_run_id)
  check <- compare_uploads(model_name = "death number empirical", model_type = "data", current_run_id = new_run_id, comparison_run_id = recent_completed_run_id,
                           summary_by_vars = c("ihme_loc_id", "source_type_id"), return_granular = F, send_slack = T)
  readr::write_csv(check, paste0())

}

if(mark_as_best){
  mortdb::update_status("death number empirical", "data", run_id = new_run_id, new_status = "best")
}

################################################################################
## upload_5q0
## Description: Upload 5q0 data into database
################################################################################

library(mortdb)
library(readr)
library(data.table)

master_folder <- "FILEPATH"
input_folder <- paste0(master_folder, "/inputs")
output_folder <- paste0(master_folder, "/outputs")

under_five_path <- paste0(output_folder, "/5q0_upload.csv")

under_five <- fread(paste0(output_folder, "/5q0_data.csv"))
under_five$adjustment <- 0
under_five$reference <- 0
under_five <- under_five[!is.na(nid),]
write_csv(under_five, under_five_path)

upload_results(filepath = under_five_path, model_name = "5q0", model_type = "data", run_id = new_run_id)
if(mark_best){
  update_status("5q0", "data", run_id = new_run_id, new_status = "best")
}
# Upload results to the db
rm(list=ls())

library(data.table)
library(argparse)
library(devtools)
library(methods)

library(mortdb, lib = "FILEPATH")


# # Get arguments
version_id <- gen_new_version(model_name = "5q0",
                              model_type = "estimate",
                              comment = "COMMENT HERE")

# Create a new version for the bias adjustment estimate
bias_adjustment_version_id <- gen_new_version(
  model_name = "5q0 bias adjustment",
  model_type = "estimate",
  comment = paste("Bias adjustment for 5q0 version", version_id))

# Upload bias adjustment file
bias_adjustment_upload_file_path <- "FILEPATH"
upload_results(filepath = bias_adjustment_upload_file_path,
               model_name = "5q0 bias adjustment",
               model_type = "estimate",
               run_id = bias_adjustment_version_id)

# Upload death number estimate file
upload_file_path <- "FILEPATH"
upload_results(filepath = upload_file_path,
               model_name = "5q0",
               model_type = "estimate",
               run_id = version_id)

pop_version <- get_proc_version(model_name = "population",
                                model_type = "estimate",
                                run_id = "best")

input_data_version <- get_proc_version(model_name = "5q0",
                                       model_type = "data",
                                       run_id = "best")

# Archive outlier set
archive_outlier_set("5q0", version_id)

# Generate parent-to-child relationship for the pop and 5q0 data
parent_list <- list("population estimate" = pop_version,
                    "5q0 data" = input_data_version,
                    "5q0 bias adjustment estimate" = bias_adjustment_version_id)
gen_parent_child(parent_runs = parent_list,
                 child_process = "5q0 estimate",
                 child_id = version_id)

# Mark as best
update_status(model_name = "5q0",
              model_type = "estimate",
              run_id = version_id,
              new_status = "best", assert_parents=F)

# Upload results to the db

rm(list=ls())

library(data.table)
library(argparse)
library(devtools)
library(methods)

library(mortdb, lib = "FILEPATH")


# # Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of 5q0')
parser$add_argument('--mark_best', type="character", required=TRUE,
                    help='Determine whether to mark run as best')
parser$add_argument('--data_5q0_version', type="character", required=TRUE,
                    help='Version of 5q0 data used for this run')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='the current gbd year')
args <- parser$parse_args()

version_id <- args$version_id
mark_best <- args$mark_best
data_5q0_version <- args$data_5q0_version
gbd_year <- args$gbd_year

# Create a new version for the bias adjustment estimate
bias_adjustment_version_id <- gen_new_version(
  model_name = "5q0 bias adjustment",
  model_type = "estimate",
  comment = paste("Bias adjustment for 5q0 version", version_id),
  gbd_year=gbd_year)


# Upload bias adjustment file
bias_adjustment_upload_file_path <- paste0("FILEPATH")
bias_adjustment_upload_file <- fread(bias_adjustment_upload_file_path)
bias_adjustment_upload_file[is.na(adjust_re_fe), adjust_re_fe:= 1]
bias_adjustment_upload_file=bias_adjustment_upload_file[!is.na(adjust_mean)&!is.na(variance),]
fwrite(bias_adjustment_upload_file,bias_adjustment_upload_file_path)

upload_results(filepath = bias_adjustment_upload_file_path,
               model_name = "5q0 bias adjustment",
               model_type = "estimate",
               run_id = bias_adjustment_version_id)
# Upload death number estimate file
upload_file_path <- paste0("FILEPATH")
upload_results(filepath = upload_file_path,
               model_name = "5q0",
               model_type = "estimate",
               run_id = version_id)

pop_version <- get_proc_version(model_name = "population",
                                model_type = "estimate",
                                run_id = "best",
                                gbd_year=gbd_year)

if (!data_5q0_version %in% c("best", "recent")) data_5q0_version <- as.numeric(data_5q0_version)
input_data_version <- get_proc_version(model_name = "5q0",
                                       model_type = "data",
                                       run_id = data_5q0_version,
                                       gbd_year=gbd_year)

# Archive outlier set
archive_outlier_set("5q0", version_id)

# Generate parent-to-child relationship for 5q0 bias adjustment estimate
parent_list <- list("5q0 bias adjustment estimate" = bias_adjustment_version_id)
gen_parent_child(parent_runs = parent_list,
                 child_process = "5q0 estimate",
                 child_id = version_id)

# Mark as best
if (mark_best){
  update_status(model_name = "5q0",
                model_type = "estimate",
                run_id = version_id,
                new_status = "best", assert_parents=F)
  
  update_status(model_name = "5q0 bias adjustment",
                model_type = "estimate",
                run_id = bias_adjustment_version_id,
                new_status = "best", assert_parents=F)
}

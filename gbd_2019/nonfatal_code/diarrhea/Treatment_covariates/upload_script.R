rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))

#upload_bundle_data(bundle_id,decomp_step,filepath)
#save_bundle_version(bundle_id,decomp_step)
#save_crosswalk_version(bundle_version_id,cw_filepath)

###prep model outputs###
##find old results
format_covariate <- function(run_id,filepath){
  old_results <- model_load(run_id, "raked")
  ##copy into new folder
  input_dir <- paste0(fix_path(filepath),"/")
  ##take mean/upper bound/lower bound of draw data
  for (loc_id in unique(old_results$location_id)) {
    #subset data by loc id
    out <- subset(old_results, location_id == loc_id)
    setnames(
      out,
      c("gpr_mean", "gpr_lower", "gpr_upper"),
      c("mean_value", "lower_value", "upper_value")
    )
    #write to file in new_dir
    write.csv(out, file = "FILEPATH",row.names=FALSE)
  }
}
##save as mean_value/upper_value/lower_value
#

input_dir <- fix_path("ADDRESS")
input_file_pattern <- "{location_id}.csv"
covariate_id <- 1103
description <- "Antibiotics Treatment for LRI"
decomp_step <- "step3"
gbd_round_id <- 7
format_covariate(run_id,input_dir)
save_results_covariate(input_dir=input_dir,
                       input_file_pattern=input_file_pattern,
                       covariate_id=covariate_id,
                       description=description,
                       decomp_step=decomp_step,
                       mark_best=TRUE,
                       gbd_round_id=gbd_round_id)

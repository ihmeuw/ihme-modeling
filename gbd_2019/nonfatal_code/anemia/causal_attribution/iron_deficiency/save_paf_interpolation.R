rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "/home/j/"
  h <- "/homes/USERNAME/"
}
#################

source("FILEPATH")



save_results_risk(input_dir = "FILEPATH",
                 input_file_pattern = "FILEPATH",
                 modelable_entity_id = 95,
                 description = "Saving PAF results Interpolated for all years - 1/23/20",
                 risk_type = 'paf',
                 year_id = c(1990:2019),
                 gbd_round_id = 6,
                 decomp_step = 'step4',
                 mark_best=TRUE)

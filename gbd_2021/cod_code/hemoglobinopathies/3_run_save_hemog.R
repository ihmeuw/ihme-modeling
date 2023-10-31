
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

arg <- commandArgs(trailingOnly = TRUE)
cause <- arg[1]
sex_id <- arg[2]
gbd_round <- arg[3]
decomp_step <- arg[4]
out_dir <- arg[5]
mark_best_flag <- arg[6]
save_description <- arg[7]
input_file_pattern <- arg[8]
scaled_results <- arg[9]

#############################################

source("FILEPATH")

#############################################

print(input_file_pattern)



if (scaled_results == TRUE){
  save_results_cod(cause_id = cause,
                   input_dir = paste0(out_dir, "scaled_files/", cause, "/"),
                   input_file_pattern = input_file_pattern,
                   description = save_description,
                   year_id = c(1980:2022),
                   sex_id = sex_id,
                   metric_id = 3,
                   gbd_round_id = gbd_round,
                   decomp_step = decomp_step, mark_best = mark_best_flag)
}

if (scaled_results == FALSE){
  save_results_cod(cause_id = cause,
                   input_dir = paste0(out_dir, "interp_files/", cause, "/"),
                   input_file_pattern = input_file_pattern,
                   description = save_description,
                   year_id = c(1980:2022),
                   sex_id = sex_id,
                   metric_id = 3,
                   gbd_round_id = gbd_round,
                   decomp_step = decomp_step, mark_best = mark_best_flag)
}



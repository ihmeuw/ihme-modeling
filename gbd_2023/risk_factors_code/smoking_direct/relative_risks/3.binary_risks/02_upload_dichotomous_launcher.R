rm(list = ls())
source(FILEPATH)

results_folder <- FILEPATH
ARCHIVE <- FILEPATH
out_dir <- FILEPATH

pair_info <- list(
  smoking_hip_fracture = list(
    rei_id = "99",
    cause_id = "878",
    model_path = pasteo(FILEPATH, "fractures_cov_finder_no_sex_0.9.pkl")
  ),
  smoking_non_hip_fracture = list(
    rei_id = "99",
    cause_id = "923",
    model_path = paste0(FILEPATH, "fractures_cov_finder_no_sex_0.9.pkl")
  )
)

for (pair in names(pair_info)) {
  print(paste0("upload pair=", pair))
  results_folder <- file.path(ARCHIVE, pair)
  if (!dir.exists(results_folder)) {
    dir.create(results_folder)
  }
  do.call(upload_results, c(pair_info[[pair]], list(results_folder = results_folder)))
}

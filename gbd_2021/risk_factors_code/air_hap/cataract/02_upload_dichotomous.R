#
# 02_upload_dichotomous.R
#
#
WORK_DIR <- "FILEPATH"
setwd(WORK_DIR)

rm(list = ls())
source("./upload_dichotomous.R")

results_folder <- "FILEPATH"

pair_info <- list(
  hap_cataract = list(
    rei_id = 87,
    cause_id = 671,
    model_path = file.path("FILEPATH/model.pkl")
  ))

for (pair in names(pair_info)) {
  print(paste0("upload pair=", pair))
  results_folder <- file.path(ARCHIVE, pair)
  if (!dir.exists(results_folder)) {
    dir.create(results_folder)
  }
  do.call(upload_results, c(pair_info[[pair]], list(results_folder = results_folder)))
}

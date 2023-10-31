################################################################################
##                                                                            ##
## Description: This script sbatches the fertility run all                    ##
## Edit the run all script as needed for resumes etc.                         ##
##                                                                            ##
################################################################################

library(data.table)
library(mortcore)

# Load config
username <- Sys.getenv("USER")
code_dir <- fs::path(
  "FILEPATH"
  )

config <- config::get(
  file = fs::path(
    "FILEPATH"
  ),
  use_parent = FALSE
)

# Update shell path
config$shell_path_r <- gsub("PYTHONPATH= ", "", config$shell_path_r)
config$run_all_image_path <- "FILEPATH"

# launch pipeline
Sys.setenv("R_LIBS" = "")

mortcore:::sbatch(
  jobname = "Fert_pipeline",
  code = fs::path(code_dir, "00_fertility_run_all.R"),
  mem = 4,
  threads = 1,
  partition = "all",
  runtime = "48:00:00",
  shell = config$shell_path_r,
  pass_shell = list(i = config$run_all_image_path)
)

###########################################
## purpose: Mark Best and add Metadata   ##
###########################################

library(data.table)
library('ihme.covid', lib.loc = 'FILEPATH')
require(yaml)

R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

output_root <- "FILEPATH"
save_dir <- ihme.covid::get_latest_output_dir(root = output_root)
output_version <- basename(save_dir)

if (interactive()){
  mark_latest <- TRUE
  mark_best <- FALSE
  note <- "" 
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--mark_latest", type="character", help="If TRUE, will update the 'latest' symlink")
  parser$add_argument("--mark_best", type="character", help="If TRUE, will update the 'best' symlink")
  parser$add_argument("--note", type="character", help="Note item in mandates metadata.yaml; leave blank unless there were meaningful changes")
  
  args <- parser$parse_args(get_args())
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  # Update variables
  mark_latest = as.logical(mark_latest)
  mark_best = as.logical(mark_best)
  if (note == "jobmon") {
    note <- ""
  }

  print("Finalized arguments are:")
  for (arg in names(args)){
    print(paste0(arg, ": ", get(arg)))
  }
}

# get etl version
tmp_data <- fread(paste0(output_root, output_version, "/all_data_prepped.csv"))
etl_version <- unique(tmp_data$etl_version)

time <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")

working_dir <- getwd()
setwd(output_root)

if (mark_best) {
  # mark best
  system("unlink best")
  system(paste0("ln -s ", output_version, " best"))
}

if (mark_latest) {
  # mark latest
  system("unlink latest")
  system(paste0("ln -s ", output_version, " latest"))
  
}

# write metadata
filename <- file.path(output_root, output_version, "metadata.yaml")

write_yaml(
  data.frame(
    output_path = file.path(output_root, output_version),
    versioned_on = time,
    sdc_data_version = etl_version,
    notes = note
  ),
  file = filename
)

# Reset working directory
setwd(working_dir)

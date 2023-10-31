###########################################
## purpose: Mark Best and add Metadata   ##
###########################################

library(data.table)
library('ihme.covid', lib.loc = 'FILEPATH')
require(yaml)

R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Determine directories
output_root <- "FILEPATH"
output_version <- basename(ihme.covid::get_latest_output_dir(root = output_root)) #mobility forecast version
gpr_version <- basename(ihme.covid::get_latest_output_dir(root = paste0(output_root, "gpr_outputs/"))) #gpr version
mobility_data_snapshot_date <- as.character(as.Date(gsub('_', '-', substr(gpr_version, 1, 10))) - 1) #mobility data date
sd_lift_pred_version <- basename(ihme.covid::get_latest_output_dir(root = paste0(output_root, "mandates/"))) #mandate version
social_distancing_snapshot_date <- as.character(as.Date(gsub('_', '-', substr(sd_lift_pred_version, 1, 10))) - 1) #mandate data date

if (interactive()){
  mark_best <- FALSE
  mark_latest <- TRUE
  note <- "" # LEAVE BLANK UNLESS THERE WERE MEANINGFUL CHANGES
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--note", type="character", help="Note item in forecast metadata.yaml; leave blank unless there were meaningful changes")
  parser$add_argument("--mark_latest", type="character", help="If TRUE, will update the 'latest' symlink")
  parser$add_argument("--mark_best", type="character", help="If TRUE, will update the 'best' symlink")

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

setwd(output_root)
time <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")

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
    sdc_data_date = social_distancing_snapshot_date,
    mobility_data_date = mobility_data_snapshot_date,
    mobilty_path = file.path(output_root, "gpr_outputs", gpr_version),
    sd_lift_path = file.path(output_root, "mandates", sd_lift_pred_version),
    notes = note
  ),
  file = filename
)


# update the gpr versions csv
if (mark_best) {
  gpr_versions <- fread('FILEPATH/gpr_versions.csv')
  new <- data.table(gpr_ids=gpr_version)
  gpr_versions <- rbind(gpr_versions, new)
  fwrite(gpr_versions, 'FILEPATH/gpr_versions.csv')
}

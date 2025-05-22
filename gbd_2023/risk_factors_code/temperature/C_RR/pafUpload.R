source("FILEPATH/save_results_risk.R")
source("FILEPATH/get_demographics.R")

arg         <- commandArgs(trailingOnly = T)
risk        <- arg[1]
paf_version <- arg[2]
release     <- arg[3]
best        <- as.logical(arg[4])


out_dir <- paste0("/FILEPATH/FILEPATH_", release, "/FILEPATH/FILEPATH/", paf_version)
in_dir  <- paste0(out_dir, "/", risk)


demog <- get_demographics('epi', release_id = release)
years <- min(demog$year_id):max(demog$year_id)


if (risk == "heat") {
  meid <- 20263
} else if (risk == "cold") {
  meid <- 20262
} else {
  warning("Risk must be either heat or cold")
}

message(risk)
message(paf_version)
message(release)
message(best)
message(meid)
message(years)
message(in_dir)

save_results_risk(input_dir = in_dir,
                  input_file_pattern = "{location_id}_{year_id}.csv",
                  modelable_entity_id = meid,
                  year_id = years,
                  n_draws = 500,
                  description = paste0(risk, " PAFs: ", paf_version),
                  risk_type = "paf",
                  measure = 4,
                  mark_best = best,
                  release_id = release)
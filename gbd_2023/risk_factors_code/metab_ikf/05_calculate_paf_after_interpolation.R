### Kidney dysfunction
# launch PAFs

rm(list = ls())
library("ihme.cc.paf.calculator", lib.loc = "FILEPATH")

# launch pafs for annual estimation years
model_version_id <- launch_paf_calculator(
  rei_id = 341,
  cluster_proj = "proj_birds",
  release_id = 16,
  year_id = c(1990:2024),
  n_draws = 250) 
message("My PAF model version is ", model_version_id) # put as vid below

# scatters
source("FILEPATH/paf_scatter.R")
vid <- 879759

paf_scatter(
  rei_id = 341, year_id = 1990, loc_level = 4, version_id = vid, release_id = 16, measure_id = 3,
  file_path = paste0("FILEPATH", vid, "/kd_scatter_1990_ylds.pdf")
)

paf_scatter(
  rei_id = 341, year_id = 2000, loc_level = 4, version_id = vid, release_id = 16, measure_id = 3,
  file_path = paste0("FILEPATH", vid, "/kd_scatter_2000_ylds.pdf")
)

paf_scatter(
  rei_id = 341, year_id = 2010, loc_level = 4, version_id = vid, release_id = 16, measure_id = 3,
  file_path = paste0("FILEPATH", vid, "/kd_scatter_2010_ylds.pdf")
)

paf_scatter(
  rei_id = 341, year_id = 2021, loc_level = 4, version_id = vid, release_id = 16, measure_id = 3,
  file_path = paste0("FILEPATH", vid, "/kd_scatter_2021_ylds.pdf")
)

paf_scatter(
  rei_id = 341, year_id = 1990, loc_level = 4, version_id = vid, release_id = 16, measure_id = 4,
  file_path = paste0("FILEPATH", vid, "/kd_scatter_1990_ylls.pdf")
)

paf_scatter(
  rei_id = 341, year_id = 2000, loc_level = 4, version_id = vid, release_id = 16, measure_id = 4,
  file_path = paste0("FILEPATH", vid, "/kd_scatter_2000_ylls.pdf")
)

paf_scatter(
  rei_id = 341, year_id = 2010, loc_level = 4, version_id = vid, release_id = 16, measure_id = 4,
  file_path = paste0("FILEPATH", vid, "/kd_scatter_2010_ylls.pdf")
)

paf_scatter(
  rei_id = 341, year_id = 2021, loc_level = 4, version_id = vid, release_id = 16, measure_id = 4,
  file_path = paste0("FILEPATH", vid, "/kd_scatter_2021_ylls.pdf")
)

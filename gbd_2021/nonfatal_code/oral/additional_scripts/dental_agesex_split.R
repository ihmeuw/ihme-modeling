######Run script for dental non-fatal script




library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')


repo_dir <- "FILEPATH"


#Set arguments
args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
measure_name <- args[2]
gbd_round_id <- args[3]
decomp_step <- args[4]
save_dir <- args[5]
out_dir <- args[6]

#Importing age map/location map/sex map for crosswalks
age_map <- fread("FILEPATH")
age_map$order <- NULL
#age_map$age_end[2] <- 0.077

locs <- get_location_metadata(35, gbd_round_id = 7, decomp_step = 'iterative')
locs <- locs[, c('location_id', 'super_region_id')]

sex_names <- fread("FILEPATH")

map <- fread("FILEPATH")
me_id <- map[bundle_id == bun_id, me_id]

version_map <- read.xlsx("FILEPATH")
bun_version <- version_map$bundle_version_id[version_map$bundle_id == bun_id]


if (measure_name == 'prevalence'){
  agesex_file <- agesex_split(bun_id = bun_id, bun_version = bun_version, 
                              gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                              measure_name = measure_name, save_dir = save_dir)
} else{
  agesex_file <- agesex_split_linear(bun_id = bun_id, bun_version = bun_version, 
                              gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                              measure_name = measure_name, save_dir = save_dir)
}

break

new_bundle_version <- create_bundle_version(input_data_path = agesex_file, bundle_id = bun_id, 
                                            gbd_round_id = gbd_round_id, decomp_step = decomp_step)



if (bun_id == 212){
  create_crosswalk(bundle_id = bun_id, bundle_version = new_bundle_version, measure = measure_name,
                   trim = 'trim', trim_percent = 0.1, out_dir = out_dir, cv = 'cv_dx_chemical')
}


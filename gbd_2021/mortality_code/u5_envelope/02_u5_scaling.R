# scale u5_envelope subnationals to national level for both deaths and person years

library(data.table)
library(mortdb, lib.loc = "FILEPATH")
library(mortcore, lib.loc = "FILEPATH")
library(assertable)
library(argparse)

# parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type = "integer", required = TRUE,
                    help = 'The version_id for this run')
parser$add_argument('--location_id', type = "integer", required = TRUE,
                    help = 'Country id to run')
parser$add_argument('--gbd_year', type = "integer", required = TRUE,
                    help = "GBD year")

args <- parser$parse_args()
version_id <- args$version_id
parent_location <- args$location_id
gbd_year <- args$gbd_year

version_dir <- paste0("FILEPATH")
data_ids <- c("location_id", "year", "sim", "sex")
output_dir <- paste0("FILEPATH")
dir.create(paste0("FILEPATH"))


# find all children with the parent in the path-to-top parent
locations <- get_locations(gbd_year = gbd_year, level = "all")

child_locations <- unique(locations[
  grepl(paste0(",", parent_location, ","), path_to_top_parent) & location_id != parent_location,
  location_id
])

run_location_ids <- c(parent_location, child_locations)
scale_locations <- locations[location_id %in% run_location_ids, c("location_id", "ihme_loc_id")]

## import the u5 envelope draw files

input_draws <- assertable::import_files(
  filenames = paste0("FILEPATH"),
  folder = version_dir,
  multicore = TRUE,
  mc.cores = 10,
  use.names = TRUE
)

input_draws <- merge(scale_locations, input_draws, by = "ihme_loc_id")
input_draws[, "ihme_loc_id" := NULL]

# convert the dataset to long form because scale_results can only scale one value variable
input_draw_long <- melt(data = input_draws, id.vars = data_ids)

# scale the envelope from subnational to national
scaled_data <- mortcore::scale_results(
  input_draw_long,
  id_vars = c(data_ids, "variable"),
  value_var = "value",
  location_set_id = 21,
  exception_gbr_1981 = TRUE,
  exclude_parent = "CHN",
  gbd_year = gbd_year
)

scaled_data <- merge(scaled_data, scale_locations, by = "location_id")
scaled_data[, "location_id" := NULL]

# convert back the dataset to the wide form
scaled_data_wide <- dcast(scaled_data, ... ~ variable, value.var = "value")

# output files
for (ihme_id in unique(scaled_data_wide$ihme_loc_id)) {

  scaled_data_loc <- scaled_data_wide[ihme_loc_id == ihme_id]
  loc_id <- scale_locations[ihme_loc_id == ihme_id, "location_id"]

  readr::write_csv(scaled_data_loc, paste0("FILEPATH"))
  readr::write_csv(scaled_data_loc, paste0("FILEPATH"))

}

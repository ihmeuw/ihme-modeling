#### Aggregate up HIV adjustments and shock numbers to non-lowest levels

library(pacman)
pacman::p_load(assertable, data.table, argparse, readr)
library(mortcore, lib=FILEPATH)
library(mortdb, lib=FILEPATH)



parser <- argparse::ArgumentParser()
parser$add_argument('--shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The with shock death number estimate version for this run')
parser$add_argument('--no_shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The no shock death number estimate version for this run')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD Year')
parser$add_argument('--end_year', type="integer", required=TRUE,
                    help = 'last year of estimation')
parser$add_argument('--location_id', type="integer", required=TRUE,
                    help='National aggregate level location_id to run this job for')

args <- parser$parse_args()
shock_death_number_estimate_version <- args$shock_death_number_estimate_version
no_shock_version <- args$no_shock_death_number_estimate_version
gbd_year <- args$gbd_year
current_loc_id <- args$location_id
end_year <- args$end_year

# Setup ------------------------------------------------------------------------

# Get location map

input_dir <- FILEPATH

full_loc_map <- fread(FILEPATH)

current_ihme <- full_loc_map[location_id==current_loc_id, ihme_loc_id]

loc_map <- full_loc_map[ihme_loc_id %like% current_ihme]
lowest_locs <- fread(FILEPATH)[ihme_loc_id %like% current_ihme]

# Separate aggregates from lowest level locations
agg_locs <- setdiff(loc_map$location_id, lowest_locs$location_id)
lowest_loc_ids <- lowest_locs$location_id

# check that there are no mistakes in splitting
missing_locs <- setdiff(loc_map$location_id, c(lowest_loc_ids, agg_locs))
if (length(missing_locs) > 0) stop(paste0("Missing location ids ", missing_locs))


# Set ID variables
id_vars <- c('location_id', 'year_id', 'sex_id', 'age_group_id')
draw_ids <- c(id_vars, 'draw')


# Aggregation ------------------------------------------------

flt_dir <- FILEPATH

#1) Aggregate HIV

# bring in HIV adjustment files
hiv_adjust_folder <- FILEPATH
hiv_filenames <- FILEPATH

hiv_adjustment <- assertable::import_files(hiv_filenames,
                                           folder = hiv_adjust_folder,
                                           FUN = fread,
                                           multicore = 2)

# Aggregate up to non-lowest locations
aggregated_hiv <- mortcore::agg_results(data=hiv_adjustment,
                                        value_vars = c('implied_lt_hiv', 'with_hiv_mx', 'no_hiv_mx', 'mx_spec_hiv'),
                                        id_vars = draw_ids,
                                        end_agg_level = 3,
                                        tree_only = current_ihme,
                                        gbd_year = gbd_year)

# ensure aggregation worked
adjustment_ids <- list(
  location_id = loc_map$location_id,
  year_id = 1950:end_year,
  sex_id=1:2,
  age_group_id = c(1,23,24,40),
  draw = 0:999
)

assertable::assert_ids(aggregated_hiv, adjustment_ids)


# For each non-lowest location, split out the dataframe and write
output_dir <- FILEPATH
for (loc in agg_locs) {

  temp <- aggregated_hiv[location_id == loc]
  readr::write_csv(temp, FILEPATH)

}


# 2. Aggregate shock deaths
# import files
shocks_folder <- FILEPATH
shock_filenames <- FILEPATH

shocks <- assertable::import_files(shock_filenames,
                                           folder = shocks_folder,
                                           FUN = fread,
                                           multicore = 2)

# Aggregate up to non-lowest locations
aggregated_shocks <- mortcore::agg_results(data=shocks,
                                        value_vars = c('deaths'),
                                        id_vars = draw_ids,
                                        end_agg_level = 3,
                                        tree_only = current_ihme,
                                        gbd_year = gbd_year)

# ensure aggregation worked
shocks_ids <- list(
  location_id = loc_map$location_id,
  year_id = 1950:end_year,
  sex_id=1:2,
  age_group_id = unique(shocks$age_group_id),
  draw = 0:999
)

assertable::assert_ids(aggregated_shocks, shocks_ids)

# For each non-lowest location, split out the dataframe and write
output_dir <- FILEPATH
for (loc in agg_locs) {

  temp <- aggregated_shocks[location_id == loc]
  readr::write_csv(temp, FILEPATH)

}

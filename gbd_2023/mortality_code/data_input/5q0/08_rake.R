rm(list=ls())

library(data.table)
library(argparse)
library(devtools)
library(methods)
library(readr)
library(mortdb, lib.loc = "FILEPATH")
library(mortcore, lib.loc = "FILEPATH")

# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of 5q0')
parser$add_argument('--gbd_round_id', type="integer", required=TRUE,
                    help='The gbd_round_id for this run of 5q0')
parser$add_argument('--start_year', type="integer", required=TRUE,
                    help='The starting year for this run of 5q0')
parser$add_argument('--end_year', type="integer", required=TRUE,
                    help='The ending year for this run of 5q0')
parser$add_argument('--raking_id', type="integer", required=TRUE,
                    help='The raking model for this ')
parser$add_argument('--parent_id', type="integer", required=TRUE,
                    help='National level location to rake to')
parser$add_argument('--child_ids', type="integer", required=TRUE, nargs='+',
                    help='The subnational locations to rake')
parser$add_argument('--code_dir', type="character", required=TRUE,
                    help='Directory where child-mortality code is cloned')

args <- parser$parse_args()
list2env(args, .GlobalEnv)

gbd_year=get_gbd_year(gbd_round=gbd_round_id)

yml_dir <- gsub("child-mortality", "", code_dir)
yml <- readLines(paste0("FILEPATH"))
yml <- yml[grepl("release_id", yml)]
release_id <- as.numeric(gsub("\\D", "", yml))

index_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "sim")

# Set core directories
output_dir <- paste0("FILEPATH")
raking_output_dir <- paste0("FILEPATH")
dir.create(paste0("FILEPATH"), showWarnings = FALSE)
dir.create(raking_output_dir, showWarnings = FALSE)

# Load the GBD specific libraries
source(paste0("FILEPATH"))

# Get data
print(paste(start_year, end_year, output_dir))
dc = DataContainer$new(release_id = release_id, start_year = start_year,
                       end_year = end_year, output_dir = output_dir, version_id = version_id)
input_draws = list()
i <- 1
for (location_id in c(parent_id, child_ids)) {
    file_path <- paste0("FILEPATH")
    if (file.exists(file_path)) {
      print(file_path)
      temp <- fread(file_path)
      temp$location_id <- location_id
      input_draws[[i]] <- temp
      i <- i + 1
    }
}
input_draws = do.call(rbind, input_draws)
input_draws <- input_draws[location_id %in% c(parent_id, child_ids)]
input_draws$year_id <- input_draws$year - 0.5
input_draws$sex_id <- 3
input_draws$age_group_id <- 1

# Output the draw and drop from child_ids
if (raking_id==3) {

  udmurt <- input_draws[location_id==44951, .(location_id, year_id, viz_year = year, sex_id, age_group_id, sim, mort)]
  readr::write_csv(udmurt, paste0("FILEPATH"))

  input_draws <- input_draws[location_id != 44951]

}

readr::write_csv(input_draws, paste0("FILEPATH"))

# Make population for Telangana and Andhra Pradesh
population_data <- dc$get('population')
old_ap_data <- population_data[location_id %in% c(4841, 4871), ]
old_ap_data$location_id <- 44849
old_ap_data <- old_ap_data[, .(pop = sum(pop)),
                           by = c('location_id', 'year_id', 'sex_id',
                                  'age_group_id')]
old_ap_data$ihme_loc_id <- "IND_44849"
population_data <- rbind(population_data, old_ap_data, use.names=T, fill = T)
population_data <- population_data[, c("location_id", "year_id", "sex_id",
                                       "age_group_id", "pop")]

# Merge population onto data
input_draws  <- merge(input_draws, population_data,
                      by = c("location_id", "year_id", "sex_id", "age_group_id"))

## Convert from qx to mx and then to death space
input_draws[, mort := log(1-mort)/-5]
input_draws[, mort := mort * pop]

# Scale everything except South Africa
input_draws <- input_draws[, c("location_id", "year_id", "sex_id",
                               "age_group_id", "sim", "mort")]
if (parent_id == 196) {
  # Aggregate ZAF, rather than scaling
  input_draws <- input_draws[location_id != parent_id]
  scaled_data <- agg_results(input_draws,
                             id_vars = index_cols,
                             value_vars = "mort",
                             end_agg_level = 3,
                             loc_scalars = F,
                             tree_only = "ZAF",
                             gbd_year=gbd_year)
} else if(parent_id==62) {

  merge_ids <- c('year_id', 'sim')

  # Scaling basics:
  rus <- input_draws[location_id==62,]
  scalars <- input_draws[location_id != 62, .(mort_subnat = sum(mort)),
                         by=merge_ids]
  scalars <- merge(scalars, rus[,.SD, .SDcols=c(merge_ids, 'mort')],
                   by=merge_ids)
  scalars[, scaling_factor := mort/mort_subnat]
  scalars[, c('mort', 'mort_subnat') := NULL]


  subnats <- merge(input_draws[location_id != 62], scalars, by=merge_ids)
  subnats[, mort := mort * scaling_factor]
  subnats[, scaling_factor := NULL]

  scaled_data <- rbind(rus, subnats, use.names=T)


} else {

  if(parent_id %in% c(95)) {

    loc_map <- get_locations(gbd_year = gbd_year)
    parent_ihme <- copy(parent_id)
    parent_ihme <- loc_map[location_id == parent_ihme, ihme_loc_id]

    agg_21 <- agg_results(
      data = input_draws[
        year_id >= 2021 &
          (location_id %in% loc_map[ihme_loc_id %like% parent_ihme & level == 4, location_id])
      ],
      id_vars = index_cols,
      value_vars = "mort",
      location_set_id = 82,
      gbd_year = gbd_year,
      start_agg_level = 4,
      end_agg_level = 3,
      tree_only = parent_ihme
    )

    setnames(agg_21, "mort", "agg_mort")
    input_draws <- merge(
      input_draws,
      agg_21,
      by = c("location_id", "year_id", "sex_id", "age_group_id", "sim"),
      all = TRUE
    )

    input_draws[!is.na(agg_mort), mort := agg_mort]
    input_draws[, agg_mort := NULL]

  }

  ## Scale results with GBR 1981 exception
  scaled_data <- scale_results(input_draws, id_vars = index_cols,
                               value_var = "mort", location_set_id = 82,
                               exception_gbr_1981 = T,
                               exclude_parent = "CHN",
                               gbd_year=gbd_year)

}

# Merge population onto data
scaled_data <- scaled_data[, c("location_id", "year_id", "sex_id",
                               "age_group_id", "sim", "mort")]
scaled_data  <- merge(scaled_data, population_data,
                      by = c("location_id", "year_id", "sex_id", "age_group_id"))

# Convert death space to mx to qx
scaled_data[, mort := mort / pop]
scaled_data[, mort := 1 - exp(-5 * mort)]

# Reformat
scaled_data$viz_year <- scaled_data$year_id + 0.5
scaled_data <- scaled_data[, c("location_id", "year_id", "viz_year", "sex_id",
                               "age_group_id", "sim", "mort")]

# Save
for (l in unique(scaled_data$location_id)) {
  temp <- scaled_data[location_id == l]
  fwrite(temp, paste0("FILEPATH"), row.names=F)
}

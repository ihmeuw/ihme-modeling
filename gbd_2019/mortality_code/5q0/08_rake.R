rm(list=ls())

library(data.table)
library(argparse)
library(devtools)
library(methods)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

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
                    help='The ending year for this run of 5q0')
parser$add_argument('--child_ids', type="integer", required=TRUE, nargs='+',
                    help='The ending year for this run of 5q0')

args <- parser$parse_args()
version_id <- args$version_id
gbd_round_id <- args$gbd_round_id
start_year <- args$start_year
end_year <- args$end_year
raking_id <- args$raking_id
parent_id <- args$parent_id
child_ids <- args$child_ids
gbd_year=get_gbd_year(gbd_round=gbd_round_id)

index_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "sim")

# Set core directories
root <- "FILEPATH"
username <- Sys.getenv("USER")
code_dir <- "FILEPATH"
output_dir <- "FILEPATH"
raking_output_dir <- "FILEPATH"
dir.create(paste0(output_dir, "/raking"), showWarnings = FALSE)
dir.create(raking_output_dir, showWarnings = FALSE)

# Load the GBD specific libraries
source(paste0(code_dir, "/data_container.R"))

# Get data
print(paste(gbd_round_id, start_year, end_year, output_dir))
dc = DataContainer$new(gbd_round_id = gbd_round_id, start_year = start_year,
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
    i = i + 1
  }
}
input_draws = do.call(rbind, input_draws)
input_draws <- input_draws[location_id %in% c(parent_id, child_ids)]
input_draws$year_id <- input_draws$year - 0.5
input_draws$sex_id <- 3
input_draws$age_group_id <- 1
fwrite(input_draws, paste0(output_dir, "/raking/", raking_id, "/input.csv"),
       row.names = FALSE)

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

# Scale everything except South Africa (which we will aggregate)
source(paste0(root, "FILEPATH/get_locations.r"))
input_draws <- input_draws[, c("location_id", "year_id", "sex_id",
                               "age_group_id", "sim", "mort")]
if (parent_id == 196) {
  # Aggregate ZAF, rather than scaling
  input_draws <- input_draws[location_id != parent_id]
  scaled_data <- agg_results(input_draws,
                             id_vars = index_cols,
                             value_var = "mort",
                             end_agg_level = 3,
                             loc_scalars = F,
                             tree_only = "ZAF",
                             gbd_year=gbd_year)
} else {
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
  fwrite(temp, paste0(output_dir, "/raking/", raking_id, "/", l, ".csv"), row.names=F)
}

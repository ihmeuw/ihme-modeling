################################################################################
## DESCRIPTION Rake subnational estimates from GPR to national and copy
# national gpr to output file
## INPUTS GPR simulations for subnationals and births dataset
## OUTPUT final summary files for all locations
## Steps
## - Read in configuration files
## - Load inputs
##   -	get the start and end location aggregation level
## - Raking prep
##   - merge gpr and birth data
##   - convert srb from percentage to count
##   - Telangana and Andhra Pradesh require additional steps to map to National India
## - Rake
##   - merge raked data with births
##   - convert srb back to percentage
## - Save outputs into `ihme_loc_id` specific files
##   - calculate mean and 95% CI for raked locations
##   - read into output directory
################################################################################

rm(list = ls())

# load libraries
library(assertable)
library(data.table)
library(mortcore)
library(mortdb)

## A. Read in configuration files ----------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  help = "base directory for this version run"
)
parser$add_argument(
  "--location_id",
  type = "character",
  required = !interactive(),
  help = "level 3 location code for raking"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# initial inputs
if(interactive()){
  version_id <- "Run id"
  main_dir <- paste0("FILEPATH/", version_id)
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
  ploc <- "Input location"
}else{
  ploc <- location_id
}

config <- config::get(
  file = fs::path(main_dir, "/srb_detailed.yml"), use_parent = FALSE)
list2env(config, .GlobalEnv)

# B. Load inputs --------------------------------------------------------------

# get loc map
loc_map <- fread(fs::path(input_dir, "loc_map.csv"))
loc_map <- loc_map[, parent_id := substr(ihme_loc_id, 1, 3)]

# CHN revise
loc_map[
  grepl("CHN", ihme_loc_id), 
  parent_id := ifelse(level > 4, "CHN_44533", ihme_loc_id)
]

# get all child locations
child_ids <- loc_map[parent_id == ploc, ihme_loc_id]

# get start and end location aggregation levels
ploc_level <- loc_map[ihme_loc_id %in% c(ploc, child_ids), level]
ploc_start <- min(ploc_level)
ploc_end <- max(ploc_level)

# print statements to review in output
print(ploc_start)
print(ploc_end)

# get births
births <- fread(paste0(input_dir, "births.csv"))

# load parent and child draws
file_paths <- Sys.glob(fs::path(
  gpr_dir, 
  paste0("gpr_", c(ploc, child_ids), "_sim"),
  ext = "txt"
)) |> unique()
input_draws <- import_files(filenames = file_paths, FUN = fread)

## make sure all tree data is included
obs_ids <- input_draws[, ihme_loc_id] |> unique()
stopifnot(length(setdiff(c(ploc, child_ids), obs_ids)) == 0)

input_draws[, year_id := floor(year)]

# C. Raking Prep ----------------------------------------------------------------

# merge births
input_draws <- merge(
  input_draws,
  births, 
  by = c("ihme_loc_id", "year_id"),
  all.x = TRUE
)

# check merge
assert_values(input_draws, "births")

# convert val to count space (i.e. number of male births)
input_draws[, val := val * births]

# special case for Telangana and Andhra Pradesh: aggregate to get combined srb for processes which require it

if(ploc == "IND"){
  births_old_ap <- births[location_id %in% c(4841, 4871)]
  births_old_ap[, `:=`(location_id = 44849, ihme_loc_id = "IND_44849")]
  births_old_ap[, births := sum(births), by = c("location_id", "year_id")]
  births_old_ap <- unique(births_old_ap)
  births <- rbind(births_old_ap, births)

  input_draws_old_ap <- input_draws[location_id %in% c(4841, 4871)]
  input_draws_old_ap[, `:=`(location_id = 44849, ihme_loc_id = "IND_44849")]
  input_draws_old_ap[
    , 
    c("births", "val") := lapply(.SD, sum),
    .SDcols = c("births", "val"), 
    by = c("location_id", "year_id", "sim")
  ]
  
  input_draws_old_ap <- unique(input_draws_old_ap)
  input_draws <- rbind(input_draws_old_ap, input_draws)
}

# add and subset to required cols
index_cols <- c(
  "location_id",
  "year_id",
  "sex_id",
  "age_group_id",
  "sim"
)

input_draws[, `:=` (age_group_id = 164, sex_id = 3)]
input_draws <- input_draws[, c(index_cols, "val"), with = FALSE]

# D. Rake -----------------------------------------------------------------------

# decision point, if getting old Andhra Pradesh and Telengana outputs, use GBD 2021 location hierarchy
# otherwise, use 2023

gbd_year <- ifelse(ploc == "IND", 2021, 2023) 

scaled_data <- scale_results(
  input_draws,
  id_vars = index_cols,
  value_var = "val",
  location_set_id = 82,
  gbd_year = gbd_year,
  parent_start = ploc_start,
  parent_end = ploc_end,
  exclude_parent = "CHN"
)

# merge births onto data
scaled_data  <- merge(
  scaled_data,
  births, 
  by = c("location_id", "year_id"),
  all.x = TRUE
)

# check merge
assert_values(scaled_data, "births")

# convert back to proportion male
scaled_data[, val := val/births]
scaled_data[, births := NULL]

# E. Save outputs into `ihme_loc_id` specific files --------------------------

# calculate mean, lower, upper for raked locations (using 0.025 and 0.975)
for(loc in unique(scaled_data$location_id)){
  print(loc)
  temp <- scaled_data[location_id == loc]
  readr::write_csv(temp, paste0(draws_dir, "draws_", loc, ".csv"))

  temp <- summarize_data(
    data = temp,
    outcome_var = "val",
    metrics = c("mean", "lower", "upper"),
    id_vars = gsub("sim", "ihme_loc_id", index_cols)
  )
  readr::write_csv(temp, paste0(output_dir, "prop_male_", loc, ".csv"))
}


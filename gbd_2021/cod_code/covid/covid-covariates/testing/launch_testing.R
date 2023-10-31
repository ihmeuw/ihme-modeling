#=====================================================================#
# Description: Testing ########################################
# The purpose of this code is to take smoothed testing per capita 
# from prep_data.R and project testing into the future and to fill
# testing for missing locations in forecast_test_pc.R.
# To forecast test per capita, the code linearly extends the 
# trend in testing rates. It caps testing rate at a frontier
# identified using SDI from back in summer/fall 2020. 
# For locations without any testing data and non-subnationals,
# it uses SDI to determine an average rate of change ARC to 
# make projections into the future.
# Written by INDIVIDUAL_NAME INDIVIDUAL_NAME, maintained and adapted by INDIVIDUAL_NAME INDIVIDUAL_NAME
#
# Requires: current_version
# File created in "FILEPATH/prep_data.R"
# 
# Developed by ssbyrne & phamtom from 2022-02-07
#   INDIVIDUAL_NAME INDIVIDUAL_NAME EMAIL_ADDRESS
#   INDIVIDUAL_NAME INDIVIDUAL_NAME EMAIL_ADDRESS
# 
# This script expects the following: 
# - Your repo lives here: FILEPATH
#=====================================================================#


# Use:      Testing Covariate Pipeline Starts Here 
# 
# Purpose:  1. Builds environment for Testing pipeline 
#           2. Starts pipeline functions
#           3. Splices and hot fixes
#           4. Write out data files (daily per-capita testing)
#           5. Plot output diagnostics (scatters and smoothed curve)


# Setup -------------

# clear objects & user-attached packages to ensure functions don't overlap
# consider ctrl + shift + F10 as well
if(length( names(sessionInfo()$otherPkgs) )){
  to_detach <- paste0("package:", names(sessionInfo()$otherPkgs))
  lapply(to_detach, detach, character.only=TRUE, unload=TRUE, force = TRUE)
} 
rm(list = ls(all.names = TRUE))


.start_time <- Sys.time()
Sys.umask("002")
Sys.setenv(MKL_VERBOSE = 0)

library(dplyr)
library(plyr)
library(data.table)
library(parallel)
library(ggrepel)
library(zoo)
library(gridExtra)
library(glue)
library(yaml)
library(purrr) 
library(tidyverse)

loadNamespace("ihme.covid", lib.loc = "FILEPATH")
setDTthreads(1)
ncores <- 20



#=====================================================================#
# Global arguments ####
#=====================================================================#

## Data versions -----------------------------------------------

# "model-inputs" version to compare against (often Mon overnight run by "svccovidci")

# Keep a history going back 3-6 months 

# testing_data_version <- '2022_08_24.01' # data review
# testing_data_version <- '2022_08_27.01' # prod
# testing_data_version <- '2022_09_23.01' # data review
# testing_data_version <- '2022_09_23.01' # prod - newer versions missing many locations
# testing_data_version <- '2022_11_02.02' # data review
# testing_data_version <- '2022_11_09.04' # data review
# testing_data_version <- '2022_11_12.01' # prod
# testing_data_version <- '2022_12_01.01' # data review
testing_data_version <- '2022_12_08.04' # data review


# "testing-outputs" versions

# Previous best "Testing" version (pinned in #covid-19-modeling)
# previous_best_version <- '2022_03_07.01'
# previous_best_version <- '2022_03_30.01' # prod 
# previous_best_version <- '2022_05_03.02' # prod 
# previous_best_version <- '2022_06_07.03' # prod 
# previous_best_version <- '2022_07_07.05' # prod
# previous_best_version <- '2022_08_01.01' # prod-unused
# previous_best_version <- '2022_08_29.01' # prod
# previous_best_version <- '2022_10_17.05' # prod
previous_best_version <- '2022_11_14.04' # prod

# Splice locations 
splice_locs <- c(
  135, # Brazil 2022-03-07 w_11
  4775, # Sao Paulo 2022-03-07 w_11
  349, # Greenland- uses frontier only
  197, #Eswatini - lost a middle chunk of data
  20, #Viet Nam 2022-07-07 (from 2022-03-07.01)
  416, # Tuvalu - uses frontier only
  369 # Nauru - uses frontier only
) 


## Roots --------------

user <- Sys.info()["user"]
CODE_ROOT <- file.path("FILEPATH", user, "covid-beta-inputs")
OUTPUT_ROOT <- "FILEPATH"
SEIR_COVARIATES_ROOT <- "FILEPATH"
TESTING_PATH <- "FILEPATH"
MODEL_INPUTS <- "FILEPATH"
FUNX_PATH <- file.path(CODE_ROOT, "FILEPATH")


# Paths ----------

# RECONNECT ----

if (FALSE) { # Use with caution for development purposes - reconnect to an existing folder
  .reconnect_version <- "2022_07_21.03"
  output_dir <- paste0(OUTPUT_ROOT, .reconnect_version)
} else {
  output_dir <- ihme.covid::get_output_dir(OUTPUT_ROOT, "today")
}

plot_scenarios_out_path <- file.path(output_dir, "forecast_test_pc.pdf")
code_dir <- file.path(CODE_ROOT, "FILEPATH")
model_inputs_version <- file.path(MODEL_INPUTS, testing_data_version)
missing_locs_out_path <- file.path(output_dir, paste0("missing_testing_locs_", Sys.Date(), ".csv"))
data_smooth_path <- file.path(output_dir, "data_smooth.csv") # used by many scripts
first_case_path <- file.path(output_dir, "first_case_date.csv") # used by many scripts
prep_data_metadata_path <- file.path(output_dir, "prep_data.metadata.yaml")
plot_prod_comp_out_path <- file.path(output_dir, "plot_comp_w_previous.pdf")

# LOGGING Start ------------------------------------------------------------------
# Create run_log.txt with all messages and errors for overnight runs
# sdtout will go to one file, conditions (stderr()) will go to another

# WARNING: function calls WILL print to console, but NOT messages/errors/warnings
# use sink.number() to find open connections & ?sink to troubleshoot connections

LOG_CONDITIONS_TO_FILE <- F 

if(LOG_CONDITIONS_TO_FILE) {
  LOG_CONDITIONS <- file(file.path(output_dir, "LOG_CONDITIONS.txt"), open = "wt")
  LOG_OUTPUTS <- file(file.path(output_dir, "LOG_OUTPUTS.TXT"), open = "wt")
  sink(LOG_CONDITIONS, type = "message", append = T)
  sink(LOG_OUTPUTS, type = "output", append = T)
}

# Functions --------------

# Functions directory
f <- function(path) for (i in list.files(path, pattern = "\\.[Rr]$")) source(file.path(path, i))
f(FUNX_PATH); rm(f)

source(file.path("FILEPATH/get_location_metadata.R"))


# Hierarchies -----------------
lsid_covid <- 111
lsvid_covid <- 1158
release_covid <- 9
lsid_covar <- 115
lsvid_covar <- 1155
release_covar <- 9

hierarchy <- get_location_metadata(location_set_id = lsid_covar, location_set_version_id = lsvid_covar, release_id = release_covar)
gbd_hier <- fread(glue("FILEPATH/gbd_analysis_hierarchy.csv"))
all_locs <- hierarchy[, location_id]


# Output versions -----------------

# Get the current version based on newly created dir (output_dir)
tmp <- unlist(strsplit(output_dir, '/'))
current_version <- tmp[length(tmp)]
end_date <- as.Date("2023-12-31") # How far out to extend forecasts

# Counties -----------------------------------------------

# If saving counties, create new dir and version for it
save_counties <- F # Do you want to save a set of US county projections in new folder directory?
if (save_counties) {
  tmp <- unlist(strsplit(current_version, '[.]'))
  v <- as.numeric(tmp[2]) + 1
  z <- ifelse(v < 10, '0', '')
  county_version <- glue('{tmp[1]}.{z}{v}')
  county_version <- as.character(county_version)
  county_dir <- paste(OUTPUT_ROOT, county_version, sep='/')
  
  # Make sure that counties dir does not already exist to prevent overwrite
  if (dir.exists(county_dir)) {
    stop('Warning! Counties directory already exists! Concurrent runs may have occured!')
  }
}

# Parameters --------------
case_scalar <- 2 # How many times greater must daily testing counts be than daily reported cases?
frontier <- 500 / 1e5 # If use_estimated_frontier == F, the cap for daily testing per capita
use_estimated_frontier <- TRUE # If this is true, use location-specific frontier values otherwise, use frontier on next line
use_parent_frontier <- FALSE # If true, level 4 locations use the highest frontier among subnationals from its parent. 

# Messages 
message(paste0("Output root is ... ", OUTPUT_ROOT)) 
message(paste0("Output directory is ... ", output_dir)) 
message(paste0("SEIR covariates root is ... ", SEIR_COVARIATES_ROOT))
message(paste0("Model inputs version ... ", testing_data_version)) 
message(paste0("Previous best Testing version ... ", previous_best_version)) 
message(paste0("Current Testing version ... ", current_version)) 

# Run Pipeline -------------

## Data Prep & Filter ---------------------------------------------------
## This step sources some functions, all of which are in
## the /data_prep/ directory
## Outliers are defined in this file: /data_prep/filter_data.R

message("Launch: Starting data prep.")
source(paste0("FILEPATH", Sys.info()['user'],"FILEPATH/prep_data.R"))


## Forecasting -------------------------------------------------------
## This step sources some functions, all of which are in
## the /forecast_test_pc/ directory

message("Launch: Starting forecasting.")
source(paste0("FILEPATH", Sys.info()['user'],"FILEPATH/forecast_test_pc.R"))

# Splice locations -----------------------------------------------

message("Launch: Splicing locations.")
previous_estimates <- fread(glue("FILEPATH/forecast_raked_test_pc_simple.csv"))
previous_estimates[, date := as.Date(date)]

spliced_dt <- rbind(
  simple_dt[!(location_id %in% splice_locs)],
  previous_estimates[location_id %in% splice_locs], 
  fill=T
)

full_gbd_dt <- make_all_gbd_locations(spliced_dt)
output_dt <- rbind(spliced_dt, full_gbd_dt)

## Viet Nam: Custom splice
# Retain this code as an example for future custom splices 

# Reintroduce historic data prior to reversion to frontier
# vietnam <- fread(file.path(OUTPUT_ROOT, "2022_03_07.01", "forecast_raked_test_pc_simple.csv"))[location_id == 20]
# vietnam[,date := as.Date(date)]
# output_dt <- rbind(output_dt[!location_id == 20],
#              vietnam)

## China: REDACTED Custom splice for provinces  --------------------------------
# Replace subnat values with China national value 

# Get the children of mainland China
mainland_china_provinces <- hierarchy[parent_id == 44533]

# Seperate out the china subnats
mainland_china_provinces_output_dt <- output_dt[location_id %in% mainland_china_provinces$location_id]

# For each china subnat location id, join in China national test_pc by date
# Replace its own value with that and combine back together
china_subnats_replaced <- mainland_china_provinces_output_dt %>% 
  split(.$location_id) %>% 
  map(
    .f = function(x) {
      
      # Get China values
      china <- output_dt[location_id == 6]
      china <- china %>% 
        select(date, test_pc)
      
      # For the subsetted df join China's value on date and replace its own
      # test_pc with the joined value
      x <- x %>% 
        left_join(china, by = 'date') %>% 
        select(-test_pc.x) %>% 
        dplyr::rename(test_pc = test_pc.y) %>% 
        relocate(test_pc, .after = observed)
      
      # Cap test_pc at the frontier
      x <- x %>% 
        mutate(test_pc = ifelse(test_pc > frontier, frontier, test_pc))
    }
  ) %>% 
  bind_rows()

# Remove the China subnats from the output_dt and then bind in the new ones
output_dt <- output_dt[!location_id %in% china_subnats_replaced$location_id]
output_dt <- rbind(output_dt, china_subnats_replaced)

# Write  ---------------------------

message("Writing output_dt to disk.")
write.csv(output_dt, paste0(OUTPUT_ROOT,current_version,"/forecast_raked_test_pc_simple.csv"), row.names = F)

# Quick check
#x <- output_dt[location_id == 35506]
#plot(x$date, x$test_pc, col='red', main=x$location_name[1])
#points(x$date, x$test_pc_better)
#points(x$date, x$test_pc_worse, col='blue')

# Plots (Scatter) ------------------------------------------------------------

message("Preparing data for scatterplots (plot_scatter_prep)")
plot_dt_all <- .plot_scatter_prep(previous_best_version = previous_best_version,
                                  output_dt_new = output_dt,
                                  hierarchy = hierarchy)

percent_diff_cumul <- c(0.05, 0.08, 0.1) # what % differences from reference to label in plots?
percent_diff_daily <- c(0.50, 0.75)

message("Scatterplots, new data vs. previous best (plot_scatter_compare)")
pdf(file.path(output_dir, 
              glue("testing_panel_scatter_{testing_data_version}_v_{previous_best_version}_test.pdf")),
    height=10, width=15)


for (i in percent_diff_cumul){
  .plot_scatter_compare(DATASET = plot_dt_all, 
                        PERCENT_DIFF = i, 
                        VARIABLE = "cumulative_test_pc")
}

for (i in percent_diff_daily){
  .plot_scatter_compare(DATASET = plot_dt_all, 
                        PERCENT_DIFF = i, 
                        VARIABLE = "test_pc")
}

dev.off()


# Plots (Previous vs. Best) -------------------------------------


## Want plots to show the spliced locations
simple_dt <- copy(spliced_dt)

message("Plotting scenarios...")

## This takes forever, convert to a submitted job?
message(plot_scenarios_out_path)
message("Plotting comparison to previous best...")
#plot_prod_comp(simple_out_path, input_data_path, seir_testing_reference, plot_prod_comp_out_path, hierarchy)

# 5th version of plotting code
plot_prod_comp5(current_version,
                previous_best_version,
                hierarchy,
                out_path=file.path(output_dir, glue("testing_plot_QC_{previous_best_version}_{current_version}.pdf")))


# Metadata ----------------------------

.end_time <- Sys.time()
.run_time <- format(.end_time - .start_time, digits = 2)

# Which exact hash/commit were outputs run from?
git_logs <- fread(file.path(CODE_ROOT, "FILEPATH"), header = F)
git_log_last <- git_logs[nrow(git_logs)]
git_hash <- stringr::str_split_fixed(git_log_last[["V1"]], " ", n = Inf)[2]

metadata_versions <- list(
  "pipeline"  = "COVID_testing_covariate",
  "user"      = user,
  "rundate"   = as.character(Sys.time()),
  "starttime" = as.character(.start_time),
  "endtime"   = as.character(.end_time),
  "runtime"   = .run_time,
  "model_inputs_version" = testing_data_version,
  "previous_production_testing_outputs_version" = previous_best_version,
  "current_testing_output_version" = current_version,
  "counties_testing_output_version" = ifelse(save_counties, county_version, "Not run"),
  PATHS = list(
    output_dir           = output_dir,
    code_dir             = code_dir,
    model_inputs_version = model_inputs_version
  ),
  PARAMS = list(
    case_scalar            = case_scalar,
    frontier               = frontier,
    use_estimated_frontier = use_estimated_frontier,
    use_parent_frontier    = use_parent_frontier
  ),
  GIT = list(
    "git_branch"   = gsub("\n", "", readr::read_file(file.path(CODE_ROOT, ".git/HEAD"))),
    "git_log_last" = git_log_last,
    "git_hash"     = git_hash
  ),
  SUBMISSION_COMMANDS = extract_submission_commands()
)
yaml::write_yaml(metadata_versions, file = file.path(output_dir, "metadata_versions.yaml"))

message(paste("Done! Output directory:", output_dir))
message(paste("Max observed date of data:", max(simple_dt[observed == 1, date])))


# LOGGING Stop --------------------------------------------------------------
# Closing sink() connections to complete logging
if(LOG_CONDITIONS_TO_FILE) {
  sink(type = "output")
  sink(type = "message")
  close(LOG_OUTPUTS)
  close(LOG_CONDITIONS)
}
# Purpose: Second script to convert draw-level all-age/sex LF prevalence estimates from MBG model to age/sex-specific estimates for GBD.
  ## Project estimates for missing GBD years
  ## Load and format age-pattern from crosswalking results
  ## Convert all-age/sex estimates to age/sex-specific
  ## Write-out draw files by location_id 

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
user <- Sys.info()[["user"]]
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"
options(max.print=999999)

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- "FILEPATH"
  draws_dir <- "FILEPATH"
  interms_dir <- "FILEPATH"
  logs_dir <- "FILEPATH"
}

# 1. Setup libraries, functions, parameters, filepaths, etc. ---------------------------------------------------------------------
# load libraries
library(dplyr)
library(ggplot2)
library(data.table)
library(magrittr)

# source functions
source("FILEPATH")

# establish run dates
xwalk_run_date <- "FILEPATH"  # the run_date directory for xwalk results to use for age pattern

# output directories
code_dir <- "FILEPATH"
shell <- "FILEPATH"
params_dir <- "FILEPATH"
run_file <- fread("FILEPATH")
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
crosswalks_dir <- "FILEPATH"
inf_draws_dir <- "FILEPATH"
if (!dir.exists(file.path(inf_draws_dir))){dir.create(inf_draws_dir)}

# input directories
root_dir <- "FILEPATH"
cw_dir <- paste0(root_dir, "FILEPATH", xwalk_run_date) # saved crosswalk model objects

# set parameters 
draws <- 1000
release_id <- ADDRESS
age_group_set_id <- 24

# create inverse logit function to convert from logit to prevalence space
logit <- function(x) {
  log(x/(1-x))
}

inv.logit <- function(x) {
  exp(x)/(1+exp(x))
}

# 2. Load GBD location information ---------------------------------------------------------------------
# get years and locations for current GBD 
demo <- get_demographics(gbd_team = 'epi', release_id = release_id)
gbd_years <- sort(demo$year_id)
gbd_loc_ids <- sort(demo$location_id)
gbd_ages <- sort(demo$age_group_id)
gbd_sex_id <- sort(demo$sex_id)

# load LF geographic restrictions
lf_endems <- fread("FILEPATH")[value_endemicity == 1,]
lf_endems <- sort(unique(lf_endems$location_id))
## add India level-5 sub-nationals
### add Rural units to lf_endems and Urban to zero_locs
lvl5_rural <- c(43908,43909,43910,43911,43913,43916,43917,43918,43919,43920,43921,43922,
                43923,43924,43926,43927,43928,43929,43930,43931,43932,43934,43935,43936,
                43937,43938,43939,43940,43941,43942,44539)
lvl5_urban <- c(43872,43873,43874,43875,43877,43880,43881,43882,43883,43884,43885,43886,
                43887,43888,43890,43891,43892,43893,43894,43895,43896,43898,43899,43900,
                43901,43902,43903,43904,43905,43906,44540)
lvl_india <- c(lvl5_rural,lvl5_urban)
lf_endems <- c(lf_endems,lvl_india)
lf_endems <- unique(lf_endems)

# load location metadata to add ihme_loc_id
loc_meta <- get_location_metadata(location_set_id = 35, release_id = release_id)
loc_meta <- loc_meta[, c('location_id','ihme_loc_id')]

# 3. Project missing years ---------------------------------------------------------------------
# load pre-splitting results from step_8a and add ihme_loc_id
df_combo <- fread("FILEPATH")
df_combo_loc <- left_join(df_combo,loc_meta, by = 'location_id')

# take mean of draws to find optimization value
df_combo_mean <- mean_ui(df_combo_loc)  

# keep full range of data for weight calculation
start_yr_weight <- 1990 

w_range <- seq(0, 1, by = 0.001)
withhold_years <- 1  # years to withhold for finding optimal w
start_year <- 2018 # evaluate multiple windows starting with this year
optimization_results <- optimize_w(dt = df_combo_mean,
                                   w_range = w_range,
                                   start_year = start_year,
                                   withhold_years = withhold_years,
                                   optimize_metric = "median_abs_error", # Most robust to outliers
                                   return_comparison_table = TRUE)
w_optimal <- optimization_results$optimal_w
message(paste0("w_optimal: ", w_optimal))

# apply weights at draw level
logit_diffs_dt <- calc_logit_diff_draw(df_combo_loc)

weighted_diffs <- NULL
for (loc in unique(logit_diffs_dt$ihme_loc_id)){
  
  logit_diffs_dt_sub <- logit_diffs_dt[ihme_loc_id == loc,]
  
  weighted_logit_diffs_dt <- calc_weighted_logit_diff_draw(logit_diffs_dt_sub, w = w_optimal)  
  weighted_diffs <- rbind(weighted_diffs,weighted_logit_diffs_dt)
  
}

projected_means_dt <- project_future_means_draw(df_combo_loc, weighted_diffs, years_forward = 2)
setnames(projected_means_dt, paste0("projected_mean_",0:999), paste0("draw_",0:999))

projected_means_dt <- left_join(projected_means_dt,loc_meta, by = 'ihme_loc_id')
df_combo <- rbind(df_combo_loc, projected_means_dt)

# save combined, pre-split data object with projected years -- used for splitting data
saveRDS(df_combo, "FILEPATH")

# 4. Load and format age pattern --------------------------------------------------------------------------------------
xwalk_age_pattern <- fread("FILEPATH")

setnames(xwalk_age_pattern, as.character(1:draws), paste0("draw_", 1:draws))

ages <- get_age_metadata(age_group_set_id=age_group_set_id,
                         release_id=release_id)[,.(age_group_id, 
                                                   age_group_years_start, 
                                                   age_group_years_end)]
setorderv(ages, "age_group_years_start")

# For the age groups wider than 1 year, collapse by taking mean across year draws
## remove ages under 1 year of age
xwalk_age_pattern_wide_groups <- subset(xwalk_age_pattern, age_start >= 1)
xwalk_age_pattern_wide_groups[, age_group_id := cut(age_mid, breaks = c(ages$age_group_years_start, 125), labels = ages$age_group_id, right = T, )]
xwalk_age_pattern_wide_groups$age_group_id <- as.integer(as.character(xwalk_age_pattern_wide_groups$age_group_id))
xwalk_age_pattern_wide_groups <- merge(xwalk_age_pattern_wide_groups, ages, by = "age_group_id", all.x = T)

# then collapse
draw_cols <- paste0("draw_", 1:draws)
xwalk_age_pattern_wide_groups <- 
  xwalk_age_pattern_wide_groups[, lapply(.SD, mean),
                                by = c("age_group_id", 
                                       "age_group_years_start", 
                                       "age_group_years_end"),
                                .SDcols = draw_cols]

# For age groups < 1 year (young ages), assume that all have the same prevalence as 0-1 years
x1_u1_row <- subset(xwalk_age_pattern, age_start == 0, select = draw_cols)
xwalk_age_pattern_narrow_groups <- lapply(c(2,3,388, 389), function(age_grp_id) {
  age_group_row <- copy(x1_u1_row)
  age_group_row[, age_group_id := age_grp_id]
  age_group_row[, age_group_years_start := ages[age_group_id == age_grp_id, ]$age_group_years_start]
  age_group_row[, age_group_years_end := ages[age_group_id == age_grp_id, ]$age_group_years_end]
  return(age_group_row)
}) %>% rbindlist

xwalk_age_pattern_collapsed <- rbindlist(list(xwalk_age_pattern_wide_groups, xwalk_age_pattern_narrow_groups), use.names = TRUE)
setorderv(xwalk_age_pattern_collapsed, "age_group_years_start")

# save formatted age-pattern in crosswalking directory
write.csv(xwalk_age_pattern_collapsed, "FILEPATH", row.names = FALSE)

# 5. Convert MBG estimates to GBD estimates, export draw files & save to Epi database -----------------------
# load GBD population data
pop <- get_population(location_id = unique(df_combo$location_id), 
                      year_id = unique(df_combo$year),
                      age_group_id = unique(xwalk_age_pattern_collapsed$age_group_id), 
                      sex_id = 3, 
                      release_id = release_id)

setnames(pop,"year_id","year")

# save pop file for launching jobs
write.csv(pop, "FILEPATH", row.names = FALSE)

# create param_map for launching jobs
end_locs <- sort(unique(df_combo$location_id))
end_locs <- end_locs[end_locs %in% lf_endems]
param_map <- as.data.frame(end_locs)
setnames(param_map, 'end_locs','location_id')
param_map$meid <- ADDRESS
write.csv(param_map, "FILEPATH", row.names = FALSE)

# SBATCH - split endemic locations
i <- -1
fthreads <- 1
mem_alloc <- 4
time_alloc <- "00:20:00"
arg_name <- list("--param_path", "FILEPATH")
param_map <- fread("FILEPATH")

sbatch(job_name = "LF_age_sex_splits",
       shell    = shell,
       code     = "FILEPATH",
       output   = "FILEPATH",
       error    = "FILEPATH",
       args     = arg_name,
       project  = "proj_ntds",
       num_jobs = nrow(param_map),
       threads  = fthreads,
       memory   = mem_alloc,
       time     = time_alloc,
       queue    = "all")

# 6. Save infection prevalence estimates --------------------------------------------------------------
###### manually save results ######
# will return a new model version id
save_results_epi(input_dir = inf_draws_dir, 
                 input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = ADDRESS,
                 description = "fixed lfmda cov measure and projected years",
                 measure_id = 5,
                 release_id = release_id,
                 mark_best = TRUE,
                 crosswalk_version_id = ADDRESS, 
                 bundle_id = ADDRESS
)

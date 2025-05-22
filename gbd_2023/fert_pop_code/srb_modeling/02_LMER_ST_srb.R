################################################################################
## DESCRIPTION Run first stage regression and ST. Prepare data for GPR.
## INPUTS prepped SRB data and location map
## OUTPUTS 
## -- hyper-parameters
## -- stage1/2 predictions
## -- gpr input
## -- beta exceptions
## -- predictions from LMER and ST
## -- Formatted data for GPR
## -- Outliers
## -- gpr_variance summary (if in jobmon)
## Steps
## - Read in configuration files
## - Read in prepped data and functions
##   - merge `loc map` to include locations with estimates
##   - separate outliers from rest of data
## - Create scalars for census variance inputs
##    - Adjust census inputs
## - Create hyper-parameter assignment table
## - Fit random effects model
##   - calculate logit value
##   - run LMER regression
##   - incorporate predictions
##   - return predictions to inverse logit value
## - hyper-parameter calculations
##   - Calculate data denstity and categorize countries
##   - code in hyper-parameter exceptions
## - Run space-time functions
##   - Calculate residuals
##   - create predictor for each location year
##   - run `resid_space_time` function
##   - format and subset only kept locations according to space-time location hierarchy
##   - calculate smoothed residual for Stage 2 prediction 
## - Format for gpr
##   - Calculate amplitude (using locations with high VR data density) 
##     - calculate residual
##     - find variance in residuals for each location
##     - calculate average variance per location
##       - delta logit transform variance
##   - gather summary of variance and save to output if not interactive
## - Save Intermediate Data and Outputs
################################################################################

rm(list = ls())

# load libraries
library(argparse)
library(assertable)
library(data.table)
library(lme4)
library(ltcore)
library(mortdb)

## A. Read in configuration files ----------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", 
  type = "character", 
  required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

## set inputs

if(interactive()){
  version_id <- "Run id"
  main_dir <- paste0("FILEPATH/", version_id)
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}

config <- config::get(
  file = fs::path(main_dir,"/srb_detailed.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

## B. Read in prepped data and functions -----------------------------------

source(fs::path(shared_dir, "space_time.R")) 

## inputs
fs::path(input_dir, c("loc_map", "prepped_data_srb"), ext = "csv") |> 
  as.list() |> purrr::map(fread) |> 
  setattr("names", c("loc_map", "prepped_data")) |> 
  list2env(envir = environment())

to_outlier <- loc_map[, .(ihme_loc_id, location_id, level)]
loc_map <- loc_map[, .(location_id, region_id, super_region_id, ihme_loc_id)]

## loading space time location hierarchy

st_locs <- get_spacetime_loc_hierarchy(
  prk_own_region = FALSE, 
  old_ap = FALSE, 
  gbd_year = gbd_year
)

# load data

# Add location identification and separate outliers (with locs we estimate for)
prepped_data <- merge(
  prepped_data, loc_map[, -("ihme_loc_id")], by = "location_id"
)

# separate data and outliers

prepped_data <- split(prepped_data, by = "ihme_loc_id")
n <- names(prepped_data)

c("RWA", "ZMB", "BWA", "NAM", "ZWE", "BEN", "CIV", "LBR", "MLI") |> 
  purrr::map(
    ~prepped_data[[n[n == .]]][source == "vr" & outlier == 0, outlier := 1]
  )

prepped_data[["BGR"]][outlier == 0 & year_id == 2003, outlier := 1]
prepped_data[["BOL"]] |> 
  _[source == "vr" & year_id == 2010 & outlier == 0, outlier := 1]
prepped_data[["CHL"]] |> 
  _[source == "vr" & year_id %in% c(1960, 1991) & outlier == 0, outlier := 1]
cub_y <- c(1966, 1980, 1985, 1989:1993, 1995, 1996)
prepped_data[["CUB"]] |> 
  _[source == "vr" & year_id %in% cub_y & outlier == 0, outlier := 1]
prepped_data[["FIN"]] |> 
  _[source == "vr" & year_id == 1962 & outlier == 0, outlier := 1]
prepped_data[["GEO"]] |> 
  _[between(year_id, 1997, 2001) & outlier == 0 & source == "vr", outlier := 1]
prepped_data[["HRV"]] |> 
  _[outlier == 0 & source %like% "census" & year_id == 1960, outlier := 1]
prepped_data[["IRQ"]] |> 
  _[source == "vr" & year_id == 1977 & outlier == 0, outlier := 1]
prepped_data[["IRQ"]] |> 
  _[source == "censusU1" & year_id < 1965 & outlier == 0, outlier := 1]
prepped_data[["JOR"]] |> 
  _[source == "vr" & year_id < 1970 & outlier == 0, outlier := 1]
prepped_data[["KOR"]] |> 
  _[source == "vr" & year_id %in% 1975:1980 & outlier == 0, outlier := 1]
prepped_data[["LBN"]] |> 
  _[source == "censusU5" & year_id == 2005 & outlier == 0, outlier := 1]
prepped_data[["MCO"]] |> 
  _[source == "vr" & year_id == 1951 & outlier == 0,outlier := 1]
prepped_data[n[grepl("MEX", n)]] |> 
  purrr::map(~.[source == "vr" & outlier == 0 & year_id <= 2016, outlier := 1])
prepped_data[["MKD"]] |> 
  _[source %like% "census" & year_id < 1980 & outlier == 0, outlier := 1]
prepped_data[["MMR"]] |> 
  _[year_id == 2012 & source == "vr" & outlier == 0, outlier := 1]
prepped_data[["QAT"]] |> 
  _[source == "vr" & year_id %in% c(1984, 1987) & outlier == 0, outlier := 1]
prepped_data[["PAK"]] |> 
  _[year_id %in% 1968:1971 & outlier == 0 & source_type_id < 6, outlier := 1]
prepped_data[["PER"]] |> 
  _[source == "vr" & year_id == 2002 & outlier == 0, outlier := 1]
prepped_data[n[grepl("PHL", n)]] |> 
  purrr::map(~.[source == "vr" & outlier == 0 & year_id <= 1961, outlier := 1])
prepped_data[["PLW"]] |> 
  _[source == "vr" & year_id %in% 1998:1999 & outlier == 0, outlier := 1]
prepped_data[["PNG"]] |> 
  _[source == "vr" & year_id %in% c(1977, 1980) & outlier == 0, outlier := 1]
prepped_data[["RUS_44983"]][year_id == 2001 & outlier == 0, outlier := 1]
prepped_data[["SAU"]] |> 
  _[source == "vr"& year_id %in% c(2010, 2017) & outlier == 0, outlier := 1]
prepped_data[["SYC"]] |> 
  _[source == "censusU1"& year_id == 1959 & outlier == 0, outlier := 1]
prepped_data[["SYR"]] |> 
  _[source == "vr"& year_id < 1990 & outlier == 0, outlier := 1]
prepped_data[["TUV"]][year_id < 1980 & outlier == 0, outlier := 1]
prepped_data[["UKR_44939"]] |> 
  _[source == "censusU5" & outlier == 0 & year_id == 2019, outlier := 1]
prepped_data[["URY"]] |> 
  _[source == "vr" & year_id %in% c(1976, 1981) & outlier == 0, outlier := 1]
prepped_data[["USA"]][year_id == 1983 & outlier == 0, outlier := 1]
prepped_data[["UZB"]] |> 
  _[year_id %in% 1991:1993 & source == "vr" & outlier == 0,outlier := 1]
prepped_data[["VEN"]] |> 
  _[source == "vr" & year_id == 2000 & outlier == 0, outlier := 1]
prepped_data[["ZAF"]][source == "vr" & outlier == 0, outlier := 1]

# keep all outliered values from Monaco and San Marino
## Note: unoutliering Monaco values caused curve to overshoot downwards; reverted decision
prepped_data[["SMR"]][outlier == 1 & data < .5, outlier := 0]
prepped_data <- rbindlist(prepped_data)

outliers <- prepped_data[outlier >= 1]
prepped_data <- prepped_data[!outliers, on = names(outliers)]
fwrite(prepped_data, paste0(input_dir, "prepped_data_srb_outlier_adjusted.csv"))

prepped_data <- prepped_data |> 
  unique(by = c("nid", "ihme_loc_id", "year_id"))

# C. Create hyper-parameter assignment table (based on 5q0 and stillbirths) ----

data_density_table <- data.table(
  data_density_category = c(
    "0_to_10",
    "10_to_20",
    "20_to_30",
    "30_to_50",
    "50_plus"
  ),
  zeta = c(0.7, 0.7, 0.8, 0.9, 0.99),
  lambda = c(0.7, 0.5, 0.4, 0.3, 0.2),
  scale = c(15, 15, 15, 10, 5)
)

data_density_table[, c("amp2x", "best") := 1]

hyp_beta <- data.table(
  density_min = c(0L, 20L, 40L),
  density_max = c(20L, 40L,  Inf),
  beta = c(10, 30, 100)
)

## D. Fit random effects model  ----------------------------------------------

# check to make sure there are data in every region
check <- setdiff(unique(loc_map$r), unique(prepped_data$r))
stopifnot(check == 0)

# calculate logit (val scaled between zero and one because it is proportion male)
prepped_data[, logit_val := ltcore::logit(data)]

# create model objects (fit using data we do have)
# NOTE: don't use standard locations b/c model only has random intercept

srb_model_w_reg <- lmer(
  logit_val ~ (1|super_region_id/region_id), 
  data = prepped_data
)

# create square data set
data <- CJ(location_id = unique(st_locs$l), year_id = start_year:end_year) 
data <- merge(data, loc_map, by = "location_id", all.x = TRUE)

assertable::assert_values(data, "ihme_loc_id")
data[, `:=`(
  me_name = "prop_male_births",
  age_group_id = 164,
  sex_id = 3,
  outlier = 0
)]

# merge square data onto raw data
data <- merge(data, prepped_data, by = c(names(data)), all = TRUE)

# identify regions with no data
data[
  j = obs_per_reg := ifelse(!is.na(data), sum(!is.na(data)), 0),
  by = "region_id"
]
assert_values(data, "obs_per_reg")

# predict values 
# (if region has data use "full" model, use "short" model otherwise)

data <- data[obs_per_reg != 0]
data$logit_val_preds1 <- predict(
  srb_model_w_reg,
  newdata = data, 
  allow.new.levels = TRUE
)
logit_vals_1_result <- unique(
  data[, .(region_id, super_region_id, logit_val_preds1)]
)

# check there are no missing predictions
assert_values(data, "logit_val_preds1")

# transform prediction
data[, val_preds1 := ltcore::invlogit(logit_val_preds1)]

## E. hyper-params -------------------------------------------------------------

# fill missing values for summations later
list(data = data, outliers = outliers) |> 
  purrr::map(\(dt) return(dt[is.na(is_vr), is_vr := 0])) |> 
  list2env(envir = environment())

# check no missing values
assert_values(data[!is.na(data)], "is_vr")
assert_values(outliers, "is_vr")

# check that each row is a unique location-source-year
mortcore::check_multiple_data_points(
  data, 
  id_vars = c("location_id", "nid", "year_id")
)

# Changing Data Density calculation to include both VR and Census
params <- data

# calculate density
params[, data_density := as.numeric(is_vr | source_type_id %in% c(2,5))] |> 
  _[, data_density := sum(data_density), by = "location_id"]
params <- params[, .(location_id, data_density)] |> unique()

# merge space time locations and fill missing values
params <- merge(params, st_locs, by = "location_id", all.y = TRUE)

# categorize data density
params <- dplyr::mutate(params, data_density_category = dplyr::case_when(
  data_density < 10 ~ "0_to_10",
  data_density < 20 ~ "20_to_30",
  data_density < 30 ~ "30_to_50",
  TRUE ~ "50_plus"
))

# check for missingness
assert_values(params, "data_density")

# merge data density table
params <- merge(params, data_density_table, by = "data_density_category")

# Use alternative Beta assignment
params[, data_density := as.numeric(data_density)]
params[
  hyp_beta, 
  beta := i.beta, 
  on = .(data_density >= density_min, data_density < density_max)
]

## E.a. define hyper-parameter exceptions --------------------------------

params[ihme_loc_id == "EGY", `:=` (beta = 30, scale = 10)]
params[ihme_loc_id %like% "IND_", `:=` (beta = 10, scale = 30, zeta = .7)]
params[ihme_loc_id == "JPN", `:=` (beta = 200, scale = 2)]
params[grepl(ihme_loc_id, "MEX"), `:=` (
  beta = ifelse(grepl("_", ihme_loc_id), 20, 10),
  scale = 15,
  zeta = .7
)]
params[ihme_loc_id == "MCO", `:=` (beta = 10, scale = 30)]
params[ihme_loc_id == "PAK", beta := 10]
params[ihme_loc_id == "SAU", `:=` (zeta = .9, scale = 5)]
params[ihme_loc_id == "SMR", beta := 15]

# subset to required columns
param_col <- c(
  "ihme_loc_id", "location_id", "scale", "amp2x", "lambda", "zeta", "best"
)
if(run_beta) param_col <- gsub("lambda", "beta", param_col)
params <- params[, ..param_col] |> unique()

## F. Run space-time functions--------------------------------------------------

# merge space-time locations
data <- merge(
  data,
  st_locs, 
  by = c("location_id", "ihme_loc_id"), 
  all.x = TRUE, 
  allow.cartesian = TRUE
)

# calculate residuals in data
data[, resid := logit_val_preds1 - logit_val]

# create one residual for each loc-year: subset to unique loc-year 
st_data <- data[
  j = .(region_name, resid = mean(resid)),
  by= .(ihme_loc_id, year = year_id)
] |> unique()

# merge keep variable back onto data for space time
st_data <- merge(st_data, st_locs, by = c("ihme_loc_id", "region_name"))

# shift to mid-year (ST expects this)
st_data[, year := year + 0.5]

# run space time
st_pred <- resid_space_time(
  data = st_data,
  max_year = end_year,
  params = params,
  dd_weight_lc = FALSE,
  dd_weight_c = FALSE,
  no_weight_c = TRUE,
  tw_beta = run_beta,
  prk_own_region = FALSE,
  old_ap = FALSE
)
fwrite(st_pred, paste0(output_dir, "post_stpred.csv"))

setDT(st_pred)

# format and subset to kept locations according to space-time loc hierarchy
st_pred <- st_pred[
  keep == 1, 
  .(location_id, year_id = floor(year), resid_preds2 = pred.2.resid)
]
st_pred <- merge(
  st_pred,
  data[keep == 1], 
  all = TRUE, 
  by = c("location_id", "year_id")
)

# calculate second stage prediction with smoothed residual
st_pred[, logit_val_preds2 := logit_val_preds1 - resid_preds2]
st_pred[, val_preds2 := invlogit(logit_val_preds2)]

## G. format for gpr --------------------------------------------------------

gpr_input <- copy(st_pred)
setnames(gpr_input, "year_id", "year")

## calculate amplitude - use locs with high VR data density
high_data_density <- copy(
  gpr_input[data_density >= complete_vr_threshold & year >= year_threshold,]
)

# check density
if(nrow(high_data_density) == 0) stop("No high data density values")

high_data_density[, diff := logit_val_preds2 - logit_val]
variance_diff <- tapply(
  high_data_density$diff, 
  high_data_density$ihme_loc_id,
  function(x) 
    var(x, na.rm = TRUE)
)
amp_value <- mean(variance_diff)
print(amp_value)

# check value
if(is.na(amp_value) | amp_value == 0) stop("MSE is NA or zero")

gpr_input[, mse :=  amp_value]

## Adjust variance based on MAD: implement like 45q15
#' 1. census (all census sources)
#' 2. cbh
gpr_input[, diff := logit_val - logit_val_preds2]
gpr_input[grep("census", source), mad_source := "census"]
gpr_input[source == "cbh", mad_source := "cbh"]

gpr_input[ihme_loc_id == "TWN" & source == "censusU1", mad_source := NA]

## MAD = median(|X_i - X\bar)
gpr_input[, mad := mad(diff, constant = 1, na.rm = TRUE), by = "mad_source"]
gpr_input[, .(mad_source, mad)] |> unique()
gpr_input[!is.na(mad_source), variance := (1.4826*mad)^2]

# transform variance to logit variance (using delta method)
gpr_input[, `:=`(
  mad_source = NULL,
  data_var = ((1/(data*(1-data)))^2)*variance
)]

## variance hotfix

gpr_input[
  grepl("IND_", ihme_loc_id) & source == "sample_registration", 
  data_var := data_var * 10
]
gpr_input[
  grepl("MEX", ihme_loc_id), 
  data_var := ifelse(source == "vr", data_var * 15, data_var * 2)
]
gpr_input[
  (grepl("PAK", ihme_loc_id) | ihme_loc_id == "BGD") & source == "vr", 
  data_var := data_var * 100
]

# print summary of data_var for output review, or save to output file
if(interactive()){
  summary(gpr_input[!is.na(data_var),data_var])
  nrow(gpr_input[is.na(data_var)])
}else{
  saveRDS(summary(gpr_input$data_var), fs::path(
    output_dir,
    "summary_variance.rds"
  ))
}

## create additional variables required for GPR

# create dummy for presence of raw data
gpr_input[, is_raw_data := as.numeric(!is.na(logit_val))]

# check data variance is not missing for data
assert_values(gpr_input[!is.na(logit_val)], "data_var")

# categorize sources
gpr_input <- gpr_input |> dplyr::mutate(category = dplyr::case_when(
  is_vr == 0 ~ "other",
  is.na(is_vr) ~ NA,
  data_density >= complete_vr_threshold ~ "vr_unbiased",
  TRUE ~ "vr_other"
))
outliers[, category := ifelse(is_vr == 0, "other", "vr_other")]

# check all data is categorized
for(dt in c("gpr_input", "outliers")){
  print(dt)
  assert_values(get(dt)[!is.na(is_vr)], "category")
} 

# remove detailed source variables (not needed for gpr and makes file too big)
gpr_input[, c("source",  "source_type_id") :=  NULL]

## H. save outputs ---------------------------------------------------------

# save model object
save(srb_model_w_reg, file = paste0(output_dir, "model_parameters_w_reg.rda"))

# save hyper-parameters used
readr::write_csv(params, paste0(input_dir, "hyper_params.csv"))

# save final output from ST and LMER
readr::write_csv(st_pred, paste0(input_dir, "stage_1_2_output.csv"))

# save final output for GPR
readr::write_csv(gpr_input, paste0(input_dir, "gpr_input.csv"))

# save outliers for graphing
readr::write_csv(outliers, paste0(output_dir, "outliers.csv"))

#' @author 
#' @date 2019/05/08
#' @description model of CFR on HAQi

rm(list=ls())

pacman::p_load(data.table, ggplot2, boot, plotly, openxlsx)

# USER INPUTS -------------------------------------------------------------
main_dir         <- "filepath"
dem_dir          <- "filepath"         # dir for input data
out_dir          <- "filepath"
offset_dir       <- "filepath" # dir to output offset values
plot_dir         <- "filepath"   # dir to output model plots
model_object_dir <- "filepath" # dir to output MR-BRT model objects
mrbrt_dir        <- "filepath" # dir to output MR-BRT model outputs

bundle_id <- 7181
ds <- 'iterative'
gbd_round_id <- 7

date <- "date"

etiologies <- c("meningitis_pneumo", 
                "meningitis_hib", 
                "meningitis_meningo", 
                "meningitis_gbs",
                "meningitis_other")

# SOURCE FUNCTIONS --------------------------------------------------------
# Source GBD shared functions
invisible(sapply(list.files("/filepath/", full.names = T), source))

source(paste0("/filepath/helper_functions/find_nondismod_locs.R"))
source(paste0("filepath/cfr_age_split.R"))

# DEFINE FUNCTIONS --------------------------------------------------------
# MARKS ROWS FOR AGE-SEX AGGREGATION
find_all_age_both_sex <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[, agg_age_sex := 0]
  # mark all clinical data for aggregation
  dt[cv_inpatient == 1, agg_age_sex := 1]
  # loop over literature data studies
  lit_nids <- unique(dt[cv_inpatient != 1 | is.na(cv_inpatient), nid])
  for (n in lit_nids) {
    study_etiologies <- dt[nid == n, unique(case_name)]
    # check for each etiology that the study contains ages between 0 and 95 and 
    # either both sex or male and female
    for (e in study_etiologies) {
      row_min_age <- min(dt[nid == n & case_name == e, age_start])
      row_max_age <- max(dt[nid == n & case_name == e, age_end])
      row_sexes <- dt[nid == n & case_name == e, unique(sex)]
      if (row_min_age == 0 & row_max_age >= 95) {
        if (length(row_sexes) == 2) {
          if (isTRUE(all.equal(row_sexes, c("Male", "Female")))) {
            dt[nid == n & case_name == e, agg_age_sex := 1]
          }
        } else if (length(row_sexes) == 1) {
          if (row_sexes == "Both") {
            dt[nid == n & case_name == e, agg_age_sex := 1]
          }
        }
      }
    }
  }
  return(dt)
}

# AGGREGATE OVER ALL AGES AND BOTH SEXES
# aggregate over all years too 
age_sex_agg <- function(raw_dt, agg_year = T, agg_age = T, age_cats = NULL) {
  dt <- copy(raw_dt)
  dt[, `:=` (year_mid = (year_start+year_end)/2, n = 1)]
  message(paste("Dropping", nrow(dt[year_mid < 1980]), "rows before 1980"))
  dt <- dt[year_mid >= 1980]
  dt_agg <- dt[agg_age_sex == 1 & !is.na(cases) & !is.na(sample_size)]
  dt_no_agg <- dt[agg_age_sex == 0 | is.na(cases) | is.na(sample_size)]
  # dt_agg[, `:=` (age_start = 0, age_end = 99, sex = "Both")]
  col_names <- names(dt)[!names(dt) %in% c("cases", "sample_size")]
  agg_start <- c("seq") # to keep one seq from the parent 
  agg_end <- c()
  if (agg_year == T){
    agg_start <- c(agg_start, "year_start")
    agg_end <- c(agg_end, "year_end")
  } 
  if (agg_age == T){
    agg_start <- c(agg_start, "age_start")
    agg_end <- c(agg_end, "age_end")
  } 
  bycols <- setdiff(c("age_start", "age_end", "year_start", "year_end", "seq"), c(agg_start, agg_end))
  if (agg_age == T | agg_year == T){
    # prep age/year ranges if needed 
    dt_agg <- dt_agg[ , (agg_start) := lapply(.SD, min), .SDcols = agg_start, 
                    by = c("nid", "location_id", "location_name", 
                           "case_name", "measure", 
                           "cv_inpatient", "clinical_data", 
                           bycols, age_cats)]
    dt_agg <- dt_agg[ , (agg_end) := lapply(.SD, max), .SDcols = agg_end, 
                      by = c("nid", "location_id", "location_name", 
                             "case_name", "measure", 
                             "cv_inpatient", "clinical_data", 
                             bycols, age_cats)]
  }
  # aggregate across sex and age/year if needed (years and ages are pre-adjusted above) 
  dt_agg <- dt_agg[, lapply(.SD, sum, na.rm=TRUE), 
                   by = c("nid", "location_id", "location_name", 
                          "case_name", "measure", 
                          "cv_inpatient", "clinical_data",
                          "seq",
                          "year_start", "year_end",
                          "age_start", "age_end",
                          age_cats), 
                   .SDcols = c("cases", "sample_size", "n")]

  dt_agg[, `:=` (year_mid = (year_start+year_end)/2, 
                 mean = cases / sample_size,
                 sex = "Both")]
  # some rows may be missing mean if they don't have cases and sample_size
  null_row_count <- nrow(dt_agg[is.na(mean)])
  message(paste("Dropping", null_row_count, 
                "rows that could not be aggregated"))
  dt_agg <- dt_agg[!is.na(mean)]
  dt_no_agg <- dt_no_agg[, names(dt_agg), with = F]
  message(paste("Final data table has", nrow(dt_agg), "aggregated rows and", nrow(dt_no_agg), "non-aggregated rows"))
  dt <- rbind(dt_agg, dt_no_agg)
  #dt <- dt_agg
  return(dt)
}

# CALCULATE STD ERROR BASED ON FORMULA FOR BINOMIAL VARIANCE
get_se <- function(raw_dt, method) {
  dt <- copy(raw_dt)
  dt$standard_error <- as.double(dt$standard_error)
  z <- qnorm(0.975)
  dt[is.na(standard_error) & !is.na(sample_size) & measure == "cfr", 
     standard_error := sqrt(((cases/sample_size)*(1-(cases/sample_size)))/(sample_size))]
  null_row_count <- nrow(dt[is.na(standard_error)])
  message(paste("Dropping", null_row_count, 
                "rows that do not have standard error"))
  dt <- dt[!is.na(standard_error)]
  return(dt)
}

wilson_std_err <- function(raw_dt){
  dt <- copy(raw_dt)
  dt$standard_error <- as.double(dt$standard_error)
  z <- qnorm(0.975)
  dt[, cf_component := mean * (1 - mean)]
  dt[is.na(standard_error) & !is.na(sample_size) & measure == "cfr", 
     standard_error := sqrt((cf_component / sample_size) + ((z^2) / (4 * sample_size^2))) / (1 + z^2 / sample_size)]
  null_row_count <- nrow(dt[is.na(standard_error)])
  message(paste("Dropping", null_row_count, 
                "rows that do not have standard error"))
  dt <- dt[!is.na(standard_error)]
}

# GET LOCATION METADATA
get_loc_metadata <- function(dt) {
  loc_meta <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = ds)
  loc_meta <- loc_meta[, .(location_id, region_id, region_name, 
                           super_region_id, super_region_name)]
  dt <- merge(dt, loc_meta, by = "location_id", all.x = T)
  return(dt)
}

# GET HAQi
get_haqi <- function(raw_dt) { 
  haqi_dt <- get_covariate_estimates(covariate_id = 1099, 
                                    location_id = 'all', 
                                    year_id = 'all', 
                                    gbd_round_id = gbd_round_id, 
                                    decomp_step = 'step3')
  haqi_dt <- haqi_dt[, .(location_id, year_id, haqi = mean_value)]
  # Merge by location and year midpoint
  dt[, year_id := year_mid]
  dt <- merge(dt, haqi_dt, by = c("location_id", "year_id"), all.x = T)
  # for year midpoints less than 1980 there are no HAQ values, also drop old norway locations
  null_row_count <- nrow(dt[is.na(haqi)])
  message(paste("Dropping", null_row_count, "rows that do not have HAQi"))
  dt <- dt[!is.na(haqi)]
  return(dt)
}

# DROP MEAN == 1 FOR LOGISTIC REGRESSION
drop_mean_1 <- function(raw_dt) {
  dt <- copy(raw_dt)
  message(paste("Dropping", nrow(dt[mean == 1]), "rows that have mean = 1"))
  dt <- dt[mean != 1]
}

# OFFSET MEAN == 0 FOR LOGISTIC REGRESSION
# WRITES OFFSET CSV and saves in offset_dir
offset_mean_0 <- function(raw_dt) {
  dt <- copy(raw_dt)
  offset_list <- c()
  for(e in etiologies) {
    offset <- 0.01 * median(dt[mean != 0 & case_name == e, mean])
    dt[mean == 0 & case_name == e, mean := mean + offset]
    offset_list <- c(offset, offset_list)
  }
  offset_dt <- data.table(etiology = etiologies, offset = offset_list)
  offset_filename <- paste0(offset_dir, model_prefix, "_offset_values.csv")
  print("Offset values:")
  print(offset_dt)
  fwrite(offset_dt, offset_filename)
  return(dt)
}

# LOGIT TRANSFORM MEAN AND SE
logit_transform <- function(raw_dt) {
  dt <- copy(raw_dt)
  # transform mean and SE into logit space using delta method
  dt[, logit_mean := log(mean / (1 - mean))]
  dt[, delta_logit_se := sqrt((1/(mean - mean^2))^2 * standard_error^2)]
  return(dt)
}

# CREATE DUMMY VARIABES FOR ETIOLOGIES
onehot_encode <- function(raw_dt) {
  dt <- copy(raw_dt)
  # one-hot encoding for etiologies
  dt[, etio_pneumo  := ifelse(case_name == "meningitis_pneumo",  1, 0)]
  dt[, etio_hib     := ifelse(case_name == "meningitis_hib",     1, 0)]
  dt[, etio_meningo := ifelse(case_name == "meningitis_meningo", 1, 0)]
  dt[, etio_gbs     := ifelse(case_name == "meningitis_gbs",     1, 0)]
  dt[, etio_other   := ifelse(case_name == "meningitis_other",   1, 0)]
  return(dt)
}

# RUN FUNCTIONS -----------------------------------------------------------
# get bundle version id from bv tracking sheet

agg <- "all"
agg_year <- T
agg_age <- T
drop_zero <- F
trim_pct <- 0.0
min_ss <- 1

# name prefix used in offset csv, model object, MR-BRT directory, and plots
model_prefix <- paste0(date, "_agg_", agg, 
                       (if (drop_zero == T) "_drop_zero"), 
                       (if (agg_year == T) "_year_agg"), 
                       (if (agg_age == T) "_age_agg"),
                       (if (trim_pct == 0) "_no_trim" else "_10pct_trim"), 
                       "_one_hot_encode_etiologies_min_ss_", min_ss)
model_prefix <- paste0(model_prefix, "_age_splitting_applied")

bundle_version_dir <- "filepath"
bv_tracker <- fread(paste0(bundle_version_dir, 'bundle_version_tracking.csv'))
bundle <- bundle_id
bv_row <- bv_tracker[bundle_id == bundle & current_best == 1]
bv_id <- bv_row$bundle_version
dt <- get_bundle_version(bv_id, fetch = "all")
dt <- find_nondismod_locs(dt)
dt <- dt[group_review == 1 | is.na(group_review)]

# age-binning
age_cats <- c("age_u5", "age_5_65", "age_65plus")
dt[, age_u5 := 0]; dt[age_end <= 5, age_u5 := 1]
dt[, age_5_65 := 0]; dt[age_start >= 5 & age_end <= 65, age_5_65 := 1]
dt[, age_65plus := 0]; dt[age_start >= 65, age_65plus := 1]

dt[, clinical_data := 0]
dt[clinical_data_type != "", clinical_data := 1]
# drop pre-1980 before aggregating years
dt <- get_loc_metadata(dt)
if (agg == "select") dt <- find_all_age_both_sex(dt) # THIS DROPS ROWS FROM FINAL
if (agg == "all")    dt[, agg_age_sex := 1]
dt <- age_sex_agg(dt, agg_year = agg_year, agg_age = agg_age, age_cats = age_cats)
# age split here
test <- age_split_data(dt, gbd_round_id = 7, ds = "iterative")
dt <- copy(test)
dt[, age_u5 := 0]; dt[age_end <= 5, age_u5 := 1]
dt[, age_5_65 := 0]; dt[age_start >= 5 & age_end <= 65, age_5_65 := 1]
dt[, age_65plus := 0]; dt[age_start >= 65, age_65plus := 1]
# round year_mid so that HAQ can be gotten
dt[, year_mid := round(year_mid)]
dt[, age_mid := (age_start + age_end) / 2]
dt <- get_haqi(dt)
# drop aggregated rows with ss below min_ss
message(paste("dropping", nrow(dt[sample_size <= min_ss]), "rows of data with sample size less than", min_ss))
dt <- dt[sample_size >= min_ss]
# also drop rows fitting Mohsen's criteria
message(paste("also dropping", nrow(dt) - nrow(dt[sample_size > 5 | cases > 0]), "rows of data according to Mohsen's criteria"))
dt <- dt[sample_size > 5 | cases > 0]

# add small amount to deaths when deaths=0, subtract small amount from deaths when deaths=admissions
# prevents -Inf and Inf logits
dt$cases <- case_when(dt$cases == 0 ~ 0.01,
                           dt$cases == dt$sample_size ~ dt$sample_size - 0.01,
                           TRUE ~ dt$cases)

dt <- as.data.table(dt)
dt[, mean := cases/sample_size]
# SE from binomial distribution
dt <- get_se(dt)
# # SE from wilson score
# dt <- wilson_std_err(dt)

dt <- logit_transform(dt)
dt <- onehot_encode(dt)

# concatenate nid and location to use as MR-BRT study ID
dt[, nid_location := paste0(nid, "_", location_id)]

# add interaction columns
etio_cols <- c("etio_pneumo", "etio_meningo", "etio_hib", "etio_other", "etio_gbs")
int_cols <- c("pneumo_haqi", "meningo_haqi", "hib_haqi", "other_haqi", "gbs_haqi")
dt[, (int_cols) := lapply(.SD, function(x) 
  x * dt[['haqi']] ), .SDcols = etio_cols]

# add year bin column
dt[, year_after_2000 := 0]; dt[year_mid > 2000, year_after_2000 := 1]

# write this dt out as a crosswalk version
loc_meta <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = ds)
nats <- loc_meta[level == 3]$location_id
dt[, crosswalk_parent_seq := seq]
dt[, underlying_nid := nid]
dt[, source_type := "Facility - inpatient"]
dt[, sampling_type := NA]
dt[, representative_name := "Nationally and subnationally representative"]
dt[location_id %in% nats, representative_name := "Nationally representative only"]
dt[, urbanicity_type := "Unknown"]
dt[, recall_type := "Not Set"]
dt[, recall_type_value := NA]
dt[, unit_type := "Person"]
dt[, input_type := NA]
dt[, upper := NA]
dt[, lower := NA]
dt[, effective_sample_size := NA]
dt[, design_effect := NA]
dt[, is_outlier := 0]
dt[, unit_value_as_published := 1]
dt[, uncertainty_type_value := NA]
dt[, uncertainty_type := NA]
dt[, data_id := 1:nrow(dt)]
dt$seq <- NA
crosswalk_path <- paste0("filepath", model_prefix, ".xlsx")
write.xlsx(dt, crosswalk_path, sheetName = "extraction")
description <- paste("train data for MR-BRT CFR on HAQ model", model_prefix)
result <- save_crosswalk_version(bundle_version_id = bv_id,
                                 crosswalk_path,
                                 description)

if (result$request_status == "Successful") {
  df_tmp <- data.table(bundle_id = bundle_id,
                       bundle_version_id = bv_id,
                       crosswalk_version = result$crosswalk_version_id, 
                       parent_crosswalk_version = NA,
                       is_bulk_outlier = 0,
                       filepath = crosswalk_path,
                       current_best = 1,
                       date = date,
                       description = description)
  
  dir_2020 <- "filepath"
  cv_tracker <- as.data.table(read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx')))
  cv_tracker[bundle_id == bundle_id, current_best := 0]
  cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
  write.xlsx(cv_tracker, paste0(dir_2020, 'crosswalk_version_tracking.xlsx'))
}

dt_copy <- copy(dt)

# RUN MR-BRT --------------------------------------------------------------
train_dt <- dt[, .(nid, location_id, nid_location, age_start, age_end, age_mid, 
                   year_mid, sex,mean, cases, sample_size, clinical_data,
                   standard_error, logit_mean, delta_logit_se, haqi, data_id,
                   etio_pneumo, etio_hib, etio_meningo, etio_other, etio_gbs,
                   pneumo_haqi, meningo_haqi, hib_haqi, other_haqi, gbs_haqi,
                   age_u5, age_5_65, age_65plus, year_after_2000)]

# PREDICT MR-BRT ----------------------------------------------------------
# create datasets to make predictions on x-covs and z-covs
library(dplyr)
library(data.table)
library(boot)
library(ggplot2)
library(reticulate, lib.loc = "/filepath/")
library(mrbrt001, lib.loc = "/filepath/") # for R version 3.6.3

# add a covariate for year
# add a covariate for clinical vs lit data? 

dat3 <- MRData()
dat3$load_df(
  data = train_dt, col_obs = "logit_mean", col_obs_se = "delta_logit_se",
  col_covs = list("haqi", 
                  # "clinical_data",
                  # "year_mid",
                  'year_after_2000',
                  "etio_pneumo", "etio_meningo", "etio_hib", "etio_gbs",
                  "pneumo_haqi", "meningo_haqi", "hib_haqi", "gbs_haqi", 
                  "age_u5", "age_65plus"
                  ),
  col_study_id = "nid_location" )

mod5 <- MRBRT(
  data = dat3,
  cov_models = list(
    LinearCovModel("intercept", use_re = T), 
    LinearCovModel("year_after_2000", use_re = F), 
    LinearCovModel("etio_pneumo", use_re = T), # should I have these random effects?
    LinearCovModel("etio_hib", use_re = T),
    LinearCovModel("etio_meningo", use_re = T),
    LinearCovModel("etio_gbs", use_re = T),
    LinearCovModel("haqi", use_re = T), 
    LinearCovModel("pneumo_haqi", use_re = F), # should I have these random effects?
    LinearCovModel("hib_haqi", use_re = F),
    LinearCovModel("meningo_haqi", use_re = F),
    LinearCovModel("gbs_haqi", use_re = F),
    LinearCovModel("age_u5", use_re = F),
    # LinearCovModel("age_5_65", use_re = F),
    LinearCovModel("age_65plus", use_re = F)
    # LinearCovModel("clinical_data", use_re = F)
  ),
  inlier_pct = 1 - trim_pct
)

py_save_object(object = mod5, filename = paste0(model_object_dir, model_prefix, ".pkl"), pickle = "dill")

mod5$fit_model(inner_print_level = 5L, inner_max_iter = 500L, outer_max_iter = 100L)

# make predictions
# function to generate UI
add_ui <- function(dat, x_var, lo_var, hi_var, color = "darkblue", opacity = 0.2) {
  polygon(
    x = c(dat[, x_var], rev(dat[, x_var])),
    y = c(dat[, lo_var], rev(dat[, hi_var])),
    col = adjustcolor(col = color, alpha.f = opacity), border = FALSE
  )
}

# generate data to predict on
x_pred_dt <- expand.grid(intercept    = 1,
                         year_after_2000 = 1,
                         clinical_data= 0,
                         age_u5       = c(0, 1),
                         # age_5_65     = c(0, 1),
                         age_65plus   = c(0, 1),
                         haqi         = seq(0, 100, 0.1),
                         etio_pneumo  = c(0, 1),
                         etio_meningo = c(0, 1),
                         etio_gbs     = c(0, 1),
                         etio_hib     = c(0, 1))
setDT(x_pred_dt)
int_cols <- int_cols[int_cols != "other_haqi"]
etio_cols <- etio_cols[etio_cols != "etio_other"]
x_pred_dt[, (int_cols) := lapply(.SD, function(x) 
  x * x_pred_dt[['haqi']] ), .SDcols = etio_cols]

# drop rows with more than 1 indicator variable marked 1
x_pred_dt <- x_pred_dt[etio_pneumo + etio_meningo + etio_hib + etio_gbs <= 1]
x_pred_dt <- x_pred_dt[age_u5 + age_65plus <= 1]
x_pred_dt[, data_id := 1:nrow(x_pred_dt)]

dat_pred1 <- MRData()

dat_pred1$load_df(
  data = x_pred_dt, 
  col_covs=list("haqi", 
                "year_after_2000", 
                # "clinical_data",
                "age_u5", "age_65plus",
                "etio_pneumo", "etio_hib", "etio_meningo", "etio_gbs", 
                "pneumo_haqi", "meningo_haqi", "hib_haqi", "gbs_haqi"
                )
)

# x_pred_dt$pred1 <- pred1
predict_re <- F

logit2prob <- function(logit){
  odds <- exp(logit)
  prob <- odds / (1 + odds)
  return(prob)
}

n_samples <- 1000L
samples <- mrbrt001::core$other_sampling$sample_simple_lme_beta(sample_size = 1000L, model = mod5) # switch to this new function from Peng
model_betas <- data.table(mean = apply(samples,2,mean),
                          lower = apply(samples,2,quantile,0.025),
                          upper = apply(samples,2,quantile,0.975))
gamma_draws <- t(matrix(replicate(1000,mod5$gamma_soln), nrow=6))
draws3 <- mod5$create_draws(
 data = dat_pred1,
 beta_samples = samples,
 gamma_samples = gamma_draws,
 random_study = predict_re )
x_pred_dt$pred3 <- mod5$predict(data = dat_pred1, sort_by_data_id = TRUE)
x_pred_dt$pred3_lo <- apply(draws3, 1, function(x) quantile(x, 0.025))
x_pred_dt$pred3_hi <- apply(draws3, 1, function(x) quantile(x, 0.975))

with(train_dt, plot(haqi, logit_mean))
with(x_pred_dt, lines(haqi, pred3))
# add_ui(x_pred_dt, "haqi", "pred3_lo", "pred3_hi")

names(draws3) <- paste("draw", 0:999, sep = "_")
x_pred_dt <- cbind(x_pred_dt, draws3)
preds <- copy(x_pred_dt)

# drop predictions with more than 1 indicator variable is marked 1
# convert one hot encoding to categorical case name field 
preds[                 , case_name := "meningitis_other"]
preds[etio_pneumo  == 1, case_name := "meningitis_pneumo"]
preds[etio_hib     == 1, case_name := "meningitis_hib"]
preds[etio_gbs     == 1, case_name := "meningitis_gbs"]
preds[etio_meningo == 1, case_name := "meningitis_meningo"]
# convert one hot encoding to formatted name field, for plotting
preds[                 , etiology := "Other"]
preds[etio_pneumo  == 1, etiology := "Pneumococcal"]
preds[etio_hib     == 1, etiology := "HiB"]
preds[etio_gbs     == 1, etiology := "Group B Strep"]
preds[etio_meningo == 1, etiology := "Meningococcal"]
# do the same for train_dt
train_dt[                 , etiology := "Other"]
train_dt[etio_pneumo  == 1, etiology := "Pneumococcal"]
train_dt[etio_hib     == 1, etiology := "HiB"]
train_dt[etio_gbs     == 1, etiology := "Group B Strep"]
train_dt[etio_meningo == 1, etiology := "Meningococcal"]
# convert one hot encoding to formatted name field, for plotting
preds[                 , age_cat  := "5 to 65"]
preds[age_u5       == 1, age_cat  := "Under 5"]
# preds[age_5_65     == 1, age_cat  := "5 to 65"]
preds[age_65plus   == 1, age_cat  := "65 plus"]
preds$age_cat <- factor(preds$age_cat , levels = unique(preds$age_cat ))
# do the same for train_dt
train_dt[                 , age_cat  := "5 to 65"]
train_dt[age_u5       == 1, age_cat  := "Under 5"]
# train_dt[age_5_65     == 1, age_cat  := "5 to 65"]
train_dt[age_65plus   == 1, age_cat  := "65 plus"]
train_dt$age_cat <- factor(train_dt$age_cat , levels = unique(train_dt$age_cat ))

# pull in training data for plotting and mark outliers with weights < 0.5
mod_data <- mod5$data
mod_data$outlier  <- round(abs(mod_data$outlier - 1))
# for no outliering
mod_data$outlier <- 0
# inverse logit back to linear space
preds[, `:=` (mean_lin    = inv.logit(pred3),
               mean_lo_lin = inv.logit(pred3_lo), 
               mean_hi_lin = inv.logit(pred3_hi))]

# CREATE PLOTS ------------------------------------------------------------
pdf(paste0(plot_dir, model_prefix, ".pdf"), height = 6, width = 8)
myplot_lin <- ggplot(data = preds, aes(x = haqi, y = mean_lin)) + 
  geom_line(color = "blue", size = 1.5, alpha = 0.8) +
  geom_ribbon(aes(ymin = mean_lo_lin, ymax = mean_hi_lin), alpha = 0.3) +
  geom_point(data = train_dt, aes(x = haqi, y = mean, size = 1/standard_error^2,
                                  col = year_mid, 
                                  alpha = 0.6)) +
  labs(x = "HAQ", y = "CFR", col = "Year") + theme_minimal() +
  facet_grid(rows = vars(etiology), cols = vars(age_cat)) +
  guides(size = F) 
print(myplot_lin)

# plot etiologys together
myplot <- ggplot(data = preds, 
                 aes(x = haqi,
                     y = mean_lin,
                     color = etiology, 
                     fill  = etiology)) + 
  geom_line(size = 1.5, alpha = 0.8) +
  geom_ribbon(aes(ymin = mean_lo_lin,
                  ymax = mean_hi_lin),
             alpha = 0.075, colour = NA, show.legend = F) +
  labs(x = "HAQ", y = "CFR", col = "Etiology") + theme_minimal() +
  facet_wrap(~age_cat)+
  theme(legend.position = "bottom")
print(myplot)
dev.off()

fwrite(preds, paste0("/filepath/", model_prefix, ".csv"))


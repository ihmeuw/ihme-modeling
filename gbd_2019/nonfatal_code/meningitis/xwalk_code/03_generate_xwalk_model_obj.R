## EMPTY THE ENVIRONMENT
rm(list = ls())

## LOAD FUNCTIONS
k <- # filepath
source(paste0(k, "save_crosswalk_version.R"))
source(paste0(k, "get_covariate_estimates.R"))
library(splitstackshape)
library(data.table)
library(ggplot2)
library(gtools)
library(readxl)

## SOURCE MR-BRT
repo_dir <- # filepath
source(paste0(repo_dir, "mr_brt_functions.R"))

#############################################################################################
###                               HELPER OBJECTS AND FUNCTIONS                            ###
#############################################################################################

## HELPER OBJECTS
date         <- date <- gsub("-", "_", Sys.Date())
decomp_step  <- "step2"
me <- "meningitis"
data_type <- "surveillance"
CF_type <- "CF2"
split_data_dir <- # filepath

if (data_type == "claims_2000") {
  model_label <- paste0(date, "_claims_2000_", CF_type)
  file_name <- paste0(me, "_inpatient", CF_type, "_claims2000.csv")
} else if (data_type == "claims") {
  model_label <- paste0(date, "_claims_2010_2016_", CF_type)
  file_name <- paste0(me, "_inpatient", CF_type, "_claims_2010_2016.csv")
} else if (data_type == "surveillance") {
  model_label <- paste0(date, "_surveillance_NULL_", CF_type)
  file_name <- paste0(me, "_inpatient", CF_type, "_surveillance.csv")
}
  
adj_data_dir <-  # filepath
mrbrt_dir  <-  # filepath

#############################################################################################
###                                   DATA PREPARATION                                    ###
#############################################################################################

# load in data
dt <- as.data.table(fread(paste0(split_data_dir, file_name)))
# format with correct column headers
setnames(dt, c("mean_inpatient", "standard_error_inpatient"), c("ref_mean", "ref_se"))
setnames(dt, c(paste0("mean_", data_type), paste0("standard_error_", data_type)), c("alt_mean", "alt_se"))

## LOGIT TRANSFOMRATION OF SE
dt[, `:=` (alt_se_logit = sqrt(alt_se^2*(1/(alt_mean*(1-alt_mean)))^2), ref_se_logit = sqrt(ref_se^2*(1/(ref_mean*(1-ref_mean)))^2))]

## LOGIT TRANSFORMATION OF MEAN
dt[, `:=` (alt_mean_logit = logit(alt_mean), ref_mean_logit = logit(ref_mean))]

## COMPUTE DIFFERENCE
dt[, logit_diff := alt_mean_logit - ref_mean_logit][, logit_diff_se := sqrt(alt_se_logit^2 + ref_se_logit^2)]

## CREATE NETWORK DUMMY
dt[, dt := 1][, dt := 0]

## APPEND
dt <- dt[order(location_id, year_start, age_start, sex)]

#############################################################################################
###                             RUN NETWORK ANALYSIS IN MR-BRT                            ###
#############################################################################################

## FIT THE MODEL
dt[, age_mid := (age_start + age_end) / 2]
dt[age_end >100 , age_mid := 97]

# drop rows with infinite se logit diff
dt <- dt[(logit_diff_se != Inf & logit_diff !=Inf ) ,]

#covs for MS 2000 and 2010
# covs1 <- list(cov_info("age_mid", "X",
#                        bspline_gprior_mean = "0, 0, 0", bspline_gprior_var = "inf, inf, inf",
#                        degree = 3, n_i_knots = 2, r_linear = TRUE, l_linear = FALSE, type="continuous"))

# covs for surveillance 
# dt[, year_match := floor((year_start + year_end) / 2)]
# haqi <- get_covariate_estimates(1099, gbd_round_id = 6, decomp_step = decomp_step)
# haqi[, year_match := year_id]
# dt[, haqi := NULL]
# dt<- merge(dt, haqi[, .(location_id, year_match, haqi = mean_value)], by = c("location_id", "year_match"), all.x = T)
# covs1 <- list(cov_info("haqi", "X", type = "continuous"))

# create study ID variable
dt[, nid_nid2_loc := paste0(nid_inpatient, "_", nid_surveillance, "_", location_id)]

fit1<- run_mr_brt(
  output_dir = mrbrt_dir, 
  model_label = paste0(model_label),
  data = dt,
  mean_var = "logit_diff",
  se_var = "logit_diff_se",
  covs = covs1,
  method = "trim_maxL",
  study_id = "nid_nid2_loc",
  trim_pct = 0.1,
  max_iter = 200,
  overwrite_previous = TRUE
)

plot_mr_brt(fit1)

## CHECK FOR OUTPUTS
check_for_outputs(fit1)

## SAVE RDS OBJECT
saveRDS(fit1, paste0(mrbrt_dir, "model_objects/", paste0(model_label, "_keep_grouping"), ".RDS"))

## EMPTY THE ENVIRONMENT
rm(list = ls())

## LOAD FUNCTIONS
source(paste0("filepath/save_crosswalk_version.R"))
source(paste0("filepath/get_covariate_estimates.R"))
library(splitstackshape)
library(data.table)
library(ggplot2)
library(gtools)
library(readxl)

## SOURCE MR-BRT
repo_dir <- "filepath"
source(paste0(repo_dir, "mr_brt_functions.R"))
source(paste0(k, "libraries/current/r/get_location_metadata.R"))

#############################################################################################
###                               HELPER OBJECTS AND FUNCTIONS                            ###
#############################################################################################

## HELPER OBJECTS
date         <- date <- gsub("-", "_", Sys.Date())
decomp_step  <- "iterative"
me <- "encephalitis"
data_type <- "claims2000" # claims2000, claims, or surveillance
CF_type <- "CF2"

model_label <- paste0(date, "_", data_type)
split_data_dir <- "filepath"
file_name <- paste0(me, "_inpatient_", data_type, ".csv")

adj_data_dir <- "filepath"
mrbrt_dir  <- "filepath"

#############################################################################################
###                                   DATA PREPARATION                                    ###
#############################################################################################

# load in data from Hmwe
dt <- as.data.table(fread(paste0(split_data_dir, file_name)))
# format with correct column headers
setnames(dt, c("mean.x", "standard_error.x"), c("ref_mean", "ref_se"))
setnames(dt, c("mean.y", "standard_error.y"), c("alt_mean", "alt_se"))

## LOGIT TRANSFOMRATION OF SE
dt[, `:=` (alt_se_logit = sqrt(alt_se^2*(1/(alt_mean*(1-alt_mean)))^2), ref_se_logit = sqrt(ref_se^2*(1/(ref_mean*(1-ref_mean)))^2))]

## LOGIT TRANSFORMATION OF MEAN
dt[, `:=` (alt_mean_logit = logit(alt_mean), ref_mean_logit = logit(ref_mean))]

## COMPUTE DIFFERENCE
dt[, logit_diff := alt_mean_logit - ref_mean_logit][, logit_diff_se := sqrt(alt_se_logit^2 + ref_se_logit^2)]

## CREATE NETWORK DUMMY
dt[, dt := 1][, dt := 0]

## APPEND
dt <- dt[order(location_id, closest_year, age_start, sex)]

#############################################################################################
###                             RUN NETWORK ANALYSIS IN MR-BRT                            ###
#############################################################################################
## FIT THE MODEL
dt[, age_mid := (age_start + age_end.x) / 2]
dt[age_end.x >100 , age_mid := 97]

# drop rows with infinite se logit diff
dt <- dt[(logit_diff_se != Inf & logit_diff !=Inf ) ,]

if (data_type == "claims2000"){
  # covs for MS 2000 (enceph)
  covs1 <- list(cov_info("age_mid", "X",
                         bspline_gprior_mean = "0, 0, 0", bspline_gprior_var = "inf, inf, inf",
                         bspline_cvcv = "concave", bspline_cvcv_start = "0.4995", bspline_cvcv_end = "14.1245625",
                         degree = 3, n_i_knots = 2, r_linear = TRUE, l_linear = FALSE, type="continuous"))
  
} else if (data_type == "claims"){
  # covs for MS (2000 and 2010 meningo, 2010 only enceph)
  covs1 <- list(cov_info("age_mid", "X",
                         bspline_gprior_mean = "0, 0, 0", bspline_gprior_var = "inf, inf, inf",
                         degree = 3, n_i_knots = 2, r_linear = TRUE, l_linear = FALSE, type="continuous"))
}

# create study ID variable
dt[, nid_nid2_loc := paste0(nid.x, "_", nid.y, "_", location_id)]

fit1<- run_mr_brt(
  output_dir = mrbrt_dir, 
  model_label = model_label,
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
saveRDS(fit1, paste0(mrbrt_dir, model_label, ".RDS"))
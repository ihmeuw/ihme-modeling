################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR
################################################################################

rm(list=ls())

# Import installed libraries
library(foreign)
library(zoo)
library(nlme)
library(plyr)
library(data.table)
library(devtools)
library(methods)
library(argparse)


# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of 5q0')
parser$add_argument('--submodel_id', type="integer", required=TRUE,
                    help='The submodel_id for this submodel of 5q0')
parser$add_argument('--input_location_ids', type="integer", required=TRUE,
                    nargs='+', help='The input ids for this submodel of 5q0')
parser$add_argument('--output_location_ids', type="integer", required=TRUE,
                    nargs='+', help='The output ids for this submodel of 5q0')
parser$add_argument('--gbd_round_id', type="integer", required=TRUE,
                    help='The gbd_round_id for this run of 5q0')
parser$add_argument('--start_year', type="integer", required=TRUE,
                    help='The starting year for this run of 5q0')
parser$add_argument('--end_year', type="integer", required=TRUE,
                    help='The ending year for this run of 5q0')
parser$add_argument('--st_loess', type="integer", required=TRUE,
                    help='Whether to use the st_loess')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
submodel_id <- args$submodel_id
input_location_ids <- args$input_location_ids
output_location_ids <- args$output_location_ids
gbd_round_id <- args$gbd_round_id
start_year <- args$start_year
end_year <- args$end_year
st_loess <- args$st_loess


# Set core directories
code_dir <- "FILEPATH"
output_dir <- "FILEPATH"


# Get hyperparameters
spacetime_parameters_file <- "FILEPATH"
spacetime_parameters <- "FILEPATH"


# Get data
dc = DataContainer$new(gbd_round_id = gbd_round_id, start_year = start_year,
                       end_year = end_year, output_dir = output_dir)

# Get the stage 1 prediction
data <- dc$get_submodel(submodel_id, 'stage_1_prediction')
data <- data.table(data)


# set ZAF subnational VR to be treated like a survey (trying to get the HIV bump)
ZAF_subnats <- grep("ZAF_", unique(data$ihme_loc_id), value=T)
if (length(ZAF_subnats) > 0) {
  data[(ihme_loc_id %in% ZAF_subnats) & vr==1 & !is.na(vr), ]$to_correct <- FALSE
  data[(ihme_loc_id %in% ZAF_subnats) & vr==1 & !is.na(vr), ]$corr_code_bias <- FALSE
  data[(ihme_loc_id %in% ZAF_subnats) & vr==1 & !is.na(vr), ]$vr <- 0
}


#####################
#Get adjusted data points - only for non-incomplete-VR sources
####################
#Calculate mort w/ survey random effects removed for residual calculation (2nd Stage Model) - not biased VR
#The nbs.ind determines which points should be bias (mixed-effects) adjusted
#to_correct means that a point will be corrected in the vr step (some "biased" points aren't actually corrected there)

# Get index values of all data points we want to correct
nbs.ind <- (data$data == 1 & ((data$category != 'vr_unbiased' & data$vr_no_overlap != 1) | (data$category == 'vr_unbiased' & data$ihme_loc_id %in% c("DZA", "PHL", "TKM", "ECU"))))

# Generate mortality rate prediction with survey random effects
data$mx2[nbs.ind] <- exp(((data$fixed_effect_beta_log_ldi[nbs.ind] + data$b1.re[nbs.ind]) * data$log_ldi[nbs.ind]) + ((data$fixed_effect_beta_maternal_edu[nbs.ind] + data$b2.re[nbs.ind]) * data$maternal_edu[nbs.ind]) + data$fixed_effect_beta_intercept[nbs.ind] + data$ctr_re[nbs.ind] + data$summe[nbs.ind]) + (data$fixed_effect_beta_hiv[nbs.ind] * data$hiv[nbs.ind]) + data$residual_covariate_model[nbs.ind]


#don't let complete VR go down (don't believe there are fewer deaths than counted)
# data$mx2[data$data==1 & data$category == "vr_unbiased" & data$mx2 < data$mx] <- data$mx[data$data==1 & data$category == "vr_unbiased" & data$mx2<data$mx]

#AFG
#find mean adjustment for category other sources in afghanistan, and use it to adjust the 'national demographic and family guidance survey' points
#ugh. this is still necessary 3/14/14 to keep the points in their semi-original spots -CAL
afg.oth.m <- mean(data$adjre_fe[data$data == 1 & data$ihme_loc_id == "AFG" & grepl("other", data$source.type, ignore.case =T) & data$source != "national demographic and family guidance survey"])
afg.ind <- (data$data == 1 & data$ihme_loc_id == "AFG" & data$source == "national demographic and family guidance survey")
data$mx2[afg.ind] <-  exp((data$fixed_effect_beta_log_ldi[afg.ind] + data$b1.re[afg.ind]) * data$log_ldi[afg.ind] + (data$fixed_effect_beta_maternal_edu[afg.ind] + data$b2.re[afg.ind]) * data$maternal_edu[afg.ind] + data$fixed_effect_beta_intercept[afg.ind] + data$ctr_re[afg.ind] + data$re2[afg.ind] + data$st.fe[afg.ind] - afg.oth.m) + data$fixed_effect_beta_hiv[afg.ind] * data$hiv[afg.ind] + data$residual_covariate_model[afg.ind]


#I believe you only need to do this when outliers are included in the dataset - here impute negative mx2's as 0.0001
data <- data.table(data)
data$mx2[data$mx2 <= 0] <- 0.0001

#Transform back to qx space
data$mort2 <- 1-exp(-5*(data$mx2))
data$log10_mort2 <- log(data$mort2, base=10)

# Do not adjust any reference, unbiased vr, or VR with no overlap (and remove adjustment factor)
data[reference == 1 | category == 'vr_unbiased' | vr_no_overlap == 1, mort2 := mort]
data[reference == 1 | category == 'vr_unbiased' | vr_no_overlap == 1, adjre_fe := 0]

dc$save_submodel(data, submodel_id, 'bias_adjusted')

#-------------------Header------------------------------------------------
# Author: USERNAME
# Date: 4/20/2021
# Purpose: Run global models and spline cascades on expected/obs output from RegMod for measles and flu separately to calculate vpds changes in 2020 with COVID
#
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

start_time <- Sys.time()
username <- Sys.info()[[7]]
project <- "-P proj_cov_vpd " # -p must be set on the cluster in order to get slots and not be in trouble
sge.output.dir <- " -o FILEPATH -e FILEPATH "
rshell <- "/FILEPATH/execRscript.sh"


# load packages, install if missing

packages <- c("data.table","magrittr","ggplot2", "lubridate", "gridExtra", "grid", "stringr",
              "msm", "wCorr", "timeDate", "dplyr", "pbapply", "tictoc", "msm")


for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    library(p, character.only = T, lib.loc = "/FILEPATH")
  } else {
    library(p, character.only = T)
  }
}

library(mrbrt001, lib.loc = "FILEPATH/")


# source custom functions
spline_cascade_code_dir <- paste0("/FILEPATH/spline_cascade/")
source(paste0(spline_cascade_code_dir, "diy_funnelplot.R"))
source(paste0(spline_cascade_code_dir, "calculate_alpha.R"))
source("/FILEPATH/collapse_point.R")


# Directories -------------------------------------------------------------

covar_version <-  "2021_05_05"


description <- "Attempting to scale CHN RegMod uncert. Downsample to 1000 from 100000 draws so betas match pt pred. Added EGY and IRQ December flu outliers. TUR in flu outliers. Back to capping uncert draws at 1. Running with RegMod flu model last knot 2017 updated locations, measles last knot 2019. Using mean to std to janfeb rather than sum. Yes emr, yes uncert. Updated covar version, using regional average for locs missing covar val and fixing for J/F in SE Asia (so that CHN and TWN don't increase reg avg from 0). Dropping fake 0s and keeping Regmod outputs for all months in china (not just Mar-Dec) to use directly. Floor of 99% disruption (not just 0s, anything with > 99% dirsuption set to 99% disruption). Using china regmod results directly. Visually id'd measles drop locs in data prep. Dropping rows with inf draws, fixed plotting_ratio to correcty ID column to plot (data_standardization = janfeb, not none). No uncertainty. No alpha adjustment. Dropping manually identified flu locs as well as those with > 20 pct missing or below 5pctle of case numbers; no MDG for measles. Back to standardizing data. model on regmod outputs. Adding uniform beta to mask use in cascade to constrain between -3 and 0.
\nAlso gaussian prior on mask use of glb mask use beta and 5*se of glb mask use beta. Dropped data with 0 for 2020 and/or prev_yrs_avg.
\nSeparate global models and cascades by cause. Loose thetas.
\nGlobal linear effect on mask use. Non-cumulative mobility. Yes monotonic decreasing on s1 model"#Std to gbd modeled cases for measles (4-15 data prep). Larger max outer iter (400) for meas trim.

emr_project <- TRUE

date <- "2021-06-14"
data_date <- "2021-06-07-B"
data_standardization <- "janfeb" #were the data standardized to janfeb vals? or were janfeb values just dropped? ("none")

sim_mask_val <- c(0, 0.25, 0.5, 0.75, 1) # what values of mask use to simulate predictions for at glb, SR, country level?


model_name <- "loose"

thetas_1 <- c(1, 15)
inlier_pct <- 0.90


# CHANGE THESE IF YOU NEED UNCERTAINTY
uncertainty <- TRUE # Indicator if you need uncertainty Increases run time by a lot!
n_draws <- 1000000  # Number of draws for calculating uncertainty (only used if uncertainty is true)
final_n_draws <- 1000L
draw_cols <- paste0("draw_", 0:(final_n_draws - 1)) # only used if uncertainty is true
region_weight <- 10 # for countries with no data, countries from the same region are X times as likely to be sampled as all other countries

last_days <- as.Date(timeLastDayInMonth(c(paste0("2020-", 1:12, "-01"), paste0("2021-", 1:5, "-01"))))

logit <- function(x){log(x/(1-x))}
inv_logit <- function(x){exp(x)/(1+exp(x))}

work_dir <- paste0("FILEPATH", "/gbd_2020/spline_cascade_models/", date, "/", model_name)  # Outputs will be stored here
if(!dir.exists(work_dir)) dir.create(work_dir, recursive = T)

meas_work_dir <- paste0(work_dir, "/measles")
if(!dir.exists(meas_work_dir)) dir.create(meas_work_dir)
flu_work_dir <- paste0(work_dir, "/flu")
if(!dir.exists(flu_work_dir)) dir.create(flu_work_dir)

# save txt file of descrip of model settings and inputs
cat(description, file=file.path(work_dir, "MODEL_DESCRIPTION.txt"), sep = "\n")
cat(paste0("Model_name: ", model_name),
    file=file.path(work_dir, "MODEL_DESCRIPTION.txt"), sep="\n", append=TRUE)
cat(paste0("Mask and Mob version ", covar_version),
    file=file.path(work_dir, "MODEL_DESCRIPTION.txt"), sep="\n", append=TRUE)
cat(paste0("Stage 1 thetas ", thetas_1),
    file=file.path(work_dir, "MODEL_DESCRIPTION.txt"), sep="\n", append=TRUE)
cat(paste0("Input data date: ", data_date),
    file=file.path(work_dir, "MODEL_DESCRIPTION.txt"), sep="\n", append=TRUE)
cat(paste0("Data std: ", data_standardization),
    file=file.path(work_dir, "MODEL_DESCRIPTION.txt"), sep="\n", append=TRUE)

regmod_data_path <- "FILEPATH"

# source functions
source(paste0("/FILEPATH/spline_cascade/utils.R"))

source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path("FILEPATH/get_population.R"))
find_closest <- function(number, vector){
  index <- which.min(abs(number-vector))
  closest <- vector[index]
  return(closest)
}

hierarchy <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")

countries <- hierarchy[level == 3, location_id]



# Prepare mobility and mask use for prediciton ---------------------------------\
## Mobility
if(covar_version %like% "USERNAME"){
  mob <- fread("/FILEPATH/03_17_2021_mobility_est.csv")
  mob <- mob[, .(location_id, date = as.Date(date), mob_avg = mobility_reference * -0.01)]
} else {
  mob <- fread(paste0("/FILEPATH/", covar_version, "/reference/output_summaries/mobility.csv"))
  mob <- mob[, .(location_id, date = as.Date(date), mob_avg = mean * -0.01)]
}

# Set Jan and Feb to be zero except China and Taiwan
mob[date < as.Date("2020-03-01") & !(location_id %in% c(6,8)), mob_avg := 0]
# drop mob estimates after April 2021
mob <- mob [date < as.Date("2021-05-01")][order(mob_avg)]

# cap mobility at zero disruption
mob[mob_avg<0, mob_avg := 0]

mob <- mob[order(location_id, date)]

# create monthly national average mobility disruption est
mob_nat <- mob[location_id %in% countries]

mob_nat[, `:=` (month= month(date), year = year(date))]
mob_nat[, mob_avg_month := mean(mob_avg), by = c("year", "month", "location_id")]
mob_nat <- mob_nat[date %in% last_days] #keep 1 row of covaraite per location-month (mob_avg_month is same value for all days(ie rows) in month) and we have only data that is assigned to last day of month (monthly data for meas, monthly avg for flu)

# for ITA, Spain, Ind, Pak (missing Jan 2020 in more recent covar files), add on with mob 0 because we are assuming no disrup before March 2020
if(!(covar_version %like% "USERNAME")){
  jan_locs_mob <- expand.grid(location_id = c(86, 92, 163, 165),
                          date = as.Date("2020-01-31"),
                          mob_avg = 0,
                          month = 1,
                          year = 2020,
                          mob_avg_month = 0)

  mob_nat <- rbind(mob_nat, jan_locs_mob)
}

# if value not avail for a country, use regional avg
missing_locs <- countries[!(countries %in% mob_nat$location_id)]
locs_to_add <- expand.grid(location_id = missing_locs,
                           date = unique(mob_nat$date)) %>% as.data.table
locs_to_add[, `:=` (month = month(date), year = year(date))]
mob_nat <- rbind(mob_nat, locs_to_add, fill = TRUE)
mob_nat <- merge(mob_nat, hierarchy[,.(location_id, region_id)], by = "location_id")
mob_nat[, `:=` (reg_mob_avg_month = mean(mob_avg_month, na.rm = TRUE)),
        .(region_id, date)]
mob_nat <- merge(mob_nat, regional_mob, by = c("location_id", "date"))
mob_nat[is.na(mob_avg_month), mob_avg_month := reg_mob_avg_month]
# for locs in SE Asia SR that get set to reg avg (nonzero bc CHN and TWN nonzero in Jan/Feb), reset Jan Feb to 0 (only CHN and TWN shld be nonzero in J/F)
mob_nat[date < as.Date("2020-03-01") & !(location_id %in% c(6,8)), mob_avg_month := 0]


## Mask
if(covar_version %like% "USERNAME"){
  mask <- fread("/FILEPATH/03_17_2021_mask_use_est.csv")
  mask <- mask[, .(location_id, date = as.Date(date), mask_avg = mask_use_reference)]
} else {
  mask <- fread(paste0("/FILEPATH/", covar_version, "/reference/output_summaries/mask_use.csv"))
  mask <- mask[, .(location_id, date = as.Date(date), mask_avg = mean)]
}



# Set Jan and Feb to be zero except China and Taiwan
mask[date < as.Date("2020-03-01") & !(location_id %in% c(6,8)), mask_avg := 0] # ELBR: do we want to do this for masks
# drop mask estimates after April 2021
mask <- mask[date < as.Date("2021-05-01")][order(mask_avg)]

mask <- mask[order(location_id, date)]

mask_nat <- mask[location_id %in% countries]

mask_nat[, `:=` (month= month(date), year = year(date))]

mask_nat[, mask_avg_month := mean(mask_avg), by = c("year", "month", "location_id")]
mask_nat <- mask_nat[date %in% last_days] #keep 1 row per location-month (mask_avg_month is same value for all days(ie rows) in month)
if(!(covar_version %like% "USERNAME")){
  jan_locs_mask <- expand.grid(location_id = c(86, 92, 163, 165),
                               date = as.Date("2020-01-31"),
                               mask_avg = 0,
                               month = 1,
                               year = 2020,
                               mask_avg_month = 0)

  mask_nat <- rbind(mask_nat, jan_locs_mask)
}


# if value not avail for a country, use regional avg
missing_locs <- countries[!(countries %in% mask_nat$location_id)]
locs_to_add <- expand.grid(location_id = missing_locs,
                           date = unique(mask_nat$date)) %>% as.data.table
locs_to_add[, `:=` (month = month(date), year = year(date))]
mask_nat <- rbind(mask_nat, locs_to_add, fill = TRUE)
mask_nat <- merge(mask_nat, hierarchy[,.(location_id, region_id)], by = "location_id")
mask_nat[, `:=` (reg_mask_avg_month = mean(mask_avg_month, na.rm = TRUE)),
         .(region_id, date)]
mask_nat[is.na(mask_avg_month), mask_avg_month := reg_mask_avg_month]

# for locs in SE Asia SR that get set to reg avg (nonzero bc CHN and TWN nonzero in Jan/Feb), reset Jan Feb to 0 (only CHN and TWN shld be nonzero in J/F)
mask_nat[date < as.Date("2020-03-01") & !(location_id %in% c(6,8)), mask_avg_month := 0]

# Run MRBRT ---------------------------------------------------------------
pdf(paste0(work_dir, "/by_cause_spline_fit_plots.pdf"), width = 11, height = 8.5)

raw_dt <- fread(regmod_data_path)
raw_dt[ ,plotting_ratio :=  as.numeric(plotting_ratio)]
dt <- raw_dt[data_drop == 0]
setnames(dt, "ratio_se_log", "log_ratio_se")
dt[, date := as.Date(end_date)]

# id which column to use for plotting the data based on how the data were standardized (ie which ever ratio was log transformed to go into the model) if data was prepped before this was added to data prep script (05-17-A)
if(!("plotting_ratio" %in% colnames(dt))){
  if(data_standardization == "none"){
    dt[, plotting_ratio := ratio]
  } else if (data_standardization == "janfeb"){
    dt[, plotting_ratio := standardized_ratio]
  }
}


# mobility
dt <- merge(dt, mob_nat, by=c("location_id", "month", "date"))

#masks
dt <- merge(dt, mask_nat, by=c("location_id", "month", "date", "region_id", "year"))

dt <- merge(dt, hierarchy[,.(location_id, super_region_id, super_region_name, ihme_loc_id)], by = "location_id", all.x = T)

# specify levels for spline
dt[, loc_cause := paste0(location_id, "_", cause)]
dt[, sr_cause := paste0(super_region_id, "_", cause)]

# exclude CHN from data used to fit (will use RegMod preds directly)
chn <- dt[location_id == 6]
dt <- dt[location_id != 6]

# grab data by cause to fit cause specific models
measles <- dt[cause == "measles"]
flu <- dt[cause == "flu"]

## Run separate mr-brt by cause --------------------------------------------
# MEASLES fit and predict glb model, fit cascade  --------------------------
measles[, log_ratio_se_adj := log_ratio_se]

# load data
dat_1_measles <- MRData()
dat_1_measles$load_df(
  data = measles,  col_obs = "log_ratio", col_obs_se = "log_ratio_se_adj",
  col_covs = list("mob_avg_month", "mask_avg_month"),
  col_study_id = "loc_cause"
)

# specify model options. no intercept argument means intercept forced to zero (ie no disruption) when mask use 0 and mob disrup 0, as we would expect epidemiologically
meas_model <- MRBRT(data = dat_1_measles,
                    cov_models = list(LinearCovModel("mob_avg_month",
                                                     use_spline = TRUE,
                                                     use_re = FALSE,
                                                     spline_degree = 3L,
                                                     prior_spline_monotonicity = "decreasing",
                                                     spline_knots_type = "domain",
                                                     spline_knots = array(c(0, 0.15, 0.3, 0.45, 0.6, 1)),
                                                     spline_r_linear = TRUE,
                                                     spline_l_linear = FALSE,
                                                     prior_spline_maxder_gaussian = rbind(c(0, 0, 0, 0, 0), c(1, 1, 1, 1, 1))),
                                      LinearCovModel("mask_avg_month",
                                                     use_spline = FALSE,
                                                     use_re = FALSE)),
                    inlier_pct = inlier_pct)

# help with fixing knot placement
data_temp <- MRData(covs = list(mob_avg_month = array(seq(0,0.75,length.out = 306)))) # no masks here b/c not a spline, only needed for splines

meas_model$attach_data(data_temp)


meas_model$fit_model(outer_max_iter = 500L) # higher helps with convergence issues
py_save_object(object = meas_model, filename = paste0(meas_work_dir, "/global_model.pkl"), pickle = "dill")

# check which points got trimmed, scatter and funnel plot
meas_outlier <- cbind(meas_model$data$to_df(), data.frame(w = meas_model$w_soln))
meas_outlier <- as.data.table(meas_outlier)
meas_outlier[, location_id := as.integer(gsub( "_.*$", "", study_id))]
meas_outlier[, plotting_ratio := exp(obs)] # get standardized ratios out of log space for merging later
fwrite(meas_outlier, paste0(meas_work_dir, "/meas_input_data_with_trimming_status.csv"))

gg_meas_trim <- ggplot(meas_outlier, aes(x = mob_avg_month, y = plotting_ratio, color = as.factor(w))) + geom_point() +
  ggtitle(paste0("Mealses data outliering in global spline cascade with ", 100*(1-inlier_pct), " percent trimming")) +
  ylab("Standardized ratio (log scale)") + xlab("Monthly average mobility") +
  scale_y_continuous(trans = "log10")
print(gg_meas_trim)

meas_funnel <- diy_funnelplot(model = meas_model, cause = "measles")
print(meas_funnel)

# predict glb model spline

global_pred_meas <- expand.grid(location_id = unique(measles$location_id),
                                cause = "measles",
                                mob_avg_month = c(unique(measles$mob_avg_month)),
                                mask_avg_month = sim_mask_val) %>% as.data.table()

dat_pred_meas <- MRData()

dat_pred_meas$load_df(
  data = global_pred_meas,
  col_covs=list("mob_avg_month", "mask_avg_month")
)

global_pred_meas$pred <-  meas_model$predict(dat_pred_meas) %>% exp()

global_pred_meas <- merge(global_pred_meas, hierarchy[,.(location_id, location_name)])

# merge on data used to fit model and plot fit over data
global_pred_meas <- merge(global_pred_meas,
                          measles[, .(location_id, mob_avg_month, orig_mask_avg_month = mask_avg_month, obs = plotting_ratio, weight, cause)],
                          by = c("mob_avg_month", "location_id", "cause"),
                          allow.cartesian = T, all.x = T)

gg <- ggplot(global_pred_meas, aes(x = mob_avg_month, y = pred))+
  geom_point(data = global_pred_meas[!is.na(obs)], aes(y = obs, size = weight), alpha = 0.25, show.legend = F)+
  geom_line(aes(color = as.factor(mask_avg_month)))+
  labs(title = "Global splines - Measles: disruption vs. mobility by average mask use value",
       x = "Average Mobility", # this is  average mobility disruption
       y = "Measles Disruption Ratio")+
  scale_y_continuous(limits = c(0, 10), oob = scales::squish)+
  scale_size_area() + # makes 0 have 0 area
  theme_bw()

print(gg)

gg_a <- ggplot(global_pred_meas, aes(x = mob_avg_month, y = pred))+
  geom_point(data = global_pred_meas[!is.na(obs)], aes(y = obs, size = weight), alpha = 0.25, show.legend = F)+
  geom_line(aes(color = as.factor(mask_avg_month)))+
  labs(title = "Global splines - Measles: disruption vs. mobility by average mask use value",
       x = "Average Mobility",
       y = "log(Measles Disruption Ratio)")+
  scale_y_continuous(oob = scales::squish, trans = "log10")+
  scale_size_area() + # makes 0 value points have 0 area
  theme_bw()

print(gg_a)

# strong mask use prior -- modify prior of mask in glb model object so glb model mask beta passed as gaussian prior to cascade
meas_beta_samples <- mrbrt001::core$other_sampling$sample_simple_lme_beta(sample_size = 1000L, model = meas_model)
sds_meas_model <- apply(meas_beta_samples, 2, sd)
meas_model$cov_models[[2]]$prior_beta_gaussian <- matrix(c(meas_model$fe_soln$mask_avg_month, 5*sds_meas_model[7]), nrow = 2) # 5 is working how theta works for spline
meas_model$cov_models[[2]]$prior_beta_uniform <- matrix(c(-3, 0))

# cascading splines -  two levels (for SR and location) both levels are cause specific because only feeding in data for 1 cause
meas_fit1 <- run_spline_cascade(
  stage1_model_object = meas_model,
  df = measles,
  col_obs = "log_ratio",
  col_obs_se = "log_ratio_se",
  col_study_id = "location_id",
  stage_id_vars = c("sr_cause", "loc_cause"),
  gaussian_prior = TRUE,
  inner_print_level = 1L,
  thetas = thetas_1,
  output_dir = meas_work_dir,
  model_label = "stage1",
  overwrite_previous = TRUE
)

# FLU fit and predict glb model, fit cascade model -----------------------------
flu[, log_ratio_se_adj := log_ratio_se]


dat_1_flu <- MRData()
dat_1_flu$load_df(
  data = flu,  col_obs = "log_ratio", col_obs_se = "log_ratio_se_adj",
  col_covs = list("mob_avg_month", "mask_avg_month"), #cum_mob
  col_study_id = "loc_cause"
)


flu_model <- MRBRT(data = dat_1_flu,
                   cov_models = list(LinearCovModel("mob_avg_month",
                                                    use_spline = TRUE,
                                                    use_re = FALSE,
                                                    spline_degree = 3L,
                                                    prior_spline_monotonicity = "decreasing",
                                                    spline_knots_type = "domain",
                                                    spline_knots = array(c(0, 0.15, 0.3, 0.45, 0.6, 1)),
                                                    spline_r_linear = TRUE,
                                                    spline_l_linear = FALSE,
                                                    prior_spline_maxder_gaussian = rbind(c(0, 0, 0, 0, 0), c(1, 1, 1, 1, 1))),
                                     LinearCovModel("mask_avg_month",
                                                    use_spline = FALSE,
                                                    use_re = FALSE)),
                   inlier_pct = inlier_pct)

data_temp <- MRData(covs = list(mob_avg_month = array(seq(0,0.75,length.out = 306)))) # no masks here, only needed for splines b/c for knot placement

flu_model$attach_data(data_temp)


flu_model$fit_model(outer_max_iter = 500L)
py_save_object(object = flu_model, filename = paste0(flu_work_dir, "/global_model.pkl"), pickle = "dill")

# check which points got trimmed, scatter and funnel plot
flu_outlier <- cbind(flu_model$data$to_df(), data.frame(w = flu_model$w_soln))
flu_outlier <- as.data.table(flu_outlier)
flu_outlier[, location_id := as.integer(gsub( "_.*$", "", study_id))]
flu_outlier[, plotting_ratio := exp(obs)] # get standardized ratios out of log space for merging later
fwrite(flu_outlier, paste0(flu_work_dir, "/flu_input_data_with_trimming_status.csv"))
gg_flu_trim <- ggplot(flu_outlier, aes(x = mob_avg_month, y = plotting_ratio, color = as.factor(w))) + geom_point() +
  ggtitle(paste0("Flu data outliering in global spline cascade with ", 100*(1-inlier_pct), " percent trimming")) +
  ylab("Standardized ratio (log scale)") + xlab("Monthly average mobility") +
  scale_y_continuous(trans = "log10")
print(gg_flu_trim)

flu_funnel <- diy_funnelplot(model = flu_model, cause = "flu")
print(flu_funnel)

# predict glb model spline

global_pred_flu <- expand.grid(location_id = unique(flu$location_id),
                               cause = "flu",
                               mob_avg_month = c(unique(flu$mob_avg_month)),
                               mask_avg_month = sim_mask_val) %>% as.data.table()

dat_pred_flu <- MRData()

dat_pred_flu$load_df(
  data = global_pred_flu,
  col_covs=list("mob_avg_month", "mask_avg_month")
)

global_pred_flu$pred <-  flu_model$predict(dat_pred_flu) %>% exp()

global_pred_flu <- merge(global_pred_flu, hierarchy[,.(location_id, location_name)])

# merge on data used to fit model and plot fit over data
global_pred_flu <- merge(global_pred_flu,
                         flu[, .(location_id, mob_avg_month, orig_mask_avg_month = mask_avg_month, obs = plotting_ratio, weight, cause)],
                         by = c("mob_avg_month", "location_id", "cause"),
                         allow.cartesian = T, all.x = T)

gg_flu <- ggplot(global_pred_flu, aes(x = mob_avg_month, y = pred))+
  geom_point(data = global_pred_flu[!is.na(obs)], aes(y = obs, size = weight), alpha = 0.25, show.legend = F)+
  geom_line(aes(color = as.factor(mask_avg_month)))+
  labs(title = "Global splines - Flu: disruption vs. mobility by average mask use value",
       x = "Average Mobility",
       y = "Flu Disruption Ratio")+
  scale_y_continuous(limits = c(0, 10), oob = scales::squish)+
  scale_size_area() + # makes 0 have 0 area
  theme_bw()

print(gg_flu)

gg_a_flu <- ggplot(global_pred_flu, aes(x = mob_avg_month, y = pred))+
  geom_point(data = global_pred_flu[!is.na(obs)], aes(y = obs, size = weight), alpha = 0.25, show.legend = F)+
  geom_line(aes(color = as.factor(mask_avg_month)))+
  labs(title = "Global splines - Flu: disruption vs. mobility by average mask use value",
       x = "Average Mobility",
       y = "log(Flu Disruption Ratio)")+
  scale_y_continuous(oob = scales::squish, trans = "log10")+
  scale_size_area() + # makes 0 value points have 0 area
  theme_bw()

print(gg_a_flu)
dev.off()
# strong mask use prior -- modify prior of mask in glb model object so glb model mask beta passed as gaussian prior to cascade
flu_beta_samples <- mrbrt001::core$other_sampling$sample_simple_lme_beta(sample_size = 1000L, model = flu_model)
sds_flu_model <- apply(flu_beta_samples, 2, sd)
flu_model$cov_models[[2]]$prior_beta_gaussian <- matrix(c(flu_model$fe_soln$mask_avg_month, 5*sds_flu_model[7]), nrow = 2) # 3_31, fe_soln was -1.847, se = 0.0529; 3_29_2021-A the fe_soln for measles masks was -2.535
flu_model$cov_models[[2]]$prior_beta_uniform <- matrix(c(-3, 0))


# cascading splines - only two levels (for SR and location) both are inherently cause specific because only feeding in data for 1 cause
flu_fit1 <- run_spline_cascade(
  stage1_model_object = flu_model,
  df = flu,
  col_obs = "log_ratio",
  col_obs_se = "log_ratio_se",
  col_study_id = "location_id",
  stage_id_vars = c("sr_cause", "loc_cause"),
  gaussian_prior = TRUE,
  inner_print_level = 1L,
  thetas = thetas_1,
  output_dir = flu_work_dir,
  model_label = "stage1",
  overwrite_previous = TRUE
)

############################################################################
######## MAKE PREDICTIONS AND PLOT FIT ######################################
############################################################################

### MAKE PREDICTIONS
# prep prediction frame for cascade model (used to predict for meas and flu), using obs mask value with obs mob value and sim mask vals with sim mob values for SR fits
pred_dt <- copy(mob_nat[location_id %in% countries & date < "2021-01-31"])
pred_dt <- merge(pred_dt, mask_nat, by=c("location_id", "year", "month", "date", "region_id"))

pred_loc_sim <- expand.grid(location_id = countries,
                            mob_avg_month = seq(0,.7,0.01),
                            mask_avg_month = sim_mask_val,
                            date = as.Date("2020-06-01"))
# SR Preds
sr_dt <- expand.grid(location_id = hierarchy[location_id == super_region_id, location_id],
                     mob_avg_month = seq(0,.7,0.01),
                     mask_avg_month = sim_mask_val,
                     date = as.Date("2020-06-01")) # arbitrary date to get SR preds for simulated mask and mob vals

# will be used for cause specific preds for global (location_id == 1) from the cascade model
glb_dt <- expand.grid(location_id = 1,
                      mob_avg_month = seq(0, .7, 0.01),
                      mask_avg_month = sim_mask_val,
                      date = as.Date("2020-06-01"))

pred_dt <- rbindlist(list(pred_dt, pred_loc_sim, sr_dt, glb_dt), fill = T)

pred_dt <- merge(pred_dt, hierarchy[, .(location_id, super_region_id, super_region_name)], by = "location_id", all.x = T)


# PREDICT OUT FROM  EACH CAUSE'S CASCADE MODEL ---------------------------
meas_pred_dt <- copy(pred_dt)
meas_pred_dt[, cause := "measles"]
meas_pred_dt[, loc_cause := paste0(location_id, "_", cause)]
meas_pred_dt[, sr_cause := paste0(super_region_id, "_", cause)]

meas_preds1 <- predict_spline_cascade(
  fit = meas_fit1,
  newdata = meas_pred_dt
) %>% as.data.table()

meas_preds1[, pred:= exp(pred)]
meas_preds1[, location_id := as.integer(location_id)]
meas_preds1 <- merge(meas_preds1, hierarchy[, .(location_id, location_name)], by = "location_id")


flu_pred_dt <- copy(pred_dt)
flu_pred_dt[, cause := "flu"]
flu_pred_dt[, loc_cause := paste0(location_id, "_", cause)]
flu_pred_dt[, sr_cause := paste0(super_region_id, "_", cause)]

flu_preds1 <- predict_spline_cascade(
  fit = flu_fit1,
  newdata = flu_pred_dt
) %>% as.data.table()

flu_preds1[, pred:= exp(pred)]
flu_preds1[, location_id := as.integer(location_id)]
flu_preds1 <- merge(flu_preds1, hierarchy[, .(location_id, location_name)], by = "location_id")

# for CHN, replace cascade est of disrup ratio with RegMod output dirup ratio (using regmod vs casc spl won't affect final meas case counts b/c use measles vals directly for CHN, but will impact flu)
chn <- chn[,.(cause, date, loc_cause, location_id, location_name = "China", mask_avg, mask_avg_month, mob_avg, mob_avg_month, month, pred = standardized_ratio, sr_cause, super_region_name, super_region_id)]
# remove and replace the non-simulated est for CHN
meas_preds1 <- meas_preds1[!(location_id == 6 & !is.na(month) & date %in% unique(chn[cause == "measles"]$date))]
meas_preds1 <- rbind(meas_preds1, chn[cause == "measles"], fill = TRUE)
flu_preds1 <- flu_preds1[!(location_id == 6 & !is.na(month) & date %in% unique(chn[cause == "flu"]$date))]
flu_preds1 <- rbind(flu_preds1, chn[cause == "flu"], fill = TRUE)

# If running for EMR handoff, make preds through April 2021 ----------------------
if(emr_project){
  pred_dt2 <- copy(mob_nat[location_id %in% countries])
  pred_dt2 <- merge(pred_dt2, mask_nat, by=c("location_id", "month", "date", "region_id", "year"))
  pred_dt2 <- merge(pred_dt2, hierarchy[, .(location_id, super_region_id, super_region_name)], by = "location_id", all.x = T)

  meas_pred_dt2 <- copy(pred_dt2)
  meas_pred_dt2[, cause := "measles"]
  meas_pred_dt2[, loc_cause := paste0(location_id, "_", cause)]
  meas_pred_dt2[, sr_cause := paste0(super_region_id, "_", cause)]

  meas_preds2 <- predict_spline_cascade(
    fit = meas_fit1,
    newdata = meas_pred_dt2
  ) %>% as.data.table()

  meas_preds2[, pred:= exp(pred)]
  meas_preds2[, location_id := as.integer(location_id)]
  meas_preds2 <- merge(meas_preds2, hierarchy[, .(location_id, location_name)], by = "location_id")


  flu_pred_dt2 <- copy(pred_dt2)
  flu_pred_dt2[, cause := "flu"]
  flu_pred_dt2[, loc_cause := paste0(location_id, "_", cause)]
  flu_pred_dt2[, sr_cause := paste0(super_region_id, "_", cause)]

  flu_preds2 <- predict_spline_cascade(
    fit = flu_fit1,
    newdata = flu_pred_dt2
  ) %>% as.data.table()

  flu_preds2[, pred:= exp(pred)]
  flu_preds2[, location_id := as.integer(location_id)]
  flu_preds2 <- merge(flu_preds2, hierarchy[, .(location_id, location_name)], by = "location_id")

  # SAVE 2 versions -- diff only for CHN
  # 1: better for if want smooth est if using as covariate -- version with China all from spline cascade to avoid discontinuity from switching from regmod to cascade spline at end of 2020
  fwrite(meas_preds2, paste0(meas_work_dir, "/measles_preds_thru_apr21_chn_spline.csv"))
  fwrite(flu_preds2, paste0(flu_work_dir, "/flu_preds_thru_apr21_chn_spline.csv"))

  # 2: using RegMod for CHN in all of 2020, then casc spline for jan-apr 2021
  # fix CHN for 2020 to be regmod outputs
  meas_preds2 <- meas_preds2[!(location_id == 6 & !is.na(month) & date %in% unique(chn[cause == "measles"]$date))]
  meas_preds2 <- rbind(meas_preds2, chn[cause == "measles"], fill = TRUE)
  flu_preds2 <- flu_preds2[!(location_id == 6 & !is.na(month) & date %in% unique(chn[cause == "flu"]$date))]
  flu_preds2 <- rbind(flu_preds2, chn[cause == "flu"], fill = TRUE)

  fwrite(meas_preds2, paste0(meas_work_dir, "/measles_preds_thru_apr21_chn_hybrid.csv"))
  fwrite(flu_preds2, paste0(flu_work_dir, "/flu_preds_thru_apr21_chn_hybrid.csv"))

}

################## PLOTTING DATA PREP, FUNCTION, AND FUNCTION CALL #######################
# Prep data for plots (using preds1 because only plotting results through Dec 2020) ----------------------------------------------------------------
# prep orig data for plotting
measles <- merge(measles, hierarchy[, .(location_id, location_name)], by = "location_id")
flu <- merge(flu, hierarchy[, .(location_id, location_name)], by = "location_id")

# bin mask values to those used for preds so can color on plot - mask_avg_month is the simulated mask vals for all global_pred dts, just copy over to bin column because it's already in bins we want
global_pred_meas[, mask_use_bin := mask_avg_month]
global_pred_flu[, mask_use_bin := mask_avg_month]

# bin some rows of preds
# rows corresponding to each month's actual estimated mask and mob data (vs simulated vals) will have val for month and for those we don't want to bin
# want to use these rows for time series plots of predictions for each month
meas_preds1[!is.na(month), mask_use_bin := sapply(mask_avg_month, find_closest, vector = sim_mask_val)]
meas_preds1[is.na(mask_use_bin), mask_use_bin := mask_avg_month]
flu_preds1[!is.na(month), mask_use_bin := sapply(mask_avg_month, find_closest, vector = sim_mask_val)]
flu_preds1[is.na(mask_use_bin), mask_use_bin := mask_avg_month]
# bin for observed measles and flu data for input data
measles[, mask_use_bin := sapply(mask_avg_month, find_closest, vector = sim_mask_val)]
flu[, mask_use_bin := sapply(mask_avg_month, find_closest, vector = sim_mask_val)]


#save the pred1 file!
fwrite(meas_preds1, file = paste0(meas_work_dir, "/preds1_measles.csv"))
fwrite(flu_preds1, file = paste0(flu_work_dir, "/preds1_flu.csv"))

# Make plots
# 1 pg per country, 8 plots per page: 1 column per cause
# in each column, 4 plots. 3 of these plots are mob vs disruption (1 for glb fit, 1 for SR fit, 1 for country fit) and 1 time series color points by mask use level, lines by mask use level
# these first two plots look very similar to gg and gg_a, just color of points is specified in a more explicit way (by official binning)
gg_glb_meas <- ggplot(global_pred_meas, aes(x = mob_avg_month, y = pred, color = as.factor(mask_use_bin)))+
  geom_point(data = measles, aes(y = plotting_ratio, size = weight), alpha = 0.1, show.legend = F)+
  geom_line(aes(color = as.factor(mask_avg_month)))+
  labs(title = "Global splines - Measles: disruption vs. mobility by average mask use value",
       x = "Average Mobility", # this is avg mobility disruption
       y = "Measles Disruption Ratio")+
  scale_y_continuous(limits = c(0, 10), oob = scales::squish)+
  scale_size_area() + # makes 0 have 0 area
  theme_bw() +
  theme(text = element_text(size = 8), legend.position = "bottom")

gg_glb_log_meas <- ggplot(global_pred_meas, aes(x = mob_avg_month, y = pred, color = as.factor(mask_use_bin)))+
  geom_point(data = measles, aes(y = plotting_ratio, size = weight), alpha = 0.25, show.legend = F)+
  geom_line(aes(color = as.factor(mask_avg_month)))+
  labs(title = "Global splines - Measles: disruption vs. mobility by average mask use value",
       x = "Average Mobility", # this is avg mobility disruption
       y = "Measles DR (log scale)")+
  scale_y_continuous(trans = "log10")+
  scale_size_area() + # makes 0 have 0 area
  theme_bw() +
  theme(text = element_text(size = 8), legend.position = "bottom")

gg_glb_flu <- ggplot(global_pred_flu, aes(x = mob_avg_month, y = pred, color = as.factor(mask_use_bin)))+
  geom_point(data = flu, aes(y = plotting_ratio, size = weight, color = as.factor(mask_use_bin)), alpha = 0.1, show.legend = F)+
  geom_line(aes(color = as.factor(mask_avg_month)))+
  labs(title = "Global splines - Flu: disruption vs. mobility by average mask use value",
       x = "Average Mobility",
       y = "Flu Disruption Ratio")+
  scale_y_continuous(limits = c(0, 10), oob = scales::squish)+
  scale_size_area() + # makes 0 have 0 area
  theme_bw() +
  theme(text = element_text(size = 8), legend.position = "bottom")

gg_glb_log_flu <- ggplot(global_pred_meas, aes(x = mob_avg_month, y = pred, color = as.factor(mask_use_bin)))+
  geom_point(data = flu, aes(y = plotting_ratio, size = weight), alpha = 0.25, show.legend = F)+
  geom_line(aes(color = as.factor(mask_avg_month)))+
  labs(title = "Global splines - Flu: disruption vs. mobility by average mask use value",
       x = "Average Mobility",
       y = "Flu DR (log scale)")+
  scale_y_continuous(trans = "log10")+
  scale_size_area() + # makes 0 have 0 area
  theme_bw() +
  theme(text = element_text(size = 8), legend.position = "bottom")


### HELPER FUNCTION FOR MAKING BIG VETTING PDF
sr_flu_plot <- function(data, my_super_region_name) {
  gg_sr_flu <- ggplot(data[grepl("sr_cause__", cascade_prediction_id) & super_region_name == my_super_region_name & cause == "flu" & is.na(month)], # CAUTION HEAVY HANDED FIGURE OUT WHY HAVE PREDS FOR BOTH OBS COUNTRY VALS AND SIM SR VALS FROM SR MODEL???
                      aes(x = mob_avg_month, y = pred, color = as.factor(mask_use_bin)))+
    geom_point(data = flu[super_region_name == my_super_region_name], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F)+
    geom_line() +
    scale_y_continuous(trans = "log10") +
    labs(title = paste0("SR Flu fit"),
         x = "Average Mobility",
         y = "Flu DR (log scale)") +
    theme_bw() +
    theme(text = element_text(size = 8), legend.position = "none")
}
sr_measles_plot <- function(data, my_super_region_name) {
  gg_sr_measles <- ggplot(data[grepl("sr_cause__", cascade_prediction_id) & super_region_name == my_super_region_name & is.na(month)], # CAUTION HEAVY HANDED FIGURE OUT WHY HAVE PREDS FOR BOTH OBS COUNTRY VALS AND SIM SR VALS FROM SR MODEL???
                          aes(x = mob_avg_month, y = pred, color = as.factor(mask_use_bin)))+
    geom_point(data = measles[super_region_name == my_super_region_name], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F)+
    geom_line() +
    scale_y_continuous(trans = "log10") +
    labs(title = paste0("SR Measles fit"),
         x = "Average Mobility",
         y = "Measles DR (log scale)")+
    theme_bw() +
    theme(text = element_text(size = 8), legend.position = "none")
}

sr_flu_plots <- lapply(unique(flu$super_region_name), sr_flu_plot, data = flu_preds1)
names(sr_flu_plots) <- unique(flu$super_region_name)

sr_measles_plots <- lapply(unique(measles$super_region_name), sr_measles_plot, data = meas_preds1)
names(sr_measles_plots) <- unique(measles$super_region_name)

# Make time series and print the plots
super_region_mob_model_plots <- function(my_work_dir, sr_name){

  pdf(paste0(my_work_dir, "/", sr_name, "_stage1_plots_by_country.pdf"), width = 8.5, height = 11)

  # one page for each country whether we have measles OR flu data or not, showing fit to overall data and fit to country data.
  # if did for(location in dt[location_id %in% countries)]), would only have pages for locs with flu OR measles data. This lets us see fits for all locs, incl those w/o data for either cause
  for(location in unique(hierarchy[location_id %in% countries & super_region_name == sr_name, location_id])){
    my_location_name <- hierarchy[location_id == location, location_name ]
    message(my_location_name)

    # plot smooth fits (from simulated mob data -- is.na(month) for each cause
    country_meas_sim <- ggplot(meas_preds1[grepl("loc", cascade_prediction_id) & location_id == location & is.na(month)],
                               aes(x = mob_avg_month, y = pred, color = as.factor(mask_use_bin)))+
      geom_point(data = measles[location_id == location], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F)+
      geom_line() +
      scale_y_continuous(trans = "log10") +
      labs(title = paste0("Measles fit"),
           x = "Average Mobility",
           y = "Measles DR (log scale)") +
      theme_bw() +
      theme(text = element_text(size = 8), legend.position = "none")

    country_flu_sim <- ggplot(flu_preds1[grepl("loc", cascade_prediction_id) & location_id == location & is.na(month)],
                              aes(x = mob_avg_month, y = pred, color = as.factor(mask_use_bin)))+
      geom_point(data = flu[location_id == location], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F)+
      geom_line() +
      scale_y_continuous(trans = "log10") +
      labs(title = paste0("Flu fit"),
           x = "Average Mobility",
           y = "CDR (log scale)") +
      theme_bw() +
      theme(text = element_text(size = 8), legend.position = "none")

    # plot model fit to actual mask and mob values as estimated by covid team for each month (!is.na(month)) over observed data
    measles_ts <- ggplot(meas_preds1[ location_id == location & !is.na(month)],
                         aes(x = month, y = pred)) +
      geom_point(data = measles[location_id == location], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F) +
      geom_line() + theme_bw() + scale_y_continuous(trans="log10") +
      labs(title = paste0("Measles"),
           y = "Predicted Ratio (log scale)",
           x = "Month") +
      theme_bw() +
      theme(text = element_text(size = 8), legend.position = "none")

    flu_ts <- ggplot(flu_preds1[location_id == location & !is.na(month)],
                     aes(x = month, y = pred)) +
      geom_point(data = flu[location_id == location], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F) +
      geom_line() + theme_bw() + scale_y_continuous(trans="log10") +
      labs(title = paste0("Flu"),
           y = "Predicted Ratio (log scale)",
           x = "Month") +
      theme_bw() +
      theme(text = element_text(size = 8), legend.position = "none")

    sr_measles <- sr_measles_plots[[sr_name]]
    sr_flu <- sr_flu_plots[[sr_name]]

    grid.arrange(grobs = list(gg_glb_log_meas, gg_glb_log_flu,
                              sr_measles, sr_flu,
                              country_meas_sim, country_flu_sim,
                              measles_ts, flu_ts),
                 ncol = 2, top = paste0(my_location_name, ": ", sr_name))
  }
  dev.off()
}

### Call big plotting function and track time ### (takes ~ 20 minutes if can parallelize over cl=4)
tic()
pblapply(unique(dt$super_region_name), super_region_mob_model_plots, my_work_dir = work_dir, cl = 4)
toc()

########################################
#### MAKE UNCERTAINTY INTERVALS ########
########################################

if(uncertainty){

  for(my_cause in c("measles", "flu")){

    if(my_cause == "measles") my_work_dir <- copy(meas_work_dir)
    if(my_cause == "flu") my_work_dir <- copy(flu_work_dir)
    draw_dir <- file.path(my_work_dir, "draws")
    if(!dir.exists(draw_dir)) dir.create(draw_dir, recursive = T)

    final_pred_dt <- mob_nat[date %in% last_days & year(date) == 2020][order(location_id, date)] # mob_nat should already only have last days in it
    final_pred_dt <- merge(final_pred_dt, mask_nat[date %in% last_days & year(date) == 2020], by = c("location_id", "date", "month", "year", "region_id"))
    final_pred_dt <- merge(final_pred_dt, hierarchy[, .(location_id, location_name, super_region_id, super_region_name)], by = "location_id")

    write.csv(final_pred_dt, file.path(my_work_dir, "pred_frame.csv"), row.names = F)

    write.csv(dt[cause == my_cause], file.path(my_work_dir, "input_data.csv"), row.names = F) # save input data to model

    for(loc_id in unique(dt[cause == my_cause]$location_id)){
      script <- file.path(spline_cascade_code_dir, "02b_simple_spline_cascade_draws_vpds.R")

      args <- paste(n_draws, loc_id, FALSE, my_work_dir, my_cause, region_weight, final_n_draws)
      mem <- "-l m_mem_free=5G"
      fthread <- "-l fthread=1"
      runtime <- "-l h_rt=00:05:00"
      archive <- "-l archive=TRUE"
      jname <- paste0("-N ", my_cause, "_loc_", loc_id)

      system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,
                   "-i /FILEPATH/ihme_rstudio_3631.img","-s",script,args))

    }


    # for super regions that have countries that do not have data in the input, this loop will run and sample from countries with countries in same SR more likely to be sampled
    for(sr_id in hierarchy[!(location_id %in% dt[cause == my_cause]$location_id) & level == 3, unique(super_region_id)]){
      script <- file.path(spline_cascade_code_dir, "02b_simple_spline_cascade_draws_vpds.R")

      args <- paste(n_draws, sr_id, TRUE, my_work_dir, my_cause, region_weight, final_n_draws)
      mem <- "-l m_mem_free=5G"
      fthread <- "-l fthread=1"
      runtime <- "-l h_rt=00:05:00"
      archive <- "-l archive=TRUE"
      jname <- paste0("-N ", my_cause, "_sr_", sr_id)

      system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,
                   "-i /FILEPATH/ihme_rstudio_3631.img","-s",script,args))

    }

    job_hold(my_cause)

    # Retry (with more memory) any countries or SRs that failed:
    message("retrying failed locs")
    for(loc_id in unique(dt[cause == my_cause]$location_id)){ #unique(dt[cause == my_cause]$location_id)

      if(paste0(loc_id, "_draws.csv") %in% list.files(draw_dir)){}else{
        script <- file.path(spline_cascade_code_dir, "02b_simple_spline_cascade_draws_vpds.R")
        args <- paste(n_draws, loc_id, FALSE, my_work_dir, my_cause, region_weight, final_n_draws)
        mem <- "-l m_mem_free=100G"
        fthread <- "-l fthread=1"
        runtime <- "-l h_rt=00:05:00"
        archive <- "-l archive=TRUE"
        jname <- paste0("-N ", my_cause, "_loc_", loc_id)

        system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,
                     "-i /FILEPATH/ihme_rstudio_3631.img","-s",script,args))
      }
    }

    for(sr_id in hierarchy[!(location_id %in% dt[cause == my_cause]$location_id) & level == 3, unique(super_region_id)]){

      if(paste0(sr_id, "_draws.csv") %in% list.files(draw_dir)){}else{
        script <- file.path(spline_cascade_code_dir, "02b_simple_spline_cascade_draws_vpds.R")

        args <- paste(n_draws, sr_id, TRUE, my_work_dir, my_cause, region_weight, final_n_draws)
        mem <- "-l m_mem_free=100G"
        fthread <- "-l fthread=1"
        runtime <- "-l h_rt=00:05:00"
        archive <- "-l archive=TRUE"
        jname <- paste0("-N ", my_cause, "_sr_", sr_id)

        system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,
                     "-i /FILEPATH/ihme_rstudio_3631.img","-s",script,args))
      }
    }

    job_hold(my_cause)

    final_pred_draws <- lapply(list.files(draw_dir, full.names = T), fread) %>% rbindlist()
    ratio_draws <- paste0("ratio_draw_", 0:(final_n_draws-1))
    final_pred_draws[, (ratio_draws) := lapply(.SD, as.numeric), .SDcols = ratio_draws]
    final_pred_draws[, date := as.Date(date)]

    final_pred_draws[, (ratio_draws) := lapply(.SD, function(x) ifelse(x > 1, 1, x)), .SDcols=ratio_draws]

    # clean up RegMod CHN draws and overwrite China draws from SR model with direct RegMod results for months we have observed cases for
    chn_draws <- fread(paste0("/FILEPATH/",username, "/gbd_2020/spline_cascade_models/", data_date, "/data_capped_w_ratio_draws.csv" ))[location_id == 6 & cause == my_cause]
    std_ratio_draws <- paste0("std_ratio_draw_", 0:999)
    keep_chn_cols <- c("location_id", "month", std_ratio_draws)
    chn_draws <- chn_draws[, keep_chn_cols, with = FALSE]

    # call function to calc adjustment scalar for CHN draws
    adj_chn_draws <- scale_regmod_draws(my_cause = my_cause, input_data_date = data_date, unadj_draws = chn_draws, casc_spl_draws = final_pred_draws)

    # # make ratio draw names match those of casc spl output

    adj_chn_draws[, date := timeLastDayInMonth(paste0("2020-", month, "-1")) %>% as.Date()]
    adj_chn_draws <- merge(adj_chn_draws, mob_nat, by = c("location_id", "month", "date"))
    adj_chn_draws <- merge(adj_chn_draws, mask_nat[,], by = c("location_id", "month", "date", "region_id", "year"))
    adj_chn_draws <- merge(adj_chn_draws, hierarchy[,.(location_id, super_region_id, super_region_name, location_name)], by = "location_id")

    final_pred_draws <- final_pred_draws[!(location_id == unique(chn_draws$location_id) & date %in% adj_chn_draws$date)]
    final_pred_draws <- rbind(final_pred_draws, adj_chn_draws)

    write.csv(final_pred_draws, file.path(file.path(my_work_dir, "all_draws.csv")), row.names = F)
    message("wrote out rbinded draws!")

    # save summarized draws
    final_pred_draws_long <- melt.data.table(final_pred_draws,
                                             id.vars = c("location_id", "month", "location_name", "super_region_name", "super_region_id"),
                                             measure.vars = patterns("ratio_draw_"),
                                             variable.name = "draw",
                                             value.name = "ratio")
    final_pred_sum <- final_pred_draws_long[, .(ratio_mean = mean(ratio),
                                                ratio_lower = quantile(ratio, p = 0.025),
                                                ratio_upper = quantile(ratio, p = 0.975)),
                                            by = c("location_id", "month", "location_name", "super_region_name", "super_region_id")] %>% unique()

    write.csv(final_pred_sum, file.path(file.path(my_work_dir, "summary_results.csv")), row.names = F)
    message("go team we love draws! wrote out summarized draws! ")
  }

  final_pred_sum_measles <- fread(paste0(meas_work_dir, "/summary_results.csv"))
  final_pred_sum_measles[, cause := "measles"]
  final_pred_sum_measles[, ratio_mean := as.numeric(ratio_mean)]
  final_pred_sum_measles[, ratio_lower := as.numeric(ratio_lower)]
  final_pred_sum_measles[, ratio_upper := as.numeric(ratio_upper)]

  final_pred_sum_flu <- fread(paste0(flu_work_dir, "/summary_results.csv"))
  final_pred_sum_flu[, cause := "flu"]
  final_pred_sum_flu[, ratio_mean := as.numeric(ratio_mean)]
  final_pred_sum_flu[, ratio_lower := as.numeric(ratio_lower)]
  final_pred_sum_flu[, ratio_upper := as.numeric(ratio_upper)]


  final_pred_sum <- rbind(final_pred_sum_measles, final_pred_sum_flu)


  # save pdf of plots to show uncertainty (same time series as in 8 plots per page file, but with ribbon addded)
  pdf(paste0(work_dir, "/pred_ratio_ts_with_uncert_w_sr.pdf"))
  for(location in unique(final_pred_sum[location_id %in% countries, location_id])){
    my_location_name <- hierarchy[location_id == location, location_name ]
    message(my_location_name)

    # plot model fit to observed mask and mob values (!is.na(month)) over observed data
    meas_ts_w_uncert <- ggplot(meas_preds1[ location_id == location & !is.na(month)],
                         aes(x = month, y = pred)) +
      geom_point(data = measles[location_id == location], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F) +
      geom_line() + theme_bw() + scale_y_continuous(trans="log10") +
      geom_line(data = final_pred_sum_measles[location_id == location], aes(y = ratio_mean), linetype = "dashed")+
      geom_ribbon(data = final_pred_sum_measles[location_id == location], aes(x = month, ymin = ratio_lower, ymax = ratio_upper), inherit.aes = FALSE, alpha = 0.15, show.legend = F) +
      labs(title = paste0("Measles"),
           y = "Predicted Ratio (log scale)",
           x = "Month") +
      theme_bw() +
      theme(text = element_text(size = 8), legend.position = "none")

    flu_ts_w_uncert <- ggplot(flu_preds1[location_id == location & !is.na(month)],
                     aes(x = month, y = pred)) +
      geom_point(data = flu[location_id == location], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F) +
      geom_line() + theme_bw() + scale_y_continuous(trans="log10") +
      geom_line(data = final_pred_sum_flu[location_id == location], aes(y = as.numeric(ratio_mean)), linetype = "dashed")+
      geom_ribbon(data = final_pred_sum_flu[location_id == location], aes(x = month, ymin = ratio_lower, ymax = ratio_upper), inherit.aes = FALSE, alpha = 0.15, show.legend = F) +
      labs(title = paste0("Flu"),
           y = "Predicted Ratio (log scale)",
           x = "Month") +
      theme_bw() +
      theme(text = element_text(size = 8), legend.position = "none")
    grid.arrange(meas_ts_w_uncert, flu_ts_w_uncert,
                 top = paste0(my_location_name, " (", hierarchy[location_id == location, ihme_loc_id], ")"))

  }
  dev.off()

  # Make set of plots for measles vetting that only inlcudes untrusted locs (locs that the ratio will be used for)
  pdf(paste0(work_dir, "/untrusted_locs_pred_ratio_ts_with_uncert_w_sr.pdf"))
  elimination_locs <- c("BTN", "LKA", "MDV", "CHN_354", "CHN_361", "TLS", "BHR", "KHM", "PRK", "OMN", "IRN", "JOR", "CHN") # elimination locations outside of trusted CN superregions.
  trusted_srs <- c(64, 31, 103)
  # set up trusted_locs
  trusted_locs <- hierarchy[super_region_id %in% trusted_srs | ihme_loc_id %in% elimination_locs, location_id]
  for(location in unique(final_pred_sum[!(location_id %in% trusted_locs), location_id])){
    my_location_name <- hierarchy[location_id == location, location_name ]
    message(my_location_name)

    # plot model fit to observed mask and mob values (!is.na(month)) over observed data
    meas_ts_w_uncert <- ggplot(meas_preds1[ location_id == location & !is.na(month)],
                               aes(x = month, y = pred)) +
      geom_point(data = measles[location_id == location], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F) +
      geom_line() + theme_bw() + scale_y_continuous(trans="log10") +
      geom_line(data = final_pred_sum_measles[location_id == location], aes(y = ratio_mean), linetype = "dashed")+
      geom_ribbon(data = final_pred_sum_measles[location_id == location], aes(x = month, ymin = ratio_lower, ymax = ratio_upper), inherit.aes = FALSE, alpha = 0.15, show.legend = F) +
      labs(title = paste0("Measles"),
           y = "Predicted Ratio (log scale)",
           x = "Month") +
      theme_bw() +
      theme(text = element_text(size = 8), legend.position = "none")

    flu_ts_w_uncert <- ggplot(flu_preds1[location_id == location & !is.na(month)],
                              aes(x = month, y = pred)) +
      geom_point(data = flu[location_id == location], aes(y = plotting_ratio, size = weight), alpha = 0.2, show.legend = F) +
      geom_line() + theme_bw() + scale_y_continuous(trans="log10") +
      geom_line(data = final_pred_sum_flu[location_id == location], aes(y = as.numeric(ratio_mean)), linetype = "dashed")+
      geom_ribbon(data = final_pred_sum_flu[location_id == location], aes(x = month, ymin = ratio_lower, ymax = ratio_upper), inherit.aes = FALSE, alpha = 0.15, show.legend = F) +
      labs(title = paste0("Flu"),
           y = "Predicted Ratio (log scale)",
           x = "Month") +
      theme_bw() +
      theme(text = element_text(size = 8), legend.position = "none")
    grid.arrange(meas_ts_w_uncert, flu_ts_w_uncert,
                 top = paste0(my_location_name, " (", hierarchy[location_id == location, ihme_loc_id], ")"))

  }
  dev.off()
}

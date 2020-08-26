#############################################################
##Date: 3/12/2019
##Purpose: Child script to actually run MR-BRT for CFs
##Notes: Parallelize your submission of the model
##Updates: 3/12: Run with 20% trim with new code
##Notes2: Use same data as before because that hasn't changed and still has covariates
##Notes3: Update to make predictions for all hospital data places
###########################################################

#### Setup -----
# Get arguments from qsub
rm(list = ls())
print(commandArgs())
commands <- commandArgs()[6]
user <- Sys.info()[7]

#bundle <- 3039
library(data.table)
library(parallel)
library(readr)
library(magrittr)
library(dplyr)
library(ggplot2)
library(gridExtra)
library(RMySQL)
library(stringr)
library(gtools)

source(paste0('db_utilities.R'))
source(paste0('mr_brt_plots.R'))

# Parse commands cause it like is maxing me out
# Bundle is the ICG or bundle you are taking in (the ID)
# Type: Bundle or ICG
# CF is the CF as a string, inputs are 'cf1', 'cf2', and 'cf3'
commands <- strsplit(commands, ',')
bundle <- commands[[1]][1] %>% print()
type <- commands[[1]][2] %>% print()
cf <- commands[[1]][3] %>% print()
run <- commands[[1]][4] %>% print()
draws <- commands[[1]][5] %>% as.logical() %>% print()
trim <- commands[[1]][6] %>% print()
write_folder <- commands[[1]][7] %>% print()

#locs <- get_location_metadata(35)
# Libraries and sources from Reed

repo_dir <- paste0(FILEPATH)
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

# Folders where your stuff is, depends on bundle and cf
# data_folder: contains prepped cf estimates, one file per bundle/cf
# write_folder: place for final estimates, stored in separate files for each age/sex combo
# bun_write_folder: place for model outputs, will contain raw model summaries/predictions for troubleshooting

data_folder <- paste0(FILEPATH)
bun_write_folder <- paste0(write_folder,'by_cf', type, '/')

print(write_folder)
if(!file.exists(bun_write_folder)){
  dir.create(bun_write_folder, recursive = TRUE)
}

bundle <- as.numeric(bundle)

# Set variable name based on cf (because logit for CF1)
if(cf == 'cf1'){
  y_var = 'logit_mean'
  se_var = 'logit_se'
} else{
  y_var = 'log_mean'
  se_var = 'log_se'
}

# Check if only a single sex or not. Check data from model or not. Also check if only one or two age groups
sex_check <- fread(paste0(data_folder, bundle, '.csv'))
sex_check[, num_sex_points := .N, by = 'sex_id']


#### MR-BRT function -----
### Checks for data availability by sex & age group. Only includes covariates where data exists. Includes age without a spline when limited age groups are available.
mrbrtrun <- function(tr = trim, nage, nsex, num_sex_points){
  if(nsex > 1 & num_sex_points > 5){
    print('Sex cov')
    # Check for 4 or less age groups, if so then don't run a 3-knot spline on age. Some are getting trimmed (bundle 82) but now this works.
    if(nage <= 4 & nage > 1){
      print('Age cov no splining')
      covs <- list( 
        #cov_info("zcov1", "Z"),
        #cov_info("log_haqi", "X"),
        cov_info("sex_id", "X"),
        cov_info("age_start", "X"))
    }else if(nage == 1){
      print('No age cov')
      covs <- list( 
        cov_info("sex_id", "X"))
    }else{
      print('Age cov')
      covs <- list( 
        cov_info("sex_id", "X"),
        cov_info('age_start', 'X',
                 gprior_mean = 0, 
                 gprior_var = 'inf',
                 bspline_gprior_mean = "0, 0, 0", 
                 bspline_gprior_var = "inf, inf, inf",
                 degree = 3,
                 n_i_knots = 2,
                 r_linear = TRUE,
                 l_linear = TRUE) )
    }
  }else{
    print('No sex cov')
    if(nage == 1){
      print('Only one age and one sex')
    }else if(nage < 3){
      print('Age cov no splining')
      covs <- list( 
        cov_info("age_start", "X"))
    }else{
      print('Age cov')
      covs <- list( 
        cov_info('age_start', 'X',
                 gprior_mean = 0, 
                 gprior_var = 'inf',
                 bspline_gprior_mean = "0, 0, 0", 
                 bspline_gprior_var = "inf, inf, inf",
                 degree = 3,
                 n_i_knots = 2, 
                 r_linear = TRUE,
                 l_linear = TRUE) )
    }
  }
  
  fit <- run_mr_brt(
    output_dir = bun_write_folder, 
    model_label = paste0("mrbrt_",bundle,"_",cf),
    data = paste0(data_folder, bundle, '.csv') ,
    mean_var = y_var,
    se_var = se_var,
    covs = covs,
    overwrite_previous = TRUE,
    #project = 'proj_hospital',
    method = 'trim_maxL', trim_pct = tr)
  return(fit)
}


#### Run model & check trimming -----
fit20 <- mrbrtrun(trim, nage = length(unique(sex_check$age_group_id)), nsex = length(unique(sex_check$sex_id)), num_sex_points = max(sex_check$num_sex_points))

# Sometimes models just don't run. Try again in that case.
tries <- 0
while(tries < 3 & !file.exists(paste0(write_folder,'by_cfbundle/mrbrt_',bundle,"_",cf,'/model_summaries.csv'))){
  print('Relaunching the model, something happened')
  fit20 <- mrbrtrun(trim, nage = length(unique(sex_check$age_group_id)), nsex = length(unique(sex_check$sex_id)), num_sex_points = max(sex_check$num_sex_points))
  tries <- tries + 1
}

if(tried == 3 & is.na(error)){
  error <- "model didn't run for this bundle!"
  model_stats <- data.frame("date" = as.character(Sys.time()),
                            "run" = run, 
                            "bundle" = bundle, 
                            "CF" = cf, 
                            "trim" = trim,
                            "sex_cov" = "",
                            "age_cov" = "",
                            "min_age_start" = "",
                            "max_age_start" = "",
                            "error" = error)
  write_csv(model_stats, paste0(write_folder, 'mr_brt_run_log.csv'), append = TRUE)
}

# Re-run model if age variability is getting trimmed. Need to make more flexible if two or three age groups (breaking with singular matrix)
w_check <- fread(paste0(bun_write_folder,'mrbrt_',bundle,'_',cf,'/train_data.csv'))
w_check <- w_check[w == 1]
nage_w <- length(unique(w_check$age_start))
nsex_w <- length(unique(w_check$sex_id))

# Setting dummy n_sex_points - doesn't make sense w trimmed data
if(nage_w <= 3 | nsex_w != 2){
  print('Re-running model since variability lost with trimming')
  fit20 <- mrbrtrun(trim, nage = nage_w, nsex = nsex_w, num_sex_points = 5)
}

error <- NA

# Log if model doesn't get run
if (!dir.exists(paste0(write_folder,'by_cfbundle/mrbrt_',bundle,"_",cf)) & is.na(error)){
  error = "model didn't run for this bundle!"
  model_stats <- data.frame("date" = as.character(Sys.time()),
                            "run" = run, 
                            "bundle" = bundle, 
                            "CF" = cf, 
                            "trim" = trim,
                            "sex_cov" = "",
                            "age_cov" = "",
                            "min_age_start" = "",
                            "max_age_start" = "",
                            "error" = error)
  write_csv(model_stats, paste0(write_folder, 'mr_brt_run_log.csv'), append = TRUE)
}


# Get restrictions -----
# Download restrictions to see what ages and sexes there should be. Subset preddf for it
# Sets the min and max to PREDICT for
restrictions <- get_bundle_restrictions() %>% as.data.table() %>% .[bundle_id == bundle]
preddf <- fread(paste0(write_folder,'age_sex_preddf.csv'))
preddf <- preddf[age_start >= restrictions$yld_age_start & age_start <= restrictions$yld_age_end]
if(restrictions$male == 0 & restrictions$female == 1){
  preddf <- preddf[sex_id == 2]
} else if(restrictions$male == 1 & restrictions$female == 0){
  preddf <- preddf[sex_id == 1]
}

# Re-configure ages to what they're being predicted on
preddf[, age_start := (age_start + age_end)/2]
preddf[age_start == 110, age_start := 97.5]

## Predict from model ----
# Takes an argument whether to write draws or not which drastically increases the time it takes to run
print(draws)
if(draws == TRUE){
  pred20 <- predict_mr_brt(fit20, newdata = preddf, write_draws = TRUE) 
  print('YAY, done! Draws written')
  
} else{
  pred20 <- predict_mr_brt(fit20, newdata = preddf, write_draws = FALSE)
}

### Extensions -----
# Extend using min and max ages in input data to apply model fit from where data exists to age groups without data. Otherwise MR-BRT can have crazy tail behavior.
train_data <- fit20$train_data %>% as.data.table()
train_data[w < 0.5, trimmed := 'Trimmed data'][w >= 0.5, trimmed := 'Untrimmed data']
train_data[sex_id == 1, sex := 'Males'][sex_id == 2, sex := 'Females']
untrimmed <- train_data[trimmed == 'Untrimmed data']
untrimmed[, num_points := .N, by = 'age_start']

# Sets the min and max THRESHOLD of age to predict at (due to unstability in data sparse ages)
min_age <- min(untrimmed[w == 1 & num_points >= 10]$age_start)
max_age <- max(untrimmed[w == 1 & num_points >= 10]$age_start)

# If there are less than 10 points (or less than 2 later)
# Iterate through a series of thresholds.
if(min_age == Inf){
  min_age <- min(untrimmed[w == 1 & num_points >= 5]$age_start)
  max_age <- max(untrimmed[w == 1 & num_points >= 5]$age_start)
}

if(min_age == Inf){
  min_age <- min(untrimmed[w == 1 & num_points >= 2]$age_start)
  max_age <- max(untrimmed[w == 1 & num_points >= 2]$age_start)
}

if(min_age == Inf){
  min_age <- min(untrimmed[w == 1 & num_points >= 1]$age_start)
  max_age <- max(untrimmed[w == 1 & num_points >= 1]$age_start)
}

# Reset minimum age to the neonatal age if the min age is 0.5
if(min_age == 0.5){
  min_age <- min(preddf$age_start)
}

sex_cov <- as.logical("sex_id" %in% fit20$input_metadata$covariate)
age_cov <- as.logical("age_start" %in% fit20$input_metadata$covariate)

extender <- function(df, preddf, sex_cov, age_cov){
  # Subset to only the ages you are going to predict for, will extend out from there based on min and max in preddf
  if(age_cov){
    df <- df[X_age_start >= min_age & X_age_start <= max_age] %>% unique()
  }

  # Add sexes if the model didn't predict on them
  if(!sex_cov){
    df1 <- copy(df) %>% .[, X_sex_id := 1]
    df2 <- copy(df) %>% .[, X_sex_id := 2]
    df <- rbind(df1, df2) 
    df <- df[X_sex_id %in% unique(preddf$sex_id)]
    df[, sex := NULL] # This was showing up weirdly. May have been an extraneous line somewhere below. Nothing wrong with adding this here though to be safe
  }
  
  limits <- df[X_age_start == min_age | X_age_start == max_age]
  stopifnot('X_sex_id' %in% names(limits))
  #limits <- limits[, c('X_sex_id', 'X_age_start', 'Y_mean', 'Y_mean_lo', 'Y_mean_hi')]
  setnames(limits, 'X_age_start', 'limit')
  limits <- unique(limits)
  
  # Make limits square for the sexes you want to predict for
  extra_ages <- preddf[age_start < min_age | age_start > max_age]
  extra_ages[age_start <= min_age, limit := min_age][age_start >= max_age, limit := max_age]
  setnames(extra_ages, 'sex_id', 'X_sex_id')
  
  # Merge on extra ages to limits to extend
  extend <- merge(limits, extra_ages, by = c('limit', 'X_sex_id'), allow.cartesian = TRUE)
  setnames(extend, 'age_start', 'X_age_start')
  
  df <- rbind(df, extend, fill = TRUE)
  df[X_sex_id == 1, sex := 'Males'][X_sex_id == 2, sex := 'Females']
  df <- df %>% select(-index, -limit, -age_group_id, -age_end, -sex)
  
  return(df)
    
}

# Extend both draws and predictions
pred20_draws <- pred20$model_draws %>% as.data.table()
pred20_cfs <- pred20$model_summaries %>% as.data.table()
pred20_cfs <- pred20_cfs[,c('X_sex_id', 'X_age_start', 'Y_mean', 'Y_mean_lo', 'Y_mean_hi')]

model_draws <- extender(pred20_draws, preddf, sex_cov, age_cov)
model_draws <- model_draws %>%
  select(-X_intercept, -Z_intercept)
all_cfs <- extender(pred20_cfs, preddf, sex_cov, age_cov)

# Data validation -----
# Series of checks for age and sex. Should be no NAs and every demographic in preddf should be there
stopifnot(nrow(all_cfs[is.na(X_age_start)]) == 0 & 
            nrow(all_cfs[is.na(X_sex_id)]) == 0 &
            nrow(model_draws[is.na(X_age_start)]) == 0 & 
            nrow(all_cfs[is.na(X_sex_id)]) == 0 &
            nrow(model_draws) == nrow(all_cfs)) 
            #nrow(model_draws[, count := .N, by = list(X_age_start, X_sex_id)][count > 1]) != 0 &
            #nrow(all_cfs[, count := .N, by = list(X_age_start, X_sex_id)][count > 1]) != 0)

model_stats <- data.frame("date" = as.character(Sys.time()),
                    "run" = run, 
                    "bundle" = bundle, 
                    "CF" = cf, 
                    "trim" = trim,
                    "sex_cov" = sex_cov,
                    "age_cov" = age_cov,
                    "min_age_start" = min_age,
                    "max_age_start" = max_age,
                    "error" = "")

# Final formatting -----
age_groups <- fread(paste0(write_folder,'age_groups.csv'))

ernielite <- function(df){
  # Modify age variables
  df <- merge(df, age_groups, by.x = "X_age_start", by.y = "age_midpoint")
  
  df[, "cf" := cf]
  df[, "bundle_id" := bundle]
  df[, "X_age_start" := NULL]
  setnames(df, 'X_sex_id', 'sex_id')
  setcolorder(df, c('age_start', 'age_group_id', 'sex_id', 'bundle_id', 'cf'))
  
  # Get outta log/logit space
  if(cf == 'cf1'){
    tform <- function(x) inv.logit(x)
  } else {
    tform <- function(x) exp(x)
  }
  df[,6:length(df)] <- lapply(df[,6:length(df)], tform)
  return(df)
}

all_cfs <- ernielite(all_cfs)
model_draws <- ernielite(model_draws)

# Write data ----
# Writes one row to a run log w/ metadata about each model run. Also writes prediction for each age/sex/bundle/cf as rows in the estimates file and draws file.
write_csv(model_stats, paste0(write_folder, 'mr_brt_run_log.csv'), append = TRUE)
write_csv(all_cfs, paste0(write_folder, 'cf_estimates.csv'), append = TRUE)
# Writes plots
dir.create(paste0(write_folder,'validationplots/'))
pdf(paste0(write_folder,'validationplots/',bundle,'_',cf,'_model_plots.pdf'), onefile = TRUE, height = 9, width = 8)
cf_plots(bundle, cf, write_folder)
dev.off()

model_draws <- split(model_draws, by = c("age_group_id", "sex_id"))
lapply(names(model_draws), function(x){
  xn <- str_replace(x, "[.]","_")
  write_csv(model_draws[[x]], path = paste0(write_folder, "by_agesex/", xn, ".csv", sep = ""))
})

print('DONE!!')





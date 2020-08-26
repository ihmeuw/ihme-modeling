
#######################################################################################################
## Prepare data for network
## Step 1. Prepared dataset for network
## Step 2. Run network meta-analysis with MR-BRT with logit transformation
## Step 3. Apply ratios
## Step 4. Save datasets with mean original and mean adjusted
## Step 5. Save datasets with adjusted mean only ready to upload
########################################################################################################
##
## Perform a crosswalk with logit transformation
##
## Step 1. Find matches
##
## Step 2. Logit transform the mean prevalence of the reference and alternative definition 
## logit(p) = log(p/1-p) = log(p) - log(1-p)
## invlogit function = exp(x)/(1 + exp(x)
##
## Step 3. Estimate the "ratio"  =  difference  of the logit transformed  mean prevalence reference and alternative definition
## diff_logit = logit_ref - logit_alt
##
## Step 4. Logit transform the standard error of the alternative and reference means
##
## Step 5. Calculate the standard error of the logit difference
####################################################################

## ******************************************************************
##  Set enviroment
## ******************************************************************

rm(list=ls())

library('openxlsx')
library('tidyr')
library('dplyr')
library("LaplacesDemon")
library("carData")
library("car")
library('boot')

source(paste0(FILEPATH/get_ids.R"))

## ******************************************************************
## Step 1. Prepare data to crosswalk (find matches)
## ******************************************************************

save_dir <- FILEPATH
outdir <- FILEPATH # where you want your mr-brt folder saved
outdirectory  <- FILEPATH # where you want your mr-brt folder saved

bundle <- 203
bundle_id <- 203
bundle_name <- "Genital prolapse"
measure_name <- 'prevalence'
measure <- 'prevalence'

match_type <- 'between_study' # between_study or within_study
trim <- 'trim' # trim or no_trim

map <- fread(FILEPATH)
bun_metadata <- map[bundle_id == bun_id]

age_map <- fread(FILEPATH)
age_map$order <- NULL

sex_names <- fread(FILEPATH)


####################################################################################
# reading and formatting all split data  #
##########################################
# split <- fread(paste0("/share/mnch/crosswalks/agesex_split_data/split_data_only/", bundle, "_prevalence.csv"))
bun_data  <- fread(paste0(save_dir, "FILEPATH", bundle, "_", measure, "_for_modeling.csv" ))
all_split <- fread(paste0(save_dir, "FILEPATH/", bundle, "_", measure, "_for_modeling.csv" ))
all_split <- as.data.table(all_split)

all_split <- merge(all_split, sex_names, by = 'sex', all.x = TRUE)

all_split <- merge(all_split, age_map, by = c('age_start', 'age_end'), all.x = TRUE)
all_split <- all_split[, age := round((age_start + age_end)/2)]

all_split <- all_split[, year_id := round((year_start + year_end) / 2)]
all_split <- all_split[, year := year_id]

all_split <- all_split[, c('nid', 'location_id', 'location_name', 'age', "year_id", 'mean', 'standard_error', 'sample_size', cv), with = F]

cv <- c('cv_clinical', 'cv_acog',  'cv_self_report', 'cv_dx_symptomatic_only')

####################################################################################
# Find all possible matches
##########################################

match_split <- split(all_split, all_split[, c(cv), with = FALSE])

if(length(match_split) == 2){
  
  if(match_type == 'within_study'){
    matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('nid', 'location_id', 'year_id', 'age', 'sex_id', 'sample_size'), all.x = TRUE, allow.cartesian=TRUE)
  } 
  if(match_type == 'between_study'){
    matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('location_id', 'age'), all.x = TRUE, allow.cartesian=TRUE)
  }
  
  matched_merge <- matched_merge[!is.na(mean.y)]
  matched_merge <- matched_merge[!mean.x==0] # uncomment me if you want to get rid of ALTERNATE means that are zero
  matched_merge <- matched_merge[!mean.y==0] # uncomment me if you want to get rid of REFERENCE means that are zero
  
  matched_merge <- matched_merge[, mean:= mean.x / mean.y]
  matched_merge <- matched_merge[, standard_error:= sqrt( (mean.x^2 / mean.y^2) * ( (standard_error.x^2 / mean.x^2) + (standard_error.y^2 / mean.y^2) ) ) ]
  if(nrow(matched_merge) == 0){
    print(paste0('no ', match_type, ' matches between demographics for ', cv))
  } else{
    write.csv(matched_merge, paste0(outdirectory, bundle, "_", cv, "_", refe, ".csv"), row.names = FALSE)
  }
  
} else{
  print('only one value for given covariate')
}


######################################################################################################
##Run single MR BRT for each comparison

repo_dir <- FILEPATH
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "mr_brt_functions.R"))

library(dplyr)
library(data.table)
library(metafor)
library(msm)
library(msm)
library(msm)
library(readxl)
library(plyr)


# Select variables of interest

logit_metareg <- as.data.frame(matched_merge) %>%
  select(age, location_name.x, paste0(cv, ".x"), mean.x, standard_error.x,  mean.y, standard_error.y, mean, standard_error)

# Logit transform the means and compute the "ratio" == difference in logit space
logit_metareg <- (logit_metareg) %>%
  mutate (logit_mean_alt = logit(mean.x)) %>%
            mutate (logit_mean_ref = logit(mean.y)) %>%
                      mutate(logit_diff = logit_mean_alt - logit_mean_ref)
                    
                    
# Logit transform the SE of the altermative mean
logit_metareg$logit_se_alt<- sapply(1:nrow(logit_metareg), function(a) {
 ratio_a <- logit_metareg[a, "mean.x"]
 ratio_se_a <- logit_metareg[a, "standard_error.x"]
 deltamethod(~log(x1), ratio_a, ratio_se_a^2)
})


# Logit transform the SE of the reference mean
logit_metareg$logit_se_ref<- sapply(1:nrow(logit_metareg), function(r) {
  ratio_r <- logit_metareg[r, "mean.y"]
  ratio_se_r <- logit_metareg[r, "standard_error.y"]
  deltamethod(~log(x1), ratio_r, ratio_se_r^2)
})


# Calculate the standard error of the ratio = logit  difference
logit_metareg$logit_se_diff <- sqrt(logit_metareg$logit_se_alt^2 + logit_metareg$logit_se_ref^2)




#####################################################################################################
## Create comparisons and covariates for the network

# dummy k = 1 if k is the numerator = mean_1, 0 otherwise
fib_network <-logit_metareg %>%
  mutate(comparison = paste0(dx_1, "/", dx_2)) 

fib_network <- fib_network %>%
  mutate(cv_clinical = if_else(dx_1 == "clinical", 1, 0)) %>%
         mutate(cv_self_report = if_else(dx_1 == "self_report", 1, 0)) %>%
         mutate(cv_symptomatic =  if_else(dx_1 == "symp", 1,0))
      
         
        
#dummy k =  -1  if k is the denominator
  fib_network$cv_clinical[(fib_network$dx_2 == "clinical")] <- -1
  fib_network$cv_self_report[(fib_network$dx_2 == "self_report")] <- -1
  fib_network$cv_symptomatic[(fib_network$dx_2 == "symp")] <- -1

  View(fib_network)

write.xlsx(fib_network, 
                     file = paste0(save_dir, "network_prep/02_input_mrbrt_network/", bun_id, "_net_prep.xlsx"),
                     row.names = FALSE)

#######################################################################################################################
###############################################################################################################

#matched_merge <- fib_network

fib_network <- fread(paste0("FILEPATH", bun_id, "_net_prep_exc_means.csv"))


# Exclude observations with mean < 0.005 
matched_merge <- fib_network %>%
  filter(mean_1 >0.005) %>%
  filter(mean_2 >0.005)

write.xlsx(matched_merge, 
                     file = paste0(save_dir, "network_prep/02_input_mrbrt_network/", bun_id, "_net_prep_exc_means_s4.xlsx"),
                     row.names = FALSE)


dat_metareg <- as.data.frame(matched_merge)
extra_vars <- c("cv_clinical", "cv_self_report", "cv_symptomatic")


# Select variables of interest
logit_metareg <- as.data.frame(matched_merge) %>%
  select(age, location_name.x, mean_1, se_1,  mean_2, se_2, mean, standard_error, extra_vars)


# Logit transform the means and compute the "ratio" == difference in logit space
logit_metareg <- (logit_metareg) %>%
  mutate (logit_mean_alt = logit(mean_1)) %>%
  mutate (logit_mean_ref = logit(mean_2)) %>%
  mutate(logit_diff = logit_mean_alt - logit_mean_ref)


# Logit transform the SE of the altermative mean
logit_metareg$logit_se_alt<- sapply(1:nrow(logit_metareg), function(a) {
  ratio_a <- logit_metareg[a, "mean_2"]
  ratio_se_a <- logit_metareg[a, "se_2"]
  deltamethod(~log(x1), ratio_a, ratio_se_a^2)
})


# Logit transform the SE of the reference mean
logit_metareg$logit_se_ref<- sapply(1:nrow(logit_metareg), function(r) {
  ratio_r <- logit_metareg[r, "mean_1"]
  ratio_se_r <- logit_metareg[r, "se_1"]
  deltamethod(~log(x1), ratio_r, ratio_se_r^2)
})


# Calculate the standard error of the ratio = logit  difference
logit_metareg$logit_se_diff <- sqrt(logit_metareg$logit_se_alt^2 + logit_metareg$logit_se_ref^2)

# Standardize variable names 
ratio_var <- "logit_diff"
ratio_se_var <- "logit_se_diff"
extra_vars <- extra_vars
metareg_vars <- c(ratio_var, ratio_se_var, extra_vars)

tmp_metareg <- as.data.frame(logit_metareg) %>%
  .[, metareg_vars] %>%
  setnames(metareg_vars, c("ratio_logit", "ratio_se_logit", extra_vars))


tmp_metareg$intercept <- 1

source("/FILEPATH/mr_brt_functions.R")
tmp_metareg <- fread("/FILEPATH/input_data.csv")

outdir <- "/FILEPATH/"
bundle <- 203
mod_lab <- paste0(bundle,  "_", 'network_logit')

fit_1 <- run_mr_brt(
  output_dir = outdir, # user home directory
  model_label = mod_lab,
  data = tmp_metareg,
  mean_var = "ratio_logit",
  se_var = "ratio_se_logit",
  covs =  list(
    cov_info("cv_clinical", "X"),
    cov_info("cv_self_report", "X"),
    cov_info("cv_symptomatic", "X")),
  trim_pct = 0.1,
  method = "trim_maxL",
  remove_x_intercept = TRUE,
  overwrite_previous = TRUE
)

plot <- plot_mr_brt(fit_1)

########################################################

## Apply ratios

check_for_outputs(fit_1)
results <- load_mr_brt_outputs(fit_1)
names(results)
coefs <- results$model_coefs
metadata <- results$input_metadata

#######
# Create data sets with standarized variables names

###Apply ratios 
# load all original bundle data
# -- read in data frame
#dat_original <- fread(paste0("/ihme/mnch/crosswalks/gyne/network_prep/00_clinical_crosswalked/step4/", bun_id, "_for_modeling_s4.csv"))
dat_original <- read_xlsx("/FILEPATH/203_prevalence_for_modeling.xlsx")

dat_original <- as.data.table(dat_original)
glimpse(dat_original)



# --  identify reference studies
dat_original <- dat_original %>%
  mutate(cv_case_def_acog = 0) %>%
  mutate(cv_self_report = 0) %>% 
  mutate(cv_dx_symptomatic_only  = 0) %>%
  mutate(cv_case_def_acog = 0) %>%
  mutate(cv_clinical =  1) %>%
  mutate(dx =  "NA")
  

dat_original[, reference := 0]
dat_original[cv_case_def_acog  == 1, reference := 1]
dat_original[, age := round((age_start + age_end)/2)]
dat_original[, dx := "NA"]
dat_original[cv_self_report == 1, dx := "self_report"]
dat_original[cv_dx_symptomatic_only == 1,  dx := "symptomatic"]
dat_original[cv_clinical == 1,  dx := "clinical"]

# -- identify mean and standard error variables and covariates
reference_var <- "reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- c("cv_clinical", "cv_self_report", "cv_dx_symptomatic_only")

# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var, cov_names)
# metareg_vars2 <- c(ratio_var, ratio_se_var, extra_vars) # defined before running the model

tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

#transform original data <0
tmp_orig$mean[(tmp_orig$mean == 0 & tmp_orig$ref == 0)] <- 0.025

tmp_orig$mean_logit <- logit(tmp_orig$mean)
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

df_pred <- as.data.frame(tmp_orig[, cov_names]) # cov_names from line 55
names(df_pred) <- extra_vars
pred1 <- predict_mr_brt(fit_1, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

tmp_preds <- preds %>%
  mutate(
    beta0 = Y_mean,
    # don't need to incorporate tau as with metafor; MR-BRT already included it in the uncertainty
    beta0_se_tau = (Y_mean_hi - Y_mean_lo) / 3.92) %>%
  select(beta0, beta0_se_tau)


tmp_orig <- cbind(tmp_orig, tmp_preds)
tmp_orig <- select(tmp_orig, -cv_clinical, -cv_self_report, -cv_dx_symptomatic_only)

tmp_orig2 <- tmp_orig %>%
  mutate(
    mean_logit_tmp = mean_logit - beta0, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    var_logit_tmp = se_logit^2 + beta0_se_tau^2, # adjust the variance
    se_logit_tmp = sqrt(var_logit_tmp)
  )

# if original data point was a reference data point, leave as-is
# if original data point was a reference data point, leave as-is
tmp_orig3 <- tmp_orig2 %>%
  mutate(
    mean_logit_adjusted = if_else(ref == 1, mean_logit, mean_logit_tmp),
    se_logit_adjusted = if_else(ref == 1, se_logit, se_logit_tmp),
    lo_logit_adjusted = mean_logit_adjusted - 1.96 * se_logit_adjusted,
    hi_logit_adjusted = mean_logit_adjusted + 1.96 * se_logit_adjusted,
    mean_adjusted = invlogit(mean_logit_adjusted),
    lo_adjusted = invlogit(lo_logit_adjusted),
    hi_adjusted = invlogit(hi_logit_adjusted) )

head(tmp_orig3)
tail(tmp_orig3)


#estimate the adjusted SE using the delta method
tmp_orig3$se_adjusted <- sapply(1:nrow(tmp_orig3), function(i) {
  ratio_i <- tmp_orig3[i, "mean_logit_adjusted"]
  ratio_se_i <- tmp_orig3[i, "se_logit_adjusted"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})


# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original, 
  tmp_orig3[, c("ref", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)


final_data[is.na(se_adjusted) & ref == 1, se_adjusted := standard_error]
final_data[ref == 1, mean_adjusted:= mean]
final_data[ref == 1, se_adjusted:= standard_error]
final_data[ref == 1, hi_adjusted := upper]
final_data[ref == 1, lo_adjusted := lower]
final_data[ref == 1, hi_adjusted := upper]

write.csv(final_data,
          file = paste0(save_dir, "/FILEPATH/", bun_id, "_", measure, "_s4_rerun.csv"),
          row.names = FALSE)

## Delete duplicative mean and se rows for modeling
final_data$is_outlier[(final_data$mean == 0 & final_data$cv_clinical == 1)] <- 1 
final_data$is_outlier[(final_data$mean <= 0.005 & final_data$cv_clinical == 1)] <- 1 

for_modeling <- final_data %>%
  mutate(mean = mean_adjusted) %>%
  mutate(lower = lo_adjusted) %>%
  mutate(upper = hi_adjusted) %>%
  mutate(standard_error = se_adjusted) %>%
  mutate(step2_location_year = if_else(ref == 0, "crosswalked clinical/sympt/selfreport data, logit MR-BRT", "original data point"))


write.csv(for_modeling,
          file = paste0(save_dir, "network_prep/04_adjusted_data/step4/", bun_id, "_for_modeling_rerun.csv"),
          row.names = FALSE)




# #######################################################################################
# ############################# MATCH AND MR-BRT SCRIPT #################################
# #######################################################################################
# Purpose: 
#   - matches data between alternate to reference covariate values, and creates mean ratio and standarderror
#   - runs a mr-brt model and generates a plot mr-brt job call
# 
# Arguments:
#   - outdir -- where you want your mr-brt folder saved, users must change this to their own directory
#   - bundle -- bundle_id of data to run
#   - cv -- covariate of interest, only runs a single covariate at a time
#   - measure -- measure of data to be run, either prevalence or incidence
#       - user specified at the moment, as not all bundle data have both measures
#   - match_type -- either between or within study
#       - columns to match on can be changed within the function manually
#   - trim -- either trim or no-trim
#   - trim_percent -- percentage of trimming desired
# 
# #######################################################################################

### set environment
rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- paste0("USERNAME")
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
# library('pdftools')
library("LaplacesDemon")
library("car")

source("FILEPATH")

####################################################################################
# set arguments
##########################################
outdir <- "FILEPATH" 

args <- commandArgs(trailingOnly = TRUE)
bundle <- args[1]
measure <- args[2]
trim <- args[3]
trim_percent <- args[4]

print(args)

bundle <- 261
measure <- 'prevalence'
trim <- 'trim' # trim or no_trim
trim_percent <- 0.2

cv <- 'cv_d_conversion'
match_type <- 'within_study' # between_study or within_study

if (trim_percent == 0) {
  mod_lab <- paste0(bundle, "_", cv, "_", match_type, '_', trim)
} else {
  mod_lab <- paste0(bundle, "_", cv, "_", match_type, '_', trim, '_', trim_percent)
}

####################################################################################
# reading and formatting all split data  #
##########################################
all_split <- fread(paste0("FILEPATH"))
#incidence_rows <- all_split[all_split$cv_dmf_incidence == 1 & cv_dmf_units_surfaces == 1]
#non_incidence <- all_split[!all_split$cv_dmf_incidence == 1]

age_map <- fread("FILEPATH")
age_map$order <- NULL
all_split <- all_split[,unique(names(all_split)),with=FALSE]
age_map$age_end[2] <- 0.077

locs <- get_location_metadata(35, gbd_round_id = 6)
#locs_super <- c('location_id', 'super_region_name')
locs <- locs[, c('location_id', 'super_region_id')]
sdi_map <- fread('FILEPATH')

all_split <- merge(all_split, locs, by = 'location_id', all.x = TRUE)
all_split <- merge(all_split, sdi_map, by = 'super_region_id', all.x = TRUE)

#inc_rows <- all_split[!all_split$cv_whs == 1]
#whs_rows <- all_split[all_split$cv_whs == 1]


#drop any rows where cv_excludes_chromos == 1 if were running livestill
if(cv %like% "cv_livestill"){
all_split <- all_split[is.na(cv_excludes_chromos) | cv_excludes_chromos == 0]
all_split <- all_split[is.na(cv_worldatlas_to_total) | cv_worldatlas_to_total == 0]
all_split <- all_split[is.na(cv_icbdms_to_total) | cv_icbdms_to_total == 0]
all_split <- all_split[is.na(cv_ndbpn_to_total ) | cv_ndbpn_to_total == 0]
all_split <- all_split[is.na(cv_congmalf_to_total) | cv_congmalf_to_total == 0]
}

#drop any rows where cv_livestill == 1 if were running chromo
if(cv %like% "cv_excludes_chromos"){
  all_split <- all_split[is.na(cv_livestill) | cv_livestill == 0]
  all_split <- all_split[is.na(cv_worldatlas_to_total) | cv_worldatlas_to_total == 0]
  all_split <- all_split[is.na(cv_icbdms_to_total) | cv_icbdms_to_total == 0]
  all_split <- all_split[is.na(cv_ndbpn_to_total ) | cv_ndbpn_to_total == 0]
  all_split <- all_split[is.na(cv_congmalf_to_total) | cv_congmalf_to_total == 0]
}


sex_names <- fread("FILEPATH")
all_split <- merge(all_split, sex_names, by = c('sex'), all.x = TRUE)

all_split <- merge(all_split, age_map, by = c('age_start', 'age_end'), all.x = TRUE)

all_split <- all_split[all_split$narrow_age == 1, age_start := orig_age_start]
all_split <- all_split[all_split$narrow_age == 1, age_end := orig_age_end]

all_split <- all_split[, age := round((age_start + age_end)/2)]


all_split <- all_split[, year_id := round((year_start + year_end) / 2)]

all_split <- all_split[!is.na(standard_error)]


all_split <- all_split[!all_split$cv_atchloss4ormore == 1 | !all_split$cv_atchloss5ormore == 1]

all_split <- all_split[, c('nid', 'location_id', 'sex_id', 'age', 'age_start', 'age_end', 'year_id', 'mean', 'standard_error', 'sample_size', cv), with = F]

#only keep rows where the mean is not super high
print(paste0('# rows with mean > 0.01: ',nrow(all_split[mean > 0.01])))

####################################################################################
# match data
##########################################

match_split <- split(all_split, all_split[, c(cv), with = FALSE])

if(length(match_split) == 2){

  if(match_type == 'within_study'){
    matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('nid', 'location_id', 'age_start', 'age', 'age_end', 'sex_id', 'year_id'), all.x = TRUE, allow.cartesian=TRUE)
  } 
  if(match_type == 'between_study'){
    matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('location_id', 'age_start', 'age_end', 'sex_id', 'year_id'), all.x = TRUE, allow.cartesian=TRUE)
  }
  
  #drop rows where that loc/age/sex/year was only in the first dataset
  matched_merge <- matched_merge[!is.na(mean.y)]
  
  #drop rows where both means are 0
  matched_merge <- matched_merge[!(mean.x == 0 & mean.y == 0)]

  #add offset where both values are zero in order to be able to calculate std error
  offset <- 0.5 * median(all_split[mean != 0, mean])
  matched_merge[(mean.x == 0 | mean.y == 0), `:=` (mean.x = mean.x + offset,
                                                   mean.y = mean.y + offset)]
  
  print(paste0('# rows where livestill is nonzero but live is zero: ',nrow(matched_merge[mean.y > 0 & mean.x == 0])))
  
   matched_merge <- matched_merge[!mean.x==0] # uncomment me if you want to get rid of ALTERNATE means that are zero
   matched_merge <- matched_merge[!mean.y==0] # uncomment me if you want to get rid of REFERENCE means that are zero
  
  matched_merge <- matched_merge[, mean:= mean.x / mean.y]
  matched_merge <- matched_merge[, standard_error:= sqrt( (mean.x^2 / mean.y^2) * ( (standard_error.x^2 / mean.x^2) + (standard_error.y^2 / mean.y^2) ) ) ]
  
  #output csv if the ratio is less than zero when looking at cv_livestill
  #output csv if the ratio is greater than zero if looking at cv_excludes_chromos
  #then drop from crosswalk
  #add here
  
  if(nrow(matched_merge) == 0){
    print(paste0('no ', match_type, ' matches between demographics for ', cv))
  } else{
    write.csv("FILEPATH", row.names = FALSE)
  }

} else{
  print('only one value for given covariate')
}

matched_merge <- matched_merge[!matched_merge$standard_error > 6]


#changes
matched_merge_within <- copy(matched_merge)
matched_merge_between <- copy(matched_merge)





####################################################################################
# mrbrt start
##########################################
repo_dir <- "FILEPATH"
source(paste0("FILEPATH"))


library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH")
library(msm)
library(readxl)
library(plyr)

## *******************************************************************************
## Step 2. Logit transformation
## --Transform the mean prevalence of the reference and alternative definitions 
##   logit(p) = log(p/1-p) = log(p) - log(1-p)
##   invlogit function = exp(x)/(1 + exp(x)
##
## --Estimate the "ratio" in normal space =  difference in logit space  of the logit transformed  mean prevalence reference and alternative definition
##   diff_logit = logit_alt - logit_ref
##
## --Logit transform the standard error of the alternative and reference means
##
## --Calculate the standard error of the logit difference
## *******************************************************************************


# Select variables of interest
logit_metareg <- as.data.frame(matched_merge) %>%
  select(sex_id, year_id, cv_d_conversion.x, mean.x, standard_error.x,  mean.y, standard_error.y, mean, standard_error)

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


## *******************************************************************************
## Step 3. Run MR-BRT and check predictions
## *******************************************************************************

# Standardize variable names 
ratio_var <- "logit_diff"
ratio_se_var <- "logit_se_diff"
cov_names <- c('sex_id')
metareg_vars <- c(ratio_var, ratio_se_var, cov_names)

tmp_metareg <- as.data.frame(logit_metareg) %>%
  .[, metareg_vars] %>%
  setnames(metareg_vars, c("ratio_logit", "ratio_se_logit", 'sex_id'))






########### Use below for non-logit crosswalk

dat_metareg <- as.data.frame(matched_merge)
dat_metareg$sex_id <- ifelse(dat_metareg$sex_id == 2, 0, 1)

ratio_var <- "mean"
ratio_se_var <- "standard_error"
age_var <- "age"
sex_var <- "sex_id"
sdi_var <- "sdi_var"
cov_names <- c('sex_id')
metareg_vars <- c(ratio_var, ratio_se_var, sex_var)

tmp_metareg <- as.data.frame(dat_metareg) %>%
  .[, metareg_vars] %>%
  setnames(metareg_vars, c("ratio", "ratio_se", 'sex_id'))

tmp_metareg$ratio_log <- log(tmp_metareg$ratio)
tmp_metareg$ratio_se_log <- sapply(1:nrow(tmp_metareg), function(i) {
  ratio_i <- tmp_metareg[i, "ratio"]
  ratio_se_i <- tmp_metareg[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})



#MR-BRT modeling


if(trim == 'no_trim'){
  fit1 <- run_mr_brt(
    output_dir = outdir, # user home directory
    model_label = mod_lab,
    data = tmp_metareg,
    mean_var = "ratio_logit",
    se_var = "ratio_se_logit",
    overwrite_previous = TRUE
  )
} else if(trim == 'trim'){
  ## trim ###
  fit1 <- run_mr_brt(
    output_dir = outdir, # user home directory
    model_label = mod_lab,
    data = tmp_metareg,
    mean_var = "ratio_logit",
    se_var = "ratio_se_logit",
    overwrite_previous = TRUE,
    method = "trim_maxL",
    trim_pct = trim_percent,
    covs = list(
      cov_info('sex_id', 'X')
    )
  )
}

##########################################
# plot mr-brt
plot <- plot_mr_brt_custom(fit1)
#plot <- plot_mr_brt(fit1)

#Chris's code
#Review MR-BRT results (w/covariates)
check_for_outputs(fit1)
results <- load_mr_brt_outputs(fit1)
names(results)
coefs <- results$model_coefs
metadata <- results$input_metadata

#store the mean_effect model with covariates
df_pred <- data.frame(expand.grid(sex_id = c(1,2)))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
check_for_preds(pred1)

pred_object <- load_mr_brt_preds(pred1)
names(pred_object)
preds <- pred_object$model_summaries
draws <- pred_object$model_draws

##plot with splines
plot <- plot_mr_brt_custom(pred1, dose_vars = 'age')

beta0 <- preds$Y_mean # predicted ratios by age  
beta0_se_tau <-  (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92 # standard
sex_id <- preds$X_sex_id

test <- as.data.frame(cbind(beta0, beta0_se_tau, sex_id))
colnames(test)[colnames(test)=="beta0"] <- "ratio"
colnames(test)[colnames(test)=="beta0_se_tau"] <- "se"

output_file <- write.csv(test, paste0('FILEPATH'))


## *******************************************************************************
## Step 3-4. Apply ratios 
#-- transform original bundle data in logit space
#-- transform logit to normal space
#-- apply ratios 
## *******************************************************************************

cvs <- c('cv_d_conversion', 'cv_dmf_units_surfaces')
measures <- c('incidence', 'prevalence')

#cvs <- as.vector(unlist(strsplit(cov_map[bundle_id==bun_id]$covariates, ",")))
#cv_filenames <- as.vector(unlist(strsplit(cov_map[bundle_id==bun_id]$covariates_file_name, ",")))

dat_previous <- read.csv(paste0('FILEPATH'), stringsAsFactors = FALSE)

dat_prev <- read.csv(paste0('FILEPATH'), stringsAsFactors = FALSE)
dat_inc <- read.csv(paste0('FILEPATH'), stringsAsFactors = FALSE)

dat_original <- rbind(dat_prev, dat_inc)

dat_original$cv_d_conversion <- 0
dat_original$cv_dmf_units_surfaces <- 0
dat_original$cv_dmf_incidence[dat_original$measure == 'incidence'] <- 1


tmp_orig2 <- as.data.table(dat_original)
# if original data point was a reference data point, leave as-is
tmp_orig2 <- tmp_orig2[, ref:=0]
tmp_orig2 <- tmp_orig2[measure == 'prevalence' & cv_d_conversion == 0 & cv_dmf_units_surfaces == 0, ref:=1]
tmp_orig2 <- tmp_orig2[measure == 'incidence' & cv_dmf_units_surfaces == 0, ref:=1]

if(bun_id %in% c(437, 436, 438, 638, 3029)){
  tmp_orig2 <- tmp_orig2[cv_livestill == 0, ref:=1]
}
if(!(bun_id %in% c(437, 436, 438, 638, 3029))){
  tmp_orig2 <- tmp_orig2[cv_livestill == 0 & cv_excludes_chromos == 0 & cv_worldatlas_to_total_target == 0 &
                           cv_icbdms_to_total_target == 0 & cv_nbdpn_to_total_target == 0 & 
                           cv_congmalf_to_total_target == 0, ref:=1]
}

#For bundles 260/261
ratio_dconv_prev <- fread(paste0('FILEPATH'))
ratio_dmf_prev <- fread(paste0('FILEPATH'))
ratio_dmf_inc <-  fread(paste0('FILEPATH'))

beta_dconv <- ratio_dconv_prev$ratio
beta_se_dconv <- exp(ratio_dconv_prev$se)
sex_id <- ratio_dconv_prev$sex_id

beta_dmf <- ratio_dmf_inc$ratio
beta_se_dmf <- exp(ratio_dmf_inc$se)
sex_id <- ratio_dmf_inc$sex_id

#For bundle 262
ratio_atch4_prev <- fread(paste0('FILEPATH'))
beta_atch4 <- ratio_atch4_prev$ratio
beta_se_atch4 <- exp(ratio_atch4_prev$se)
age_atch4 <- ratio_atch4_prev$age

ratio_atch5_prev <- fread(paste0('FILEPATH'))
beta_atch5 <- ratio_atch5_prev$ratio
beta_se_atch5 <- exp(ratio_atch5_prev$se)
age_atch5 <- ratio_atch5_prev$age

ratio_cpi3_prev <- fread(paste0('FILEPATH'))
beta_cpi3 <- ratio_cpi3_prev$ratio
beta_se_cpi3 <- exp(ratio_cpi3_prev$se)
age_cpi3 <- ratio_cpi3_prev$age

for (i in 1:length(cvs)){
  print(i)
  cv <- cvs[i]
  cv_filename <- cv_filenames[i]
  print(paste(cv, ",", cv_filename))
  
  if(!(cv %like% "livestill")){
    print("not livestill")
    # pull beta plus haqi correction 
    beta <- exp(ratio$ratio)
    # TO DO : does SE come in linear space 
    beta_se <- exp(ratio$se)
    # new variance of the adjusted data point is just the sum of variances
    # because the adjustment is a sum rather than a product in log space
    tmp_orig2 <- tmp_orig2[tmp_orig2[[cv]]==1, mean_adjusted := mean / beta] # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    tmp_orig2 <- tmp_orig2[tmp_orig2[[cv]]==1, var_adjusted := standard_error^2 * beta_se^2] # adjust the variance 
    tmp_orig2 <- tmp_orig2[tmp_orig2[[cv]]==1, se_adjusted := sqrt(var_adjusted)]  
    
    
    tmp_orig2 <- tmp_orig2[,lo_adjusted := mean_adjusted - 1.96 * se_adjusted]
    tmp_orig2 <- tmp_orig2[,hi_adjusted := mean_adjusted + 1.96 * se_adjusted]
    tmp_orig2 <- as.data.table(tmp_orig2) 
  }
  
  if(cv %like% "livestill"){
    print("livestill")
    ratio <- ratio[sex_id==1, sex:="Male"]
    ratio <- ratio[sex_id==2, sex:="Female"]
    ratio <- ratio[, c("sex_id", "V1"):=NULL]
    tmp_orig2 <- merge(tmp_orig2, ratio, by=c("location_id", "year_id", "sex"), all.x = TRUE)
    tmp_orig2 <- tmp_orig2[, beta_l:=exp(ratio)]
    tmp_orig2 <- tmp_orig2[, beta_se_l:=exp(se)]
    tmp_orig2 <- tmp_orig2[tmp_orig2[[cv]]==1, mean_adjusted := mean / beta_l] # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    tmp_orig2 <- tmp_orig2[tmp_orig2[[cv]]==1, var_adjusted := standard_error^2 * beta_se_l^2] # adjust the variance 
    tmp_orig2 <- tmp_orig2[tmp_orig2[[cv]]==1, se_adjusted := sqrt(var_adjusted)]  
    
    
    tmp_orig2 <- tmp_orig2[,lo_adjusted := mean_adjusted - 1.96 * se_adjusted]
    tmp_orig2 <- tmp_orig2[,hi_adjusted := mean_adjusted + 1.96 * se_adjusted]
    tmp_orig2 <- as.data.table(tmp_orig2) 
    
  }
}







# load all original bundle data
# -- read in data frame
dat_original <- read.csv(paste0('FILEPATH'))
dat_original <- as.data.table(dat_original)
dat_original <- dat_original[!dat_original$standard_error == 0]

# --  identify reference studies
dat_original[, reference := 1]
dat_original[cv_d_conversion == 1, reference := 0]

#dat_original <- dat_original[, age := round((age_start + age_end)/2)]
#dat_original <- dat_original[dat_original$narrow_age == 1, age_start := orig_age_start]
#dat_original <- dat_original[dat_original$narrow_age == 1, age_end := orig_age_end]

dat_original[cv_atchloss4ormore == 1, covariate := 'cv_atchloss4ormore']
dat_original[cv_atchloss5ormore == 1, covariate := 'cv_atchloss5ormore']
dat_original[cv_cpiclass3 == 1, covariate := 'cv_cpiclass3']

dat_original[reference == 1, covariate := 'reference']

# -- identify mean and standard error variables and covariates
reference_var <- "reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
#covariate <- 'covariate'
cov_names <- "sex_id"

# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var, cov_names)
# metareg_vars <- c(ratio_var, ratio_se_var, cov_names) # defined before running the model

sex_names <- fread("FILEPATH")
dat_original <- merge(dat_original, sex_names, by = c('sex'), all.x = TRUE)

#dat_original <- dat_original[, age := round((age_start + age_end)/2)]

tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", "sex_id")) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

# tmp_metareg #defined before running the model

# logit  transform the original bundle data
# -- SEs transformed using the delta method

tmp_orig$mean_logit <- logit(tmp_orig$mean)
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1), mean_i, se_i^2)
})




# With covariates
#test <- as.data.frame(cbind(beta0, beta0_se_tau, sex_id))
test_dconv <- as.data.frame(cbind(beta_dconv, beta_se_dconv, sex_id))
test_dmf <- as.data.frame(cbind(beta_dmf, beta_se_dmf, sex_id))

test_atch4 <- as.data.frame(cbind(beta_atch4, beta_se_atch4, age_atch4))
test_atch5 <- as.data.frame(cbind(beta_atch5, beta_se_atch5, age_atch5))
test_cpi3 <- as.data.frame(cbind(beta_cpi3, beta_se_cpi3, age_cpi3))


#merge orig_dat and results for the model with covariates, do not merge when the models dont have covariates
#tmp_orig <- merge(cbind(tmp_orig, x=row.names(tmp_orig)), cbind(test, variable=rownames(test))) %>% select(-x, -variable) 
tmp_orig <- join(tmp_orig, test_dconv, by = "sex_id", type = "left", match = "first")
tmp_orig <- join(tmp_orig, test_dmf, by = "sex_id", type = "left", match = "first")

colnames(test_atch4)[colnames(test_atch4)=="age_atch4"] <- "age"
tmp_orig <- join(tmp_orig, test_atch4, by = 'age', type = "left", match = 'first')

colnames(test_atch5)[colnames(test_atch5)=="age_atch5"] <- "age"
tmp_orig <- join(tmp_orig, test_atch5, by = 'age', type = "left", match = 'first')

colnames(test_cpi3)[colnames(test_cpi3)=="age_cpi3"] <- "age"
tmp_orig <- join(tmp_orig, test_cpi3, by = 'age', type = "left", match = 'first')


head(tmp_orig)
tail(tmp_orig)

# Estimate new variance in logit space   
# new variance of the adjusted data point is just the sum of variances
# because the adjustment is a sum rather than a product in log space
tmp_orig2 <- copy(tmp_orig)
#tmp_orig2 <- tmp_orig2[tmp_orig2$cv_d_conversion == 1 | cv_dmf_units_surface]

tmp_orig2 <- tmp_orig %>%
  mutate(
    mean_logit_tmp = mean_logit - beta_dconv, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    var_logit_tmp = se_logit^2 + beta_se_dconv^2, # adjust the variance
    se_logit_tmp = sqrt(var_logit_tmp)
  )




tmp_orig2 <- tmp_orig %>%
  mutate(
    mean_logit_tmp = ifelse(tmp_orig2$covariate == 'cv_atchloss4ormore', mean_logit - beta_atch4, ifelse(tmp_orig2$covariate == 'cv_atchloss5ormore', mean_logit - beta_atch5, mean_logit - beta_cpi3)),# adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    var_logit_tmp = ifelse(tmp_orig2$covariate == 'cv_atchloss4ormore', se_logit^2 + beta_se_atch4^2, ifelse(tmp_orig2$covariate == 'cv_atchloss5ormore', se_logit^2 + beta_se_atch5^2, se_logit^2 + beta_se_cpi3^2)), # adjust the variance
    se_logit_tmp = sqrt(var_logit_tmp)
  )

head(tmp_orig2)
tail(tmp_orig2)


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

head(tmp_orig3)
tail(tmp_orig3)


# Check for NaN SE 
check <- select(tmp_orig3,  sex_id, ref,  mean, mean_adjusted, se, se_adjusted, lo_adjusted, hi_adjusted, mean_logit, se_logit, mean_logit_adjusted , se_logit_adjusted)
head(check)
tail(check)

filter(check,is.na(se_adjusted))
filter(check, se_adjusted == "Inf")

# Correct NaN SE 
#tmp_orig4<- tmp_orig3 %>%
#  mutate(
#    se_adjusted = if_else(ref == 0 & mean == 0, se, se_adjusted), 
#    lo_adjusted = if_else(ref == 0 & mean == 0 & lo_adjusted == 0, mean_adjusted - 1.96 * se, lo_adjusted), 
#    hi_adjusted = if_else(ref == 0 & mean == 0, mean_adjusted + 1.96 * se, hi_adjusted))

# Correct for infinite adjusted SE
tmp_orig4 <- tmp_orig3 %>%
  mutate(
    se_adjusted = if_else(ref == 0 & se_adjusted == "Inf", se, se_adjusted))

tmp_orig4 <- tmp_orig4 %>%
  mutate(
    se_adjusted = if_else(ref == 1 & se_adjusted == "Inf", se, se_adjusted))

tmp_orig4

check <- select(tmp_orig4,  sex_id, ref,  mean, mean_adjusted, se, se_adjusted, lo_adjusted, hi_adjusted, mean_logit, se_logit, mean_logit_adjusted , se_logit_adjusted)

filter(check,is.na(se_adjusted))
filter(check, se_adjusted == "Inf")
filter(check, ref == 0 & mean == 0)


# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original, 
  tmp_orig4[, c("ref", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)

final_data$ref <- NULL
final_data$covariate <- NULL

#Reed's code leaves 47 rows of reference data in my bundle with NaNs in the se_adjusted
#column, instead of just copying over the standard_error. Something going awry with logging
#these small numbers back and forth. Adding thsi band-aid fix for now.

#For bundle 260/261, don't run code below

final_data[is.na(se_adjusted) & ref == 1, se_adjusted := standard_error]
final_data[ref == 1, mean_adjusted:= mean]
final_data[ref == 1, se_adjusted:= standard_error]
final_data[ref == 1, hi_adjusted := upper]
final_data[ref == 1, lo_adjusted := lower]
final_data[ref == 1, hi_adjusted := upper]

final_data[cv_d_conversion == 1 & mean == 0, mean_adjusted := mean]
final_data[cv_d_conversion == 1 & mean == 0, se_adjusted := standard_error]
final_data[cv_d_conversion == 1 & mean == 0, hi_adjusted := upper]


final_data_prev <- write.csv(final_data, 'FILEPATH')

#“--plot_note paste0(“bundle: “, bundle, “, bundle_name: “, bundle_name, ” ,crosswalk: “, cv, “, trim: “, trim_pct)”
final_prev <- fread('FILEPATH')
final_inc <- fread('FILEPATH')

final_prev$V1 <- NULL
final_inc$V1 <- NULL




final_data <- rbind(final_prev, final_inc, fill = TRUE)

#Split England locations

locs <- get_location_metadata(9, gbd_round_id = 6)
locs_35 <- get_location_metadata(35, gbd_round_id = 6)
england <- locs_35[!locs_35$location_id %in% locs$location_id]
eng_data <- final_data[final_data$location_id %in% england$location_id]

final_wo_eng <- final_data[!final_data$location_id %in% unique(eng_data$location_id)]

eng_data <- eng_data[eng_data$location_name == "Yorkshire and the Humber", location_name := "Yorkshire and The Humber"]

uk_pop_weights <- read.xlsx('FILEPATH')
uk_pop_weights <- uk_pop_weights[, c('location_id', 'location_name', 'parent', 'weight')]

#eng_data_step <- merge(eng_data, uk_pop_weights[, c("location_name", "location_id")], by = 'location_id', allow.cartesian = TRUE)

eng_data_merged <- merge(eng_data, uk_pop_weights, by.x = 'location_name', by.y = "parent", allow.cartesian = TRUE)


eng_data_merged$group_review <- ifelse(eng_data_merged$location_name == eng_data_merged$location_name.y, 0, 1)
eng_data_merged$location_name <- eng_data_merged$location_name.y
eng_data_merged$location_name.y <- NULL
eng_data_merged$location_id.x <- eng_data_merged$location_id.y
colnames(eng_data_merged)[colnames(eng_data_merged)=="location_id.x"] <- "location_id"
eng_data_merged$location_id.y <- NULL
eng_data_merged$sample_size <- eng_data_merged$sample_size * eng_data_merged$weight
eng_data_merged$weight <- NULL
eng_data_merged$step2_location_year <- "Splitting England subnationals"
eng_data_merged <- eng_data_merged[eng_data_merged$group_review == 1 | is.na(eng_data_merged$group_review) | eng_data_merged$group_review == '']
eng_data_merged <- eng_data_merged[is.na(eng_data_merged$group) & eng_data_merged$specificity == '', group_review := NA]

final_data2 <- rbind(final_wo_eng, eng_data_merged, fill=TRUE)

#all_split <- all_split[!all_split$location_id %in% england$location_id]

#Remove any group review studies, any data where standard error = 0, and create a parent seq

final_data2 <- final_data2[!is.na(final_data2$standard_error)]
final_data2 <- final_data2[final_data2$group_review == 1 | is.na(final_data2$group_review) | final_data2$group_review == ' ', ]
final_data2$crosswalk_parent_seq <- final_data2$parent_seq
#final_data2$step2_location_year <- ''

#Writing final file out and uploading crosswalk

write.xlsx(final_data2,
          file = paste0("FILEPATH"),
          row.names = FALSE, sheetName = "extraction")

result <- save_crosswalk_version(1685, 
                                 data_filepath = paste0('FILEPATH'),
                                 description = "261_Permanent_Carries_Allcovariate_Crosswalk")

test_version <- get_crosswalk_version(2528)







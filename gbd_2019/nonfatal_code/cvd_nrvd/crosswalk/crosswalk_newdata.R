
## 
## Purpose: Crosswalk new clinical informatics data for step 4 using step 2 crosswalk.
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table)


###### Paths, args
#################################################################################

central <- "FILEPATH"

brt_root <- "FILEPATH"
brt_code_root <- "FILEPATH"

mes <- data.table(me_name = c("aort", "mitral"), 
                  bundle_id = c(2987, 2993),
                  modelable_entity = c(16601, 16602),
                  modelable_entity_version = c(410420, 410426))

decomp_step <- "step4"
gbd_round_id <- 6


###### Functions
#################################################################################

## Central functions to upload/pull down data
source(paste0(central, 'save_crosswalk_version.R'))
source(paste0(central, 'get_bundle_version.R'))
source(paste0(central, 'save_bundle_version.R'))
source(paste0(central, 'get_model_results.R'))
source(paste0(central, 'get_age_metadata.R'))
source(paste0(central, 'get_location_metadata.R'))

## MR-BRT wrapper function
source(paste0(brt_code_root, "mr_brt_functions.R"))
source('master_mrbrt_crosswalk_func.R')


###### Pull in data and crosswalk
#################################################################################

## Pull in the crosswalk from step 2
load(paste0(brt_root, me, '/prevalence.Rdata'))

## Pull in NEW clinical informatics data from the step 4 bundle
b_id <- save_bundle_version(bundle_id = mes[me_name==me, bundle_id], decomp_step = decomp_step, include_clinical = T)
data <- get_bundle_version(bundle_version_id = b_id$bundle_version_id)

n <- names(data)

## Mark covariate columns
data[, cv_claims := ifelse(clinical_data_type == "claims", 1, 0)]
data[, cv_inpatient := ifelse(clinical_data_type == "inpatient", 1, 0)]
if ("Both" %in% unique(data$sex)) stop("You need to sex-split.")
data[, male := ifelse(sex == "Male", 1, 0)]
data[, age_mid := (age_start + age_end)/2]
data[, age_scaled := (age_mid - mean(crosswalk_holder$xw_ratios$age_mid_alt))/sd(crosswalk_holder$xw_ratios$age_mid_alt)]
data[, id_var := .GRP, by=nid]
data[, id := 1:nrow(data)]

## Logit-transform 
data[, mean_logit := log(mean / (1-mean))]
data$logit_se <- sapply(1:nrow(data), function(i) {
  mean_i <-  data[i, mean]
  se_i <-  data[i, standard_error]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

## Predict
message("Estimated betas are:")
print(crosswalk_holder$xw_covs)
preds <- predict_mr_brt(model_object = crosswalk_holder$xw_results, prediction_data = data)
preds <- preds$model_summaries
preds[, id := 1:nrow(preds)]
preds[, prediction_se_logit := (Y_mean_hi - Y_mean_lo)/3.92]

## Adjust data
data <- merge(preds, data, by="id")
data[, mean_logit_adjusted := mean_logit - Y_mean]
data[, se_logit_adjusted := sqrt(prediction_se_logit^2 + logit_se^2)]
data[, high_logit_adjusted := mean_logit_adjusted + 1.96*se_logit_adjusted]
data[, low_logit_adjusted := mean_logit_adjusted - 1.96*se_logit_adjusted]
data[, upper := inv.logit(high_logit_adjusted)]
data[, low := inv.logit(low_logit_adjusted)]

# Bring back to normal space
data$se_adjusted <- sapply(1:nrow(data), function(i) {
  mean_i <- data[i, mean_logit_adjusted]
  se_i <- data[i, se_logit_adjusted]
  deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
})
data[, mean_adj := inv.logit(mean_logit_adjusted)]

## If there is mean = 0 we adjust the SE in normal space (see comment in master_mrbrt_crosswalk_func.R)
data$pred_se_normal <- sapply(1:nrow(data), function(i) {
  mean_i <- data[i, Y_mean]
  se_i <- data[i,prediction_se_logit]
  deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
})

data[mean==0, se_adjusted := sqrt(standard_error^2 + pred_se_normal^2)]
data[mean==0, `:=` (upper=mean_adj + 1.96*se_adjusted, lower=mean_adj - 1.96*se_adjusted)]
data[upper>1, upper := 1]
data[lower<0, lower := 0]

print(summary(data$mean_adj))


###### Plot data
#################################################################################

## Pull in age groups and location IDs
ages <- get_age_metadata(age_group_set_id = 12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
data <- merge(data, ages[, .(age_start, age_group_id)], by="age_start")
loc_meta <- get_location_metadata(location_set_id = 9)

## Pull in Dismod results
dismod <- get_model_results(gbd_team = "epi", gbd_id = mes[me_name==me, modelable_entity], measure_id = 5, age_group_id = unique(data$age_group_id),
                            sex = c(1, 2), year = 2015, decomp_step = "step3")
dismod <- merge(dismod, ages[, .(age_start, age_group_id)], by="age_group_id")
dismod <- merge(dismod, loc_meta[, .(location_id, location_name)], by="location_id")

## Adjust for subnationals
dismod[location_id %in% c(4944, 4940), location_id := 93] #Sweden

## Prepare for dismod merge
data[, sex_id := ifelse(sex=="Male", 1, 2)]
data[, year_id := year_start]


###### Upload data
#################################################################################

data[, `:=` (mean = mean_adj, standard_error = se_adjusted, upper = upper, lower = lower,
                cases = "", sample_size = "", uncertainty_type_value = "",
                note_sr = paste0(note_sr, " | crosswalked with logit difference: ", round(Y_mean, 2), " (",
                                      round(prediction_se_logit), ")"))]
data <- data[, ..n]

data[, crosswalk_parent_seq := seq]
data[, seq := NA]

data$group_review <- NA
data$specificity <- NA
data$group <- NA

data$upper <- NA
data$lower <- NA
data$uncertainty_type_value <- NA

data$cv_claims <- NULL
data$cv_hospital <- NULL
data$cv_inpatient <- NULL

data$sampling_type <- NA

write.xlsx(data, "FILEPATH", sheetName="extraction")
description <- "Appending step 4 CI data, crosswalked with step 2 ratios, 9/3/19."

save_crosswalk_version(bundle_version_id = b_id$bundle_version_id, data_filepath = paste0("FILEPATH"),
                       description = description)


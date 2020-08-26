###########################################################
### Project: GBD Nonfatal Estimation
### Purpose: Epilepsy Splits
###########################################################

##Setup
rm(list=ls())

user <- Sys.info()[['user']]
j_root <- "FILEPATH" 
share_path <- "FILEPATH"

set.seed(98736)

##Set directories/objects
date <- gsub("-", "_", Sys.Date())
functions <- "FILEPATH"
all_output <- "FILEPATH"
output <- paste0(all_output, date, "/")
if (!file.exists(output)){dir.create(output,recursive=T)}
csv_input <- "FILEPATH"
draws <- paste0("draw_", 0:999)

splits_bundle_path <- "FILEPATH"
# specify decomp step
ds <- "step2"

##Load packages
library(data.table)
library(ggplot2)
library(lme4)
library(arm)
library(boot)
library(haven)
library(readr)
library(merTools)
library(openxlsx)

##Central functions
source(paste0(functions, "get_covariate_estimates.R"))
source(paste0(functions, "get_location_metadata.R"))
source(paste0(functions, "get_outputs.R"))

##User functions
format_data <- function(dt){
  dt[, year_id := floor((year_start + year_end)/2)]
  ##no studies group reviewed out
  dt <- dt[group_review == 1 | is.na(group_review)] 
  dt[, cases := sum(cases), by = c("nid", "year_id", "location_id")]
  dt[, sample_size := sum(sample_size), by = c("nid", "year_id", "location_id")]
  dt[, mean := cases/sample_size]
  ##sum cases and sample size over nid and year so only one data point per both sex all age
  dt <- unique(dt, by = c("nid", "year_id", "location_id")) 
  dt[, var := mean * sample_size * (1 - mean)]
  dt <- merge(dt, covs, by= c("location_id", "year_id"))
  dt[, weight := round(scale(1/var, center = F, scale = T), 0)]
  dt[weight == 0, weight := 1]
  return(dt)
}

predictions <- function(frame, model){
  frame <- frame[most_detailed == 1]
  predictions <- predictInterval(merMod = model, newdata = frame, level = 0.95, n.sims = 1000, 
                                 stat = "mean", type = "probability", returnSims = T)
  sims <- attr(predictions, "sim.results")
  predictions <- as.data.table(cbind(predictions, sims))
  setnames(predictions, c(4:1003), draws)
  predictions <- cbind(frame, predictions)
  predictions[, (draws) := lapply(.SD, function(x) inv.logit(x)), .SDcols = draws]
  predictions <- predictions[most_detailed == 1]
  predictions <- predictions[, c("location_id", "year_id", draws), with = F]
  return(predictions)
}

##Get location metadata
print("getting locations")
##get epi locations
locations <- get_location_metadata(location_set_id = 9) 
##get rid of global keep necessary variables
locations <- locations[!location_id==1, .(location_id, location_name, parent_id, super_region_name, most_detailed, is_estimate)]
most_detailed <- locations[most_detailed == 1]
# check to make sure correct locations are being pulled in
is_est <- most_detailed[is_estimate == 1]
num_locs <- length(is_est$location_id)
print("Checking to make sure all locations are present")
if (num_locs == 990) {
  print("All locations are present for custom modeling!")
} else {
  stop(paste("Number of locations is not equal to 990, instead", num_locs))
}

##Get covariates
print("getting covariates")
haqi <- get_covariate_estimates(covariate_id = 1099, decomp_step = ds)
setnames(haqi, "mean_value", "haqi") ##change column name
haqi[, log_haqi := log(haqi)]
haqi <- haqi[,.(location_id, year_id, haqi, log_haqi)] ##keep necessary variables

pigmeat <- get_covariate_estimates(covariate_id = 99, decomp_step = ds)
setnames(pigmeat, "mean_value", "pigmeat")
pigmeat[, log_pig := log(pigmeat)]
pigmeat <- pigmeat[,.(location_id, year_id, pigmeat, log_pig)]

sanitation <- get_covariate_estimates(covariate_id = 142, decomp_step = ds)
setnames(sanitation, "mean_value", "sanitation")
sanitation <- sanitation[, .(location_id, year_id, sanitation)]

muslim <- get_covariate_estimates(covariate_id = 1218, decomp_step = ds)
setnames(muslim, "mean_value", "muslim")
muslim <- muslim[, .(location_id, year_id, muslim)]

hsa <- get_covariate_estimates(covariate_id = 44, decomp_step = ds)
setnames(hsa, "mean_value", "hsa")
hsa <- hsa[, .(location_id, year_id, hsa)]

sdi <- get_covariate_estimates(covariate_id = 881, decomp_step = ds)
setnames(sdi, "mean_value", "sdi")
sdi <- sdi[, .(location_id, year_id, sdi)]

##get under five mortality 
under_5 <- get_outputs(topic = "cause", age_group_id = 1, sex_id = 3, year_id = 2017,
                       metric_id = 3, location_id = "all", location_set_id = 9, gbd_round_id = 5)
setnames(under_5, "val", "mortality")
under_5 <- merge(under_5, locations, by = "location_id", all = T)
for (id in under_5[is.na(mortality), unique(parent_id)]){
  val <- under_5[location_id == id, mortality]
  under_5[is.na(mortality) & parent_id == id, mortality := val]
}
under_5[, log_mortality := log(mortality)]
under_5 <- under_5[, .(location_id, mortality, log_mortality)]

## get epilepsy deaths
epilepsy <- get_outputs(topic = "cause", age_group_id = 27, sex_id = 3, year_id = 2017, location_id = "all",
                        metric_id = 3, location_set_id =9, gbd_round_id = 5, cause_id = 545)
setnames(epilepsy, "val", "epilepsy")
epilepsy <- merge(epilepsy, locations, by = "location_id", all = T)
for (id in epilepsy[is.na(epilepsy), unique(parent_id)]){
  val <- epilepsy[location_id == id, epilepsy]
  epilepsy[is.na(epilepsy) & parent_id == id, epilepsy := val]
}
epilepsy[, log_epilepsy := log(epilepsy)]
epilepsy <- epilepsy[, .(location_id, epilepsy, log_epilepsy)]

merge_dts <- list(haqi, pigmeat, sanitation, muslim, hsa, sdi)
covs <- Reduce(function(...) merge(..., all = T, by = c("location_id", "year_id")), merge_dts)
merge_dts2 <- list(covs, under_5, epilepsy, locations)
covs <- Reduce(function(...) merge(..., by = c("location_id")), merge_dts2)

##Get proportion data
print("getting data")
splits_data <- as.data.table(openxlsx::read.xlsx(splits_bundle_path, na.strings = ""))
idiopathic_prop <- splits_data[split == "idiopathic"]
severe_prop <- splits_data[split == "severe"]
tg_prop <- splits_data[split == "tg"]
treated_nf_prop <- splits_data[split == "tnf"]

##IDIOPATHIC REGRESSION#########################
print("idiopathic regression")
##reverse code quality
idiopathic_prop[quality==0, quality:=2]
idiopathic_prop[quality==1, quality:=0]
idiopathic_prop[quality==2, quality:=1]

idiopathic_prop <- idiopathic_prop[!exclude == 1] ##exclude studies with age restrictions
idiopathic_prop <- format_data(idiopathic_prop)

##calculate standard error (from uploader code)
z <- qnorm(0.975)
idiopathic_prop[, se := sqrt(mean * (1 - mean) / sample_size + z^2 / (4 * sample_size^2))]

##correct for quality first
quality_model <- glm(mean ~ quality, data = idiopathic_prop, family = "binomial")
beta <- inv.logit(summary(quality_model)$coefficients[2,1])
standard_error <- inv.logit(summary(quality_model)$coefficients[2,2])
idiopathic_prop[, se := se^2 * standard_error^2 + se^2 * beta^2 + standard_error^2 * mean]
idiopathic_prop[quality == 1, mean := mean * beta]

##mixed effects model
model_idiopathic <- glmer(mean ~ sdi + (1 | super_region_name),
                           data = idiopathic_prop, family = "binomial")

##generate a frame for the predictions, want all countries years 1980-2017
predict_idio <- copy(covs)
predict_idio <- predict_idio[year_id>=1980,]

predictions_idio <- predictions(predict_idio, model_idiopathic)

write_rds(predictions_idio, paste0(output, "idio_draws.rds"))
sink(paste0(output, "model_idiopathic.txt"))
print(summary(model_idiopathic))
sink()

##SEVERE REGRESSION#############################
print("severe regression")
severe_prop <- format_data(severe_prop)

model_severe <- glmer(cases/sample_size ~ log_haqi + (1|super_region_name),
                      data = severe_prop, family = "binomial") ##mixed effects glm

##generate a frame for the predictions, want all countries years 1980-2017
predict_severe <- copy(covs)
predict_severe <- predict_idio[year_id>=1980,]

predictions_severe <- predictions(predict_severe, model_severe)

write_rds(predictions_severe, paste0(output, "severe_draws.rds"))
sink(paste0(output, "model_severe.txt"))
print(summary(model_severe))
sink()

##TREATMENT GAP REGRESSION######################
print("treatment gap regression")
tg_prop <- format_data(tg_prop)

model_tg <- glmer(cases/sample_size ~  log_haqi + (1|super_region_name),
                  data = tg_prop, family = "binomial") ##mixed effects glm

##generate a frame for the predictions, want all countries years 1980-2017
predict_tg <- copy(covs)
predict_tg <- predict_tg[year_id>=1980,]

predictions_tg <- predictions(predict_tg, model_tg)

write_rds(predictions_tg, paste0(output, "tg_draws.rds"))
sink(paste0(output, "model_tg.txt"))
print(summary(model_tg))
sink()

##TREATMENT NO FITS REGRESSION######################
print("tnf regression")
treated_nf_prop <- format_data(treated_nf_prop)

model_treated_nf <- glm(cases/sample_size ~ log_haqi,
                        data = treated_nf_prop, family = "binomial") ##glm

##generate a frame for the predictions, all most detailed countries years 1980-2017
predict_treated_nf <- copy(covs)
predict_treated_nf <- predict_treated_nf[year_id>=1980 & most_detailed == 1,]

predictions_treated_nf <- as.data.table(predict(model_treated_nf, newdata = predict_treated_nf, se.fit = T)) ##get predictions and standard errors
predictions_treated_nf <- cbind(predict_treated_nf, predictions_treated_nf) ##bind onto identifying data
alloc.col(predictions_treated_nf, 1000)
predictions_treated_nf <- predictions_treated_nf[, (draws) := as.list(inv.logit(rnorm(1000, fit, se.fit))), by = fit] ##get wide draws
predictions_treated_nf <- predictions_treated_nf[, c("location_id", "year_id", draws), with = F]

write_rds(predictions_treated_nf, paste0(output, "tnf_draws.rds"))
sink(paste0(output, "model_tnf.txt"))
print(summary(model_treated_nf))
sink()

## CREATE DIAGNOSTIC PLOTS
source(paste0(share_path, "/overtime_splitsdiagnostic.R"))
source(paste0(share_path, "/regression_maps.R"))

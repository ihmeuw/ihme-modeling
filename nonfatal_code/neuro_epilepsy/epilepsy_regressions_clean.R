###########################################################
### Author: 
### Adapted from Code written by 
### Date: 12/9/2016
### Project: GBD Nonfatal Estimation
### Purpose: Epilepsy Splits
###########################################################

##Setup
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j" 
  h_root <- "/homes/USERNAME"
} else { 
  j_root <- "J:"
  h_root <- "H:"
}
set.seed(98736)

##Set directories
functions <- FILEPATH
output <- FILEPATH
csv_input <- FILEPATH
pdf <- FILEPATH
lib <- FILEPATH

##Load packages
library(data.table, lib.loc = lib)
library(ggplot2, lib.loc = lib)
library(lme4, lib.loc = lib)
library(arm, lib.loc = lib)
library(merTools, lib.loc = lib)
library(boot, lib.loc = lib)
library(meta, lib.loc = lib)
library(haven)

#Set objects
draws <- paste0("draw_", 0:999)

##Central functions
source(paste0(functions, "get_covariate_estimates.R"))
source(paste0(functions, "get_location_metadata.R"))

##Get location metadata
locations <- as.data.table(get_location_metadata(location_set_id = 9)) ##get epi locations
locations <- locations[!location_id==1, .(location_id, location_name, parent_id, super_region_name, developed)] ##get rid of global keep necessary variables

utlas <- locations[parent_id == 4749]
utlas <- unique(utlas[, location_id])
indonesias <- locations[parent_id == 11]
indonesias <- unique(indonesias[, location_id])

##Get covariates
haqi <- as.data.table(get_covariate_estimates(covariate_name_short = "haqi"))
invisible(haqi[, log_haqi := log(mean_value)])
setnames(haqi, "mean_value", "haqi") ##change column name
haqi <- haqi[,.(location_id, year_id, haqi, log_haqi)] ##keep necessary variables

pigmeat <- as.data.table(get_covariate_estimates(covariate_name_short = "pigmeat_kcalpc"))
setnames(pigmeat, "mean_value", "pigmeat")
invisible(pigmeat[, log_pig := log(pigmeat)])
pigmeat <- pigmeat[,.(location_id, year_id, pigmeat, log_pig)]

sanitation <- as.data.table(get_covariate_estimates(covariate_name_short = "sanitation_prop"))
setnames(sanitation, "mean_value", "sanitation")
sanitation <- sanitation[, .(location_id, year_id, sanitation)]

##get under five mortality 
under_5 <- as.data.table(get_outputs(topic = "cause", age_group_id = 1, sex_id = 3, year_id = 2015,
                                     metric_id = 3, location_id = "all", location_set_id = 9, gbd_round_id = 3))
setnames(under_5, "val", "mortality")
under_5[, log_mortality := log(mortality)]
under_5 <- under_5[, .(location_id, mortality, log_mortality)]

##get utlas and england
indonesia <- under_5[location_id == 11]
in_mort <- as.numeric(indonesia[, mortality])
in_log_mort <- as.numeric(indonesia[, log_mortality])
indonesia_u5 <- data.table(location_id = indonesias, mortality = rep(in_mort, length(indonesias)),
                           log_mortality = rep(in_log_mort, length(indonesias)))
utla <- under_5[location_id == 4749]
utla_mort <- as.numeric(utla[, mortality])
utla_log_mort <- as.numeric(utla[, log_mortality])
utla_u5 <- data.table(location_id = utlas, mortality = rep(utla_mort, length(utlas)),
                      log_mortality = rep(utla_log_mort, length(utlas)))
under_5 <- rbindlist(list(under_5, indonesia_u5, utla_u5), use.names = T)

covs <- merge(haqi, pigmeat, by = c("location_id", "year_id")) ##merge covariates
covs <- merge(covs, sanitation, by = c("location_id", "year_id"))
covs <- merge(covs, under_5, by = c("location_id"))

##Get proportion data
idiopathic_prop <- fread(paste0(csv_input, "proportion_idiopathic4.csv"))
severe_prop <- fread(paste0(csv_input, "proportion_severe2.csv"))
treated_nf_prop <- fread(paste0(csv_input, "proportion_treated_no_fits2.csv"))
tg_prop <- fread(paste0(csv_input, "proportion_treatment_gap2.csv"))

##IDIOPATHIC REGRESSION#########################

##reverse code quality
idiopathic_prop[quality==0, quality:=2]
idiopathic_prop[quality==1, quality:=0]
idiopathic_prop[quality==2, quality:=1]

idiopathic_prop <- idiopathic_prop[!exclude == 1] ##exclude studies with age restrictions
idiopathic_prop <- idiopathic_prop[group_review == 1 | is.na(group_review)] ##no studies group reviewed out

idiopathic_prop[, year_id := floor((year_start + year_end)/2)]

##Sum so that one point per nid and year combo (over age and sex)
idiopathic_prop[, cases := sum(cases), by = c("nid", "year_id", "location_id")]
idiopathic_prop[, sample_size := sum(sample_size), by = c("nid", "year_id", "location_id")]
idiopathic_prop[, mean := cases/sample_size]
idiopathic_prop <- unique(idiopathic_prop, by = c("nid", "year_id", "location_id")) ##sum cases and sample size over nid and year so only one data point per both sex all age
idiopathic_prop <- merge(idiopathic_prop, covs, by= c("location_id", "year_id"))
idiopathic_prop <- merge(idiopathic_prop, locations, by=c("location_id")) ##merge on locations and covariates

## idiopathic binomial mixed effects regression models
model_idiopathic <- glmer(mean ~ mortality + log_pig + sanitation + quality + (1|super_region_name),
                          weights = sample_size, data = idiopathic_prop, family = "binomial")

##generate a frame for the predictions, want all countries years 1980-2016
predict_idio <- merge(locations, covs, by= "location_id")
predict_idio <- predict_idio[year_id>=1980,]
predict_idio[, quality := 0]

predictions_idio <- predictInterval(merMod = model_idiopathic, newdata = predict_idio, 
                                    level = 0.95, n.sims = 1000, stat = "mean",
                                    type = "probability", returnSims = T)
sims <- attr(predictions_idio, "sim.results") ##get sims
predictions_idio <- as.data.table(cbind(predictions_idio, sims)) ##create data table with predictions and sims
setnames(predictions_idio, c(4:1003), draws) ##relabel sims as draws
predictions_idio <- cbind(predict_idio, predictions_idio) ##tack on info about each prediction
invisible(predictions_idio[, (draws) := lapply(.SD, function(x) inv.logit(x)), .SDcols = draws]) ##transform the draws

##reshape long with a column called "draw"
predictions_idio <- predictions_idio[, c("location_id", "year_id", draws), with = F]
melted_idio <- copy(predictions_idio)
setnames(melted_idio, draws, as.character(c(0:999)))
melted_idio <- melt(data = melted_idio, id.vars = c("location_id", "year_id"), 
                    value.name = "proportion", variable.name = "draw")
invisible(melted_idio[, draw := as.integer(draw)])

write_dta(predictions_idio, paste0(FILEPATH, ".dta"), version = 13)

##Graphs
pdf(paste0(FILEPATH, ".pdf"))
gg_idio_haqi <- ggplot(data = idiopathic_prop, aes(x = log_haqi, y = mean)) +
  facet_wrap( ~ super_region_name) +
  geom_point()+
  geom_line(data = predictions_idio, aes(x = log_haqi, y = fit))+
  theme_bw()+
  ggtitle("Proportion Idiopathic vs. Log(HAQ Index)")+
  labs(x = "Log(HAQ Index)", y = "Proportion Idiopathic")
gg_idio_haqi
gg_idio_pig <- ggplot(data = idiopathic_prop, aes(x = log_pig, y = mean)) +
  facet_wrap( ~ super_region_name) +
  geom_point()+
  geom_line(data = predictions_idio, aes(x = log_pig, y = fit))+
  theme_bw()+
  ggtitle("Proportion Idiopathic vs. Log(Pigmeat_pc)")+
  labs(x = "Log(Pigmeat_pc)", y = "Proportion Idiopathic")
gg_idio_pig
dev.off()

##SEVERE REGRESSION#############################
invisible(severe_prop[, year_id := floor((year_start+year_end)/2)])
severe_prop <- severe_prop[group_review == 1 | is.na(group_review)] ##only studies group review 0

##Sum so that one point per nid and year combo (over age and sex)
invisible(severe_prop[, cases := sum(cases), by = c("nid", "year_id", "location_id")])
invisible(severe_prop[, sample_size := sum(sample_size), by = c("nid", "year_id", "location_id")])
severe_prop <- unique(severe_prop, by = c("nid", "year_id", "location_id")) ##sum cases and sample size over nid and year so only one data point per both sex all age
severe_prop <- merge(severe_prop, covs, by= c("location_id", "year_id"))
severe_prop <- merge(severe_prop, locations, by=c("location_id")) ##merge on locations and covariates

model_severe <- glmer(cases/sample_size ~ log_haqi + (1|super_region_name),
                      weights = sample_size, data = severe_prop, family = "binomial") ##mixed effects glm

##generate a frame for the predictions, want all countries years 1980-2016
predict_severe <- merge(locations, covs, by= "location_id")
predict_severe <- predict_idio[year_id>=1980,]

predictions_severe <- predictInterval(merMod = model_severe, newdata = predict_severe, 
                                      level = 0.95, n.sims = 1000, stat = "mean",
                                      type = "probability", returnSims = T)
sims <- attr(predictions_severe, "sim.results") ##get sims
predictions_severe <- as.data.table(cbind(predictions_severe, sims)) ##create data table with predictions and sims
setnames(predictions_severe, c(4:1003), draws) ##relabel sims as draws
predictions_severe <- cbind(predict_severe, predictions_severe) ##tack on info about each prediction
invisible(predictions_severe[, (draws) := lapply(.SD, function(x) inv.logit(x)), .SDcols = draws]) ##transform the draws

##reshape long with a column called "draw"
predictions_severe <- predictions_severe[, c("location_id", "year_id", draws), with = F]
melted_severe <- copy(predictions_severe)
setnames(melted_severe, draws, as.character(c(0:999)))
melted_severe <- melt(data = melted_severe, id.vars = c("location_id", "year_id"), 
                      value.name = "proportion", variable.name = "draw")
invisible(melted_severe[, draw := as.integer(draw)])

write_dta(predictions_severe, paste0(FILEPATH, ".dta"), version = 13)

##Graphs
pdf(paste0(FILEPATH, ".pdf"))
gg_severe_haqi <- ggplot(data = severe_prop, aes(x = log_haqi, y = mean)) +
  facet_wrap( ~ super_region_name) +
  geom_point()+
  geom_line(data = predictions_severe, aes(x = log_haqi, y = fit))+
  theme_bw()+
  ggtitle("Proportion Severe vs. Log(HAQ Index)")+
  labs(x = "Log(HAQ Index)", y = "Proportion Severe")
gg_severe_haqi
dev.off()

##TREATMENT GAP REGRESSION######################
invisible(tg_prop[, year_id := floor((year_start+year_end)/2)])
tg_prop <- tg_prop[group_review == 1 | is.na(group_review)] ##only studies group review 0

##Sum so that one point per nid and year combo (over age and sex)
invisible(tg_prop[, cases := sum(cases), by = c("nid", "year_id", "location_id")])
invisible(tg_prop[, sample_size := sum(sample_size), by = c("nid", "year_id", "location_id")])
tg_prop <- unique(tg_prop, by = c("nid", "year_id", "location_id")) ##sum cases and sample size over nid and year so only one data point per both sex all age
tg_prop <- merge(tg_prop, covs, by= c("location_id", "year_id"))
tg_prop <- merge(tg_prop, locations, by=c("location_id")) ##merge on locations and covariates

model_tg <- glmer(cases/sample_size ~ log_haqi + (1|super_region_name),
                  weights = sample_size, data = tg_prop, family = "binomial") ##mixed effects glm

##generate a frame for the predictions, want all countries years 1980-2016
predict_tg <- merge(locations, covs, by= "location_id")
predict_tg <- predict_tg[year_id>=1980,]

predictions_tg <- predictInterval(merMod = model_tg, newdata = predict_tg, 
                                  level = 0.95, n.sims = 1000, stat = "mean",
                                  type = "probability", returnSims = T)
sims <- attr(predictions_tg, "sim.results") ##get sims
predictions_tg <- as.data.table(cbind(predictions_tg, sims)) ##create data table with predictions and sims
setnames(predictions_tg, c(4:1003), draws) ##relabel sims as draws
predictions_tg <- cbind(predict_tg, predictions_tg) ##tack on info about each prediction
invisible(predictions_tg[, (draws) := lapply(.SD, function(x) inv.logit(x)), .SDcols = draws]) ##transform the draws

##reshape long with a column called "draw"
predictions_tg <- predictions_tg[, c("location_id", "year_id", draws), with = F]
melted_tg <- copy(predictions_tg)
setnames(melted_tg, draws, as.character(c(0:999)))
melted_tg <- melt(data = melted_tg, id.vars = c("location_id", "year_id"), 
                  value.name = "proportion", variable.name = "draw")
invisible(melted_tg[, draw := as.integer(draw)])

write_dta(predictions_tg, paste0(FILEPATH, ".dta"), version = 13)

##Graphs
pdf(paste0(FILEPATH, ".pdf"))
gg_tg_haqi <- ggplot(data = tg_prop, aes(x = log_haqi, y = mean)) +
  facet_wrap( ~ super_region_name) +
  geom_point()+
  geom_line(data = predictions_tg, aes(x = log_haqi, y = fit))+
  theme_bw()+
  ggtitle("Proportion Untreated vs. Log(HAQ Index)")+
  labs(x = "Log(HAQ Index)", y = "Proportion Untreated")
gg_tg_haqi
dev.off()

##TREATMENT NO FITS REGRESSION######################
invisible(treated_nf_prop[, year_id := floor((year_start+year_end)/2)])
treated_nf_prop <- treated_nf_prop[group_review == 1 | is.na(group_review)] ##only studies group review 0

##Sum so that one point per nid and year combo (over age and sex)
invisible(treated_nf_prop[, cases := sum(cases), by = c("nid", "year_id", "location_id")])
invisible(treated_nf_prop[, sample_size := sum(sample_size), by = c("nid", "year_id", "location_id")])
treated_nf_prop <- unique(treated_nf_prop, by = c("nid", "year_id")) ##sum cases and sample size over nid and year so only one data point per both sex all age
treated_nf_prop <- merge(treated_nf_prop, covs, by= c("location_id", "year_id"))
treated_nf_prop <- merge(treated_nf_prop, locations, by=c("location_id")) ##merge on locations and covariates

model_treated_nf <- glm(cases/sample_size ~ log_haqi, weights = sample_size, 
                        data = treated_nf_prop, family = "binomial") ##glm

##generate a frame for the predictions, want all countries years 1980-2016
predict_treated_nf <- merge(locations, covs, by= "location_id")
predict_treated_nf <- predict_treated_nf[year_id>=1980,]

predictions_treated_nf <- as.data.table(predict(model_treated_nf, newdata = predict_treated_nf, se.fit = T)) ##get predictions and standard errors
predictions_treated_nf <- cbind(predict_treated_nf, predictions_treated_nf) ##bind onto identifying data
alloc.col(predictions_treated_nf, 1000)
predictions_treated_nf <- predictions_treated_nf[, (draws) := as.list(inv.logit(rnorm(1000, fit, se.fit))), by = fit] ##get wide draws

##reshape long with a column called "draw"
predictions_treated_nf <- predictions_treated_nf[, c("location_id", "year_id", draws), with = F]
melted_treated_nf <- copy(predictions_treated_nf)
setnames(melted_treated_nf, draws, as.character(c(0:999)))
melted_treated_nf <- melt(data = melted_treated_nf, id.vars = c("location_id", "year_id"), 
                          value.name = "proportion", variable.name = "draw")
invisible(melted_treated_nf[, draw := as.integer(draw)])

write_dta(predictions_treated_nf, paste0(FILEPATH, ".dta"), version = 13)

##Graphs
invisible(predictions_treated_nf[, fit := inv.logit(fit)])
pdf(paste0(FILEPATH, ".pdf"))
gg_tnf_haqi <- ggplot(data = treated_nf_prop, aes(x = log_haqi, y = mean)) +
  geom_point()+
  geom_line(data = predictions_treated_nf, aes(x = log_haqi, y = fit))+
  theme_bw()+
  ggtitle("Proportion Treated without Fits vs. Log(HAQ Index)")+
  labs(x = "Log(HAQ Index)", y = "Proportion Treated without Fits")
gg_tnf_haqi
dev.off()

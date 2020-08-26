####################################################
## Description: SUICIDE DUE TO DRUGS RR META-REGRESSION
####################################################

rm(list=ls())

library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = paste0("FILEPATH"))
library(readxl)
library(ggplot2)
library(readr)

# LOADING DATA
old <- as.data.table(read_xlsx("FILEPATH"))
new <- as.data.table(read_xlsx("FILEPATH"))
new <- new[!(grepl("exclude", note_SR, ignore.case = T))]
old[, source := gsub( " .*$", "", field_citation_value)]
new[, source := gsub( "_.*$", "", study_name)]
new[, is_new := 1]
old[, is_new := 0]

old <- old[, c("source", "GBD_Risk_Factor", "effect_size", "lower", "upper", "cv_idu", "cv_all_drug", "is_new")]
new <- new[, c("source", "GBD_Risk_Factor", "effect_size", "lower", "upper", "is_new")]
data <- rbind(old, new, fill = T)
data[, cv_idu := ifelse(is.na(cv_idu), 0, cv_idu)]
data[, cv_all_drug := ifelse(is.na(cv_all_drug), 0, cv_all_drug)]
data[, `:=` (effect_log = log(effect_size), lower_log = log(lower), upper_log = log(upper))]
data[, se_log := (upper_log - lower_log)/3.92]

amphet_data <- data[GBD_Risk_Factor == "Amphetamine Use Disorders"]
cocaine_data <- data[GBD_Risk_Factor == "Cocaine Use Disorders"]
opioid_data <- data[GBD_Risk_Factor == "Opioid Use Disorders"]
amph_coke_data <- rbind(amphet_data, cocaine_data)

amphet_model <- rma(effect_log, se_log, data = amphet_data)
cocaine_model <- rma(effect_log, se_log, data = cocaine_data)
amph_coke_model <- rma(effect_log, se_log, data = amph_coke_data)
opioid_model <- rma(effect_log, se_log, mods = cbind(cv_idu, cv_all_drug), data = opioid_data)
opioid_w_idu <- rma(effect_log, se_log, data = opioid_data)

amphet_rr <- data.table(drug = "amphetamine", rr_log = amphet_model[1], se_log = amphet_model[3])
cocaine_rr <- data.table(drug = "cocaine", rr_log = cocaine_model[1], se_log = cocaine_model[3])
amph_coke_rr <- data.table(drug = "amphetamine and cocaine", rr_log = amph_coke_model[1], se_log = amph_coke_model[3])
opioid_rr <- data.table(drug = "opioid", rr_log = opioid_model[[1]][1], se_log = opioid_model[[3]][1])
opioid_w_idu_rr <- data.table(drug = "opioid broad", rr_log = opioid_w_idu[1], se_log = opioid_w_idu[3])

all_rr <- rbind(amphet_rr, cocaine_rr, amph_coke_rr, opioid_rr, opioid_w_idu_rr)
all_rr$rr_log <- as.numeric(all_rr$rr_log)
all_rr$se_log <- as.numeric(all_rr$se_log)
all_rr[, `:=` (rr = exp(rr_log), lower_log = rr_log - 1.96*se_log, upper_log = rr_log + 1.96*se_log)]

data[GBD_Risk_Factor == "Cocaine Use Disorders" | GBD_Risk_Factor == "Amphetamine Use Disorders", drug := "amphetamine and cocaine"]
data[GBD_Risk_Factor == "Opioid Use Disorders", drug := "opioid"]
data[drug == "opioid", exposure := ifelse(cv_idu == 0 & cv_all_drug == 0, "opioid", ifelse(cv_idu == 1 & cv_all_drug == 0, "idu", "any drug"))]

amphet_coke_gg <- ggplot() +
  geom_point(data = data[drug == "amphetamine and cocaine"], aes(x = effect_log, y = source, color = as.factor(GBD_Risk_Factor)), size = 3) + #shape = as.factor(is_new)
  xlim(-5,5) +
  geom_errorbarh(data = data[drug == "amphetamine and cocaine"], aes(y = source, x = effect_log, xmin = lower_log, xmax = upper_log, color = as.factor(GBD_Risk_Factor), height=0.5)) +
  geom_vline(xintercept = all_rr[drug == "amphetamine and cocaine", rr_log], linetype = "dashed", color = "darkorchid") +
  geom_vline(xintercept = 0) +
  geom_vline(xintercept = all_rr[drug == "amphetamine and cocaine", lower_log], linetype = "dashed", color = "darkorchid") +
  geom_vline(xintercept = all_rr[drug == "amphetamine and cocaine", upper_log], linetype = "dashed", color = "darkorchid") +
  labs(x = "Log Effect Size", y = "", color = "Risk Factor") +
  ggtitle(label = "Relative Risk Meta-analysis: Amphetamine and Cocaine") +
  theme_bw()+
  theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(), axis.text.y = element_blank(),
        legend.text = element_text(size=10),axis.text.x = element_text(size = 11),
        plot.title = element_text(hjust = 0.5,size = 12))

opioid_gg <- ggplot() +
  geom_point(data = data[drug == "opioid"], aes(x = effect_log, y = source, color = as.factor(exposure)), size = 2) + #shape = as.factor(is_new)
  xlim(-5,5) +
  geom_errorbarh(data = data[drug == "opioid"], aes(y = source, x = effect_log, xmin = lower_log, xmax = upper_log, color = as.factor(exposure), height=0.5)) +
  geom_vline(xintercept = all_rr[drug == "opioid", rr_log], linetype = "dashed", color = "darkorchid") +
  geom_vline(xintercept = 0) +
  geom_vline(xintercept = all_rr[drug == "opioid", lower_log], linetype = "dashed", color = "darkorchid") +
  geom_vline(xintercept = all_rr[drug == "opioid", upper_log], linetype = "dashed", color = "darkorchid") +
  labs(x = "Log Effect Size", y = "", color = "Risk Factor") +
  ggtitle(label = "Relative Risk Meta-analysis: Opioid") +
  theme_bw()+
  theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(), axis.text.y = element_blank(),
        legend.text = element_text(size=10),axis.text.x = element_text(size = 11),
        plot.title = element_text(hjust = 0.5,size = 12))

source(paste0("FILEPATH", "get_pct_change.R"))

df = get_pct_change(gbd_id_type='cause_id', gbd_id=562,
                    year_start_id=2007, year_end_id=2017,
                    location_id=1, age_group_id=24, sex_id=3,
                    gbd_round_id=5, decomp_step='None',
                    source='dalynator', change_type='pct_change_num')

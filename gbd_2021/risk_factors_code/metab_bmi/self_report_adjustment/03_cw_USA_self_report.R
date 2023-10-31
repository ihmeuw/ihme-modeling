################################################################################
## DESCRIPTION: Self-report adjustment (systematic error) for USA anthropometric data (height, weight, BMI, and derivatives) ##
## INPUTS: Age-sex split bundle version data ##
## OUTPUTS: Adjusted USA bundle version data appended to adjusted data from all other countries ready for upload to crosswalk version ##
## AUTHOR: 
## DATE: 
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
code_dir <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"

## Load dependencies
source("FILEPATH")

model_dir <- "FILEPATH"
data_dir <- "FILEPATH"

source("FILEPATH")
library(stringi, lib.loc = "FILEPATH")
library(openxlsx)
library(dplyr)
library(plyr)
library(assertable)
library(gtools)
library(ggplot2)
library(splines)
library(stringr)
library(msm)

locs <- get_location_metadata(location_set_id=1, release_id=9)[,c("location_id", "location_name_short", "super_region_id", "super_region_name",
                                                                  "region_id", "region_name", "ihme_loc_id")]
locs[, country_id := as.integer(as.factor(substr(ihme_loc_id, 1, 3)))][, country_name := substr(ihme_loc_id, 1, 3)]
locs_usa <- paste(locs$location_id[grepl("USA", locs$ihme_loc_id)])

## Get adjustment factor from crosswalk, merge to age-sex split bundle
ow_adj <- readRDS("FILEPATH")
ow_adj <- unique(ow_adj[,c("age_group_id","sex_id","pred","pred_se","diagnostic","decade")])
ow_adj[,pred_var := pred_se^2]

df_ow <- fread("FILEPATH")
df_ow_usa <- subset(df_ow, location_id %in% locs_usa)
df_ow_usa[, decade := 1990][year_id>=2000, decade := 2000][year_id>=2010, decade := 2010]
df_ow_usa[, decade := as.factor(decade)]

ob_adj <- readRDS("FILEPATH")
ob_adj <- unique(ob_adj[,c("age_group_id","sex_id","pred","pred_se","diagnostic","decade")])
ob_adj[,pred_var := pred_se^2]

df_ob <- fread("FILEPATH")
df_ob_usa <- subset(df_ob, location_id %in% locs_usa)
df_ob_usa[, decade := 1990][year_id>=2000, decade := 2000][year_id>=2010, decade := 2010]
df_ob_usa[, decade := as.factor(decade)]

ow_data <- merge(df_ow_usa, ow_adj, by=intersect(names(ow_adj), names(df_ow_usa)), all=T)
ob_data <- merge(df_ob_usa, ob_adj, by=intersect(names(ob_adj), names(df_ob_usa)), all=T)

## outlier info from original crosswalk
ow_data[age_group_id < 8 & diagnostic == 'self-report', is_outlier := 1]
ob_data[age_group_id < 8 & diagnostic == 'self-report', is_outlier := 1]

# GBD 2020
# After age-sex splitting, some rows were split such that the sample size was adjust to 0
# Fixing here
ow_data[sample_size == 0, sample_size := 10] # very arbitrary, should return to
ow_data[variance == "Inf" | is.na(variance), variance := val*(1-val)/sample_size]

ob_data[sample_size == 0, sample_size := 10] # very arbitrary, should return to
ob_data[variance == "Inf" | is.na(variance), variance := val*(1-val)/sample_size]

# After age-sex splitting, some rows have a value of 0 or 1. Set is_outlier == 1
ow_data[val %in% c(0,1) | variance == 0, is_outlier := 1]
ob_data[val %in% c(0,1) | variance == 0, is_outlier := 1]

ow_data[, se := sqrt(variance)]
ob_data[, se := sqrt(variance)]

## values cannot be 0 or 1 for logit space
offset_val <- 1e-05
ow_data[val == 0, val := val+offset_val][val == 1, val := val-offset_val][variance == 0, variance := variance+offset_val][variance == 1, variance := variance-offset_val]
ob_data[val == 0, val := val+offset_val][val == 1, val := val-offset_val][variance == 0, variance := variance+offset_val][variance == 1, variance := variance-offset_val]

# Apply adjustment and uncertainty to data. Needs to be done in logit space. Variance needs to be converted to and from logit space using delta method
# GBD 2020: only applying the adjustment for is_outlier = 0 since some rows cause the deltamethod to get stuck

ow_data[is_outlier == 0, logit_data := logit(val)]
mclapply(as.integer(row.names(ow_data[is_outlier == 0])), function(row) ow_data[row, logit_se := msm::deltamethod(~log(x1/(1-x1)), val, variance)], mc.cores = 1) %>% invisible
ow_data[, logit_adj_data := ifelse(diagnostic == 'self-report', logit_data - pred, logit_data)]
ow_data[, logit_adj_var := ifelse(diagnostic == 'self-report', logit_se^2 + pred_var, logit_se^2)]
ow_data[, adj_data := inv.logit(logit_adj_data)]
mclapply(as.integer(row.names(ow_data[is_outlier == 0])), function(row) ow_data[row, adj_var := msm::deltamethod(~exp(x1)/(1+exp(x1)), logit_adj_data, logit_adj_var)^2], mc.cores = 1) %>% invisible

ow_data[, cv_sr_adj := 0][pred != 0, cv_sr_adj := 1]
ow_data[, orig_val := val][, orig_variance := variance][, orig_se := se]
ow_data[cv_sr_adj==1, val := adj_data][cv_sr_adj==1, variance := adj_var][cv_sr_adj==1, se := sqrt(adj_var)]
ow_data[cv_sr_adj==1, `:=` (lower = val - 1.96 * se, upper = val + 1.96 * se)]
ow_data[cv_sr_adj==1, `:=` (lower = val - 1.96 * se, upper = val + 1.96 * se)]
ow_data[, c("pred", "pred_var","logit_data" ,"logit_se","logit_adj_data","logit_adj_var","adj_data","adj_var") := NULL]

ob_data[is_outlier == 0, logit_data := logit(val)]
mclapply(as.integer(row.names(ob_data[is_outlier == 0])), function(row) ob_data[row, logit_se := msm::deltamethod(~log(x1/(1-x1)), val, variance)], mc.cores = 1) %>% invisible
ob_data[, logit_adj_data := ifelse(diagnostic == 'self-report', logit_data - pred, logit_data)]
ob_data[, logit_adj_var := ifelse(diagnostic == 'self-report', logit_se^2 + pred_var, logit_se^2)]
ob_data[, adj_data := inv.logit(logit_adj_data)]
mclapply(as.integer(row.names(ob_data[is_outlier == 0])), function(row) ob_data[row, adj_var := msm::deltamethod(~exp(x1)/(1+exp(x1)), logit_adj_data, logit_adj_var)^2], mc.cores = 1) %>% invisible

ob_data[, cv_sr_adj := 0][pred != 0, cv_sr_adj := 1]
ob_data[, orig_val := val][, orig_variance := variance][, orig_se := se]
ob_data[cv_sr_adj==1, val := adj_data][cv_sr_adj==1, variance := adj_var][cv_sr_adj==1, se := sqrt(adj_var)]
ob_data[cv_sr_adj==1, `:=` (lower = val - 1.96 * se, upper = val + 1.96 * se)]
ob_data[cv_sr_adj==1, `:=` (lower = val - 1.96 * se, upper = val + 1.96 * se)]
ob_data[, c("pred", "pred_var","logit_data" ,"logit_se","logit_adj_data","logit_adj_var","adj_data","adj_var") := NULL]

## Get crosswalked data for all other countries
old_ow <- subset(df_ow, !(location_id %in% locs_usa))
old_ob <- subset(df_ob, !(location_id %in% locs_usa))

## Merge on new crosswalked USA results
ow_data <- rbind(ow_data, old_ow, fill=T)
ob_data <- rbind(ob_data, old_ob, fill=T)

ow_data <- subset(ow_data, is_outlier==0)
ob_data <- subset(ob_data, is_outlier==0)

##Variance is dropping for some, recalculate
ow_data[is.na(variance), variance := val*(1-val)/sample_size]
ob_data[is.na(variance), variance := val*(1-val)/sample_size]

##fill in missing crosswalk_parent_seq) if needed
ow_data[is.na(crosswalk_parent_seq), crosswalk_parent_seq := origin_seq]
ob_data[is.na(crosswalk_parent_seq), crosswalk_parent_seq := origin_seq]


### SAVE OUTPUTS
write.csv(ow_data, sprintf('FILEPATH', data_dir, ow_bundle_version_id), row.names = F)
write.csv(ob_data, sprintf('FILEPATH', data_dir, ob_bundle_version_id), row.names = F)


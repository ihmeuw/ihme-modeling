##############################
## Purpose: Fit the ASFR 10-14 and 50-54 models
##############################
sessionInfo()
rm(list=ls())

if (Sys.info()[1] == 'Windows') {
  username <- USERNAME
  root <- FILEPATH
  version <- "va63"
} else if (interactive()){
  username <- USERNAME
  root <- FILEPATH
  version <- "va69"
} else {
  username <- USERNAME
  root <- FILEPATH
  version <- commandArgs(trailingOnly = T)[1]
}

print(version)

## libraries
library(data.table)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(parallel)
library(magrittr)
library(MASS)
library(bindrcpp)
library(glue)
library(mortdb, lib = FILEPATH)
library(mvtnorm)

## set global options
year_start <- 1950
year_end <- 2017
draws <-F

## set directories
mort_function_dir <- FILEPATH
central_function_dir <- FILEPATH
fert_function_dir <- FILEPATH
if (Sys.info()[1] == 'Windows') fert_function_dir <- FILEPATH

base_dir <- FILEPATH

jbase             <- FILEPATH
data_dir          <- FILEPATH
out_dir      	  <- FILEPATH
param_dir         <- FILEPATH
j_data_dir        <- FILEPATH
diagnostic_dir    <- FILEPATH
j_results_dir     <- FILEPATH
map_dir           <- FILEPATH
final_results_dir <- FILEPATH
unraked_dir       <- FILEPATH

## functions
source(paste0(map_dir,"GBD_WITH_INSETS_MAPPING_FUNCTION.R"))

## creating directories
dir.create(paste0(base_dir, "results/gpr/age_10"))
dir.create(paste0(base_dir, "results/gpr/age_50"))
dir.create(paste0(unraked_dir, "/age_10"))
dir.create(paste0(unraked_dir, "/age_50"))


## set model locs, get loc and age maps
model_locs <- data.table(get_locations(level="estimate", gbd_year = 2017))
model_locs <- model_locs$ihme_loc_id

age_map <- data.table(get_age_map())[,.(age_group_id, age_group_name_short)]
setnames(age_map, "age_group_name_short", "age")

reg_map <- data.table(get_locations(level="estimate"))[,.(region_name, super_region_name, ihme_loc_id)]

## getting locations by whether it's a subnational or parent of a subnational
parents <- unique(substr(grep("_", model_locs, value = T),1 ,3))
parents_and_childs <- model_locs[grepl(paste(parents, collapse = "|"), model_locs)]
parents_and_childs <- parents_and_childs[!parents_and_childs %in% c("CHN_354", "CHN_361")]
no_child <- model_locs[!model_locs %in% parents_and_childs]
parents[parents == "CHN"] <- "CHN_44533"

## read in data/mapping files
input_data <- readRDS(paste0(j_data_dir, "asfr.RDs"))
input_data <- merge(input_data, age_map, all.x =T, by = "age_group_id")
outliers <- fread(paste0(j_data_dir, "outliered_sources.csv"))
asfr_cov <- fread(paste0(j_results_dir, "compiled_summary_gpr.csv"))

## bring in outliers
## bringing in outliers

outliers[,outlier := 1]
outliers <- outliers[,.(nid, ihme_loc_id, age, year_id, entire_timeseries, drop, outlier)]

yearly <- outliers[!is.na(year_id)]
setnames(yearly, "outlier", "outlier1")
input_data[,nid := as.character(nid)]
input_data[,age := as.numeric(age)]
input_data <- merge(input_data, yearly, by= c("ihme_loc_id", "nid", "year_id", "age"), all.x=T)
input_data[, entire_timeseries := NULL]
input_data[is.na(drop), drop := 0]
input_data <- input_data[drop != 1]
input_data <- input_data[,drop := NULL]

timeseries <- outliers[entire_timeseries ==1]
timeseries[,year_id := NULL]

input_data <- merge(input_data, timeseries, by = c("ihme_loc_id", "nid", "age"), all.x = T)
input_data[outlier1==1, outlier := 1]
input_data[,outlier1 := NULL]
input_data[is.na(outlier), outlier :=0]
input_data[is.na(drop), drop := 0]
input_data <- input_data[drop != 1]
input_data[,entire_timeseries := NULL]
input_data <- input_data[,drop := NULL]


## young ages first
young_asfr <- copy(input_data)
young_asfr <- young_asfr[age == 10 | age == 15]

## reshape to get ratio
young_asfr[, age := paste0("asfr_", age)]
## deal with nonunqiue NGA stuff here for now so I can move on
young_asfr <- young_asfr[!(nid == 253019 & year_id == 1990)]
young_asfr <- young_asfr[ihme_loc_id == "MHL", val := max(val), by = c("nid", "id", "source_type", "ihme_loc_id", "year_id", "outlier", "age")]
young_asfr <- unique(young_asfr,  by = c("nid", "id", "source_type", "ihme_loc_id", "year_id", "outlier", "age"))
young_asfr <- dcast.data.table(young_asfr, nid + id + source_type + ihme_loc_id + year_id + outlier ~ age, value.var = "val")
young_asfr <- young_asfr[!is.na(asfr_10) & !is.na(asfr_15)]

young_asfr[, ratio := asfr_10 / asfr_15]
young_asfr[,log_ratio := log(ratio)]
young_asfr[,log_asfr_15 := log(asfr_15)]


## at some point in prep code, if asfr10 was 0, it was replaced with 0.000001
## we probably want to drop all of this data? it corresponds to pretty different levels of asfr15
young_asfr <- young_asfr[asfr_10 <= 0.000001, outlier := 1]

## merge on region, superregion for random effects test
young_asfr <- merge(young_asfr, reg_map, all.x = T, by = "ihme_loc_id")

###################
## Model Fitting
###################

## young ASFR

form <-  paste0("log_ratio ~ log_asfr_15 + (1|super_region_name) + (1|region_name) + (1|ihme_loc_id)")

mod <- lmer(formula=as.formula(form), data=young_asfr[outlier == 0])

## ASFR 50-54
old_asfr <- copy(input_data)
old_asfr <- old_asfr[age == 45 | age == 50]

## reshape to get ratio
old_asfr[, age := paste0("asfr_", age)]
old_asfr <- old_asfr[ihme_loc_id == "MHL", val := max(val), by = c("nid", "id", "source_type", "ihme_loc_id", "year_id", "outlier", "age")]
old_asfr <- unique(old_asfr,  by = c("nid", "id", "source_type", "ihme_loc_id", "year_id", "outlier", "age"))

old_asfr <- dcast.data.table(old_asfr, nid + id + source_type + ihme_loc_id + year_id + outlier ~ age, value.var = "val")
old_asfr <- old_asfr[!is.na(asfr_45) & !is.na(asfr_50)]
old_asfr[,log_ratio := log(asfr_50 / asfr_45)]

## apply average ratio to all ages, but do it with a  model to get variance/covariance from it
mod2 <- lm(form = "log_ratio ~ 1", data = old_asfr[outlier ==0])

## saving model outputs to read into next step

save.image(paste0(out_dir, "young_old_asfr_workspace.RData"))


###########################
## plotting (to be moved)
##########################

asfr10_map <- copy(pred)
asfr10_map <- asfr10_map[,.(ihme_loc_id, year_id, mapvar = pred_asfr_10 *1000)]
test <- asfr10_map[year_id == 2017]

gbd_map(data = test[year_id == 2017], limits = round(quantile(test$mapvar),2), legend.title = "ASFR 10-14", na.color = "white", legend.cex = 2, col.reverse = T)

g <- copy(pred)
g <- g[,.(ihme_loc_id, year_id, pred_asfr_10, pred_asfr_10_wre, pred_asfr_50)]
g <- melt.data.table(g, id.vars = c("ihme_loc_id", "year_id"), measure.vars = c("pred_asfr_10", "pred_asfr_10_wre", "pred_asfr_50"), value.name = "asfr", variable.name = "type")

g[type %in% c("pred_asfr_10", "pred_asfr_10_wre"), age := 10]
g[type == "pred_asfr_50", age := 50]
g[type == "pred_asfr_10", type := "No Random Int"][type == "pred_asfr_10_wre", type := "Nested SR, R, loc, Random Ints"]


gdata <- copy(input_data)
gdata[,type := "asfr_data"]
gdata <- gdata[age == 10 | age ==  50]

g <- rbind(g, gdata[,.(ihme_loc_id, age, year_id, source_type, id, asfr = val, outlier, type)], fill =T)

write.csv(g, paste0(j_results_dir, "young_old_asfr_to_graph.csv"), row.names =F)

###########################################################
### Author: USERNAME
### Adapted from Code written by USERNAME
### Date: 12/9/2016
### Updated: 12/27/2017
### Project: GBD Nonfatal Estimation
### Purpose: EMR Regression for Parkinson
###########################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

set.seed(ID)
pacman::p_load(data.table, lme4, stringr, RMySQL, ggplot2)
library(openxlsx, lib.loc = paste0("FILEPATH"))

## SET OBJECTS
date <- gsub("-", "_", Sys.Date())
functions_dir <- paste0("FILEPATH")
doc_dir <- paste0("FILEPATH")
upload_dir <- paste0("FILEPATH")
draws <- paste0("draw_", c(0:999))
prev <- paste0("prev_", c(0:999))
deaths <- paste0("deaths_", c(0:999))
csmr <- paste0("csmr_", c(0:999))
emr <- paste0("emr_", c(0:999))
log_emr <- paste0("log_emr_", c(0:999))
meid <- ID
cid <- ID
bid <- ID
cod_model_f <- ID
cod_model_m <- ID
dismod_model <- ID
locations_regression <- c(ID, ID, ID, ID, ID, ID, ID) ## England, France, US, Wales, Austria, Scotland, Netherlands, Finland 
locations_replace <- c(locations_regression) ## Same countries 
zero_vars <- c("sex_issue", "year_issue", "age_issue", "age_demographer", "measure_issue", "measure_adjustment", "is_outlier", "smaller_site_unit")
blank_vars <- c("effective_sample_size",
                "cases", "sample_size", "measure_issue", "recall_type_value", "sampling_type", "response_rate", "case_name", 
                "case_definition","case_diagnostics", "group", "specificity", "group_review", "seq", "seq_parent", "input_type",
                "underlying_nid", "underlying_field_citation_value", "field_citation_value", "page_num", "table_num", "ihme_loc_id",
                "site_memo", "design_effect", "uncertainty_type", "note_SR")


## SOURCE FUNCTIONS
source(paste0("FILEPATH", "get_draws.R"))
source(paste0("FILEPATH", "get_covariate_estimates.R"))
source(paste0("FILEPATH", "get_location_metadata.R"))
source(paste0("FILEPATH", "get_model_results.R"))
source(paste0("FILEPATH", "get_ids.R"))
source(paste0("FILEPATH", "get_demographics.R"))
source(paste0("FILEPATH", "upload_epi_data.R"))
source(paste0("FILEPATH", "age_table.R"))

## USER FUNCTIONS
col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0("FILEPATH"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (name %in% names(dt) == F){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epiorder <- c(epi_order, extra_cols)
  setcolorder(dt, new_epiorder)
  return(dt)
}

## USER FUNCTIONS
col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0("FILEPATH"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (name %in% names(dt) == F){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epiorder <- c(epi_order, extra_cols)
  setcolorder(dt, new_epiorder)
  return(dt)
}

## GET DEMOGRAPHICS
dems <- get_demographics(gbd_team = "epi")
dem_ages <- dems$age_group_id[dems$age_group_id >= ID] ## only need 20 and older
dem_sexes <- dems$sex_id

## GET AGE WEIGHTS
death_results <- get_model_results(gbd_team = "cod", gbd_id = cid, measure_id = 1,
                                   location_id = locations_replace, year_id = 2017, sex_id = dem_sexes, 
                                   age_group_id = dem_ages, status = "best")
death_results[, mean_death := sum(mean_death), by = c("sex_id", "age_group_id")]
death_results <- unique(death_results, by = c("sex_id", "age_group_id"))
death_results[, weight := mean_death/sum(mean_death), by = "sex_id"]
setnames(death_results, "age_group_id", "new_age")
age_weights <- copy(death_results[, .(sex_id, new_age, weight)])
age_weights[new_age %in% 9:16, weight := sum(weight)][new_age %in% 9:16, new_age := 1]
age_weights <- unique(age_weights, by = c("new_age", "sex_id"))

## GET PREVALENCE DRAWS
prev_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = meid, source = "epi", 
                        status = "best",  measure_id = 5, sex_id = dem_sexes, 
                        age_group_id = dem_ages, location_id = locations_replace, year_id = 2017)
prev_draws <- prev_draws[ ,c("location_id", "year_id", "sex_id", "age_group_id", draws), with=F] 
setnames(prev_draws, draws, prev)

## GET CSMR DRAWS
csmr_draws_m <- get_draws(gbd_id_type = "cause_id", source= "codem", gbd_id = cid, 
                          version_id = cod_model_m, age_group_id = dem_ages, location_id = locations_replace,
                          year_id = 2017)
csmr_draws_f <- as.data.table(get_draws(gbd_id_type = "cause_id", source= "codem", gbd_id = cid, 
                                        version_id = cod_model_f, age_group_id = dem_ages, location_id = locations_replace,
                                        year_id = 2017))
csmr_draws <- rbind(csmr_draws_m, csmr_draws_f)
setnames(csmr_draws, draws, deaths)

## MERGE AND CONVERT TO CASES
all_draws <- merge(prev_draws, csmr_draws, by = c("location_id", "year_id", "age_group_id", "sex_id"))
all_draws[, (prev) := lapply(0:999, function(x) get(paste0("prev_", x)) * pop)]

## COLLAPSE BOTTOM AGE GROUPS (40-59)
all_draws <- all_draws[, new_age := age_group_id] 
all_draws <- all_draws[age_group_id<=16, new_age := 1] 
all_draws <- all_draws[, lapply(.SD, sum), .SDcols= c("pop", deaths, prev), 
                       by = c("location_id", "year_id", "new_age", "sex_id")]

## CONVERT TO CSMR AND PREVALENCE
all_draws[, (prev) := lapply(0:999, function(x) get(paste0("prev_", x)) * pop)]
all_draws[, (csmr) := lapply(0:999, function(x) get(paste0("deaths_", x)) * pop)]

## CALCULATE EMR AND LOG EMR
all_draws[, (emr) := lapply(0:999, function(x) get(paste0("csmr_", x)) / get(paste0("prev_", x)))]

## CONSOLIDATE AND SAVE
all_draws <- all_draws[, c("location_id", "sex_id", "new_age", emr), with=F] ##get rid of unnecessary columns
all_draws <- all_draws[, mean_emr := rowMeans(.SD), .SDcols = emr] 
all_draws <- all_draws[, lower_emr := apply(.SD, 1, quantile, probs= 0.025), .SDcols = emr]
all_draws <- all_draws[, upper_emr := apply(.SD, 1, quantile, probs=0.975), .SDcols = emr]
all_draws <- all_draws[, mean_logemr := log(mean_emr)]
emr_data <- all_draws[,.(location_id, sex_id, new_age, mean_emr, lower_emr, upper_emr)]
all_draws <- all_draws[location_id %in% locations_regression, .(location_id, sex_id, new_age, mean_emr, lower_emr, upper_emr, mean_logemr)]

## GET AGE STANDARDIZED
emr_standard <- copy(emr_data)
emr_standard <- merge(emr_standard, age_weights, by = c("new_age", "sex_id"))
emr_standard[, std := sum(mean_emr * weight), by = c("location_id", "sex_id")]
emr_standard <- unique(emr_standard, by = c("location_id", "sex_id", "std"))
emr_standard <- emr_standard[, .(location_id, sex_id, std)]

## DUMMY VARS
age_group_ids <- paste0("age_group", unique(all_draws$new_age)) ##list of all unique age groups
all_draws <- all_draws[, (age_group_ids) := lapply(age_group_ids, function(x) new_age==as.integer(gsub("age_group","",x)))] ##identity funciton, will create logical indicator variables for each age group
all_draws <- all_draws[, (age_group_ids) := lapply(.SD, as.integer), .SDcols = age_group_ids] ##turn logical into integer (0,1)
sex_ids <- paste0("sex_id", unique(all_draws$sex_id))
all_draws <- all_draws[, (sex_ids) := lapply(sex_ids, function(x) sex_id==as.integer(gsub("sex_id","",x)))] ##same steps for age group
all_draws <- all_draws[, (sex_ids) := lapply(.SD, as.integer), .SDcols = sex_ids]

## LINEAR REGRESSION
model <- lm(mean_logemr ~ age_group1 + age_group17 + age_group18 + age_group19 + age_group20 + age_group30 +
              age_group31 + age_group32 + sex_id1, data= all_draws)

## SAVE FOR DOCS
coef <- as.data.table(summary(model)$coefficients)
coef[, beta := c("intercept", "1", "17", "18", "19", "20", "30", "31", "32", "sex")] 
setnames(coef, "Std. Error", "se")
coef[, lower := round(Estimate - 1.96 * se, 3)]
coef[, upper := round(Estimate + 1.96 * se, 3)]
coef[, Estimate := round(Estimate, 3)]
coef[, se := round(se, 3)]
write.csv(coef, paste0("FILEPATH"), row.names = F)

## PREDICT OUT
predict_frame <- all_draws[, c("sex_id", "new_age", age_group_ids, sex_ids), with=F] ##only necessary columns for prediction
predict_frame <- unique(predict_frame, by= c("sex_id", "new_age", age_group_ids, sex_ids)) ##get only unique (not one per location)
predictions <- predict(model, predict_frame, se.fit=T) ##generate predictions
predict_frame$log.prediction <- predictions$fit ##assign as new columns in frame
predict_frame$log.st.err <- predictions$se.fit
predict_frame <- predict_frame[,.(sex_id, new_age, log.prediction, log.st.err)] 
graph_predictions <- copy(predict_frame)
predict_frame <- predict_frame[, (draws) := as.list(exp(rnorm(1000, log.prediction, log.st.err))), by= log.prediction] ##get wide draws
predict_frame <- predict_frame[, mean_emr := rowMeans(.SD), .SDcols = draws] 
predict_frame <- predict_frame[, lower_emr := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
predict_frame <- predict_frame[, upper_emr := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
predict_frame <- predict_frame[, se_emr := apply(.SD, 1, sd), .SDcols = draws]
predict_frame <- predict_frame[, cv_emr := se_emr/mean_emr]
cv_dt <- copy(predict_frame[, .(sex_id, new_age, cv_emr)])
predict_frame <- predict_frame[,.(sex_id, new_age, mean_emr, lower_emr, upper_emr)] 
predict_frame <- predict_frame[, merge := 1] 

## SAVE PREDICTED FOR DOCS
predict_doc <- copy(predict_frame)
predict_doc[, mean_emr := round(mean_emr, 3)]
predict_doc[, upper_emr := round(upper_emr, 3)]
predict_doc[, lower_emr := round(lower_emr, 3)]
predict_doc <- dcast(predict_doc, new_age ~ sex_id, value.var = c("mean_emr", "lower_emr", "upper_emr"))
predict_doc[, male := paste0(mean_emr_1, " (", lower_emr_1, " - ", upper_emr_1, ")")]
predict_doc[, female := paste0(mean_emr_2, " (", lower_emr_2, " - ", upper_emr_2, ")")]
write.csv(predict_doc, paste0(doc_dir, 'FILEPATH'), row.names = F)

## PREDICTED AGE STANDARDIZED
predict_standard <- copy(predict_frame)
predict_standard <- merge(predict_standard, age_weights, by = c("new_age", "sex_id"))
predict_standard[, std := sum(mean_emr * weight), by = "sex_id"]
predict_standard <- unique(predict_standard, by = c("sex_id", "std"))
predict_standard <- predict_standard[, .(sex_id, std)]

## GET LOCS AND CARTESIAN MERGE
loc_dt <- get_location_metadata(location_set_id = 9)
loc_dt <- loc_dt[level>=3,] ##keep if is national or subnational
loc_dt <- loc_dt[,.(location_id, location_name, level, parent_id)] 
loc_dt <- loc_dt[, merge := 1] 
all_emr <- merge(loc_dt, predict_frame, by="merge", allow.cartesian=T) ##merge to create rows of emr by age and sex for all locations

## SUB OUT TRUE EMR VALUES IF TRUE VALUE IS HIGHER THAN PREDICTION
all_emr <- merge(all_emr, cv_dt, by = c("sex_id", "new_age"))
all_emr <- all_emr[order(location_name, new_age)]
for (loc in unique(locations_replace)){
  for (sex in dem_sexes){
    if (emr_standard[location_id == loc & sex_id == sex, std] > predict_standard[sex_id == sex, std]){
      emr_dt <- emr_data[location_id == loc & sex_id == sex]
      all_emr[location_id == loc & sex_id == sex, mean_emr := emr_dt$mean_emr]
      all_emr[location_id == loc & sex_id == sex, se_emr := mean_emr * cv_emr]
      all_emr[location_id == loc & sex_id == sex, c("lower_emr", "upper_emr") := NA]
      all_emr[location_id == loc & sex_id == sex, note_modeler := paste0("country-sex using own calculated emr values from ", date)]
      true_data <- as.vector(emr_data[location_id == loc & sex_id == sex, .(mean_emr, lower_emr, upper_emr)])
      all_emr[location_id %in% loc_dt[parent_id == loc, location_id] & sex_id == sex, c("mean_emr", "lower_emr", "upper_emr") := true_data]
      all_emr[location_id %in% loc_dt[parent_id == loc, location_id] & sex_id == sex, se_emr := mean_emr * cv_emr]
      all_emr[location_id %in% loc_dt[parent_id == loc, location_id] & sex_id == sex, c("lower_emr", "upper_emr") := NA]
      all_emr[location_id %in% loc_dt[parent_id == loc, location_id] & sex_id == sex, note_modeler := paste0("country-sex using own calculated emr values from ", date)]
    }
  }
}
all_emr[, cv_emr := NULL]

## UPLOADER READY
age_row_1 <- data.table(age_group_id = 1, age_start = 20, age_end = 59)
age_dt <- rbind(ages, age_row_1)
setnames(age_dt, "age_group_id", "new_age")
all_emr <- merge(all_emr, age_dt, by = "new_age")
all_emr[sex_id == 1, sex := "Male"]
all_emr[sex_id == 2, sex := "Female"]
setnames(all_emr, c("mean_emr", "upper_emr", "lower_emr", "se_emr"), c("mean", "upper", "lower", "standard_error"))
all_emr <- all_emr[, .(location_id, location_name, sex, age_start, age_end, mean, upper, lower, standard_error, note_modeler)]
all_emr[, bundle_id := bid]
all_emr[, nid := 236209]
all_emr[, year_start :=  1990]
all_emr[, year_end := 2016]
all_emr[, source_type := "Mixed or estimation"]
all_emr[, measure := "mtexcess"]
all_emr[, unit_type := "Person*year"]
all_emr[, unit_value_as_published := 1]
all_emr[, (zero_vars) := 0]
all_emr[is.na(standard_error), uncertainty_type_value := 95]
all_emr[, representative_name := "Nationally and subnationally representative"]
all_emr[, urbanicity_type := "Unknown"]
all_emr[, recall_type := "Point"]
all_emr[, recall_type_value:= 1]
all_emr[, extractor := "eln1"]
all_emr[is.na(note_modeler), note_modeler := paste0("data estimated using linear regression on ", date)]
all_emr[, (blank_vars) := ""]
all_emr <- col_order(all_emr)

## OUTPUT AND UPLOAD
write.xlsx(all_emr, paste0(upload_dir, "FILEPATH"), sheetName = "extraction")
upload_epi_data(bundle_id = bid, filepath = paste0(upload_dir, "FILEPATH"))

## DIAGNOSTICS
gg_data <- copy(all_draws)
gg_data <- merge(gg_data, age_dt, by = "new_age")
gg_data[, age := (age_start+age_end)/2]
results <- copy(graph_predictions)
results[, prediction := exp(log.prediction)]
results <- merge(results, age_dt, by = "new_age")
results[, age := (age_start+age_end)/2]

gg <- ggplot(gg_data, aes(x = age, y = mean_emr))+
  geom_point(aes(color = as.factor(sex_id)))+
  geom_line(data = results, aes(x = age, y = prediction, color = as.factor(sex_id))) +
  theme_classic()

gg_log <- ggplot(gg_data, aes(x = age, y = mean_logemr))+
  geom_point(aes(color = as.factor(sex_id)))+
  geom_line(data = results, aes(x = age, y = log.prediction, color = as.factor(sex_id))) +
  theme_classic()

pdf(paste0(doc_dir, "emr_results", date, ".pdf"))
print(gg)
print(gg_log)
dev.off()
###################################################################################
# This script is used to model remission using procedure codes in USA claims data
###################################################################################

## Load functions and packages
home_dir <-  "FILEPATH"
library(msm, lib.loc="FILEPATH")
library(metafor, lib.loc="FILEPATH")
pacman::p_load(data.table, ggplot2, dplyr, stringr, DBI, openxlsx, gtools, plyr, readxl)

mrbrt_helper_dir <- "FILEPATH"
functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

functions_dir <-"FILEPATH"
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_covariate_estimates.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))
age_predict <- c(0,10,20,30,40,50,60,70,80,90,100) 

date <- gsub("-", "_", Sys.Date())
date <- Sys.Date()


######################################################################################################## 
## Step 1: AGGREGATE PROC/ALL DATA IN PREPARATION FOR REGRESSION
######################################################################################################## 
dt_proc <- as.data.table(read.csv("FILEPATH"))

merge_cv <- c("year", "sex", "age_start")
dt_proc[, `:=` (num_enrolid_with_proc_type = sum(num_enrolid_with_proc_type), num_enrolid_with_dx_type = sum(num_enrolid_with_dx_type)), by = merge_cv]
dt_proc[, c("type")] <- NULL
dt_proc <- unique(dt_proc, by = merge_cv)

dt_proc[, mean := num_enrolid_with_proc_type/num_enrolid_with_dx_type]

setnames(dt_proc, c("year", "sex", "num_enrolid_with_proc_type", "num_enrolid_with_dx_type"), c("year_id", "sex_id", "cases", "sample_size"))
dt_proc <- merge(dt_proc, sex_dt, by ="sex_id")
dt_proc$measure <- "remission"


## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  dt[is.na(standard_error) & measure == "remission" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "remission" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  
  return(dt)
}
dt_proc$standard_error <- NA
dt_proc$standard_error <- as.numeric(dt_proc$standard_error)
dt_proc <- get_se(dt_proc)

## ADD LOCATION ID
dt_proc$location_id <- 102


######################################################################################################## 
## Step 2: SET UP COVARIATES (HAQi, SEX, AGE)
######################################################################################################## 
# HAQi
haq <- as.data.table(get_covariate_estimates( covariate_id=1099,
                                              gbd_round_id=6,
                                              decomp_step='step3'))

haq2 <- haq[, c("location_id", "year_id", "mean_value", "lower_value", "upper_value")]
setnames(haq2, "mean_value", "haqi_mean")
setnames(haq2, "lower_value", "haqi_lower")
setnames(haq2, "upper_value", "haqi_upper")

haq2[, `:=` (haqi_mean = mean(haqi_mean) ), by = c("location_id", "year_id")]

dt_proc <- merge(dt_proc, haq2, by = c("location_id", "year_id"))


dt_proc[, `:=` (haqi_mean = mean(haqi_mean), haqi_lower = mean(haqi_lower), haqi_upper = mean(haqi_upper))]
dt_proc <- unique(dt_proc, by =  c("sex_id", "age_start")) 
dt_proc$year_id <- 2012

# SEX
dt_proc$sex_binary[dt_proc$sex_id==2] <- 0
dt_proc$sex_binary[dt_proc$sex_id==1] <- 1

# AGE
dt_proc = within(dt_proc, {age1 = ifelse((age_start <6), 0, 0)})
dt_proc = within(dt_proc, {age2 = ifelse((age_start >9 & age_start <20), 1, 0)})
dt_proc = within(dt_proc, {age3 = ifelse((age_start >19 & age_start <30), 1, 0)})
dt_proc = within(dt_proc, {age4 = ifelse((age_start >29 & age_start <55), 1, 0)})
dt_proc = within(dt_proc, {age5 = ifelse((age_start >54 & age_start <96), 1, 0)})

# AGE and SEX interaction term
dt_proc$age_sex [dt_proc$sex_id==1] <- 1

write.csv(dt_proc, "FILEPATH")


######################################################################################################## 
## Step 3: GET METADATA FOR PREDICTIONS
######################################################################################################## 

## Get meta data
loc_dt <- as.data.table(get_location_metadata(location_set_id = 22))
loc_dt1 <- subset(loc_dt, level==3 & location_id!=95)
loc_dt2 <- subset(loc_dt, level==4)
loc_dt <- rbind(loc_dt1, loc_dt2)
loc_dt[, c("location_set_id", "level", "is_estimate", "location_set_version_id", "parent_id", "path_to_top_parent", "most_detailed", "sort_order", "lancet_label", "location_ascii_name", "location_name_short", "location_type_id", "location_type", "ihme_loc_id", "local_id", "developed", "start_date", "end_date", "date_inserted", "last_updated", "last_updated_by", "last_updated_action" ) := NULL]
age_dt <- as.data.table(get_age_metadata(12))
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]
age_dt <- subset(age_dt, age_group_id >4)
setnames(age_dt, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
sex_dt <- read.csv("FILEPATH")
sex_dt <- subset(sex_dt, sex_id==c(1, 2))
year_dt <- read.csv("FILEPATH")

## Create master data frame for predictions
meta_dt <- tidyr::crossing(loc_dt, age_dt)
meta_dt <- tidyr::crossing(meta_dt, sex_dt)
meta_dt <- tidyr::crossing(meta_dt, year_dt)

## Add HAQi
meta_dt <- merge(meta_dt, haq2, by = c("location_id", "year_id"))

# SEX
meta_dt$sex_binary[meta_dt$sex_id==2] <- 0
meta_dt$sex_binary[meta_dt$sex_id==1] <- 1

# AGE
meta_dt = within(meta_dt, {age1 = ifelse((age_start <6), 0, 0)})
meta_dt = within(meta_dt, {age2 = ifelse((age_start >9 & age_start <20), 1, 0)})
meta_dt = within(meta_dt, {age3 = ifelse((age_start >19 & age_start <30), 1, 0)})
meta_dt = within(meta_dt, {age4 = ifelse((age_start >29 & age_start <55), 1, 0)})
meta_dt = within(meta_dt, {age5 = ifelse((age_start >54 & age_start <96), 1, 0)})

######################################################################################################## 
## Step 4: REGRESSOIN AND PREDICTION MODELS
######################################################################################################## 

scatter.smooth(x=dt_proc$age_start, y=dt_proc$mean)

## TEST INTERACTIONS BETWEEN COVARIATES
int1=lm(mean~haqi_mean * sex_binary, data=dt_proc , singular.ok = FALSE) #interaction term is collinear
int2=lm(mean~haqi_mean * age_start, data=dt_proc , singular.ok = FALSE) #interaction term is collinear; out from these two equations indicate that HAQi is collinear with the interaction terms with sex or age; therefore interaction terms not needed with HAQi
int3=lm(mean~haqi_mean * age1, data=dt_proc , singular.ok = FALSE) # this indicates that sex and age do not interact with one another

summary(int1)
summary(int2)
summary(int3)


## Fit a model 
reg1=lm(mean~ 0+ haqi_mean, data=dt_proc )
reg2=lm(mean~ 0+ haqi_mean + sex_binary, data=dt_proc )
reg3=lm(mean~ 0+ haqi_mean + age_start, data=dt_proc )
reg4=lm(mean~ 0+ haqi_mean  + age1+ age2, data=dt_proc )

summary(reg1) # chosen
summary(reg2) #sex not significant
summary(reg3) #age singularity issue with HAQi
summary(reg4)
summary(reg5)
summary(reg6)

AIC(reg1)
AIC(reg2)
AIC(reg3)
AIC(reg4)
AIC(reg5)
AIC(reg6)

BIC(reg1)
BIC(reg2)
BIC(reg3)
BIC(reg4)

predictions <- predict(reg1, interval="confidence", newdata = meta_dt)

meta_dt_pred <- cbind(meta_dt, predictions)


######################################################################################################## 
## Step 5: PREP META DATASET FOR EPI UPLOAD
######################################################################################################## 

## For uploader validation - add in needed columns
meta_dt_pred$seq <- NA
meta_dt_pred$sex <- ifelse(meta_dt_pred$sex_binary == 1, "Male", "Female")
meta_dt_pred$year_start <- meta_dt_pred$year_id
meta_dt_pred$year_end <- meta_dt_pred$year_id
meta_dt_pred[,c("sex_binary", "year_id", "haqi_mean")] <- NULL
meta_dt_pred$nid <- 419449
meta_dt_pred$year_issue <- 0
meta_dt_pred$age_issue <- 0
meta_dt_pred$sex_issue <- 0
meta_dt_pred$measure <- "remission"
meta_dt_pred$source_type <- "Facility - inpatient"
meta_dt_pred$extractor <- "USERNAME"
meta_dt_pred$is_outlier <- 0
meta_dt_pred$sample_size <- 1000
setnames(meta_dt_pred, c("fit", "lwr", "upr"), c("mean", "lower", "upper"))
meta_dt_pred$cases <- meta_dt_pred$sample_size * meta_dt_pred$mean
meta_dt_pred$measure_adjustment <- 1
meta_dt_pred$uncertainty_type <- "Confidence interval"
meta_dt_pred$uncertainty_type_value <- 95
meta_dt_pred$recall_type <- "Point"
meta_dt_pred$urbanicity_type <- "Mixed/both"
meta_dt_pred$unit_type <- "Person"
meta_dt_pred$representative_name <- "Nationally and subnationally representative"
meta_dt_pred$note_modeler <- "These data are modeled from the US data using the number of people with procedures codes among those with ICD code of interest, intercept at (0, 0) for HAQi"
meta_dt_pred$unit_value_as_published <- 1
meta_dt_pred$step2_location_year <- "This is modeled remisison that uses the NID for one dummy row of remisison data added to the bundle data"

write.csv(meta_dt_pred, "FILEPATH")


######################################################################################################## 
## Step 6: APPEND INPUT DATA
######################################################################################################## 

input_dt <- read_excel("FILEPATH")
appended <- rbind(input_dt, meta_dt_pred, fill=TRUE)

write.xlsx(appended, paste0("FILEPATH"),  sheetName = "extraction", col.names=TRUE)

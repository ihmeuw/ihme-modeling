####################################################################
## Fetal alcohol syndrome sex splitting 
## Purpose: Pull data from bundle, prepare crosswalk version, upload
####################################################################

# Clean up and initialize with the packages we need
rm(list = ls())
library(data.table)
library(xlsx)
library(msm)
library(ggplot2)
library(plyr)
library(parallel)
library(RMySQL)
library(stringr)
library(Hmisc)
library(mortdb, lib = "FILEPATH")


date <- Sys.Date()
date <- gsub("-", "_", Sys.Date())

draws <- paste0("draw_", 0:999)

#Mr_bert functions 
source("FILEPATH")

# Central functions
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_bundle_data", "save_crosswalk_version", "save_bundle_version", "get_bundle_version")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x, ".R"))))

mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0("FILEPATH", x, ".R"))))

#Getting locations 
locs<- get_location_metadata(location_set_id =35, gbd_round_id = 6)
locs<- locs[, c("location_id", "super_region_name", "location_name")]


#---------------## INPUT DATA 
# Fetal alcohol syndrome bundle id: 154
bid<-154
dstep<-"step2"

# Download data - n=357
df<-get_bundle_data(bundle_id = bid, decomp_step = dstep)
head(df)
table(df$measure)

#---------------## CLEAN DATA 
#Keeping desired rows (dropping group review == 0 and outliers)
df<-df[group_review == 1 | is.na(group_review)] 
df <- df[measure %in% c("prevalence", "incidence")]


#---------------## CHECK DATA FOR SEX SPLIT 
#Checking how many data with disagregated sex
investigate_sex <- df[sex%like% "Male" | sex%like%"Female"] 
unique(investigate_sex$nid) 
rm(investigate_sex)

#Checking if we need to sex split (any sex = Both)
message(paste0("You", ifelse(nrow(df[sex=="Both",])>0,"", "don't"), " need to sex-split."))

# Give a heads up about how many sex-specific data points you have to XW.
num_both_sex <- nrow(df[sex=="Both",]) 
message(paste0("Starting to sex-split. You have ", num_both_sex, " both-sex data points to be split."))


#---------------## FIND SEX MATCHES   
cv_drop<-names(df)[names(df)%like%"cv" & names(df)!="cv_passive"]

dem_sex_dt <- df[is_outlier == 0] #excluding outliers for ratio calculation only
dem_sex_dt <- get_cases_sample_size(dem_sex_dt)
dem_sex_dt <- get_se(dem_sex_dt)
dem_sex_dt <- calculate_cases_fromse(dem_sex_dt)
dem_sex_matches <- find_sex_match(dem_sex_dt, measure_vars = c("prevalence", "incidence"))
dem_sex_matches<- merge(locs, dem_sex_matches, by= "location_id")

num_matches <- nrow(dem_sex_matches) #n=3
message(paste0("Preparing for MR BRT. You have ", num_matches, " matches."))
  
  
## Pull out both-sex data and sex-specific data. both_sex is used for predicting and sex_specific will be the data we use. Includes outliers. 
both_sex <- df[sex=="Both",]
both_sex[, id := 1:nrow(both_sex)]

sex_specific <- df[sex!="Both",]
n <- names(sex_specific)
  

#---------------## MR BRT   
#Calculate ratios 
message("calculating ratios")
mrbrt_sex_dt <- calc_sex_ratios(dem_sex_matches)

#Recreating study IDs (NID-loc-year)
mrbrt_sex_dt$id <- NULL
setDT(mrbrt_sex_dt)[, id := .GRP, by = c("nid", "location_id", "year_start")]

#Explore matched ratios 
ggplot(data = mrbrt_sex_dt, aes(x= id, y = ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se), alpha = .4) +
   labs(x = "ID", y = "Female:Male Sex Ratio", title = "FAS Sex Ratios") + theme_bw() + theme(text=element_text(size = 12))  

#Output model results  
model_name <- paste0("fas_sexplit_test", date)

#Run MRBRT
sex_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = paste0(model_name),
  data = mrbrt_sex_dt,
  mean_var = "log_ratio",
  se_var = "log_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)


## Pull out covariates to return later.
sex_covariates <- sex_model$model_coefs


#---------------## PREDICT
# Predict ratio - Simple version for plotting 
preds<-as.data.table(predict_mr_brt(sex_model, newdata = expand.grid(X_intercept = 1, Z_intercept = 1), write_draws = F)$model_summaries)
cols<-c("Y_mean", "Y_mean_lo", "Y_mean_hi")
preds<-preds[, (cols):=lapply(.SD, exp), .SDcols = cols]  
head(preds) 

# Predicted ratio with write_draws = TRUE 
sex_preds <- predict_mr_brt(sex_model, both_sex, write_draws = T)
sex_draws <- data.table(sex_preds$model_draws) 

# Using the draws to get the predicted ratio & SE of male/female
sex_draws[, c("X_intercept", "Z_intercept") := NULL]
draws <- names(sex_draws)[grepl("draw", names(sex_draws))]
sex_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]  


#---------------## GET AND PREPARE POPULATION FOR SEX SPLITTING 
both_sex[, year_mid := (year_start + year_end)/2]
both_sex[, year_floor := floor(year_mid)]
both_sex[, age_mid := (age_start + age_end)/2]
both_sex[, age_floor:= floor(age_mid)]

table(both_sex$age_mid) 

# Pull out population data. We need age- sex- location-specific population.
pop <- get_population(age_group_id = "all", sex_id = "all", decomp_step = "step2", year_id = unique(floor(both_sex$year_mid)),
                     location_id=unique(both_sex$location_id), single_year_age = T)

#Get age group IDs
ids <- get_ids("age_group") ## age group IDs
head(ids)
pop <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)
head(pop)
table(pop$age_group_name)

#Before merging, adjust age group <1 year so it can match the mid age < 1
pop$age_group_name[pop$age_group_name == '<1 year'] <- '0'
table(pop$age_group_name)

#Transform age group name to numeric so it can marge to mid age 
pop$age_group_name <- as.numeric(pop$age_group_name)
table(pop$age_group_name)

## Merge in populations for both-sex and each sex.
both_sex <- merge(both_sex, pop[sex_id==3,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(both_sex, "population", "population_both")
both_sex <- merge(both_sex, pop[sex_id==1,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(both_sex, "population", "population_male")
both_sex <- merge(both_sex, pop[sex_id==2,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(both_sex, "population", "population_female")

sex_draws[, id := 1:nrow(sex_draws)] #Creating id
both_sex[, id := 1:nrow(both_sex)] #Creating id
both_sex <- both_sex[!is.na(population_both)] #No missing population 
ids <- unique(both_sex$id)
num <- nrow(df[sex=="Both",]) - length(ids)
sex_draws <- sex_draws[id %in% ids,]
if (num > 0) message(paste0("WARNING: You lost ", num, " both-sex data point(s) because we couldn't find a population for that point."))


#---------------## SEX SPLITTING 
# Calculate means that will be reported in the note_modeler column
ratio_mean <- round(sex_draws[, rowMeans(.SD), .SDcols = draws], 2)
ratio_se <- round(sex_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)

# Apply predictions of the ratio
both_sex <- merge(both_sex, sex_draws, by="id", allow.cartesian = T)

both_sex[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (population_both/(population_male + (get(paste0("draw_", x)) * population_female))))]
both_sex[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
both_sex[, m_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
both_sex[, f_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
both_sex[, m_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
both_sex[, f_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]

# get rid of draw columns
both_sex[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]

z <- qnorm(0.975)
both_sex[mean == 0, sample_sizem := sample_size * population_male/population_both]
both_sex[mean == 0 & measure == "prevalence", m_standard_error := sqrt(mean*(1-mean)/sample_sizem + z^2/(4*sample_sizem^2))]
both_sex[mean == 0 & measure == "incidence", m_standard_error := ((5-mean*sample_sizem)/sample_sizem+mean*sample_sizem*sqrt(5/sample_sizem^2))/5]
both_sex[mean == 0, sample_sizef := sample_size * population_female/population_both]
both_sex[mean == 0 & measure == "prevalence", f_standard_error := sqrt(mean*(1-mean)/sample_sizef + z^2/(4*sample_sizef^2))]
both_sex[mean == 0 & measure == "incidence", f_standard_error := ((5-mean*sample_sizef)/sample_sizef+mean*sample_sizef*sqrt(5/sample_sizef^2))/5]
both_sex[, c("sample_sizem", "sample_sizef") := NULL]

## Make male- and female-specific dts
male_dt <- copy(both_sex)
male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = "", lower = "",
                cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male",
                note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                      ratio_se, ")"))]
male_dt <- dplyr::select(male_dt, n)
female_dt <- copy(both_sex)
female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]

female_dt <- dplyr::select(female_dt, n) ## original names of data

data <- rbind(sex_specific, female_dt)
data <- rbind(data, male_dt)

sex_specific_data <- copy(data)

message("Done with sex-splitting.")

#---------------## EXPORT FINAL SEX SPLIT DATA 

write.xlsx(sex_specific_data, "FILEPATH")


# sex split for lead


rm(list=ls())

# libraries ######################################
# load packages
library(data.table)
library(tidyr)
library(dplyr)
library(reshape2)
library(msm) #to use the deltamethod function
library(openxlsx)
library(binom)
library(readr)
library(reticulate)

#MRBRT
reticulate::use_python("FILEPATH")
mr <- import("mrtool")

# set custom functions
"%unlike%" <- Negate("%like%") #function for "not like this"
"%ni%" <- Negate("%in%") #function for 'does not include"

# load shared functions
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

# values ####################################################
# set bundle, bundle version, and decomp step
bundle_id <- 4739 #Bundles are like folders where you store data
b_version <- 49239 #There are different versions each time you upload data
release<-16 #release_id
years<-c(1970:2024) #this must match the years in your data
gbd<-"GBD2022" #for filepath

#import data ###################################
# read in bundle data
data <- get_bundle_version(bundle_version_id = b_version, fetch = "all")
setnames(data, "val", "mean") #Changing the val column name to be mean instead
data[, standard_error := standard_deviation/(sqrt(sample_size))] 

if ("note_modeler" %ni% names(data)) { 
  data[, note_modeler := as.character()] 
}

#calculate standard error when there are none
data[is.na(standard_error) & !is.na(variance) & !is.na(sample_size),standard_error:=(sqrt(variance)/sqrt(sample_size))]



### SEX SPLIT ------------------------------------------------------------------------------------------

############ Format the data for sex ratio model ######################################
sex_specific <- data[sex != "Both" & is_outlier==0] #Make a separate dataset where the sex column does not equal both and are not outliers
sex_specific[, index := .GRP, by = .(nid, location_id, year_start, year_end, age_start, age_end)] 

link <- unique(sex_specific[, .(nid, index)]) #This pulls out which NID is associated with which index number

ss_long <- sex_specific[, .(index, sex, mean, standard_error)] #Makes a table of just the sex, mean, and std_error that is associated with each index number

#remove row that have NA in the standard error column
ss_long<-ss_long[!is.na(standard_error)]

ss_long2<-melt(ss_long, id.vars = c("index", "sex"))

ss <- dcast(melt(ss_long, id.vars = c("index", "sex")), index~variable+sex, fun.aggregate = sum) #Makes it a wide table (instead of a long one). Makes separate mean and
#std_error columns for each sex. First he is melting (wide to long) ss_long to an even longer table (not sure why you would do this). For the melting function the 2 column
#names would be "index" and "sex". 
#Then once we melted it, we use dcast (long to wide) to widen the table. When using dcast you use the equation: y~x. Y is the identification variable, meaning the rows are
#going to be anchored/idenified based on Y. X is the measured variables. When you melt the table, it created 2 new columns called "variable" and "value". So the measured
#variables that are using in the equation is the variable and sex columns. This means that there is going to be a separate column for each variable-sex combo, and
#the rows will include the numbers from the value columns.

ss <- merge(ss, link, by = "index", all.x = T) #all.x=T means that you keep all of the rows, even if they don't have a matching pair with the other table

setDT(ss) #converts ss to be a data.table

ss <- ss[mean_Female > 0 & mean_Male > 0] # will be running model in log space so can't have observations of 0
#Remove any rows that have 0 in M and F

##################### Custom function to calculate the sex ratios in the sex specific data sources ###################################
calc_sex_ratios <- function(dt){
  ratio_dt <- copy(dt) #making sure this doesn't mess up other data tables
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male, #Makes a new column called ratio that divides the mean of female over male
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
                  #SE= std/sqrt(n) is used on continuous data, but use the above equation when it the se of the ratio
  ratio_dt[, log_ratio := log(ratio)] #Making a column for the log of the ratio, this way you can't have negative ratios
  ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) { #Make a new column call log_se and apply this function to it. sapply applies a function to a list
    mean_i <- ratio_dt[i, "ratio"] #Making a variable to go into deltamethod
    se_i <- ratio_dt[i, "ratio_se"] #Making a variable to go into deltamethod
    deltamethod(~log(x1), mean_i, se_i^2) #This is determining the standard error of the log transformation.
                                          #x1 is essentially a place holder, required by the function, to say what function you are trying to find the standard error of.
                                          #Second arguement is the mean of the variable
                                          #Third argument is the covariance, which is se^2
  }) #deltamethod is the final function being applied to make the log _se column
  #for log_ratio_se you can't go log(ratio_se). you have to go from linear to log, using the deltamethod. This is the standard way of doing it
  return(ratio_dt) #return a table with the ratio of the sex means, standard error, log of the ratio, and standard error for that log
}

###################### calc sex ratios ###################################
#Apply the custom ratio sex function to the ss data table
mrbrt_sex_dt <- calc_sex_ratios(ss) #maintains the original ss data table but then adds the 4 columns that were created by calc_sex_ratios

#################### run MRBRT model ###############################################
#run a MRBRT model to determine the sex pattern

#Format an empty dataframe for MR BRT
ss_data <- mr$MRData() #empty dataframe that is formatted for MR BRT

#Load out dataframe, with the data, into the MR BRT model
ss_data$load_df(
  data = mrbrt_sex_dt, #This is the data we are reading in
  col_obs = "log_ratio", #mean of our dependent variable. In this case we are looking at the log of the ratio
  col_obs_se = "log_se", #se of the dependent variable. In this case it is the se of the logged ratio
  col_covs = list(), #list of covariates we will model. In this case, none. 
  col_study_id = "nid" #This is the id we will be randomizing
)

#prep the model
ss_model <- mr$MRBRT(
  data = ss_data,
  cov_models = list(
    mr$LinearCovModel("intercept", use_re = TRUE)
  )
) 

#run the model
ss_model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L) 

# predict & get draws
ss_pred_df <- data.frame(intercept = 1) #This is a dataframe that only has one column and row: intercept=1

#make a MRBRT data object
ss_pred_data <- mr$MRData() 

#fill the object with the prediction table
ss_pred_data$load_df(
  data = ss_pred_df,
  col_covs = list("intercept")
) 

#use the model ran above to predict based on the prediction
ss_pred <- ss_model$predict(data = ss_pred_data)

# draws ###############################################
#make draws
ss_samples <- ss_model$sample_soln(sample_size = 1000L)
ss_draws <- ss_model$create_draws(
  data = ss_pred_data,
  beta_samples = ss_samples[[1]],
  gamma_samples = ss_samples[[2]],
  random_study = TRUE
)
ss_draws <- data.table(ss_draws)
setnames(ss_draws, paste0("V",1:1000), paste0("draw_",0:999)) # rename columns

#the draws are in log space, transform them into linear
ss_draws[, (names(ss_draws)) := lapply(.SD, exp)]

# duplicate the rows for each year
ss_draws<-ss_draws[rep(1, length(years)), ][, year_id := years]

#export #################################
write_excel_csv(ss_draws,paste0("FILEPATH",gbd,"FILEPATH/lead_sex_pattern.csv"))
















# old #############################################
# ################# Define sex split functions #################################################
# #Get the population
# get_row <- function(n, dt, pop_dt){
#   row_dt <- copy(dt) #good to do this each time to be safe. Data.tables have the ability to change a table that you are referencing, so when you copy it, it won't
#   #mess with any of the other tables.
#   row <- row_dt[n] #copy the dt the number of times that you specify
#   row[age_start>=.999, age_end := ceiling(age_end)]#if the age is 1 or more, and the age_end is not an integer, then do the next highest integer (ie 5.6 turns into 6)
#   pops_sub <- pop_dt[location_id == row[, location_id] & as.integer(year_id) == row[, as.integer(midyear)] &
#                        age_group_years_start >= row[, age_start]][age_group_years_end <= row[, age_end] | age_group_years_end==min(age_group_years_end), ]
#               #Extract and create the following columns: location_id, year_id, age_group_years_start, age_group_years_end
#   agg <- pops_sub[, .(pop_sum = sum(population, na.rm = T)), by = c("sex")] #Find the sum of the population, by sex using the data from the above line
#   row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
#               both_N = agg[sex == "Both", pop_sum])] #Now fill the row data table with the pop sums for male, female, and both
#   return(row) #only return the row datatable
# }
#This will return a table that includes: location_id, year_id, age_group_years_start, age_group_years_end, male_n, female_n, both_n

#From here and until the end, this is all general code that is used for sex-splitting. This code was most likely done by another person and then copy and pasted into here
#from slack. There is no formal documentation for this.
# 
# ############# Split data function #################################################
# split_data <- function(input_dt, input_draws) {
# 
#   print("initializing...")
#   dt <- copy(input_dt) #Make a copy of the input data table
#   draws <- copy(input_draws) #Make a copy of the input_draws
# 
#   # nosplit_dt <- dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
#   # tosplit_dt <- dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
#   nosplit_dt <- dt[sex %in% c("Male", "Female")] #Make a table that is only M and F
#   tosplit_dt <- dt[sex == "Both"] #Make a table that is only both sexes (indicated by "both")
#   tosplit_dt[, midyear := floor((year_start + year_end)/2)] #make a column in the both table, called midyear and fill it in
#   tosplit_dt[is.na(sample_size), sample_size := (mean*(1-mean))/(standard_error^2)] #In the Both table, if the sample_size column is NA, then fill it in. We can estimate
#                                                                                     #the sample size by the mean and std error
# 
#   draw.cols <- paste0("draw_", 0:999) #Make a variable that will contain the draw number. It will display draw_#
#   draws[, (draw.cols) := lapply(.SD, exp), .SDcols = draw.cols] #Taking exp of the log
#   ratio_mean <- round(draws[, rowMeans(.SD), .SDcols = draw.cols], 2) #.SD is like using . in tidyverse
#   ratio_se <- round(draws[, apply(.SD, 1, sd), .SDcols = draw.cols], 2)
# 
#   print("getting pops...")
#   # pops <- get_population(location_id = tosplit_dt[, unique(location_id)],  year_id = tosplit_dt[, unique(midyear)],  sex_id = 1:3,
#   #                        gbd_round_id = 7, decomp_step = "iterative", age_group_id = 50:147, single_year_age = T) #pulling in pop for year age groups (>1 yrs old)
#   pops <- get_population(location_id = tosplit_dt[, unique(location_id)],  year_id = tosplit_dt[, unique(midyear)],  sex_id = 1:3,
#                          release_id = release, age_group_id = 50:147, single_year_age = T)
#   # pops <- rbind(pops, get_population(location_id = tosplit_dt[, unique(location_id)],  year_id = tosplit_dt[, unique(midyear)], sex_id = 1:3,
#   #                                    gbd_round_id = 7, decomp_step = "iterative", age_group_id = c(2,3,388,389,238))) #Pulling in pop for kid <0 yrs old
#   pops <- rbind(pops, get_population(location_id = tosplit_dt[, unique(location_id)],  year_id = tosplit_dt[, unique(midyear)], sex_id = 1:3,
#                                      release_id = release, age_group_id = c(2,3,388,389,238))) #Pulling in pop for kid <0 yrs old
# 
#   dMeta <- merge(data.frame(age_group_id = c(2,3,388,389,238,50:147), #use share_functions for this instead
#                             age_group_years_start = c(0, 0.01917808, 0.07671233, 0.5, 1:99),
#                             age_group_years_end = c(0.01917808, 0.07671233, 0.5, 1:100)),
#                  data.frame(sex_id = 1:3, sex = c("Male", "Female", "Both"), stringsAsFactors = F),
#                  all = T)
# 
#   pops <- merge(pops, dMeta, by = c("age_group_id", "sex_id"), all.x=T)
#   setDT(pops) #adding metadata
# 
#   print("splitting...")
#   tosplit_dt <- rbindlist(lapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops)))
#   tosplit_dt[, merge := 1]
#   draws[, merge := 1]
#   split_dt <- merge(tosplit_dt, draws, by = "merge", allow.cartesian = T)
#   split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
#   split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
#   split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
#   split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
#   split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
#   split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
#   split_dt[, c(draw.cols, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
# 
#   z <- qnorm(0.975)
# 
#   print("cleaning up male_dt...")
#   male_dt <- copy(split_dt)
#   male_dt[mean == 0, sample_size := sample_size * male_N/both_N]
#   male_dt[mean == 0 & measure == "prevalence", male_standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
#   male_dt[mean == 0 & measure == "incidence", male_standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
#   male_dt[is.na(male_standard_error), male_standard_error := sqrt((standard_error^2)*both_N/male_N)]
# 
#   male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA,
#                   sample_size = NA, uncertainty_type_value = NA, sex = "Male", crosswalk_parent_seq = seq,
#                   note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", ratio_se, ")"))]
#   male_dt[, variance := standard_error^2]
#   # male_dt <- dplyr::select(male_dt, c(names(dt), "crosswalk_parent_seq"))
# 
#   print("cleaning up female_dt...")
#   female_dt <- copy(split_dt)
#   female_dt[mean == 0, sample_size := sample_size * female_N/both_N]
#   female_dt[mean == 0 & measure == "prevalence", female_standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
#   female_dt[mean == 0 & measure == "incidence", female_standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
#   female_dt[is.na(female_standard_error), female_standard_error := sqrt((standard_error^2)*both_N/female_N)]
# 
#   female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA,
#                     sample_size = NA, uncertainty_type_value = NA, sex = "Female", crosswalk_parent_seq = seq,
#                     note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", ratio_se, ")"))]
#   female_dt[, variance := standard_error^2]
#   # female_dt <- dplyr::select(female_dt, c(names(dt), "crosswalk_parent_seq"))
# 
#   total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt), fill = T)
# 
#   print("done")
#   return(total_dt)
# }
# 
# #################### Run the sex split ######################################################
# predict_sex <- split_data(data, ss_draws)
# 
# #################### Clean up ######################################
# predict_sex[, `:=` (age_range = age_end - age_start, year_mid = (year_start + year_end)/2)]
# predict_sex[, gbd_year := round(year_mid/5, digits = 0) * 5][year_mid>=2016.5 & year_mid<2018, gbd_year := 2017][year_mid>=2018, gbd_year := 2019]
# predict_sex[sex == "Male", sex_id := 1][sex == "Female", sex_id := 2]
# 
# #################### Save ####################################################
# write.csv(predict_sex, "/mnt/share/erf/GBD2021/lead/exp/lead_sex_split.csv", row.names = FALSE)

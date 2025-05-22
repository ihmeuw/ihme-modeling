################################################################################
## This code takes the final ST-GPR results for dengue, runs the age-splitting
## function and age-splits the final results. This needs to be run for both sets
## of final ST-GPR results

## Originally was step_2_stgpr_allage_output.do but has been converted to R
################################################################################
rm(list = ls())

user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH")
#install.packages("BayesianTools", lib=path)
#library(BayesianTools, lib.loc=path)
library(dplyr)

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

code_root <- paste0(FILEPATH)

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/save_results_epi.R")

source(paste0(code_root, 'FILEPATH'))
library(stringr)
library(haven)
library(data.table)
library(dplyr)

################################################################################
# set paths and vars
date <- Sys.Date()
release_id <- ADDRESS
stgpr_mv <- ADDRESS
run_date <- "ADDRESS"
model_years <- c(ADDRESS)
data_root <- paste0("FILEPATH")
params_path <- paste0(data_root, "FILEPATH")
input_path <- paste0(data_root, "FILEPATH")
output_path <- paste0(data_root, "FILEPATH")
dir.create(output_path, recursive = T, showWarnings = FALSE)

################################################################################
# read in draws
files <- list.files(path = input_path, pattern = ".csv", full.names = TRUE)
temp <- lapply(files, fread, sep=",")
df <- rbindlist(temp)
rm(temp)

# read in population
all_age_pop <- get_population(release_id = release_id, age_group_id = 22, sex_id = c(1,2), 
                              year_id = model_years, location_id = unique(df$location_id))
all_age_pop$run_id <- NULL
all_age_pop$age_group_id <- NULL
df$age_group_id <- NULL

# merge on population and convert to counts
df <- left_join(df, all_age_pop, by = c("sex_id", "location_id", "year_id"))

vars <- paste0("draw_", 0:999)
cols <- colnames(df)
cols <- cols[!(cols %in% c(vars, "population"))]
df_count <- df[, lapply(.SD, function(x) x*population), .SDcols=vars, by=cols]

# expand out to create age groups
age_dis <- read_dta(paste0(params_path, "ageDistribution20.dta"))
age_dis <- as.data.table(age_dis)

# create template to merge on to df and create rows for new age groups
template <- df_count[,.(location_id, year_id, sex_id)]
template <- unique(template)

template2 <- age_dis[,.(age_group_id, sex_id)]

template <- left_join(template, template2, by = "sex_id")

df_count_age <- left_join(template, df_count, by = c("location_id", "year_id", "sex_id"))

# now merge on age distribution estimates
df_count_age <- left_join(df_count_age, age_dis, by = c("age_group_id", "sex_id"))

# get age spec pop
age_pop <- get_population(release_id = release_id, age_group_id = unique(age_dis$age_group_id), 
                          sex_id = c(1,2), year_id = model_years, location_id = unique(df$location_id))
age_pop$run_id <- NULL

# merge to and calculate counts for age_dis
df_count_age <- left_join(df_count_age, age_pop, by = c("location_id", "age_group_id", "sex_id", "year_id"))

# make sure that age_group_id 2 is 0
df_count_age$incCurve <- ifelse(df_count_age$age_group_id == 2,0, df_count_age$incCurve)

df_count_age$casesCurve <- df_count_age$incCurve * df_count_age$population
df_count_age$population <- NULL

# now sum to get total casescurve
sub <- df_count_age[,.(age_group_id, year_id, location_id, sex_id, casesCurve)]
sub$age_group_id <- NULL

cols <- colnames(sub)
cols <- cols[!(cols == "casesCurve")]
sub <- sub[, lapply(.SD, function(x) sum(x)), .SDcols="casesCurve", by=cols]
setnames(sub, "casesCurve", "totalCasesCurve")
sub <- unique(sub)

# join back on
df_count_age <- left_join(df_count_age, sub, by = c("location_id", "sex_id", "year_id"))

################################################################################
# now generate updated draws
vars <- paste0("draw_", 0:999)
cols <- colnames(df_count_age)
cols <- cols[!(cols %in% c(vars, "casesCurve", "totalCasesCurve"))]
df_count_age <- df_count_age[, lapply(.SD, function(x) ((casesCurve * x)/ totalCasesCurve)), .SDcols=vars, by=cols]

# now merge on pop again and convert back to inc rate
df_count_final <- left_join(df_count_age, age_pop, by = c("location_id", "age_group_id", "sex_id", "year_id"))

df_count_final$casesCurve <- NULL
df_count_final$totalCasesCurve <- NULL

vars <- paste0("draw_", 0:999)
cols <- colnames(df_count_final)
cols <- cols[!(cols %in% c(vars,  "population"))]
df_count_final <- df_count_final[, lapply(.SD, function(x) x / population), .SDcols=vars, by=cols]

df_count_final$population <- NULL
df_count_final$incCurve <- NULL

# now do some final edits
df_count_final$modelable_entity_id <- ADDRESS
df_count_final$measure_id <- ADDRESS

################################################################################
## and finally save out by location
x <- 1
for(i in unique(df_count_final$location_id)){
  message(paste0("Location_id: ", i, "; number: ", x))
  # subset to single location and spit out
  sub  <- df_count_final[df_count_final$location_id == i,]
  
  write.csv(sub,(paste0("FILEPATH")), row.names = FALSE) 
  x <- x+1
}
message("Script finished")

################################################################################
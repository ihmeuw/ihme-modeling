############################################################################
### Project: Breastfeeding
### Purpose: Make custom covariates for breastfeeding first stage estimates
############################################################################

###################
### Setting up ####
###################
rm(list=ls())
pacman::p_load(data.table, dplyr, ggplot2, stats, boot, MASS)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH/", user)
}

# Source functions
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_age_weights.R")
source("FILEPATH/get_population.R")

########################################
### High-BMI Women Reproductive Age ####
########################################

# Pull covariate estimates and process
df <- get_covariate_estimates(2156,decomp_step="iterative")
df <- df[sex_id==2] #subset to females
df <- df[age_group_id %in% c(7:15)] #subset to women of reproductive age (10-54 years)

# Pull age weights and merge
weights <- get_age_weights()
df <- merge(df,weights,by="age_group_id",all.x=TRUE)

# Calculate weighted mean
df[, mean_value := weighted.mean(mean_value, w=age_group_weight_value), by=c("location_id","year_id")]

# Reformat and write out
df[, `:=`(model_version_id=NULL, covariate_id=NULL, age_group_id=NULL, age_group_name=NULL, age_group_weight_value=NULL, lower_value=NULL, upper_value=NULL)]
df <- unique(df)
df[, age_group_id:=22]
df[, covariate_name_short:="high_bmi_repro"]
df[, sex_id:=3]
write.csv(df,"FILEPATH/high_bmi_repro.csv", row.names=F)

#############################################
### HIV Mortality Women Reproductive Age ####
#############################################

# Pull covariate estimates and process
df <- get_covariate_estimates(986,decomp_step="iterative")
df <- df[sex_id==2] #subset to females

# Reformat and write out
df[, `:=`(model_version_id=NULL, covariate_id=NULL, age_group_id=NULL, age_group_name=NULL)]
df <- unique(df)
df[, age_group_id:=22]
df[, covariate_name_short:="hiv_mort_repro"]
df[, sex_id:=3]
write.csv(df,"FILEPATH/hiv_mort_repro.csv", row.names=F)

###########################################
### Underweight Women Reproductive Age ####
###########################################

# Pull covariate estimates and process
df <- get_covariate_estimates(1252,decomp_step="iterative")
df <- df[sex_id==2] #subset to females

# Reformat and write out
df[, `:=`(model_version_id=NULL, covariate_id=NULL, age_group_id=NULL, age_group_name=NULL)]
df <- unique(df)
df[, age_group_id:=22]
df[, covariate_name_short:="underweight_repro"]
df[, sex_id:=3]
write.csv(df,"FILEPATH/underweight_repro.csv", row.names=F)


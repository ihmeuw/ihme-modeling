################################################################################
## Purpose:  Computes Live Births by Sex for All Locations
## Steps: (1) ASFR and Pop Import, (2) Merge on 15-49 females, (3) Compute Total Births for All Years, (4) Sex split births,
##(5) Clean and Save
## Inputs: Most Recent ASFR Ouptut, Most recent pops (shared func), Most recent sex ratios
## Outputs: Births by sex for all years from 1950
## Run instructions: For now, run on cluster to have access to shared functions
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr, haven, parallel) # load packages and install if not installed

root <- "FILEPATH"
username <- "FILEPATH"

################################################################################
### Arguments 
################################################################################

mod_id <- commandArgs()[3]
locsetid <- 21
loc_id <- commandArgs()[4]
draws <- T
collapsed <- T

################################################################################
### Functions 
################################################################################

source("FILEPATH") # load get_locations
setwd("FILEPATH")
source("FILEPATH")
source("FILEPATH")


################################################################################ 
### Paths for input and output 
################################################################################
out_dir <- "FILEPATH"
in_dir <- "FILEPATH"


################################################################################
### Data 
################################################################################

setwd(in_dir)
asfr_final <- list.files(getwd(), pattern = paste0("^",loc_id,"_raked.csv$")) %>% lapply(., fread) %>% rbindlist

##current GBD Births to take the sex ratios
births <- read_dta("FILEPATH") %>% as.data.table

pops <- get_population(age_group_id = c(8:14), location_id = loc_id, year_id = -1, sex_id = 2, location_set_id = locsetid)[, .(location_id, year_id, age_group_id, population)]

locs <- get_location_metadata(location_set_id = locsetid)

################################################################################
### Code 
################################################################################

#######################
## Step 1: Merge on Pops, Melt Long
#######################
combined <- merge(asfr_final, pops, by = c("location_id", "year_id", "age_group_id"))

combined <- melt.data.table(combined, id.vars = grep("[[:digit:]]", names(combined), invert = T, value = T), variable.name = "sim", value.name = "asfr")

#######################
## Step 2: Compute total births for all years
#######################
combined <- combined[, .(births = asfr*population), by = .(location_id, year_id, age_group_id, sim)]
total <- combined[, .(total_births = sum(births)), by = .(location_id, year_id, sim)]

#######################
## Step 3: Compute Proportion of Births by Mothers 35+, 40+ for Appropriate Covariates
#######################
combined <- merge(combined, total, by = c("location_id", "year_id", "sim"))

thirtyfive_plus <- combined[age_group_id >= 12, .(prop_births = sum(births/total_births)), by = .(location_id, year_id, sim)]

forty_plus <- combined[age_group_id >= 13, .(prop_births = sum(births/total_births)), by = .(location_id, year_id, sim)]

## Compute time lagged proportions
thirtyfive_plus_lag <- copy(thirtyfive_plus)
thirtyfive_plus_lag[, ':=' (lag_5 = shift(prop_births,5), lag_10 = shift(prop_births, 10), lag_15 = shift(prop_births, 15)), by = .(location_id, sim)]

forty_plus_lag <- copy(forty_plus)
forty_plus_lag[, ':=' (lag_5 = shift(prop_births,5), lag_10 = shift(prop_births, 10), lag_15 = shift(prop_births, 15)), by = .(location_id, sim)]


#######################
## Step 3: Compute sex ratios and sex split births
#######################

births <- dcast.data.table(births, location_id + year ~ sex , value.var = "births")
births[, ':=' (prop_male = male/both, prop_fem = female/both)]
births[, c("male", "female", "both") := NULL]
setnames(births, "year", "year_id")

total <- merge(total, births, by = c("location_id", "year_id"))
sex_split <- total[, .(both_births = total_births, male_births = total_births * prop_male, female_births = total_births * prop_fem), by = .(location_id, year_id, sim)]

#######################
## Step 4: Melt and Clean
#######################
sex_split <- melt.data.table(sex_split, id.vars = c("location_id", "year_id", "sim"), variable.name = "sex", value.name = "births")

levels(sex_split$sex) <- gsub("_births", "", levels(sex_split$sex))
sex_split[sex == "both", sex_id := 3]
sex_split[sex == "male", sex_id := 1]
sex_split[sex == "female", sex_id := 2] 
sex_split <- merge(sex_split, locs[, .(location_id, ihme_loc_id)], by = "location_id")

thirtyfive_plus_lag[, prop_births := NULL]
forty_plus_lag[, prop_births := NULL]


#######################
## Step 5: Save Draws
#######################

if (draws) {
    
    write_dta(sex_split, paste0(out_dir, "/by_sex/", loc_id, ".dta"), version = 13)

    
}

######################
## Step 6: Save collapsed births with uncertainty
######################
if (collapsed) {
    
    ## Live births by sex
    sex_split[, sex := NULL]
    sex_split[, ihme_loc_id := NULL]
    sex_split <- sex_split[, .(age_group_id = 22, mean_value = mean(births), lower_value = quantile(births, probs = .025), 
                               upper_value = quantile(births, probs = .975), covariate_id = 1106, covariate_name_short = "live_births_by_sex")
                           , by = .(location_id, year_id, sex_id)]
    
    write.csv("FILEPATH")
}


################################################################################ 
### End
################################################################################

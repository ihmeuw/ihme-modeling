################################################################################
## Purpose: Fits model using HFD data to predict 10-14 and 50-54 ASFR from adjacent standard age groups
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr) # load packages and install if not installed
 

username <- ifelse(Sys.info()[1]=="Windows","[username]",Sys.getenv("USER"))
j <- "FILEPATH"
h <- "FILEPATH"

################################################################################
### Arguments 
################################################################################
locsetid <- 21
  
################################################################################
### Functions 
################################################################################
setwd("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

clean_hf_codes <- function(df, ID) {
    
    ifelse(ID == "hfd", setnames(df, old = c("Code","Year"), new = c("ihme_loc_id","year")), setnames(df, old = "Country", "ihme_loc_id"))
    ifelse(ID != "hfd",  df[, year:= (Year1 + Year2)/2], NA)
    
    df[ihme_loc_id=="DEUTNP", ihme_loc_id := "DEU"]
    df[ihme_loc_id=="GBR_NP",ihme_loc_id := "GBR"]
    df[ihme_loc_id=="GBR_NIR",ihme_loc_id := "GBR_433"]
    df[ihme_loc_id=="GBR_SCO",ihme_loc_id := "GBR_434"] 
    df[ihme_loc_id=="FRATNP",ihme_loc_id := "FRA"]
    df[ihme_loc_id=="HKG", ihme_loc_id := "CHN_354"]
    
    
    df[ihme_loc_id== "GBRTENW",ihme_loc_id := "GBR_4749"]
    
    wales <- copy(df[ihme_loc_id == "GBR_4749"])
    wales[,ihme_loc_id := "GBR_4636"]
    
    df <- rbindlist(list(df,wales)) 
    
    df <- df[!(ihme_loc_id %in% c("DEUTE", "DEUTW"))]
    
    return(df)
}



################################################################################
### Data 
################################################################################

codes <- get_location_metadata(location_set_id = locsetid)[, .(ihme_loc_id, location_id)]

hfd <- fread("FILEPATH")
sing_yr_pops <- get_population(age_group_id = c(58:102), 
                               single_year_age = 1, 
                               location_set_id = 21, 
                               location_id = -1, 
                               year_id = -1, 
                               sex_id = 2, 
                               gbd_round_id = 4)[, .(age_group_id, location_id, year = year_id, population)]


################################################################################
### Code 
################################################################################

#######################
## Step 1: Clean HFD Codes and Condense to Non-standard five year age groups
#######################

hfd <- clean_hf_codes(hfd, "hfd")

hfd[, Age:= as.numeric(gsub("[[:punct:]]", "", Age))]
hfd <- hfd[!(Age %between% c(20,44))]
hfd <- hfd[Age < 55]
hfd <- hfd[year >= 1950]

missing_ages <- unique(hfd[, .(ihme_loc_id, year)])
missing_ages[, c("10", "11") := NA]
missing_ages <- melt.data.table(missing_ages, id.vars = c('ihme_loc_id', 'year'), variable.name = "Age", value.name = "ASFR", variable.factor = F)
missing_ages[, Age := as.numeric(Age)]
missing_ages[, ASFR:= as.numeric(ASFR)]

non_standards <- merge(hfd, missing_ages, by = c("ihme_loc_id", "year", "Age", "ASFR"), all = T)
non_standards[Age <= 12, ASFR:= mean(ASFR, na.rm = T), by = .(ihme_loc_id, year)]

setnames(non_standards, "Age", "age_group_id")
non_standards[, age_group_id := age_group_id + 48]
sing_yr_pops <- merge(sing_yr_pops, codes, by = "location_id")

non_standards <- merge(non_standards, sing_yr_pops, by = c("ihme_loc_id", "year", "age_group_id"), all.X = T)
non_standards[, condense_to := (age_group_id - 48) %/% 5]

non_standards <- non_standards[, births:= population*ASFR, by = .(location_id, year, condense_to)]
non_standards <- non_standards[, lapply(.SD, sum), by = .(location_id, year, condense_to), .SDcols = c("births", "population")]

non_standards <- non_standards[, .(asfr = births/population, age = condense_to * 5), by = .(location_id, year)]

non_standards <- dcast.data.table(non_standards, location_id + year ~ age, value.var = "asfr")
setnames(non_standards, grep("[[:digit:]]", names(non_standards), value = T), paste("asfr", grep("[[:digit:]]", names(non_standards), value = T), sep = "_"))


young_ns <- non_standards[, .(asfr_10, asfr_15)]
old_ns <- non_standards[, .(asfr_45, asfr_50)] #Dropping due to data sparsity for higher 45-49 ASFR, leading to less believable prediction for those high values

#######################
## Step 2: Fit Crosswalk Models
#######################

young.loess <- loess(asfr_10~asfr_15, data = young_ns, degree = 1, surface = "direct")
old.loess <- loess(asfr_50~asfr_45, data = old_ns, degree = 1, surface = "direct")

young_ns[, pred := predict(young.loess, newdata = young_ns)]
old_ns[, pred := predict(old.loess, newdata = old_ns)]

yempiricalmax <- young_ns[, max(pred)]
oempiricalmax <- old_ns[, max(pred)]

yempiricalmin <- young_ns[pred > 0, min(pred)]
oempiricalmin <- old_ns[pred > 0, min(pred)]

yhorizmax <- young_ns[, max(asfr_15)]
ohorizmax <- old_ns[, max(asfr_45)]


#######################
## Step 4: Save Crosswalk Models
#######################

save(young.loess, yempiricalmin, yempiricalmax, old.loess, oempiricalmin, oempiricalmax, file = "FILEPATH")

#######################
##
#######################
################################################################################ 
### End
################################################################################
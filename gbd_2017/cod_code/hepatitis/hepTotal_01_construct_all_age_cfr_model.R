
## ******************************************************************************
##
## Purpose: Model CFR as a function of HAQI
##            Steps:  1. Generate CFR as hospital deaths / incid from DisMod
##                    2. Add HAQI covariate
##                    3. Run regression (use num of admissions as sample size)
## Input:   - GBD 2017 hospital data
##          - GBD 2017 DisMod incidence
##          - GBD 2017 HAQI covariate values
## Output:  Model coefficients
##
## ******************************************************************************

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "/FILEPATH/"
  h <- "/FILEPATH/"
} else {
  j <- "J:"
  h <- "H:"
}

library(data.table)
library(plyr)
library(ggplot2)

source(paste0(j, "FILEPATH/interpolate.R"))
source(paste0(j, "FILEPATH/get_ids.R"))
source(paste0(j, "FILEPATH/get_population.R"))
source(paste0(j, "FILEPATH/get_model_results.R"))
source(paste0(j, "FILEPATH/get_covariate_estimates.R"))

#------------------------------------------------------------------------------------
# 1. Generate CFR as hospital deaths / incid from DisMod
#------------------------------------------------------------------------------------

#load hospital data
dt_total <- fread(paste0(j,"FILEPATH/hep_cfr_hospital_data.csv"))

#outlier certain hospital sources
dt_total[, outlier := 0]
dt_total[source %in% c("IND_SNH", "PHL_HICC", "NPL_HID", "IND_SNH"), outlier := 1]
dt_total <- dt_total[outlier == 0, ]

setnames(dt_total, 'year_start', 'year_id')

dt_total <- dt_total[sex_id != 3,]
dt_total <- dt_total[year_id > 1979,]

#add across age groups
dt_total[, `:=`(num_deaths_allheptotal = sum(.SD$num_deaths_allheptotal),
                num_cases_allheptotal = sum(.SD$num_cases_allheptotal)),
         by=c('location_id', 'year_id', 'sex_id')]
dt_total <- unique(dt_total[, .(location_id, year_id, sex_id, num_deaths_allheptotal, num_cases_allheptotal)])


#load incid for all ages, for every unique combo of loc/sex/year in the hospital data
inc <- data.table(location_id = NA, year_id = NA, sex_id = NA, age_group_id = NA, total_incid = NA)

#read in the dismod pulls and rbind all together
dir <- paste0(j, "FILEPATH/dismod_hep_total_pulls/")
file.list.all <- list.files(dir, full.names = F)

for (file_i in 1:length(file.list.all)) {
  temp <- fread(paste0(dir, file.list.all[file_i]))
  inc <- rbind(inc, temp)
}
inc <- inc[2:nrow(inc),]

#convert incid rate to incid number using population
pop <- get_population(age_group_id = unique(inc$age_group_id), sex_id=c(1,2), location_id=unique(inc$location_id), year_id=unique(inc$year_id))
dt_inc <- merge(inc, pop, by = c("age_group_id", "year_id", "sex_id", "location_id"), all.x = TRUE)
dt_inc[, incid_num := total_incid * population]

#add incid counts across age rows
dt_inc[, `:=`(incid_num = sum(.SD$incid_num)),
              by=c('location_id', 'year_id', 'sex_id')]
dt_inc <- unique(dt_inc[, .(location_id, year_id, sex_id, incid_num)])

#merge incid onto the hospital data
dt_total <- merge(dt_total, dt_inc, by = c('location_id', 'year_id', 'sex_id'), all.x = TRUE)

#divide deaths by number of incid case to get CFR
dt_total[, cfr := num_deaths_allheptotal / incid_num]

#------------------------------------------------------------------------------------
# 2. Add HAQI covariate
#------------------------------------------------------------------------------------

#load and merge HAQI covariate onto every row of hospital data
haqi <- get_covariate_estimates(covariate_id=1099)
haqi$sex_id <- NULL
haqi$age_group_id <- NULL
haqi$age_group_name <- NULL
setnames(haqi, 'mean_value', 'haqi')

dt_comb <- merge(dt_total, haqi, by = c('location_id', 'year_id'), all.x = TRUE)

#------------------------------------------------------------------------------------
# 3. Run regression and plot results
#------------------------------------------------------------------------------------

model <- glm(formula = cfr ~ haqi, family = binomial(link = "logit"), data = dt_comb,
             weight=dt_comb$num_cases_allheptotal)

model$df.residual <- with(model, sum(weights) - length(coefficients))

model_store <- data.table(names(coef(model)), coef(model), coef(summary(model))[, "Std. Error"])
setnames(model_store, c("V1", "V2", "V3"), c("variables", "logodds", "std_error"))
model_store[, odds := exp(logodds)]
model_store[, probability := odds / (odds + 1)]

save(model, file = paste0(j, '/FILEPATH/model.rds'))

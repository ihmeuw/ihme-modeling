
## ******************************************************************************
##
## Purpose: - Generate age pattern of total hepatitis CFR and each virus-specific
##            hepatitis CFR
##            Steps:  1. Add hospital deaths across locations/sexes/years
##                    2. Pull incidence rates and convert to counts, then add
##                       incidence counts across locations/sexes/years
##                    3. Calculate CFR per age
## Input:   - GBD 2017 hospital data
##          - GBD 2017 DisMod incidence
##          - File converting age group IDs to median age years
## Output:  - Age pattern of total hepatitis
##          - Age pattern of each hepatitis virus
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

#for approxExtrap function
install.packages("Hmisc", lib="/FILEPATH/R_packages/")
library('Hmisc', lib.loc="/FILEPATH/R_packages/")

source(paste0(j, "FILEPATH/interpolate.R"))
source(paste0(j, "FILEPATH/get_ids.R"))
source(paste0(j, "FILEPATH/get_population.R"))
source(paste0(j, "FILEPATH/get_model_results.R"))
source(paste0(j, "FILEPATH/get_covariate_estimates.R"))

#------------------------------------------------------------------------------------
# 1. Load hospital data
#------------------------------------------------------------------------------------

dt_total <- fread(paste0(j,"FILEPATH/hep_cfr_hospital_data.csv"))

setnames(dt_total, 'year_start', 'year_id')
dt_total$year_end <- NULL
dt_total$nid <- NULL
dt_total$V1 <- NULL

#outlier certain hospital sources
dt_total[, outlier := 0]
dt_total[source %in% c("IND_SNH", "PHL_HICC", "NPL_HID", "IND_SNH"), outlier := 1]
dt_total <- dt_total[outlier == 0, ]

#outlier any virus-specific zero values if the total value for that row is greater than zero
#assumption: those virus-specific zeros aren't accurate - b/c the values listed under total biologically have to belong to one of the viruses
dt_total[num_deaths_acuteheptotal > 0 & num_deaths_acute_hepa == 0 & num_deaths_acute_hepb == 0 & num_deaths_acute_hepc==0 & num_deaths_acute_hepe==0, outlier := 1]

#also outlier any rows where the total across codes doesn't actually equal the sum across the codes
dt_total[num_deaths_allheptotal > (num_deaths_acute_hepa + num_deaths_acute_hepb + num_deaths_acute_hepc + num_deaths_acute_hepe + num_deaths_unspecheptotal), outlier := 1]

dt_total[outlier == 1, num_deaths_acute_hepa := NA]
dt_total[outlier == 1, num_deaths_acute_hepb := NA]
dt_total[outlier == 1, num_deaths_acute_hepc := NA]
dt_total[outlier == 1, num_deaths_acute_hepe := NA]

dt_total$outlier <- NULL
dt_total$num_cases_unspecheptotal <- NULL
dt_total$num_deaths_unspecheptotal <- NULL
dt_total$num_cases_acuteheptotal <- NULL
dt_total$num_deaths_acuteheptotal <- NULL

#reshape data to be long instead of wide by virus
dt_total <- melt(dt_total, id.vars = c('source', 'location_id', 'age_group_id', 'year_id', 'sex_id'))
dt_total <- dt_total[!is.na(value),]

dt_total[grep('case', dt_total$variable), temp_variable := 'cases']
dt_total[grep('death', dt_total$variable), temp_variable := 'deaths']
dt_total[grep('rate', dt_total$variable), temp_variable := 'rates']
dt_total <- dt_total[temp_variable == 'cases' | temp_variable == 'deaths',]

setnames(dt_total, 'variable', 'type')
dt_total[grep('hepa',dt_total$type), type := 'a']
dt_total[grep('hepb',dt_total$type), type := 'b']
dt_total[grep('hepc',dt_total$type), type := 'c']
dt_total[grep('hepe',dt_total$type), type := 'e']
dt_total[grep('total',dt_total$type), type := 'total']

#extract cases and deaths from the type column, so that there's now a cases column, a deaths column, and a type(virus) column
dt_total <- dcast(dt_total, source + location_id + age_group_id + year_id + sex_id + type ~ temp_variable, value.var = 'value')
dt_total <- as.data.table(dt_total)
dt_total <- dt_total[!is.na(deaths),]

#drop both sex rows, any abnormal age groups, and data from before 1980
dt_total <- dt_total[sex_id != 3,]
dt_total[age_group_id == 1 | age_group_id == 28, age_group_id := 5]
age_list <- c(5:20,30,31,32,235)
dt_total <- dt_total[age_group_id %in% age_list,]
dt_total <- dt_total[year_id > 1979,]

#------------------------------------------------------------------------------------
# 2. Add incident cases across locations/years/sexes
#------------------------------------------------------------------------------------

#pull incid for all ages, for every unique combo of loc/sex/year in the hospital data
inc <- data.table(location_id = NA, year_id = NA, sex_id = NA, age_group_id = NA, total_incid = NA)
dir <- paste0(j, "FILEPATH/dismod_hep_total_pulls/")

file.list.all <- list.files(dir, full.names = F)

for (file_i in 1:length(file.list.all)) {
  temp <- fread(paste0(dir, file.list.all[file_i]))
  inc <- rbind(inc, temp)
}

inc <- inc[2:nrow(inc),]

inc$type <- 'total'
setnames(inc, 'total_incid', 'mean')

#pull incid for all ages, for every unique combo of loc/sex/year in the hospital data
inc_virus <- data.table(location_id = NA, year_id = NA, sex_id = NA, age_group_id = NA, modelable_entity_id = NA, mean = NA)
dir <- paste0(j, "FILEPATH/dismod_subtype_pulls/")

file.list.all <- list.files(dir, full.names = F)

for (file_i in 1:length(file.list.all)) {
  temp <- fread(paste0(dir, file.list.all[file_i]))
  inc_virus <- rbind(inc_virus, temp)
}

inc_virus <- inc_virus[2:nrow(inc_virus),]

setnames(inc_virus, 'modelable_entity_id', 'type')
inc_virus$type <- as.character(inc_virus$type)
inc_virus[type == 18834, type := 'a']
inc_virus[type == 18835, type := 'b']
inc_virus[type == 18836, type := 'c']
inc_virus[type == 18837, type := 'e']

inc <- rbind(inc, inc_virus)

inc <- inc[sex_id %in% c(1,2),]

#convert incid rate to incid number using population
pop <- get_population(age_group_id = unique(inc$age_group_id), sex_id=c(1,2), location_id=unique(inc$location_id), year_id=unique(inc$year_id))
dt_inc <- merge(inc, pop, by = c("age_group_id", "year_id", "sex_id", "location_id"), all.x = TRUE)
dt_inc[, incid_num := mean * population]

#combine the incid data for ages 2-5 all into group 5 to match the hospital data
dt_inc[age_group_id %in% c(2,3,4), age_group_id := 5]

#add incid counts across age 5 rows
dt_inc[, `:=`(incid_num = sum(.SD$incid_num)),
       by=c('location_id', 'year_id', 'sex_id', 'age_group_id', 'type')]
dt_inc <- unique(dt_inc[, .(location_id, year_id, sex_id, age_group_id, type, incid_num)])


#merge incid onto deaths data
dt_comb <- merge(dt_total, dt_inc, by = c('age_group_id', 'year_id', 'sex_id', 'location_id', 'type'), all.x = TRUE)

#add death counts and incid case counts across locations/year/sex (deaths per age)
dt_comb[, `:=`(deaths_num = sum(.SD$deaths),
               incid_num = sum(.SD$incid_num),
               case_num = sum(.SD$cases)),
         by=c('age_group_id', 'type')]
dt_comb <- unique(dt_comb[, .(age_group_id, type, deaths_num, incid_num, case_num)])


#------------------------------------------------------------------------------------
# 3. Calculate CFR at each age
#------------------------------------------------------------------------------------

#convert age group id to mid age year
age_mid <- fread(paste0(j,"FILEPATH/age_mid.csv"))
age_mid <- age_mid[age_group_id != 164,]
dt_comb <- merge(dt_comb, age_mid, by = c('age_group_id'), all.x = FALSE)

#create larger age pools for ages above 50
dt_comb[age_group_id %in% c(15,16), age_mid := 55]
dt_comb[age_group_id %in% c(17,18), age_mid := 65]
dt_comb[age_group_id %in% c(19,20), age_mid := 75]
dt_comb[age_group_id %in% c(30,31,32,235), age_mid := 90]

#collapse sum deaths and sum cases for the pooled groups
dt_comb[, `:=`(deaths_num = sum(.SD$deaths_num),
               incid_num = sum(.SD$incid_num),
               case_num = sum(.SD$case_num)),
        by=c('age_mid', 'type')]

#divide deaths by number of incid case to get total CFR
dt_comb[type == 'total', cfr := deaths_num / incid_num]
#divide deaths by number of hospital cases to get virus-specific CFR
dt_comb[type != 'total', cfr := deaths_num / case_num]

#smooth CFR
dt_comb[, cfr_smooth := predict(loess(cfr~age_mid, data=.SD, na.action=na.exclude, span=0.8, degree=2)), by=c('type')]

#interpolate missing years created by the age pooling, and extrapolate down to the age groups 2,3,4
#set up age_mid.y to be the true age_mid
dt_comb <- merge(dt_comb, age_mid, by = c('age_group_id'), all.y = TRUE)
dt_comb[age_mid.y == 57.5, age_mid.y := 55]
dt_comb[age_mid.y == 67.5, age_mid.y := 65]
dt_comb[age_mid.y == 77.5, age_mid.y := 75]
dt_comb[age_mid.y == 97.5, age_mid.y := 90]
dt_comb[age_group_id %in% c(2,3,4), type := 'total']

#add additional rows
dt_comb <- dt_comb[, c('age_group_id', 'cfr_smooth', 'age_mid.y', 'type')]
dt_extra_rows <- data.table(age_group_id = NA, cfr_smooth = NA, age_mid.y = c(57.5, 67.5, 77.5, 97.5), type = 'total')
dt_comb <- rbind(dt_comb, dt_extra_rows)
dt_extra_rows <- data.table(age_group_id = NA, cfr_smooth = NA, age_mid.y = c(57.5, 67.5, 77.5, 97.5), type = c('a'))
dt_comb <- rbind(dt_comb, dt_extra_rows)
dt_extra_rows <- data.table(age_group_id = NA, cfr_smooth = NA, age_mid.y = c(57.5, 67.5, 77.5, 97.5), type = c('b'))
dt_comb <- rbind(dt_comb, dt_extra_rows)
dt_extra_rows <- data.table(age_group_id = NA, cfr_smooth = NA, age_mid.y = c(57.5, 67.5, 77.5, 97.5), type = c('c'))
dt_comb <- rbind(dt_comb, dt_extra_rows)
dt_extra_rows <- data.table(age_group_id = NA, cfr_smooth = NA, age_mid.y = c(57.5, 67.5, 77.5, 97.5), type = c('e'))
dt_comb <- rbind(dt_comb, dt_extra_rows)

dt_comb <- dt_comb[age_mid.y %in% c(52.5, 62.5, 72.5, 82.5, 87.5, 92.5), cfr_smooth := NA]

dt_extra_rows <- data.table(age_group_id = c(2,3,4), cfr_smooth = NA, age_mid.y = c(0.008219178,0.027397260,0.460273973),type = c('a'))
dt_comb <- rbind(dt_comb, dt_extra_rows)
dt_extra_rows <- data.table(age_group_id = c(2,3,4), cfr_smooth = NA, age_mid.y = c(0.008219178,0.027397260,0.460273973),type = c('b'))
dt_comb <- rbind(dt_comb, dt_extra_rows)
dt_extra_rows <- data.table(age_group_id = c(2,3,4), cfr_smooth = NA, age_mid.y = c(0.008219178,0.027397260,0.460273973),type = c('c'))
dt_comb <- rbind(dt_comb, dt_extra_rows)
dt_extra_rows <- data.table(age_group_id = c(2,3,4), cfr_smooth = NA, age_mid.y = c(0.008219178,0.027397260,0.460273973),type = c('e'))
dt_comb <- rbind(dt_comb, dt_extra_rows)

dt_comb <- dt_comb[order(age_mid.y)]
dt_comb[, cfr_smooth_filled := approxExtrap(.SD$age_mid.y, .SD$cfr_smooth, xout=unique(.SD$age_mid.y), method = "linear", na.rm=T)$y, by = 'type']

#save outputs
setnames(dt_comb, 'age_mid.y', 'age_mid')
dt_comb <- dt_comb[age_mid != 55,]
dt_comb <- dt_comb[age_mid != 65,]
dt_comb <- dt_comb[age_mid != 75,]
dt_comb <- dt_comb[age_mid != 90,]

dt_comb[age_mid == 57.5, age_group_id := 16]
dt_comb[age_mid == 67.5, age_group_id := 18]
dt_comb[age_mid == 77.5, age_group_id := 20]
dt_comb[age_mid == 97.5, age_group_id := 235]

write.csv(dt_comb, paste0(j, "FILEPATH/age_specific_CFR_all_viruses.csv"),
          row.names = FALSE)

write.csv(dt_comb[type == 'total', c('age_group_id', 'age_mid', 'cfr_smooth_filled')],
          paste0(j, "FILEPATH/age_specific_CFR.csv"), row.names = FALSE)

#save virus-specific CFR age patterns as individual csvs
dt_comb[type == 'a', cause_id := 401]
dt_comb[type == 'b', cause_id := 402]
dt_comb[type == 'c', cause_id := 403]
dt_comb[type == 'e', cause_id := 404]

dt_comb$type <- NULL
dt_comb$age_mid <- NULL
dt_comb$cfr_smooth <- NULL
setnames(dt_comb, 'cfr_smooth_filled', 'cfr')
dir <- 'FILEPATH/age_specific_CFR_'
write.csv(dt_comb[cause_id == 401,], paste0(j, dir, "a.csv"), row.names = FALSE)
write.csv(dt_comb[cause_id == 402,], paste0(j, dir, "b.csv"), row.names = FALSE)
write.csv(dt_comb[cause_id == 403,], paste0(j, dir, "c.csv"), row.names = FALSE)
write.csv(dt_comb[cause_id == 404,], paste0(j, dir, "e.csv"), row.names = FALSE)

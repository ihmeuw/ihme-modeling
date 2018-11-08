## ******************************************************************************
##
## Purpose: - Produce age-specific death estimates for total hepatitis, based on the
##            modelled all-age CFR estimates and the CFR age pattern
## Input:   - All-age CFR estimates
##          - CFR age pattern
##          - incidence
## Output:  Number of hepatitis deaths for a given loc/year/sex/age, saved as a csv
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
library(ggplot2)

#pull in location_id from bash command
args <- commandArgs(trailingOnly = TRUE)
loc <- args[1]

source(paste0(j, "FILEPATH/interpolate.R"))
source(paste0(j, "FILEPATH/get_ids.R"))
source(paste0(j, "FILEPATH/get_population.R"))
source(paste0(j, "FILEPATH/get_covariate_estimates.R"))

#------------------------------------------------------------------------------------
# 1. Pull incid for every age/year/sex combo
#------------------------------------------------------------------------------------
inc <- interpolate(gbd_id_type = "modelable_entity_id",
                   gbd_id = 18834,
                   source = "epi",
                   measure_id = 6,
                   location_id = loc,
                   sex_id = c(1,2),
                   gbd_round_id = 5,
                   status = "best",
                   reporting_year_start = 1980)

for (type_i in c(18835,18836,18837)) {
  model_res <- interpolate(gbd_id_type = "modelable_entity_id",
                           gbd_id = type_i,
                           source = "epi",
                           measure_id = 6,
                           location_id = loc,
                           sex_id = c(1,2),
                           gbd_round_id = 5,
                           status = "best",
                           reporting_year_start = 1980)
  inc <- rbind(inc, model_res)
}

#delete unnecessary columns
inc$measure_id <- NULL
inc$metric_id <- NULL
inc$model_version_id <- NULL

#add the four viruses together
draw_cols <- paste0("draw_", 0:999)
inc <- inc[, lapply(.SD, sum),
           by = c('location_id', 'year_id', 'sex_id', 'age_group_id'),
           .SDcols = draw_cols]

#convert incid rate to incid number using population
pop <- get_population(age_group_id = unique(inc$age_group_id), sex_id=c(1,2), location_id=unique(inc$location_id), year_id=unique(inc$year_id))
inc <- merge(inc, pop, by = c("age_group_id", "year_id", "sex_id", "location_id"), all.x = TRUE)
inc[, (draw_cols) := lapply(.SD, function(x) x * population), .SDcols = draw_cols]

#------------------------------------------------------------------------------------
# 2. Load all-age CFR for every year and sex, and load reference age-specific CFR
#------------------------------------------------------------------------------------
all_age_cfr_table <- fread(paste0(j, "FILEPATH/", loc, ".csv"))
setnames(all_age_cfr_table, 'cfr', 'all_age_cfr')

#add all-age CFR to the main data table based on sex and year
dt <- merge(inc, all_age_cfr_table, by = c('sex_id', 'year_id'))

age_specific_cfr_table <- fread(paste0(j, "FILEPATH/age_specific_CFR.csv"))
setnames(age_specific_cfr_table, 'cfr_smooth_filled', 'cfr')
dt <- merge(dt, age_specific_cfr_table, by = 'age_group_id')

#------------------------------------------------------------------------------------
# 3. Calculate age-specific unscaled deaths
#------------------------------------------------------------------------------------
#unscaled deaths = cases * age-specific CFR
unscaled_death_cols <- paste0("unsc_deaths_", 0:999)
dt[, (unscaled_death_cols) := lapply(.SD, function(x) x * cfr), .SDcols = draw_cols]


#------------------------------------------------------------------------------------
# 4. Calculate year/sex specific correction factor
#------------------------------------------------------------------------------------
#sum the age-specific deaths
all_cols <- c(draw_cols, unscaled_death_cols)
dt_sum <- dt[, lapply(.SD, sum),
            by = c('location_id', 'year_id', 'sex_id', 'all_age_cfr'),
            .SDcols = all_cols]

#calculate all-age deaths = cases across all ages * all-age CFR
all_age_deaths <- paste0('all_age_deaths_', 0:999)
dt_sum[, (all_age_deaths) := lapply(.SD, function(x) x * all_age_cfr), .SDcols = draw_cols]

#save off for consistency check
dt_check_1 <- copy(dt_sum)
dt_check_1 <- dt_check_1[, -(draw_cols), with=FALSE]
dt_check_1 <- dt_check_1[, -(unscaled_death_cols), with=FALSE]
dt_check_1$mean_all_age_deaths_from_all_age_cfr <- rowMeans(dt_check_1[,5:1004], na.rm = T)
dt_check_1 <- dt_check_1[, -(all_age_deaths), with=FALSE]

#calculate correction factor = all-age deaths / sum of age-specific deaths
dt_sum <- dt_sum[, -(draw_cols), with=FALSE]
dt_sum <- dt_sum[, -c('all_age_cfr'), with=FALSE]

#melt datasets so that every draw is a row instead of a column
dt_long <- melt(dt_sum, id.vars = c('location_id', 'year_id', 'sex_id'))
#split the string in the variable column into two columns, one with the
#type and one with the draw number
dt_long$variable <- as.character(dt_long$variable)
dt_long[grep('unsc', variable), var := substr(variable,1,11)]
dt_long[grep('unsc', variable), draw_num := substr(variable,13,nchar(variable))]

dt_long[grep('all', variable), var := substr(variable,1,14)]
dt_long[grep('all', variable), draw_num := substr(variable,16,nchar(variable))]

dt_long$variable <- NULL

#cast to make one column for all-age deaths and one column for unscaled deaths
dt_long <- dcast(dt_long, location_id + year_id + sex_id + draw_num ~ var, value.var = 'value')
dt_long <- as.data.table(dt_long)

#calculate correction factor
dt_long[, corr_factor := all_age_deaths / unsc_deaths]

#reshape dataframe so that every draw_num is a column, where the value is the corr_factor
dt_cf <- dcast(dt_long, location_id + year_id + sex_id ~ draw_num, value.var = 'corr_factor')


#------------------------------------------------------------------------------------
# 5. Calculate age-specific scaled deaths
#------------------------------------------------------------------------------------

dt <- dt[, -(draw_cols), with=FALSE]
dt <- dt[, -c('population', 'run_id', 'all_age_cfr', 'age_mid', 'cfr')]
dt <- merge(dt, dt_cf, by = c('location_id', 'year_id', 'sex_id'))

#calculate age-specific scaled deaths (call 'draw') as age-specific unscaled deaths * correction factor

#melt again so that each row is a draw number, and there's a column for unscaled deaths and a
#column for correction factor
dt_long <- melt(dt, id.vars = c('age_group_id', 'sex_id', 'location_id', 'year_id'))

#split the string in the variable column into two columns, one with the
#type and one with the draw number
dt_long$variable <- as.character(dt_long$variable)
dt_long[grep('unsc', variable), var := substr(variable,1,11)]
dt_long[grep('unsc', variable), draw_num := substr(variable,13,nchar(variable))]

dt_long[!grep('unsc', variable), var := 'cf']
dt_long[!grep('unsc', variable), draw_num := variable]

dt_long$variable <- NULL

#cast to make one column for all-age deaths and one column for unscaled deaths
dt_long <- dcast(dt_long, location_id + year_id + sex_id + age_group_id + draw_num ~ var, value.var = 'value')
dt_long <- as.data.table(dt_long)

#calculate scaled age-specific deaths
dt_long[, scaled_deaths := cf * unsc_deaths]

#cast back to format for export
#one column for every draw, where value is from scaled deaths
dt_scaled <- dcast(dt_long, location_id + year_id + sex_id + age_group_id ~ draw_num, value.var = 'scaled_deaths')
dt_scaled <- as.data.table(dt_scaled)

#consistency check
all_cols <- paste0(0:999)
dt_check_2 <- dt_scaled[, lapply(.SD, sum),
                      by = c('location_id', 'year_id', 'sex_id'),
                      .SDcols = all_cols]
dt_check_2$mean_all_age_deaths_from_age_spec_sum <- rowMeans(dt_check_2[,4:1003], na.rm = T)
dt_check_2 <- dt_check_2[, -(all_cols), with=FALSE]
dt_check <- merge(dt_check_1, dt_check_2, by = c('year_id', 'sex_id', 'location_id'))


#------------------------------------------------------------------------------------
# 5. Save deaths
#------------------------------------------------------------------------------------
setnames(dt_scaled, old=all_cols, new=draw_cols)
output_cols <- c('age_group_id', draw_cols)
output_dir <- paste0(j, "FILEPATH/total_deaths_age_specific/")

for (year_index in 1980:2017) {
  for (sex_index in 1:2) {
    write.csv(dt_scaled[year_id == year_index & sex_id == sex_index, output_cols, with=FALSE],
              paste0(output_dir, loc, "_", year_index, "_", sex_index, ".csv"),
              row.names = FALSE)
  }
}

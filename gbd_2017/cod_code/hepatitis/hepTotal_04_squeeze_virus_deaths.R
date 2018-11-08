## ******************************************************************************
##
## Purpose: - Produce virus-specific deaths by age/year/sex/loc, scaled
##            to the total hepatitis envelope
## Input:   - Total hepatitis deaths
##          - Virus-specific hepatitis deaths (unscaled)
## Output:  - Virus-specific deaths
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
library(reshape2)

#pull in location_id from bash command
args <- commandArgs(trailingOnly = TRUE)
loc <- args[1]

source(paste0(j, "FILEPATH/interpolate.R"))
source(paste0(j, "FILEPATH/get_ids.R"))
source(paste0(j, "FILEPATH/get_population.R"))
source(paste0(j, "FILEPATH/get_covariate_estimates.R"))

#------------------------------------------------------------------------------------
# 1. Load total deaths from file for every age/year/sex combo
#------------------------------------------------------------------------------------

input_dir <- paste0(j, "FILEPATH/")
file.list.all <- list.files(input_dir, full.names = F)

loc_id <- paste0('^', loc, '_')
file.list.all <- file.list.all[(grep(loc_id, file.list.all))]

for (file_i in 1:length(file.list.all)) {
  print(file_i)
  tot_deaths <- fread(paste0(input_dir, file.list.all[file_i]))
  tot_deaths$year <- substr(file.list.all[file_i],nchar(loc)+2,nchar(loc)+5)
  tot_deaths$sex  <- substr(file.list.all[file_i],nchar(loc)+7,nchar(loc)+7)
  year <- substr(file.list.all[file_i],nchar(loc)+2,nchar(loc)+5)
  sex <- substr(file.list.all[file_i],nchar(loc)+7,nchar(loc)+7)

  #------------------------------------------------------------------------------------
  # 2. Load virus-specific deaths from file for every age/year/sex combo
  #------------------------------------------------------------------------------------

  #rbind together with a new column for virus, then add viruses together
  dir <- paste0(j, "FILEPATH/")

  #read in files for hep A
  virus_dir <- paste0(dir, 'hepA/')
  virus_deaths <- fread(paste0(virus_dir, file.list.all[file_i]))
  virus_deaths$year <- substr(file.list.all[file_i],nchar(loc)+2,nchar(loc)+5)
  virus_deaths$sex  <- substr(file.list.all[file_i],nchar(loc)+7,nchar(loc)+7)
  virus_deaths$type <- 'A'

  #read in remaining three viruses
  for (virus_index in c('B', 'C', 'E')) {
    virus_dir <- paste0(dir, 'hep', virus_index, '/')
    virus_deaths_temp <- fread(paste0(virus_dir, file.list.all[file_i]))
    virus_deaths_temp$year <- year
    virus_deaths_temp$sex  <- sex

    virus_deaths_temp$type <- virus_index
    virus_deaths <- rbind(virus_deaths, virus_deaths_temp)
  }

  #add the four subtypes together
  draw_cols <- paste0("draw_", 0:999)
  virus_deaths <- virus_deaths[, lapply(.SD, sum),
                               by = c('age_group_id', 'year', 'sex'),
                               .SDcols = draw_cols]

  #convert incid rate to incid number using population
  #virus-deaths are stored in rate space
  pop <- get_population(age_group_id = unique(virus_deaths$age_group_id), sex_id=sex, location_id=loc, year_id=year)
  setnames(pop, 'year_id', 'year')
  pop$year <- as.character(pop$year)
  setnames(pop, 'sex_id', 'sex')
  pop$sex <- as.character(pop$sex)
  virus_deaths <- merge(virus_deaths, pop, by = c("age_group_id", "year", "sex"), all.x = TRUE)
  virus_deaths[, (draw_cols) := lapply(.SD, function(x) x * population), .SDcols = draw_cols]
  virus_deaths$population <- NULL
  virus_deaths$run_id <- NULL
  virus_deaths$location_id <- NULL

  #------------------------------------------------------------------------------------
  # 3. Calculate scaling factor as total deaths divided by sum of virus-specific deaths
  #------------------------------------------------------------------------------------
  #melt both datasets so that every draw is a row instead of a column
  tot_deaths <- melt(tot_deaths, id.vars = c('age_group_id', 'year', 'sex'))
  virus_deaths <- melt(virus_deaths, id.vars = c('age_group_id', 'year', 'sex'))

  setnames(tot_deaths, 'value', 'tot_deaths')
  setnames(virus_deaths, 'value', 'virus_deaths')

  deaths_combine <- merge(tot_deaths, virus_deaths, by = c('age_group_id', 'year', 'sex', 'variable'))

  deaths_combine[, scaling_factor := tot_deaths / virus_deaths]

  #------------------------------------------------------------------------------------
  # 4. Multiply number of deaths from each virus by the scaling factor
  #------------------------------------------------------------------------------------
  #read in one file for a virus, multiply the draws by the draws of the scaling factor,
  #and then save to a new output folder

  #read in files for hep A
  virus_dir <- paste0(dir, 'hepA/')
  virus_deaths <- fread(paste0(virus_dir, file.list.all[file_i]))
  virus_deaths$year <- year
  virus_deaths$sex  <- sex

  #convert incid rate to incid number using population
  virus_deaths <- merge(virus_deaths, pop, by = c("age_group_id", "year", "sex"), all.x = TRUE)
  virus_deaths[, (draw_cols) := lapply(.SD, function(x) x * population), .SDcols = draw_cols]

  virus_deaths$population <- NULL
  virus_deaths$run_id <- NULL
  virus_deaths$location_id <- NULL

  virus_deaths_long <- melt(virus_deaths, id.vars = c('age_group_id', 'year', 'sex'))

  temp_combine_long <- merge(virus_deaths_long, deaths_combine, by = c('age_group_id', 'year', 'sex', 'variable'))
  temp_combine_long[, value := value * scaling_factor]

  temp_combine_long <- temp_combine_long[, -c('tot_deaths', 'virus_deaths', 'scaling_factor')]
  virus_deaths_scaled <- dcast(temp_combine_long, age_group_id + year + sex ~ variable, value.var = 'value')
  virus_deaths_scaled <- as.data.table(virus_deaths_scaled)

  draw_cols <- append('age_group_id', draw_cols)
  output_dir <- paste0(j, "FILEPATH/hepA_postsqueeze/")

  write.csv(virus_deaths_scaled[, draw_cols, with=FALSE],
            paste0(output_dir, loc, "_", year, "_", sex, ".csv"),
            row.names = FALSE)

  #read in remaining three viruses
  for (virus_index in c('B', 'C', 'E')) {
    virus_dir <- paste0(dir, 'hep', virus_index, '/')

    virus_deaths <- fread(paste0(virus_dir, file.list.all[file_i]))
    virus_deaths$year <- substr(file.list.all[file_i],nchar(loc)+2,nchar(loc)+5)
    virus_deaths$sex  <- substr(file.list.all[file_i],nchar(loc)+7,nchar(loc)+7)

    #convert incid rate to incid number using population
    virus_deaths <- merge(virus_deaths, pop, by = c("age_group_id", "year", "sex"), all.x = TRUE)
    draw_cols <- paste0("draw_", 0:999)
    virus_deaths[, (draw_cols) := lapply(.SD, function(x) x * population), .SDcols = draw_cols]

    virus_deaths$population <- NULL
    virus_deaths$run_id <- NULL
    virus_deaths$location_id <- NULL

    virus_deaths_long <- melt(virus_deaths, id.vars = c('age_group_id', 'year', 'sex'))

    temp_combine_long <- merge(virus_deaths_long, deaths_combine, by = c('age_group_id', 'year', 'sex', 'variable'))

    temp_combine_long[, value := value * scaling_factor]

    temp_combine_long <- temp_combine_long[, -c('tot_deaths', 'virus_deaths', 'scaling_factor')]
    virus_deaths_scaled <- dcast(temp_combine_long, age_group_id + year + sex ~ variable, value.var = 'value')
    virus_deaths_scaled <- as.data.table(virus_deaths_scaled)

    draw_cols <- append('age_group_id', draw_cols)
    output_dir <- paste0(dir, 'hep', virus_index, '_postsqueeze/')

    write.csv(virus_deaths_scaled[, draw_cols, with=FALSE],
                  paste0(output_dir, loc, "_", year, "_", sex, ".csv"),
                  row.names = FALSE)
  }

}
